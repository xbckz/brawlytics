from flask import Flask, render_template, request, redirect, session, jsonify
import json
import os
from datetime import datetime
import pandas as pd
from collections import defaultdict
import socket
from urllib.parse import unquote

from PIL import Image, ImageDraw, ImageFont
from flask import send_file
import io

import brawl_match_processor

from google.cloud import storage as gcs_storage

GCS_BUCKET_NAME = 'brawlytics'
GCS_CREDENTIALS_PATH = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
GCS_FILES = ['matches.xlsx', 'teams.xlsx']

def sync_from_gcs():
    """Download latest data files from GCS bucket to local directory."""
    try:
        if GCS_CREDENTIALS_PATH and os.path.exists(GCS_CREDENTIALS_PATH):
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GCS_CREDENTIALS_PATH
        client = gcs_storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        for filename in GCS_FILES:
            blob = bucket.blob(filename)
            if blob.exists():
                blob.download_to_filename(filename)
                print(f"☁️  Downloaded {filename} from GCS")
            else:
                print(f"⚠️  {filename} not found in GCS, using local copy")
    except Exception as e:
        print(f"⚠️  GCS sync failed (using local files): {e}")


try:
    result = brawl_match_processor.load_matches_data(
        user_id="test",
        settings_file="data/user_settings.json", 
        rosters_file="team_rosters.json",
        tracked_players_file="tracked_players.json"
    )
    print("✅ RUST IS WORKING")
except Exception as e:
    print(f"❌ RUST FAILED: {e}")
    import traceback
    traceback.print_exc()




from bot import (
    load_bot_mode as bot_load_mode
)

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', os.urandom(24))

# Production cookie settings
if os.environ.get('FLASK_ENV') == 'production':
    app.config['SESSION_COOKIE_SECURE'] = True
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
    app.config['SESSION_COOKIE_DOMAIN'] = 'brawlytix.pro'

# File paths
TOKENS_FILE = 'data/tokens.json'
AUTHORIZED_USERS_FILE = 'data/authorized_users.json'
USER_SETTINGS_FILE = 'data/user_settings.json'
MATCHES_FILE = 'matches.xlsx'
TEAMS_FILE = 'teams.xlsx'


_brawler_synergies = {}
_h2h_data = {}
_startup_done = False
_analyzer_data_cache = {'synergy': {}, 'teammate': {}, 'records': []}


CONFIG = {
    'REGIONS': ['NA', 'EU', 'SA', 'EA', 'SEA'],
    'MODES': ['Gem Grab', 'Brawl Ball', 'Heist', 'Bounty', 'Knockout', 'Hot Zone']
}

TEAM_ALIASES_FILE = 'data/team_aliases.json'

def load_team_aliases():
    """Load team name alias map from data/team_aliases.json. Returns {} on error."""
    try:
        if os.path.exists(TEAM_ALIASES_FILE):
            with open(TEAM_ALIASES_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return {k: v for k, v in data.items() if not k.startswith('_')}
    except Exception as e:
        print(f"Error loading team aliases: {e}")
    return {}


def apply_team_aliases(teams_data, aliases):
    """
    Merge teams_data entries whose keys match an alias into the canonical name.
    Modifies teams_data in place and returns it.
    """
    if not aliases:
        return teams_data
    for old_name, new_name in aliases.items():
        if old_name not in teams_data:
            continue
        old = teams_data.pop(old_name)
        if new_name not in teams_data:
            teams_data[new_name] = old
        else:
            # Merge stats into existing entry
            tgt = teams_data[new_name]
            tgt['matches'] = tgt.get('matches', 0) + old.get('matches', 0)
            tgt['wins']    = tgt.get('wins',    0) + old.get('wins',    0)
            tgt['losses']  = tgt.get('losses',  0) + old.get('losses',  0)
            # Merge players
            for tag, pdata in old.get('players', {}).items():
                if tag not in tgt.get('players', {}):
                    tgt.setdefault('players', {})[tag] = pdata
                else:
                    tp = tgt['players'][tag]
                    tp['matches']     = tp.get('matches',     0) + pdata.get('matches',     0)
                    tp['wins']        = tp.get('wins',        0) + pdata.get('wins',        0)
                    tp['star_player'] = tp.get('star_player', 0) + pdata.get('star_player', 0)
                    for b, bd in pdata.get('brawlers', {}).items():
                        tb = tp.setdefault('brawlers', {}).setdefault(b, {'picks': 0, 'wins': 0})
                        tb['picks'] += bd.get('picks', 0)
                        tb['wins']  += bd.get('wins',  0)
            # Merge brawlers
            for b, bd in old.get('brawlers', {}).items():
                tb = tgt.setdefault('brawlers', {}).setdefault(b, {'picks': 0, 'wins': 0})
                tb['picks'] += bd.get('picks', 0)
                tb['wins']  += bd.get('wins',  0)
    return teams_data


def apply_team_aliases_h2h(h2h_data, aliases):
    """
    Remap h2h_data keys (both outer team keys and inner opponent keys) using aliases.
    Merges stats when both old and new name exist.
    """
    if not aliases or not h2h_data:
        return h2h_data

    def merge_h2h_entry(tgt, src):
        """Merge src opponent dict into tgt opponent dict."""
        tgt['matches'] = tgt.get('matches', 0) + src.get('matches', 0)
        tgt['wins']    = tgt.get('wins',    0) + src.get('wins',    0)
        tgt['losses']  = tgt.get('losses',  0) + src.get('losses',  0)

    # Step 1: remap inner opponent keys in every team entry
    for team_name in list(h2h_data.keys()):
        opponents = h2h_data[team_name]
        for old_opp, new_opp in aliases.items():
            if old_opp in opponents:
                entry = opponents.pop(old_opp)
                if new_opp in opponents:
                    merge_h2h_entry(opponents[new_opp], entry)
                else:
                    opponents[new_opp] = entry

    # Step 2: remap outer team keys
    for old_name, new_name in aliases.items():
        if old_name not in h2h_data:
            continue
        old_entry = h2h_data.pop(old_name)
        if new_name not in h2h_data:
            h2h_data[new_name] = old_entry
        else:
            for opp, stats in old_entry.items():
                if opp not in h2h_data[new_name]:
                    h2h_data[new_name][opp] = stats
                else:
                    merge_h2h_entry(h2h_data[new_name][opp], stats)

    return h2h_data


VALID_MODES = {'Gem Grab', 'Brawl Ball', 'Heist', 'Bounty', 'Knockout', 'Hot Zone'}

@app.before_request
def startup_once():
    """Run heavy startup tasks on the very first request, works with any WSGI server."""
    global _startup_done
    if not _startup_done:
        _startup_done = True
        try:
            ensure_roster_files_exist()
        except Exception as e:
            print(f"⚠️ ensure_roster_files_exist failed: {e}")
        try:
            # We're inside a real request context here so session is available.
            # Use "__default__" user_id so Rust reads the default season settings
            # from user_settings.json — guarantees teams data is returned.
            print("🔥 Loading data on first request...")
            sync_from_gcs()
            data = load_matches_data()
            _cache['data'] = data
            _cache['timestamp'] = time.time()
            _cache['user_settings_hash'] = DEFAULT_CACHE_KEY
            for _mf in ['matches.xlsx', 'matches_off.xlsx']:
                if os.path.exists(_mf):
                    get_cached_matches_df(_mf)
            print("✅ First-request data load done")
        except Exception as e:
            import traceback
            print(f"⚠️ First-request data load failed: {e}")
            traceback.print_exc()

# Theme presets
THEMES = {
    'red':    {'primary': '#ef4444', 'primary_rgb': '239, 68, 68',   'bg': '#0a0a0a', 'card': '#111111', 'dark': '#1a1a1a'},
    'brawl':  {'primary': '#e94560', 'primary_rgb': '233, 69, 96',   'bg': '#1a1a2e', 'card': '#16213e', 'dark': '#0f3460'},
    'purple': {'primary': '#8b5cf6', 'primary_rgb': '139, 92, 246',  'bg': '#1e1b29', 'card': '#2d2438', 'dark': '#1a1625'},
    'blue':   {'primary': '#3b82f6', 'primary_rgb': '59, 130, 246',  'bg': '#0f172a', 'card': '#1e293b', 'dark': '#0f172a'},
    'green':  {'primary': '#10b981', 'primary_rgb': '16, 185, 129',  'bg': '#064e3b', 'card': '#065f46', 'dark': '#022c22'},
    'orange': {'primary': '#f97316', 'primary_rgb': '249, 115, 22',  'bg': '#1c1917', 'card': '#292524', 'dark': '#1c1917'},
    'mono':   {'primary': '#ffffff', 'primary_rgb': '255, 255, 255', 'bg': '#000000', 'card': '#111111', 'dark': '#0a0a0a'},
    'custom': {'primary': '#ef4444', 'primary_rgb': '239, 68, 68',   'bg': '#0a0a0a', 'card': '#111111', 'dark': '#1a1a1a'},  # placeholder, overridden by custom_color
}

from functools import lru_cache
from datetime import datetime
import time

# Global cache variables
_cache = {
    'data': None,
    'timestamp': None,
    'user_settings_hash': None
}

CACHE_DURATION = 300  # Cache for 5 minutes (300 seconds)

DEFAULT_CACHE_KEY = "season_30d__"

def warm_cache_at_startup():
    """Pre-load and cache data at startup so first user request is instant.
    Runs outside any Flask request context, so must NOT touch session/get_user_id."""
    try:
        print("🔥 Warming cache at startup...")
        start_time = time.time()

        result_json = brawl_match_processor.load_matches_data(
            user_id="__startup__",
            settings_file="data/user_settings.json",
            rosters_file="team_rosters.json",
            tracked_players_file="tracked_players.json"
        )

        global _brawler_synergies, _h2h_data
        result = json.loads(result_json)

        if 'error' in result:
            print(f"⚠️ Cache warm failed: {result['error']}")
            return

        _brawler_synergies = result.get('brawler_matchups', {})
        _h2h_data = apply_team_aliases_h2h(result.get('h2h_data', {}), load_team_aliases())

        teams_data = result.get('teams_data') or {}
        players_data = result.get('players_data') or {}
        region_stats = result.get('region_stats') or {}

        _region_remap = {'LATAM': 'SA', 'latam': 'SA', 'APAC': 'EA', 'apac': 'EA'}
        for t_data in teams_data.values():
            if isinstance(t_data, dict) and 'region' in t_data:
                t_data['region'] = _region_remap.get(t_data['region'], t_data['region'])
        for p_data in players_data.values():
            if isinstance(p_data, dict) and 'region' in p_data:
                p_data['region'] = _region_remap.get(p_data['region'], p_data['region'])
        region_stats = {_region_remap.get(k, k): v for k, v in region_stats.items()}

        # Apply team name aliases (merge renamed teams)
        teams_data = apply_team_aliases(teams_data, load_team_aliases())

        data = (
            None,
            teams_data,
            players_data,
            region_stats,
            result.get('mode_stats', {}),
            set(result.get('all_brawlers', []))
        )

        # Store under the default season cache key — any user with default settings hits this
        _cache['data'] = data
        _cache['timestamp'] = time.time()
        _cache['user_settings_hash'] = DEFAULT_CACHE_KEY

        elapsed = time.time() - start_time
        print(f"✅ Cache warmed in {elapsed:.2f}s ({len(teams_data)} teams, {len(players_data)} players)")
    except Exception as e:
        import traceback
        print(f"⚠️ Cache warm failed: {e}")
        traceback.print_exc()


_trios_cache = {
    'data': None,
    'timestamp': None,
    'cache_key': None
}

TRIOS_CACHE_DURATION = 300

def get_cache_key():
    """Generate a cache key based on user settings.
    Returns DEFAULT_CACHE_KEY for users with default/no settings so they hit the warmed cache."""
    try:
        user_settings = load_json(USER_SETTINGS_FILE)
        user_id = get_user_id()
        user_prefs = user_settings.get(user_id, {})
    except Exception:
        # Outside request context or session unavailable — return default key
        return DEFAULT_CACHE_KEY

    view_mode = user_prefs.get('view_mode', 'season') or 'season'
    date_range = user_prefs.get('date_range', '30d') or '30d'
    # Normalize None/null to empty string so key is consistent
    start_date = user_prefs.get('start_date') or ''
    end_date = user_prefs.get('end_date') or ''
    cache_key = f"{view_mode}_{date_range}_{start_date}_{end_date}"
    return cache_key


def get_user_id():
    """Get or create a unique user ID for this session"""
    if 'user_id' not in session:
        # Generate a unique ID for this browser session
        import uuid
        session['user_id'] = str(uuid.uuid4())
    return session['user_id']



def get_cached_data():
    """Get cached data if valid, otherwise reload"""
    current_cache_key = get_cache_key()
    current_time = time.time()

    # Cache hit — same key and still fresh
    if (_cache['data'] is not None and
        _cache['timestamp'] is not None and
        _cache['user_settings_hash'] == current_cache_key and
        current_time - _cache['timestamp'] < CACHE_DURATION):

        print(f"Using cached data (age: {int(current_time - _cache['timestamp'])}s)")
        return _cache['data']

    # New user with default settings — serve the warmed default cache if available
    if (current_cache_key == DEFAULT_CACHE_KEY and
        _cache['data'] is not None and
        _cache['timestamp'] is not None and
        current_time - _cache['timestamp'] < CACHE_DURATION):

        print("Serving warmed default cache to new user")
        return _cache['data']

    # Cache is invalid, reload data
    print("Loading fresh data...")
    start_time = time.time()

    sync_from_gcs()
    data = load_matches_data()  # Returns 6 values now

    # Update cache
    _cache['data'] = data
    _cache['timestamp'] = current_time
    _cache['user_settings_hash'] = current_cache_key

    elapsed = time.time() - start_time
    print(f"Data loaded in {elapsed:.2f}s")

    return data


def clear_cache():
    """Clear the cache (call this when settings change)"""
    _cache['data'] = None
    _cache['timestamp'] = None
    _cache['user_settings_hash'] = None
    _trios_cache['data'] = None
    _trios_cache['timestamp'] = None
    _trios_cache['cache_key'] = None

    ensure_roster_files_exist()
    print("🗑️ Cache cleared")

def load_json(filepath):
    if not os.path.exists(filepath):
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w') as f:
            json.dump({}, f)
        return {}
    with open(filepath, 'r') as f:
        return json.load(f)

def save_json(filepath, data):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)

def hex_to_rgb(hex_color):
    """Convert hex color (#rrggbb) to 'r, g, b' string for CSS"""
    hex_color = hex_color.lstrip('#')
    if len(hex_color) == 3:
        hex_color = ''.join(c*2 for c in hex_color)
    try:
        r = int(hex_color[0:2], 16)
        g = int(hex_color[2:4], 16)
        b = int(hex_color[4:6], 16)
        return f'{r}, {g}, {b}'
    except Exception:
        return '239, 68, 68'  # fallback red

def get_user_theme():
    """Get current user's theme or default"""
    if 'discord_id' not in session:
        return THEMES['red']

    settings = load_json(USER_SETTINGS_FILE)
    user_id = str(session['discord_id'])

    if user_id in settings:
        theme_name = settings[user_id].get('theme', 'red')
        if theme_name == 'custom':
            custom_color = settings[user_id].get('custom_color', '#ef4444')
            custom_bg    = settings[user_id].get('custom_bg',    '#0a0a0a')
            return {
                'primary':     custom_color,
                'primary_rgb': hex_to_rgb(custom_color),
                'bg':          custom_bg,
                'card':        custom_bg,
                'dark':        custom_bg,
            }
        return THEMES.get(theme_name, THEMES['red'])

    return THEMES['red']

def validate_token(token):
    tokens = load_json(TOKENS_FILE)
    if token not in tokens:
        return None, "Invalid token"
    token_data = tokens[token]
    if token_data.get('used', False):
        return None, "Token already used"
    return token_data, None

def mark_token_used(token):
    tokens = load_json(TOKENS_FILE)
    if token in tokens:
        tokens[token]['used'] = True
        save_json(TOKENS_FILE, tokens)

def is_user_authorized(discord_id):
    """Check if user is authorized and not expired"""
    authorized = load_json(AUTHORIZED_USERS_FILE)
    user_id = str(discord_id)
    
    if user_id not in authorized:
        return False
    
    user_data = authorized[user_id]
    
    # Check expiration
    expires_at = user_data.get('expires_at')
    if expires_at:
        expiration_date = pd.to_datetime(expires_at)
        if pd.Timestamp.now() > expiration_date:
            # Expired - remove from authorized list
            del authorized[user_id]
            save_json(AUTHORIZED_USERS_FILE, authorized)
            return False
    
    return True


def load_bot_mode():
    """Load current user's bot mode from their settings"""
    user_settings = load_json(USER_SETTINGS_FILE)
    user_id = get_user_id()
    user_prefs = user_settings.get(user_id, {})
    return user_prefs.get('view_mode', 'season')  # Use view_mode which already exists!

def get_config_for_mode():
    """Get configuration based on user's current mode"""
    mode = load_bot_mode()  # This now reads from user settings
    
    if mode == 'offseason':
        return {
            'MATCHES_FILE': 'matches_off.xlsx',
            'TEAMS_FILE': 'players_off.xlsx',
            'MODE_NAME': 'Off Season',
            'IS_PLAYER_MODE': True
        }
    else:
        return {
            'MATCHES_FILE': 'matches.xlsx',
            'TEAMS_FILE': 'teams.xlsx',
            'MODE_NAME': 'Season',
            'IS_PLAYER_MODE': False
        }


def calculate_synergies_for_season_filtered(brawler_name, mode_filter=None, map_filter=None):
    """Calculate synergies from matches.xlsx with filters for season mode"""
    matches_file = 'matches.xlsx'
    
    if not os.path.exists(matches_file):
        return [], [], []
    
    try:
        matches_df = pd.read_excel(matches_file)
    except Exception as e:
        print(f"Error loading {matches_file}: {e}")
        return [], [], []
    
    # Track teammate and opponent stats
    teammates_stats = defaultdict(lambda: {'picks': 0, 'wins': 0})
    opponents_stats = defaultdict(lambda: {'picks': 0, 'wins': 0})
    
    for _, match in matches_df.iterrows():
        mode = str(match.get('mode', 'Unknown'))
        map_name_row = str(match.get('map', 'Unknown'))
        
        # Apply filters
        if mode_filter and mode.lower().replace(' ', '_') != mode_filter.lower():
            continue
        if map_filter and map_name_row.lower().replace(' ', '_').replace("'", '').replace('-', '_') != map_filter.lower():
            continue
        
        winner_name = str(match.get('winner', '')).strip()
        
        # Check both teams
        for team_prefix in ['team1', 'team2']:
            # Get team name
            team_name = str(match.get(f'{team_prefix}_name', '')).strip()
            
            # Get brawlers for this team
            team_brawlers = []
            for i in range(1, 4):
                brawler_col = f'{team_prefix}_player{i}_brawler'
                brawler = str(match.get(brawler_col, '')).strip()
                if brawler and brawler != 'nan':
                    team_brawlers.append(brawler)
            
            # Check if our brawler is in this team
            if brawler_name not in team_brawlers:
                continue
            
            # This team has our brawler - track teammates
            is_winner = (winner_name == team_name)
            
            for teammate_brawler in team_brawlers:
                if teammate_brawler != brawler_name:
                    teammates_stats[teammate_brawler]['picks'] += 1
                    if is_winner:
                        teammates_stats[teammate_brawler]['wins'] += 1
            
            # Track opponents (other team)
            opponent_prefix = 'team2' if team_prefix == 'team1' else 'team1'
            opponent_brawlers = []
            for i in range(1, 4):
                brawler_col = f'{opponent_prefix}_player{i}_brawler'
                opp_brawler = str(match.get(brawler_col, '')).strip()
                if opp_brawler and opp_brawler != 'nan':
                    opponent_brawlers.append(opp_brawler)
            
            for opponent_brawler in opponent_brawlers:
                opponents_stats[opponent_brawler]['picks'] += 1
                if is_winner:
                    opponents_stats[opponent_brawler]['wins'] += 1
    
    # Convert to lists with win rates
    def convert_to_list(stats_dict, min_picks=3):
        result = []
        for brawler, data in stats_dict.items():
            if data['picks'] >= min_picks:
                win_rate = (data['wins'] / data['picks'] * 100) if data['picks'] > 0 else 0
                result.append({
                    'name': brawler,
                    'picks': data['picks'],
                    'wins': data['wins'],
                    'win_rate': win_rate
                })
        return sorted(result, key=lambda x: x['win_rate'], reverse=True)
    
    best_teammates = convert_to_list(teammates_stats)
    
    # For opponents: high win rate = good matchup
    all_matchups = convert_to_list(opponents_stats)
    best_matchups = sorted(all_matchups, key=lambda x: x['win_rate'], reverse=True)[:10]
    worst_matchups = sorted(all_matchups, key=lambda x: x['win_rate'])[:10]
    
    return best_teammates[:10], best_matchups, worst_matchups


def load_team_rosters():
    """Load valid player tags from teams.xlsx"""
    valid_players = {}
    
    if not os.path.exists(TEAMS_FILE):
        print(f"Warning: {TEAMS_FILE} not found - all players will be included")
        return None
    
    try:
        teams_df = pd.read_excel(TEAMS_FILE)
        
        for _, row in teams_df.iterrows():
            team_name = row['Team Name']
            if team_name not in valid_players:
                valid_players[team_name] = set()
            
            for i in range(1, 4):
                tag_col = f'Player {i} ID'
                if tag_col in teams_df.columns and pd.notna(row.get(tag_col)):
                    # Normalize: uppercase, strip, replace 0 with O
                    tag = str(row[tag_col]).strip().upper().replace('0', 'O')
                    valid_players[team_name].add(tag)
        
        return valid_players
    except Exception as e:
        print(f"Error loading team rosters: {e}")
        return None

def assign_brawlers_to_tiers_web(meta_scores):
    """
    Improved tier assignment for web server
    Creates balanced distributions using percentile-based approach
    """
    
    if not meta_scores:
        return None
    
    total_brawlers = len(meta_scores)
    
    # Define target percentages for each tier (more balanced)
    tier_percentages = {
        'S': 0.10,  # Top 10%
        'A': 0.20,  # Next 20%
        'B': 0.30,  # Next 30%
        'C': 0.25,  # Next 25%
        'D': 0.10,  # Next 10%
        'F': 0.05   # Bottom 5%
    }
    
    # Calculate target counts for each tier
    tier_targets = {}
    remaining = total_brawlers
    
    for tier in ['S', 'A', 'B', 'C', 'D']:
        count = max(1, int(total_brawlers * tier_percentages[tier]))
        tier_targets[tier] = count
        remaining -= count
    
    # F tier gets whatever is left (at least 0)
    tier_targets['F'] = max(0, remaining)
    
    # Assign brawlers to tiers based on counts
    tier_lists = {
        'S': [], 
        'A': [], 
        'B': [], 
        'C': [], 
        'D': [], 
        'F': []
    }
    
    tier_config = {
        'S': {'threshold': 0, 'color': '#ff4757', 'bg': '#3d1319'},
        'A': {'threshold': 0, 'color': '#ffa502', 'bg': '#3d2b0a'},
        'B': {'threshold': 0, 'color': '#ffd32a', 'bg': '#3d3610'},
        'C': {'threshold': 0, 'color': '#05c46b', 'bg': '#02311b'},
        'D': {'threshold': 0, 'color': '#0fbcf9', 'bg': '#043240'},
        'F': {'threshold': 0, 'color': '#747d8c', 'bg': '#1e2124'}
    }
    
    current_index = 0
    for tier_name in ['S', 'A', 'B', 'C', 'D', 'F']:
        count = tier_targets[tier_name]
        end_index = current_index + count
        
        # Assign brawlers
        tier_lists[tier_name] = meta_scores[current_index:end_index]
        
        # Set threshold based on lowest score in this tier
        if tier_lists[tier_name]:
            tier_config[tier_name]['threshold'] = tier_lists[tier_name][-1]['score']
        else:
            tier_config[tier_name]['threshold'] = 0
        
        current_index = end_index
    
    return tier_lists, tier_config

"****************************************"

def load_matches_data():
    """Load BOTH season and offseason data using Rust processor"""
    global _brawler_synergies, _h2h_data

    user_id = get_user_id()
    # If this user has no saved settings, fall back to __default__ so Rust
    # uses season view_mode instead of its own internal default (offseason).
    user_settings = load_json(USER_SETTINGS_FILE)
    if user_id not in user_settings:
        user_id = '__default__'

    try:
        result_json = brawl_match_processor.load_matches_data(
            user_id=user_id,
            settings_file="data/user_settings.json",
            rosters_file="team_rosters.json",
            tracked_players_file="tracked_players.json"
        )
        
        result = json.loads(result_json)
        
        if 'error' in result:
            print(f"❌ ERROR FROM RUST: {result['error']}")
            return None, {}, {}, {}, {}, set()
        
        # Extract synergies and h2h data
        _brawler_synergies = result.get('brawler_matchups', {})
        _h2h_data = apply_team_aliases_h2h(result.get('h2h_data', {}), load_team_aliases())

        # Get BOTH teams and players data
        teams_data = result.get('teams_data') or {}
        players_data = result.get('players_data') or {}
        region_stats = result.get('region_stats') or {}

        # Remap region names (LATAM -> SA, APAC -> EA)
        _region_remap = {'LATAM': 'SA', 'latam': 'SA', 'APAC': 'EA', 'apac': 'EA'}

        try:
            for t_name, t_data in teams_data.items():
                if isinstance(t_data, dict) and 'region' in t_data:
                    t_data['region'] = _region_remap.get(t_data['region'], t_data['region'])

            for p_tag, p_data in players_data.items():
                if isinstance(p_data, dict) and 'region' in p_data:
                    p_data['region'] = _region_remap.get(p_data['region'], p_data['region'])

            # Remap region_stats keys
            remapped_region_stats = {}
            for rkey, rval in region_stats.items():
                new_key = _region_remap.get(rkey, rkey)
                remapped_region_stats[new_key] = rval
            region_stats = remapped_region_stats

            # Apply team name aliases (merge renamed teams)
            teams_data = apply_team_aliases(teams_data, load_team_aliases())
        except Exception as e:
            print(f"⚠️ Region remap warning: {e}")

        return (
            None,
            teams_data,      # Season data
            players_data,    # Offseason data
            region_stats,
            result.get('mode_stats', {}),
            set(result.get('all_brawlers', []))
        )
        
    except Exception as e:
        print(f"❌ PYTHON ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, {}, {}, {}, {}, set()


def get_brawler_synergies(brawler_name):
    """Get teammates and opponents for a specific brawler"""
    global _brawler_synergies
    
    
    synergy_data = _brawler_synergies.get(brawler_name, {})
   
    
    # The Rust code returns calculated results directly
    best_teammates = synergy_data.get('best_teammates', [])
    worst_teammates = synergy_data.get('worst_teammates', [])
    best_matchups = synergy_data.get('best_matchups', [])
    worst_matchups = synergy_data.get('worst_matchups', [])
    
    # Convert from Rust format to Python format
    def convert_synergy_list(items):
        result = []
        for item in items:
            # Handle both dict and object formats
            if isinstance(item, dict):
                result.append({
                    'name': item.get('brawler', ''),
                    'picks': item.get('picks', 0),
                    'wins': item.get('wins', 0),
                    'win_rate': float(item.get('winrate', 0))
                })
        return result
    
    best_teammates = convert_synergy_list(best_teammates)
    best_matchups = convert_synergy_list(best_matchups)
    worst_matchups = convert_synergy_list(worst_matchups)
    
    
    return best_teammates, best_matchups, worst_matchups


def get_brawler_synergies_filtered(brawler_name, mode_filter=None, map_filter=None):
    """
    Calculate synergies filtered by mode and/or map
    Returns: (best_teammates, best_matchups, worst_matchups)
    """
    
    # We need to re-calculate from matches data
    matches_file = 'matches_off.xlsx' if load_bot_mode() == 'offseason' else 'matches.xlsx'
    
    if not os.path.exists(matches_file):
        print(f"Matches file not found: {matches_file}")
        return [], [], []
    
    try:
        matches_df = pd.read_excel(matches_file)
    except Exception as e:
        print(f"Error loading matches: {e}")
        return [], [], []
    
    def normalize_tag(tag):
        if not tag or str(tag) == 'nan':
            return None
        tag = str(tag).strip().upper().replace('0', 'O')
        if not tag.startswith('#'):
            tag = '#' + tag
        return tag
    
    # Track teammate and opponent stats
    teammates_stats = defaultdict(lambda: {'picks': 0, 'wins': 0})
    opponents_stats = defaultdict(lambda: {'picks': 0, 'wins': 0})
    
    for _, match in matches_df.iterrows():
        mode = str(match.get('mode', 'Unknown'))
        map_name = str(match.get('map', 'Unknown'))
        
        # Apply filters
        if mode_filter and mode.lower().replace(' ', '_') != mode_filter.lower():
            continue
        if map_filter and map_name.lower().replace(' ', '_').replace("'", '').replace('-', '_') != map_filter.lower():
            continue
        
        winner = str(match.get('winner', '')).strip()
        
        # Check both teams
        for team_prefix in ['team1', 'team2']:
            # Get brawlers for this team
            team_brawlers = []
            for i in range(1, 4):
                brawler = str(match.get(f'{team_prefix}_player{i}_brawler', '')).strip()
                if brawler and brawler != 'nan':
                    team_brawlers.append(brawler)
            
            # Check if our brawler is in this team
            if brawler_name not in team_brawlers:
                continue
            
            # This team has our brawler - track teammates
            is_winner = (winner == team_prefix)
            
            for teammate_brawler in team_brawlers:
                if teammate_brawler != brawler_name:
                    teammates_stats[teammate_brawler]['picks'] += 1
                    if is_winner:
                        teammates_stats[teammate_brawler]['wins'] += 1
            
            # Track opponents (other team)
            opponent_prefix = 'team2' if team_prefix == 'team1' else 'team1'
            opponent_brawlers = []
            for i in range(1, 4):
                opp_brawler = str(match.get(f'{opponent_prefix}_player{i}_brawler', '')).strip()
                if opp_brawler and opp_brawler != 'nan':
                    opponent_brawlers.append(opp_brawler)
            
            for opponent_brawler in opponent_brawlers:
                opponents_stats[opponent_brawler]['picks'] += 1
                if is_winner:
                    opponents_stats[opponent_brawler]['wins'] += 1
    
    # Convert to lists with win rates
    def convert_to_list(stats_dict, min_picks=3):
        result = []
        for brawler, data in stats_dict.items():
            if data['picks'] >= min_picks:
                win_rate = (data['wins'] / data['picks'] * 100) if data['picks'] > 0 else 0
                result.append({
                    'name': brawler,
                    'picks': data['picks'],
                    'wins': data['wins'],
                    'win_rate': win_rate
                })
        return sorted(result, key=lambda x: x['win_rate'], reverse=True)
    
    best_teammates = convert_to_list(teammates_stats)
    
    # For opponents: high win rate = good matchup (we win against them)
    # low win rate = bad matchup (they win against us)
    all_matchups = convert_to_list(opponents_stats)
    best_matchups = sorted(all_matchups, key=lambda x: x['win_rate'], reverse=True)[:10]
    worst_matchups = sorted(all_matchups, key=lambda x: x['win_rate'])[:10]
    
    
    return best_teammates[:10], best_matchups, worst_matchups


def assign_brawlers_to_tiers_unified(meta_scores):
    """
    UNIFIED tier assignment - produces identical results across bot and web
    Creates balanced distributions using percentile-based approach
    """
    
    if not meta_scores:
        return None
    
    total_brawlers = len(meta_scores)
    
    # Define target percentages for each tier
    tier_percentages = {
        'S': 0.10,  # Top 10%
        'A': 0.20,  # Next 20%
        'B': 0.30,  # Next 30%
        'C': 0.25,  # Next 25%
        'D': 0.10,  # Next 10%
        'F': 0.05   # Bottom 5%
    }
    
    # Calculate target counts for each tier
    tier_targets = {}
    remaining = total_brawlers
    
    for tier in ['S', 'A', 'B', 'C', 'D']:
        count = max(1, int(total_brawlers * tier_percentages[tier]))
        tier_targets[tier] = count
        remaining -= count
    
    # F tier gets whatever is left (at least 0)
    tier_targets['F'] = max(0, remaining)
    
    # Assign brawlers to tiers based on counts
    tiers = {
        'S': {'brawlers': [], 'color': (255, 71, 87), 'threshold': 0},
        'A': {'brawlers': [], 'color': (255, 165, 2), 'threshold': 0},
        'B': {'brawlers': [], 'color': (255, 211, 42), 'threshold': 0},
        'C': {'brawlers': [], 'color': (5, 196, 107), 'threshold': 0},
        'D': {'brawlers': [], 'color': (15, 188, 249), 'threshold': 0},
        'F': {'brawlers': [], 'color': (116, 125, 140), 'threshold': 0}
    }
    
    current_index = 0
    for tier_name in ['S', 'A', 'B', 'C', 'D', 'F']:
        count = tier_targets[tier_name]
        end_index = current_index + count
        
        # Assign brawlers
        tiers[tier_name]['brawlers'] = meta_scores[current_index:end_index]
        
        # Set threshold based on lowest score in this tier
        if tiers[tier_name]['brawlers']:
            tiers[tier_name]['threshold'] = tiers[tier_name]['brawlers'][-1]['score']
        else:
            tiers[tier_name]['threshold'] = 0
        
        current_index = end_index
    
    return tiers


def load_tracked_players_web():
    """Load tracked players from players_off.xlsx"""
    players_file = 'players_off.xlsx'
    
    if not os.path.exists(players_file):
        print(f"{players_file} not found")
        return {}
    
    tracked = {}
    
    try:
        df = pd.read_excel(players_file)
        
        for _, row in df.iterrows():
            tag = str(row['Player ID']).strip().upper().replace('0', 'O')
            if not tag.startswith('#'):
                tag = '#' + tag
            
            region = str(row.get('Region', 'NA')).strip().upper()
            if region in ['NAN', 'NONE', '', 'NULL'] or pd.isna(row.get('Region')):
                region = 'NA'
            
            tracked[tag] = {
                'name': str(row['Player Name']).strip(),
                'region': region,
            }
        
        return tracked
        
    except Exception as e:
        print(f"Error loading tracked players: {e}")
        return {}



REGION_DISPLAY = {
    'LATAM': 'SA',
    'latam': 'SA',
}

@app.template_filter('region_name')
def region_name_filter(value):
    """Convert internal region codes to display names."""
    return REGION_DISPLAY.get(value, value)

@app.context_processor
def inject_theme():
    """Make theme and view mode available to all templates"""
    user_settings = load_json(USER_SETTINGS_FILE)
    user_id = get_user_id()
    user_prefs = user_settings.get(user_id, {})
    view_mode = user_prefs.get('view_mode', 'season')
    
    return {
        'theme': get_user_theme(),
        'view_mode': view_mode
    }


@app.route('/')
def index():
    return redirect('/dashboard')  # Remove the session check


@app.route('/dashboard')
def dashboard():
    try:
        matches_df, teams_data, players_data, region_stats, mode_stats, all_brawlers = get_cached_data()

        # Guard against None data
        if teams_data is None:
            teams_data = {}
        if players_data is None:
            players_data = {}
        if region_stats is None:
            region_stats = {}

        user_settings = load_json(USER_SETTINGS_FILE)
        user_id = get_user_id()
        user_prefs = user_settings.get(user_id, {})
        view_mode = user_prefs.get('view_mode', 'season')

        # Use the appropriate data
        if view_mode == 'offseason':
            data = players_data
        else:
            data = teams_data

        
        # Get user's view mode
        user_settings = load_json(USER_SETTINGS_FILE)
        user_id = get_user_id()
        user_prefs = user_settings.get(user_id, {})
        view_mode = user_prefs.get('view_mode', 'season')
        
        
        if view_mode == 'offseason':
            
            
            # Check data structure
            if not isinstance(data, dict):
                print(f"❌ Expected dict, got {type(data)}")
                
            
            # Filter players with minimum 5 matches
            filtered_players = []
            for tag, player_data in data.items():
                try:
                    if not isinstance(player_data, dict):
                        print(f"⚠️ Player {tag} has invalid data type: {type(player_data)}")
                        continue
                    
                    matches = player_data.get('matches', 0)
                    if matches >= 5:
                        filtered_players.append((tag, player_data))
                except Exception as e:
                    print(f"⚠️ Error processing player {tag}: {e}")
                    continue
            
            
            
            # Sort by win rate
            try:
                top_items = sorted(
                    filtered_players,
                    key=lambda x: (x[1]['wins'] / x[1]['matches']) if x[1].get('matches', 0) > 0 else 0,
                    reverse=True
                )[:10]
                
            except Exception as e:
                print(f"❌ Error sorting players: {e}")
                import traceback
                traceback.print_exc()
                top_items = []
            
            # Calculate total matches
            try:
                total_matches = sum(
                    stats.get('total_matches', 0) 
                    for region, stats in region_stats.items() 
                    if not region.startswith('_') and isinstance(stats, dict)
                )
                print(f"Total matches: {total_matches}")
            except Exception as e:
                print(f"❌ Error calculating total matches: {e}")
                total_matches = 0
            
            # Render template
            return render_template('dashboard_offseason.html',
                                 user=session.get('discord_tag', 'Guest'),
                                 total_matches=total_matches,
                                 total_players=len(data),
                                 total_brawlers=len(all_brawlers),
                                 top_players=top_items,
                                 region_stats=region_stats)
        
        else:  # Season mode


            if not isinstance(data, dict):
                print(f"❌ Expected dict, got {type(data)}")


            top_teams = []
            worst_teams = []
            most_wins_teams = []
            most_matches_teams = []
            valid_items = []

            try:
                MIN_MATCHES = 3
                # All valid team entries
                valid_items = [(n, d) for n, d in data.items() if isinstance(d, dict) and d.get('matches', 0) >= 1]
                # Filter for WR ranking (minimum matches required)
                qualified = [(n, d) for n, d in valid_items if d.get('matches', 0) >= MIN_MATCHES]

                # Top by win rate (min 3 matches)
                top_teams = sorted(
                    qualified,
                    key=lambda x: x[1]['wins'] / x[1]['matches'],
                    reverse=True
                )[:10]

                # Worst by win rate (min 3 matches)
                worst_teams = sorted(
                    qualified,
                    key=lambda x: x[1]['wins'] / x[1]['matches'],
                    reverse=False
                )[:5]

                # Most wins — from all valid teams
                most_wins_teams = sorted(
                    valid_items,
                    key=lambda x: x[1].get('wins', 0),
                    reverse=True
                )[:5]

                # Most matches — from all valid teams
                most_matches_teams = sorted(
                    valid_items,
                    key=lambda x: x[1].get('matches', 0),
                    reverse=True
                )[:5]

            except Exception as e:
                print(f"❌ Error sorting teams: {e}")
                import traceback
                traceback.print_exc()

            # Total matches: raw sum (each series counted once per team, *2 gives total game count)
            total_matches = sum(team.get('matches', 0) for team in data.values())
            total_players = len(data) * 3

            print(f"📊 Dashboard: {len(data)} teams, {len(valid_items) if valid_items else 0} valid, most_wins={len(most_wins_teams)}, most_matches={len(most_matches_teams)}")

            return render_template('dashboard.html',
                                 user=session.get('discord_tag', 'Guest'),
                                 total_matches=total_matches,
                                 total_teams=len(data),
                                 total_players=total_players,
                                 top_teams=top_teams,
                                 worst_teams=worst_teams,
                                 most_wins_teams=most_wins_teams,
                                 most_matches_teams=most_matches_teams)

    except Exception as e:
        print(f"\n❌ DASHBOARD ERROR: {e}")
        import traceback
        traceback.print_exc()
        return render_template('dashboard.html',
                             user=session.get('discord_tag', 'Guest'),
                             total_matches=0,
                             total_teams=0,
                             total_players=0,
                             top_teams=[],
                             worst_teams=[],
                             most_wins_teams=[],
                             most_matches_teams=[])


@app.route('/teams')
def teams_page():
    matches_df, teams_data, players_data, region_stats, mode_stats, all_brawlers = get_cached_data()

    if teams_data is None:
        teams_data = {}

    # Sort by matches descending by default
    sorted_teams = sorted(
        teams_data.items(),
        key=lambda x: x[1].get('matches', 0) if isinstance(x[1], dict) else 0,
        reverse=True
    )

    total_matches = sum(t.get('matches', 0) for t in teams_data.values() if isinstance(t, dict))

    # Best team by win rate (min 3 matches)
    best_team = None
    best_wr = -1
    for name, data in teams_data.items():
        if not isinstance(data, dict):
            continue
        matches = data.get('matches', 0)
        wins = data.get('wins', 0)
        if matches >= 3:
            wr = wins / matches
            if wr > best_wr:
                best_wr = wr
                best_team = (name, data)

    return render_template('teams.html',
                           user=session.get('discord_tag', 'Guest'),
                           teams=sorted_teams,
                           total_matches=total_matches,
                           best_team=best_team)


@app.route('/region/<region_name>')
def region_page(region_name):
    try:
        region_name = region_name.upper()
        # Remap old region names
        _url_region_remap = {'LATAM': 'SA', 'APAC': 'EA'}
        region_name = _url_region_remap.get(region_name, region_name)
        matches_df, teams_data, players_data, region_stats, mode_stats, all_brawlers = get_cached_data()

        # Guard against None data
        if teams_data is None:
            teams_data = {}
        if players_data is None:
            players_data = {}

        user_settings = load_json(USER_SETTINGS_FILE)
        user_id = get_user_id()
        user_prefs = user_settings.get(user_id, {})
        view_mode = user_prefs.get('view_mode', 'season')

        # Use the appropriate data
        if view_mode == 'offseason':
            data = players_data
        else:
            data = teams_data

        if not data:
            if view_mode == 'offseason':
                return render_template('region_offseason.html',
                                     user=session.get('discord_tag', 'Guest'),
                                     region=region_name,
                                     region_code=region_name,
                                     total_matches=0,
                                     total_players=0,
                                     top_players=[],
                                     players_data={})
            else:
                return render_template('region.html',
                                     user=session.get('discord_tag', 'Guest'),
                                     region=region_name,
                                     region_code=region_name,
                                     total_matches=0,
                                     total_teams=0,
                                     top_teams=[],
                                     worst_teams=[],
                                     teams_data={})

        
        # Get user's view mode
        user_settings = load_json(USER_SETTINGS_FILE)
        user_id = get_user_id()
        user_prefs = user_settings.get(user_id, {})
        view_mode = user_prefs.get('view_mode', 'season')
        
       
        
        if view_mode == 'offseason':
            # OFF-SEASON MODE: Show players instead of teams
            
            if region_name == 'ALL':
                region_players = data  # All players
                title = "All Regions"
            else:
                if region_name not in CONFIG['REGIONS']:
                    return f"Region not found: {region_name}", 404
                # Filter players by region
                region_players = {
                    tag: player 
                    for tag, player in data.items() 
                    if isinstance(player, dict) and player.get('region') == region_name
                }
                title = f"{region_name} Region"
            
          
            
            # Filter players with minimum 5 matches
            filtered_players = [
                (tag, player_data) 
                for tag, player_data in region_players.items() 
                if isinstance(player_data, dict) and player_data.get('matches', 0) >= 5
            ]
            
           
            
            # Sort by win rate
            top_players = sorted(
                filtered_players,
                key=lambda x: (x[1]['wins'] / x[1]['matches']) if x[1].get('matches', 0) > 0 else 0,
                reverse=True
            )[:20]  # Top 20 players
            
            total_matches = sum(p.get('matches', 0) for p in region_players.values() if isinstance(p, dict))
            
            # Use the offseason template
            return render_template('region_offseason.html',
                                 user=session.get('discord_tag', 'Guest'),
                                 region=title,
                                 region_code=region_name,
                                 total_matches=total_matches,
                                 total_players=len(region_players),
                                 top_players=top_players,
                                 players_data=data)
        
        else:
            # SEASON MODE: Show teams (existing logic)
            
            if region_name == 'ALL':
                region_teams = data
                title = "All Regions"
            else:
                if region_name not in CONFIG['REGIONS']:
                    return f"Region not found: {region_name}", 404
                region_teams = {
                    name: team_data 
                    for name, team_data in data.items() 
                    if isinstance(team_data, dict) and team_data.get('region') == region_name
                }
                title = f"{region_name} Region"
            
            
            
            all_sorted = sorted(
                region_teams.items(),
                key=lambda x: (x[1]['wins'] / x[1]['matches']) if x[1].get('matches', 0) > 0 else 0,
                reverse=True
            )
            top_teams = all_sorted[:20]
            worst_teams = all_sorted[-5:] if len(all_sorted) >= 5 else list(reversed(all_sorted))
            worst_teams = list(reversed(worst_teams))

            total_matches = sum(t.get('matches', 0) for t in region_teams.values() if isinstance(t, dict))

            return render_template('region.html',
                                 user=session.get('discord_tag', 'Guest'),
                                 region=title,
                                 region_code=region_name,
                                 total_matches=total_matches,
                                 total_teams=len(region_teams),
                                 top_teams=top_teams,
                                 worst_teams=worst_teams,
                                 teams_data=data)
    
    except Exception as e:
        print(f"❌ REGION PAGE ERROR: {e}")
        import traceback
        traceback.print_exc()
        return render_template('region.html',
                             user=session.get('discord_tag', 'Guest'),
                             region=region_name,
                             region_code=region_name,
                             total_matches=0,
                             total_teams=0,
                             top_teams=[],
                             worst_teams=[],
                             teams_data={})

"""
Add this to your website.py to test which routes are registered
"""

@app.route('/test-routes')
def test_routes():
    """List all registered routes"""
    routes = []
    for rule in app.url_map.iter_rules():
        routes.append({
            'endpoint': rule.endpoint,
            'methods': ','.join(rule.methods),
            'path': str(rule)
        })
    
    routes.sort(key=lambda x: x['path'])
    
    output = "<h1>Registered Routes</h1><ul>"
    for route in routes:
        output += f"<li><b>{route['path']}</b> → {route['endpoint']} ({route['methods']})</li>"
    output += "</ul>"
    
    return output

# Also add this debug route
@app.route('/test-region')
def test_region():
    """Test if basic region route works"""
    return "Region route is working! Try /region/NA or /region/ALL"

@app.route('/team/<team_name>')
def team_page(team_name):
    matches_df, teams_data, players_data, region_stats, mode_stats, all_brawlers = get_cached_data()

    if teams_data is None:
        teams_data = {}
    if players_data is None:
        players_data = {}

    user_settings = load_json(USER_SETTINGS_FILE)
    user_id = get_user_id()
    user_prefs = user_settings.get(user_id, {})
    view_mode = user_prefs.get('view_mode', 'season')

    # Use the appropriate data
    if view_mode == 'offseason':
        data = players_data
    else:
        data = teams_data

    if team_name not in teams_data:
        return "Team not found", 404

    team = teams_data[team_name]

    # Get head-to-head stats from cached Rust data
    team_h2h = _h2h_data.get(team_name, {})
    # Convert to sorted list of tuples for the template
    h2h_sorted = sorted(team_h2h.items(), key=lambda x: x[1].get('matches', 0), reverse=True)

    return render_template('team.html',
                         user=session.get('discord_tag', 'Guest'),
                         team_name=team_name,
                         team=team,
                         h2h_stats=h2h_sorted)

@app.route('/team/<team1>/vs/<team2>')
def team_h2h_page(team1, team2):
    matches_df, teams_data, players_data, region_stats, mode_stats, all_brawlers = get_cached_data()

    if teams_data is None:
        teams_data = {}

    if team1 not in teams_data:
        return "Team not found", 404
    if team2 not in teams_data:
        return "Team not found", 404

    team1_data = teams_data[team1]
    team2_data = teams_data[team2]

    # h2h from team1's perspective
    team1_h2h = _h2h_data.get(team1, {})
    h2h = team1_h2h.get(team2, {})

    # Build recent series by reading matches.xlsx directly (matches_df is always None from cache)
    # Group consecutive games between the same two teams into series (BO3/BO5)
    recent_matches = []
    series_stats = {}
    matches_file = get_config_for_mode()['MATCHES_FILE']
    if os.path.exists(matches_file):
        try:
            df = get_cached_matches_df(matches_file)
            if df is None:
                raise ValueError("matches df is None")

            # Filter rows for this matchup (either orientation)
            # Apply aliases so renamed teams match current names
            _aliases = load_team_aliases()
            raw_games = []
            for _, row in df.iterrows():
                t1 = _aliases.get(str(row.get('team1_name', '')).strip(), str(row.get('team1_name', '')).strip())
                t2 = _aliases.get(str(row.get('team2_name', '')).strip(), str(row.get('team2_name', '')).strip())
                if not ((t1 == team1 and t2 == team2) or (t1 == team2 and t2 == team1)):
                    continue
                winner = _aliases.get(str(row.get('winner', '')).strip(), str(row.get('winner', '')).strip())
                mode = str(row.get('mode', '')).strip()
                map_name = str(row.get('map', '')).strip()

                # Extract brawlers and player names (oriented: team1 always on left)
                if t1 == team1:
                    t1_brawlers = [str(row.get(f'team1_player{i}_brawler', '')).strip() for i in range(1, 4)]
                    t2_brawlers = [str(row.get(f'team2_player{i}_brawler', '')).strip() for i in range(1, 4)]
                    t1_players  = [str(row.get(f'team1_player{i}', '')).strip() for i in range(1, 4)]
                    t2_players  = [str(row.get(f'team2_player{i}', '')).strip() for i in range(1, 4)]
                else:
                    t1_brawlers = [str(row.get(f'team2_player{i}_brawler', '')).strip() for i in range(1, 4)]
                    t2_brawlers = [str(row.get(f'team1_player{i}_brawler', '')).strip() for i in range(1, 4)]
                    t1_players  = [str(row.get(f'team2_player{i}', '')).strip() for i in range(1, 4)]
                    t2_players  = [str(row.get(f'team1_player{i}', '')).strip() for i in range(1, 4)]

                def clean_list(lst):
                    return [x for x in lst if x and x != 'nan']

                battle_time_raw = str(row.get('battle_time', '')).strip()
                # Parse battle_time format: 20260212T085117.000Z
                time_ago = ''
                game_date = ''    # e.g. "Feb 12"
                game_time = ''    # e.g. "08:51"
                try:
                    from datetime import timezone
                    bt = datetime.strptime(battle_time_raw[:15], '%Y%m%dT%H%M%S').replace(tzinfo=timezone.utc)
                    diff = datetime.now(timezone.utc) - bt
                    total_secs = int(diff.total_seconds())
                    if total_secs < 3600:
                        time_ago = f"{total_secs // 60}m ago"
                    elif total_secs < 86400:
                        time_ago = f"{total_secs // 3600}h ago"
                    elif total_secs < 86400 * 30:
                        time_ago = f"{total_secs // 86400}d ago"
                    else:
                        time_ago = bt.strftime('%b %d')
                    game_date = bt.strftime('%b %d')
                    game_time = bt.strftime('%H:%M')
                except Exception:
                    time_ago = ''

                raw_games.append({
                    'mode': mode,
                    'map': map_name,
                    'team1_brawlers': clean_list(t1_brawlers),
                    'team2_brawlers': clean_list(t2_brawlers),
                    'team1_players':  clean_list(t1_players),
                    'team2_players':  clean_list(t2_players),
                    'team1_won': (winner == team1),
                    'battle_time': battle_time_raw,
                    'time_ago': time_ago,
                    'game_date': game_date,
                    'game_time': game_time,
                    'duration': None,
                })

            # Group consecutive games into series (series ends when a team reaches 2 wins)
            series_list = []
            i = 0
            while i < len(raw_games):
                series_games = [raw_games[i]]
                t1_wins = 1 if raw_games[i]['team1_won'] else 0
                t2_wins = 0 if raw_games[i]['team1_won'] else 1
                j = i + 1
                # Keep adding games until someone hits 2 wins or mode changes significantly
                while j < len(raw_games) and t1_wins < 2 and t2_wins < 2:
                    series_games.append(raw_games[j])
                    if raw_games[j]['team1_won']:
                        t1_wins += 1
                    else:
                        t2_wins += 1
                    j += 1
                # Build series entry using the first game's composition and overall score
                first_game = series_games[0]
                # Total duration across all games in the series (in seconds)
                total_duration = sum(g['duration'] for g in series_games if g['duration'] is not None) or None
                series_list.append({
                    'mode': first_game['mode'],
                    'map': first_game['map'],
                    'team1_brawlers': first_game['team1_brawlers'],
                    'team2_brawlers': first_game['team2_brawlers'],
                    'team1_players':  first_game['team1_players'],
                    'team2_players':  first_game['team2_players'],
                    'team1_won': t1_wins > t2_wins,
                    'score_t1': t1_wins,
                    'score_t2': t2_wins,
                    'games': series_games,
                    'battle_time': first_game['battle_time'],
                    'time_ago': first_game['time_ago'],
                    'game_date': first_game['game_date'],
                    'game_time': first_game['game_time'],
                    'duration': total_duration,
                    'num_games': len(series_games),
                })
                i = j

            # Most recent first, cap at 15 series
            recent_matches = series_list[-15:][::-1]

            # Compute series-level stats for the sidebar stats box
            total_series = len(series_list)
            t1_series_wins = sum(1 for s in series_list if s['team1_won'])
            t2_series_wins = total_series - t1_series_wins
            bo3_wins = sum(1 for s in series_list if s['score_t1'] == 2 and s['score_t2'] == 1)
            bo3_losses = sum(1 for s in series_list if s['score_t1'] == 1 and s['score_t2'] == 2)
            clean_wins = sum(1 for s in series_list if s['score_t1'] == 2 and s['score_t2'] == 0)
            clean_losses = sum(1 for s in series_list if s['score_t1'] == 0 and s['score_t2'] == 2)
            avg_duration = None
            durations = [s['duration'] for s in series_list if s['duration']]
            if durations:
                avg_duration = int(sum(durations) / len(durations))

            # Brawler pick + win stats across ALL individual games
            brawler_stats_t1 = {}  # name -> {'picks': int, 'wins': int}
            brawler_stats_t2 = {}
            for s in series_list:
                for g in s['games']:
                    for b in g['team1_brawlers']:
                        entry = brawler_stats_t1.setdefault(b, {'picks': 0, 'wins': 0})
                        entry['picks'] += 1
                        if g['team1_won']:
                            entry['wins'] += 1
                    for b in g['team2_brawlers']:
                        entry = brawler_stats_t2.setdefault(b, {'picks': 0, 'wins': 0})
                        entry['picks'] += 1
                        if not g['team1_won']:
                            entry['wins'] += 1
            top_brawlers_t1 = sorted(brawler_stats_t1.items(), key=lambda x: x[1]['picks'], reverse=True)
            top_brawlers_t2 = sorted(brawler_stats_t2.items(), key=lambda x: x[1]['picks'], reverse=True)
            # keep legacy max_pick vars
            max_pick_t1 = top_brawlers_t1[0][1]['picks'] if top_brawlers_t1 else 1
            max_pick_t2 = top_brawlers_t2[0][1]['picks'] if top_brawlers_t2 else 1

            # Map records across all individual games
            map_records = {}  # map_name -> {'wins': int, 'losses': int, 'mode': str}
            for s in series_list:
                series_mode = s.get('mode', '') or ''
                for g in s['games']:
                    m = g['map']
                    if not m or m == 'nan':
                        continue
                    if m not in map_records:
                        map_records[m] = {'wins': 0, 'losses': 0, 'mode': ''}
                    # Use game mode, fall back to series mode
                    if not map_records[m]['mode']:
                        game_mode = g.get('mode', '') or ''
                        map_records[m]['mode'] = game_mode if game_mode and game_mode != 'nan' else series_mode
                    if g['team1_won']:
                        map_records[m]['wins'] += 1
                    else:
                        map_records[m]['losses'] += 1
            # Sort by total games played desc (no cap — box scrolls)
            map_records_sorted = sorted(
                map_records.items(),
                key=lambda x: x[1]['wins'] + x[1]['losses'],
                reverse=True
            )

            # Mode records across all individual games
            mode_records = {}
            for s in series_list:
                for g in s['games']:
                    mo = g['mode']
                    if not mo or mo == 'nan':
                        continue
                    if mo not in mode_records:
                        mode_records[mo] = {'wins': 0, 'losses': 0}
                    if g['team1_won']:
                        mode_records[mo]['wins'] += 1
                    else:
                        mode_records[mo]['losses'] += 1
            mode_records_sorted = sorted(
                mode_records.items(),
                key=lambda x: x[1]['wins'] + x[1]['losses'],
                reverse=True
            )

            series_stats = {
                'total': total_series,
                't1_wins': t1_series_wins,
                't2_wins': t2_series_wins,
                'bo3_wins': bo3_wins,
                'bo3_losses': bo3_losses,
                'clean_wins': clean_wins,
                'clean_losses': clean_losses,
                'avg_duration': avg_duration,
                'top_brawlers_t1': top_brawlers_t1,
                'top_brawlers_t2': top_brawlers_t2,
                'max_pick_t1': max_pick_t1,
                'max_pick_t2': max_pick_t2,
                'map_records': map_records_sorted,
                'mode_records': mode_records_sorted,
            }

        except Exception as e:
            print(f"Error loading match history: {e}")

    return render_template('team_h2h.html',
                         user=session.get('discord_tag', 'Guest'),
                         team1=team1,
                         team2=team2,
                         team1_data=team1_data,
                         team2_data=team2_data,
                         h2h=h2h,
                         recent_matches=recent_matches,
                         series_stats=series_stats)

@app.route('/team/<team_name>/mode/<mode>')
def team_mode_page(team_name, mode):
    matches_df, teams_data, players_data, region_stats, mode_stats, all_brawlers = get_cached_data()

    if teams_data is None:
        teams_data = {}
    if players_data is None:
        players_data = {}

    user_settings = load_json(USER_SETTINGS_FILE)
    user_id = get_user_id()
    user_prefs = user_settings.get(user_id, {})
    view_mode = user_prefs.get('view_mode', 'season')

    # Use the appropriate data
    if view_mode == 'offseason':
        data = players_data
    else:
        data = teams_data

    if team_name not in teams_data:
        return "Team not found", 404
    
    team = teams_data[team_name]
    
    if mode not in team['modes']:
        return "Mode not found", 404
    
    mode_data = team['modes'][mode]
    
    return render_template('team_mode.html',
                         user=session.get('discord_tag', 'Guest'),
                         team_name=team_name,
                         team=team,
                         mode=mode,
                         mode_data=mode_data)

@app.route('/team/<team_name>/mode/<mode>/map/<map_name>')
def team_map_page(team_name, mode, map_name):
    matches_df, teams_data, players_data, region_stats, mode_stats, all_brawlers = get_cached_data()

    if teams_data is None:
        teams_data = {}
    if players_data is None:
        players_data = {}

    user_settings = load_json(USER_SETTINGS_FILE)
    user_id = get_user_id()
    user_prefs = user_settings.get(user_id, {})
    view_mode = user_prefs.get('view_mode', 'season')

    # Use the appropriate data
    if view_mode == 'offseason':
        data = players_data
    else:
        data = teams_data

    if team_name not in teams_data:
        return "Team not found", 404
    
    team = teams_data[team_name]
    
    if mode not in team['modes'] or map_name not in team['modes'][mode]['maps']:
        return "Map not found", 404
    
    map_data = team['modes'][mode]['maps'][map_name]
    
    return render_template('team_map.html',
                         user=session.get('discord_tag', 'Guest'),
                         team_name=team_name,
                         team=team,
                         mode=mode,
                         map_name=map_name,
                         map_data=map_data)



@app.route('/analyzer')
def analyzer_page():
    matches_df, teams_data, players_data, region_stats, mode_stats, all_brawlers = get_cached_data()
    theme = get_user_theme()
    user_settings = load_json(USER_SETTINGS_FILE)
    user_id = get_user_id()
    user_prefs = user_settings.get(user_id, {})
    view_mode = user_prefs.get('view_mode', 'season')
    date_range = user_prefs.get('date_range', '30d') or '30d'

    # Convert stored RFC3339 dates back to YYYY-MM-DD for the custom date inputs
    saved_start = ''
    saved_end   = ''
    if user_prefs.get('start_date'):
        try:
            from datetime import datetime as _dt
            saved_start = _dt.fromisoformat(user_prefs['start_date'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
        except Exception:
            pass
    if user_prefs.get('end_date'):
        try:
            from datetime import datetime as _dt
            saved_end = _dt.fromisoformat(user_prefs['end_date'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
        except Exception:
            pass

    # Use the right data source
    data_source = teams_data or {}
    if view_mode == 'offseason':
        data_source = {}
        for ptag, p in (players_data or {}).items():
            if isinstance(p, dict):
                data_source[ptag] = {'region': p.get('region', 'NA'), 'modes': p.get('modes', {})}

    # Build per-mode, per-map brawler aggregates for filtered meta scoring
    # Structure: mode_map_raw[mode][map][brawler] = {picks, wins}
    mode_map_raw = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {'picks': 0, 'wins': 0})))
    overall_agg = defaultdict(lambda: {'picks': 0, 'wins': 0})
    total_picks = 0

    for team in data_source.values():
        if not isinstance(team, dict):
            continue
        for mode_name, mode_data in team.get('modes', {}).items():
            if mode_name not in VALID_MODES or not isinstance(mode_data, dict):
                continue
            for map_name, map_data in mode_data.get('maps', {}).items():
                if not isinstance(map_data, dict):
                    continue
                for brawler, bd in map_data.get('brawlers', {}).items():
                    if not isinstance(bd, dict):
                        continue
                    p = int(bd.get('picks', 0))
                    w = int(bd.get('wins', 0))
                    mode_map_raw[mode_name][map_name][brawler]['picks'] += p
                    mode_map_raw[mode_name][map_name][brawler]['wins'] += w
                    overall_agg[brawler]['picks'] += p
                    overall_agg[brawler]['wins'] += w
                    total_picks += p

    def build_meta_scores(agg, total):
        scores = {}
        for b, d in agg.items():
            p, w = int(d['picks']), int(d['wins'])
            if p < 3:
                continue
            wr = float(w / p * 100)
            pr = float(p / total * 100) if total > 0 else 0.0
            scores[str(b)] = {'picks': p, 'wins': w, 'win_rate': round(wr, 2),
                               'pick_rate': round(pr, 4), 'meta_score': round(wr * pr, 4),
                               'meta_score_norm': 50.0}
        if scores:
            mx = max(v['meta_score'] for v in scores.values())
            for v in scores.values():
                v['meta_score_norm'] = round(float(v['meta_score'] / mx * 100), 2) if mx > 0 else 50.0
        return scores

    # Overall meta scores (no filter)
    meta_scores = build_meta_scores(overall_agg, total_picks)

    # Build per-mode meta scores (aggregate all maps in that mode)
    mode_meta = {}
    for mode_name, maps in mode_map_raw.items():
        mode_agg = defaultdict(lambda: {'picks': 0, 'wins': 0})
        mode_total = 0
        for map_data in maps.values():
            for b, d in map_data.items():
                mode_agg[b]['picks'] += d['picks']
                mode_agg[b]['wins'] += d['wins']
                mode_total += d['picks']
        mode_meta[mode_name] = build_meta_scores(mode_agg, mode_total)

    # Build per-map meta scores
    map_meta = {}
    maps_by_mode = {}  # {mode: [map, ...]} for the UI
    for mode_name, maps in mode_map_raw.items():
        maps_by_mode[mode_name] = sorted(maps.keys())
        for map_name, map_agg in maps.items():
            map_total = sum(d['picks'] for d in map_agg.values())
            map_meta[map_name] = build_meta_scores(map_agg, map_total)

    # Build teammate synergy lookup: {brawlerA: {brawlerB: win_rate_together}}
    teammate_lookup = {}
    for brawler, data in _brawler_synergies.items():
        if not isinstance(data, dict):
            continue
        key = str(brawler)
        teammate_lookup[key] = {}
        # Prefer all_teammates (full list), fall back to best_teammates (top 10)
        source_list = data.get('all_teammates') or data.get('best_teammates') or []
        for entry in source_list:
            if not isinstance(entry, dict):
                continue
            name = str(entry.get('brawler') or '')
            if not name or name in teammate_lookup[key]:
                continue
            teammate_lookup[key][name] = {
                'win_rate': float(entry.get('winrate') or 50),
                'picks': int(entry.get('picks') or 0)
            }

    # Build synergy lookup: {brawlerA: {brawlerB: {win_rate, picks}}}
    # Use all_matchups (full list) when available, fall back to best+worst (top-10 each)
    synergy_lookup = {}
    for brawler, data in _brawler_synergies.items():
        if not isinstance(data, dict):
            continue
        key = str(brawler)
        synergy_lookup[key] = {}
        # Prefer all_matchups — contains every opponent with ≥3 games
        source_list = data.get('all_matchups') or []
        if not source_list:
            # fallback: merge best + worst (old behaviour)
            source_list = list(data.get('best_matchups') or []) + list(data.get('worst_matchups') or [])
        for entry in source_list:
            if not isinstance(entry, dict):
                continue
            name = str(entry.get('brawler') or '')
            if not name or name in synergy_lookup[key]:
                continue
            synergy_lookup[key][name] = {
                'win_rate': float(entry.get('winrate') or 50),
                'picks': int(entry.get('picks') or 0)
            }

    # Build compact match list for the "recent match" lookup in analyzer
    # matches_df from get_cached_data() is always None (Rust processor doesn't return raw df),
    # so read the Excel directly and group consecutive rows into series (same logic as h2h route).
    match_records = []
    _matches_file = get_config_for_mode()['MATCHES_FILE']
    try:
        _raw_df = get_cached_matches_df(_matches_file)
        if _raw_df is None and not os.path.exists(_matches_file):
            print(f"⚠️ analyzer: file not found: {_matches_file}")
        if _raw_df is not None and len(_raw_df) > 0:
            _match_aliases = load_team_aliases()
            # Build flat list of individual games first
            _games = []
            for _, row in _raw_df.iterrows():
                t1n = _match_aliases.get(str(row.get('team1_name', '') or '').strip(), str(row.get('team1_name', '') or '').strip())
                t2n = _match_aliases.get(str(row.get('team2_name', '') or '').strip(), str(row.get('team2_name', '') or '').strip())
                t1b, t2b = [], []
                for i in range(1, 4):
                    b1 = str(row.get(f'team1_player{i}_brawler', '') or '').strip()
                    b2 = str(row.get(f'team2_player{i}_brawler', '') or '').strip()
                    if b1 and b1 != 'nan': t1b.append(b1)
                    if b2 and b2 != 'nan': t2b.append(b2)
                if not t1b or not t2b:
                    continue
                winner = _match_aliases.get(str(row.get('winner', '') or '').strip(), str(row.get('winner', '') or '').strip())
                _games.append({
                    't1': t1n, 't2': t2n,
                    't1b': t1b, 't2b': t2b,
                    'team1_won': (winner == t1n),
                    'mode': str(row.get('mode', '') or '').strip(),
                    'map': str(row.get('map', '') or '').strip(),
                    'date': str(row.get('date', '') or '').strip(),
                    'battle_time': str(row.get('battle_time', '') or '').strip(),
                })
            # Group consecutive games into series (series ends when a team reaches 2 wins).
            # All brawler comps are normalised to series-t1 / series-t2 orientation.
            i = 0
            while i < len(_games):
                g0 = _games[i]
                series_t1 = g0['t1']   # canonical team1 for this series
                series_t2 = g0['t2']

                # normalise first game
                g0_t1_won = g0['team1_won']   # g0['t1'] == series_t1 always
                t1_wins = 1 if g0_t1_won else 0
                t2_wins = 0 if g0_t1_won else 1
                all_t1b = [g0['t1b']]
                all_t2b = [g0['t2b']]

                j = i + 1
                while j < len(_games) and t1_wins < 2 and t2_wins < 2:
                    gj = _games[j]
                    fwd = (gj['t1'] == series_t1 and gj['t2'] == series_t2)
                    rev = (gj['t1'] == series_t2 and gj['t2'] == series_t1)
                    if not (fwd or rev):
                        break
                    # normalise brawler sides and win to series orientation
                    if fwd:
                        norm_t1b = gj['t1b']
                        norm_t2b = gj['t2b']
                        series_t1_won = gj['team1_won']
                    else:   # reversed orientation — swap sides
                        norm_t1b = gj['t2b']
                        norm_t2b = gj['t1b']
                        series_t1_won = not gj['team1_won']
                    all_t1b.append(norm_t1b)
                    all_t2b.append(norm_t2b)
                    if series_t1_won:
                        t1_wins += 1
                    else:
                        t2_wins += 1
                    j += 1

                series_winner = series_t1 if t1_wins > t2_wins else series_t2
                match_records.append({
                    't1': series_t1, 't2': series_t2,
                    'all_t1b': all_t1b, 'all_t2b': all_t2b,
                    't1b': all_t1b[0], 't2b': all_t2b[0],
                    'score_t1': t1_wins, 'score_t2': t2_wins,
                    'winner': series_winner,
                    'mode': g0['mode'], 'map': g0['map'],
                    'date': g0['date'], 'battle_time': g0['battle_time'],
                    'num_games': len(all_t1b),
                })
                i = j
        print(f"✅ analyzer match_records: {len(match_records)} series from {_matches_file}")
    except Exception as _e:
        import traceback
        print(f"⚠️ analyzer match_records build failed: {_e}")
        traceback.print_exc()

    # Cache the heavy data server-side so /api/analyzer-data can serve it
    _analyzer_data_cache['synergy']  = synergy_lookup
    _analyzer_data_cache['teammate'] = teammate_lookup
    _analyzer_data_cache['records']  = match_records

    return render_template('analyzer.html',
                           theme=theme,
                           view_mode=view_mode,
                           date_range=date_range,
                           saved_start=saved_start,
                           saved_end=saved_end,
                           all_brawlers=sorted([str(b) for b in all_brawlers if b]),
                           meta_scores=meta_scores,
                           mode_meta=mode_meta,
                           map_meta=map_meta,
                           maps_by_mode=maps_by_mode)


@app.route('/api/analyzer-data')
def analyzer_data_api():
    """Return the heavy analyzer data (synergy, teammate, match_records) as JSON.
    Kept separate so the page HTML is small and loads fast."""
    from flask import jsonify
    return jsonify({
        'synergy':  _analyzer_data_cache.get('synergy', {}),
        'teammate': _analyzer_data_cache.get('teammate', {}),
        'records':  _analyzer_data_cache.get('records', []),
    })


@app.route('/about')
def about_page():
    return render_template('about.html',
                         user=session.get('discord_tag', 'Guest'))


@app.route('/settings', methods=['GET', 'POST'])
def settings():
    if request.method == 'POST':
        user_settings = load_json(USER_SETTINGS_FILE)
        user_id = get_user_id()  # FIXED: Use .get() not []
        
        theme = request.form.get('theme', 'red')
        custom_color = request.form.get('custom_color', '').strip()
        custom_bg    = request.form.get('custom_bg',    '').strip()
        date_range = request.form.get('date_range', '30d')
        start_date = request.form.get('start_date', '').strip()
        end_date = request.form.get('end_date', '').strip()
        view_mode = request.form.get('view_mode', 'season')
        
        # Convert dates to RFC3339 format if they exist
        start_date_rfc3339 = None
        end_date_rfc3339 = None
        
        if date_range == 'custom':
            if start_date:
                try:
                    # Parse date from form (YYYY-MM-DD) and convert to RFC3339
                    from datetime import datetime
                    dt = datetime.strptime(start_date, '%Y-%m-%d')
                    # Add time component (start of day) and UTC timezone
                    start_date_rfc3339 = dt.strftime('%Y-%m-%dT00:00:00Z')
                    print(f"✅ Start date converted: {start_date} -> {start_date_rfc3339}")
                except Exception as e:
                    print(f"❌ Error parsing start_date: {e}")
            
            if end_date:
                try:
                    # Parse date from form (YYYY-MM-DD) and convert to RFC3339
                    from datetime import datetime
                    dt = datetime.strptime(end_date, '%Y-%m-%d')
                    # Add time component (end of day) and UTC timezone
                    end_date_rfc3339 = dt.strftime('%Y-%m-%dT23:59:59Z')
                    print(f"✅ End date converted: {end_date} -> {end_date_rfc3339}")
                except Exception as e:
                    print(f"❌ Error parsing end_date: {e}")
        
        # Validate custom colors (must be valid hex)
        import re
        if not re.match(r'^#[0-9a-fA-F]{6}$', custom_color):
            custom_color = '#ef4444'
        if not re.match(r'^#[0-9a-fA-F]{6}$', custom_bg):
            custom_bg = '#0a0a0a'

        # Save all settings
        user_settings[user_id] = {
            'theme': theme if theme in THEMES else 'red',
            'custom_color': custom_color,
            'custom_bg': custom_bg,
            'date_range': date_range,
            'start_date': start_date_rfc3339,
            'end_date': end_date_rfc3339,
            'view_mode': view_mode
        }
        
        save_json(USER_SETTINGS_FILE, user_settings)

        # IMPORTANT: Clear cache when settings change
        clear_cache()

        session.modified = True

        next_url = request.form.get('next', '/settings')
        return redirect(next_url)
    
    # GET request - load current settings
    current_settings = load_json(USER_SETTINGS_FILE)
    user_id = get_user_id()  # FIXED: Use .get() not []
    user_prefs = current_settings.get(user_id, {})
    
    current_theme = user_prefs.get('theme', 'red')
    current_custom_color = user_prefs.get('custom_color') or ''
    current_custom_bg    = user_prefs.get('custom_bg')    or ''
    current_date_range = user_prefs.get('date_range', '30d')
    
    # Convert RFC3339 back to YYYY-MM-DD for form display
    start_date = ''
    end_date = ''
    
    if user_prefs.get('start_date'):
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(user_prefs['start_date'].replace('Z', '+00:00'))
            start_date = dt.strftime('%Y-%m-%d')
        except:
            pass
    
    if user_prefs.get('end_date'):
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(user_prefs['end_date'].replace('Z', '+00:00'))
            end_date = dt.strftime('%Y-%m-%d')
        except:
            pass
    
    current_view_mode = user_prefs.get('view_mode', 'season')
    
    return render_template('settings.html',
                         user=session.get('discord_tag', 'Guest'),
                         themes=THEMES,
                         current_theme=current_theme,
                         current_custom_color=current_custom_color,
                         current_custom_bg=current_custom_bg,
                         current_date_range=current_date_range,
                         start_date=start_date,
                         end_date=end_date,
                         current_view_mode=current_view_mode)

@app.route('/logout')
def logout():
    session.clear()
    return redirect('/')



@app.route('/meta')
def meta_page():
    # Get filter parameters
    region = request.args.get('region', 'ALL').upper()
    mode = request.args.get('mode', 'ALL')
    
    matches_df, teams_data, players_data, region_stats, mode_stats, all_brawlers = get_cached_data()

    # ADDED: Check if data is None
    if teams_data is None:
        teams_data = {}
    if players_data is None:
        players_data = {}
    
    user_settings = load_json(USER_SETTINGS_FILE)
    user_id = get_user_id()
    user_prefs = user_settings.get(user_id, {})
    view_mode = user_prefs.get('view_mode', 'season')

    # Use the appropriate data source
    if view_mode == 'offseason':
        # In offseason mode, we need to aggregate from players_data
        data_source = players_data
        
        # Build teams_data equivalent from players_data
        teams_data = {}
        for player_tag, player in data_source.items():
            if not isinstance(player, dict):
                continue
            
            player_region = player.get('region', 'NA')
            player_modes = player.get('modes', {})
            
            if not isinstance(player_modes, dict):
                continue
            
            # Create a virtual "team" for this player
            teams_data[player_tag] = {
                'region': player_region,
                'modes': player_modes
            }
    
    
    
    # Collect brawler stats based on filters
    brawler_stats = defaultdict(lambda: {
        'picks': 0,
        'wins': 0
    })
    
    total_picks = 0
    
    for team_name, team in teams_data.items():
        if not isinstance(team, dict):
            continue
            
        team_region = team.get('region', 'NA')
        
        # Filter by region
        if region != 'ALL' and team_region != region:
            continue
        
        team_modes = team.get('modes', {})
        if not isinstance(team_modes, dict):
            continue
        
        for mode_name, mode_data in team_modes.items():
            if mode_name not in VALID_MODES:
                continue
            
            if not isinstance(mode_data, dict):
                continue
            
            # Filter by mode
            if mode != 'ALL' and mode_name != mode:
                continue
            
            mode_maps = mode_data.get('maps', {})
            if not isinstance(mode_maps, dict):
                continue
                
            for map_name, map_data in mode_maps.items():
                if not isinstance(map_data, dict):
                    continue
                    
                map_brawlers = map_data.get('brawlers', {})
                if not isinstance(map_brawlers, dict):
                    continue
                    
                for brawler, brawler_data in map_brawlers.items():
                    if not isinstance(brawler_data, dict):
                        continue
                        
                    brawler_stats[brawler]['picks'] += brawler_data.get('picks', 0)
                    brawler_stats[brawler]['wins'] += brawler_data.get('wins', 0)
                    total_picks += brawler_data.get('picks', 0)
    
    meta_brawlers = []
    for brawler, data in brawler_stats.items():
        if data['picks'] >= 3:
            pick_rate = (data['picks'] / total_picks) * 100 if total_picks > 0 else 0
            win_rate = (data['wins'] / data['picks']) * 100 if data['picks'] > 0 else 0
            meta_score = win_rate * pick_rate
            meta_brawlers.append((brawler, data, meta_score))
    
    meta_brawlers.sort(key=lambda x: x[2], reverse=True)
    
    # Get just brawler and data (without score) for template
    meta_brawlers = [(b, d) for b, d, _ in meta_brawlers]
    
    # Get all modes for filter buttons
    all_modes = set()
    for team in teams_data.values():
        if not isinstance(team, dict):
            continue
        team_modes = team.get('modes', {})
        if isinstance(team_modes, dict):
            for mode_name in team_modes.keys():
                if mode_name in VALID_MODES:
                    all_modes.add(mode_name)
    
    return render_template('meta.html',
                         user=session.get('discord_tag', 'Guest'),
                         meta_brawlers=meta_brawlers,
                         total_picks=total_picks,
                         modes=sorted(all_modes),
                         current_region=region,
                         current_mode=mode)


@app.route('/brawlers')
def brawlers_page():
    """Main brawlers overview page"""
    matches_df, teams_data, players_data, region_stats, mode_stats, all_brawlers = get_cached_data()

    # ADDED: Check if data is None
    if teams_data is None:
        teams_data = {}
    if players_data is None:
        players_data = {}
    
    user_settings = load_json(USER_SETTINGS_FILE)
    user_id = get_user_id()
    user_prefs = user_settings.get(user_id, {})
    view_mode = user_prefs.get('view_mode', 'season')

    # Use the appropriate data source
    if view_mode == 'offseason':
        # In offseason mode, we need to aggregate from players_data
        data_source = players_data
        
        # Build teams_data equivalent from players_data
        teams_data = {}
        for player_tag, player in data_source.items():
            if not isinstance(player, dict):
                continue
            
            player_region = player.get('region', 'NA')
            player_modes = player.get('modes', {})
            
            if not isinstance(player_modes, dict):
                continue
            
            # Create a virtual "team" for this player
            teams_data[player_tag] = {
                'region': player_region,
                'modes': player_modes
            }
    
    
    # Collect comprehensive brawler stats
    brawler_stats = defaultdict(lambda: {
        'picks': 0,
        'wins': 0,
        'modes': defaultdict(lambda: {'picks': 0, 'wins': 0}),
        'maps': defaultdict(lambda: {'picks': 0, 'wins': 0}),
        'teammates': defaultdict(lambda: {'picks': 0, 'wins': 0}),
        'opponents': defaultdict(lambda: {'picks': 0, 'wins': 0})
    })
    
    total_picks = 0
    
    for team_name, team in teams_data.items():
        if not isinstance(team, dict):
            continue
            
        team_modes = team.get('modes', {})
        if not isinstance(team_modes, dict):
            continue
            
        for mode, mode_data in team_modes.items():
            if mode in ['Unknown', 'nan', '', 'None'] or not isinstance(mode_data, dict):
                continue
            
            mode_maps = mode_data.get('maps', {})
            if not isinstance(mode_maps, dict):
                continue
                
            for map_name, map_data in mode_maps.items():
                if not isinstance(map_data, dict):
                    continue
                    
                map_brawlers = map_data.get('brawlers', {})
                if not isinstance(map_brawlers, dict):
                    continue
                    
                for brawler, brawler_data in map_brawlers.items():
                    if not isinstance(brawler_data, dict):
                        continue
                        
                    stats = brawler_stats[brawler]
                    stats['picks'] += brawler_data.get('picks', 0)
                    stats['wins'] += brawler_data.get('wins', 0)
                    stats['modes'][mode]['picks'] += brawler_data.get('picks', 0)
                    stats['modes'][mode]['wins'] += brawler_data.get('wins', 0)
                    stats['maps'][map_name]['picks'] += brawler_data.get('picks', 0)
                    stats['maps'][map_name]['wins'] += brawler_data.get('wins', 0)
                    total_picks += brawler_data.get('picks', 0)
    
    # Sort brawlers by picks
    brawlers_list = []
    for brawler, data in brawler_stats.items():
        if data['picks'] >= 2:
            win_rate = (data['wins'] / data['picks'] * 100) if data['picks'] > 0 else 0
            pick_rate = (data['picks'] / total_picks * 100) if total_picks > 0 else 0
            brawlers_list.append({
                'name': brawler,
                'picks': data['picks'],
                'wins': data['wins'],
                'win_rate': win_rate,
                'pick_rate': pick_rate
            })
    
    brawlers_list.sort(key=lambda x: x['picks'], reverse=True)
    
    return render_template('brawlers.html',
                         user=session.get('discord_tag', 'Guest'),
                         brawlers=brawlers_list,
                         total_picks=total_picks)



@app.route('/api/meta/generate')
def generate_meta_tier_list():
    """Generate tier list image based on filters"""
    try:
        region = request.args.get('region', 'ALL').upper()
        mode = request.args.get('mode', 'ALL')
        
        
        
        matches_df, teams_data, players_data, region_stats, mode_stats, all_brawlers = get_cached_data()

        if teams_data is None:
            teams_data = {}
        if players_data is None:
            players_data = {}

        user_settings = load_json(USER_SETTINGS_FILE)
        user_id = get_user_id()
        user_prefs = user_settings.get(user_id, {})
        view_mode = user_prefs.get('view_mode', 'season')

        if view_mode == 'offseason':
            if not players_data:
                return "No player data available", 404
            data = players_data
        else:
            if not teams_data:
                return "No team data available", 404
            data = teams_data

    except:
        print("!")
        
    # Collect brawler stats based on filters
    brawler_stats = defaultdict(lambda: {'picks': 0, 'wins': 0})
    total_picks = 0
    
    for team_name, team in data.items():

        # Filter by region
        if region != 'ALL' and team['region'] != region:
            continue
        
        # Iterate through modes
        for mode_name, mode_data in team['modes'].items():
            if mode_name not in VALID_MODES:
                continue
            
            # Filter by mode
            if mode != 'ALL' and mode_name != mode:
                continue
            
            for map_name, map_data in mode_data['maps'].items():
                for brawler, brawler_data in map_data['brawlers'].items():
                    brawler_stats[brawler]['picks'] += brawler_data['picks']
                    brawler_stats[brawler]['wins'] += brawler_data['wins']
                    total_picks += brawler_data['picks']
    
    # Calculate meta scores
    meta_scores = []
    for brawler, data in brawler_stats.items():
        if data['picks'] < 3:  # Skip low sample size
            continue
        
        pick_rate = (data['picks'] / total_picks * 100) if total_picks > 0 else 0
        win_rate = (data['wins'] / data['picks'] * 100) if data['picks'] > 0 else 0
        meta_score = (win_rate * pick_rate) / 100
        
        meta_scores.append({
            'name': brawler,
            'score': meta_score,
            'pick_rate': pick_rate,
            'win_rate': win_rate,
            'picks': data['picks']
        })
    
    if not meta_scores:
        return "Not enough data", 404
    
    # Sort by meta score
    meta_scores.sort(key=lambda x: x['score'], reverse=True)
    
    # Use improved tier assignment
    tiers = assign_brawlers_to_tiers_unified(meta_scores)
    if not tiers:
        return "Not enough data", 404

    # Convert to format expected by image generation
    tier_lists = {}
    tier_config = {}
    for tier_name in ['S', 'A', 'B', 'C', 'D', 'F']:
        tier_lists[tier_name] = tiers[tier_name]['brawlers']
        tier_config[tier_name] = {
            'threshold': tiers[tier_name]['threshold'],
            'color': f"#{tiers[tier_name]['color'][0]:02x}{tiers[tier_name]['color'][1]:02x}{tiers[tier_name]['color'][2]:02x}",
            'bg': '#1e2124'  # Use a default background
        }

    
    for tier_name in ['S', 'A', 'B', 'C', 'D', 'F']:
        brawlers = tier_lists[tier_name]
        
        
    
    # Generate image
    img = generate_tier_list_image(tier_lists, region, mode, tier_config)
    
    # Send image
    img_io = io.BytesIO()
    img.save(img_io, 'PNG', optimize=False)
    img_io.seek(0)
    
    return send_file(img_io, mimetype='image/png')


def generate_tier_list_image(tier_lists, region, mode, tier_config):
    """Create the actual tier list image"""
    
    # Image dimensions - Closer spacing
    card_size = 60
    spacing = 8
    tier_box_width = 60
    max_brawlers_per_row = 14
    name_height = 16  # Height for brawler name
    
    # FIXED width based on max brawlers per row
    img_width = tier_box_width + (max_brawlers_per_row * (card_size + spacing)) + 8
    tier_height = card_size + name_height + 12  # Include space for name
    header_height = 160
    padding = 15
    
    # Calculate total rows needed
    active_tiers = [t for t in ['S', 'A', 'B', 'C', 'D', 'F'] if tier_lists[t]]
    total_rows = 0
    for tier in active_tiers:
        brawlers_in_tier = len(tier_lists[tier])
        rows_for_tier = (brawlers_in_tier + max_brawlers_per_row - 1) // max_brawlers_per_row
        total_rows += rows_for_tier
    
    img_height = header_height + (tier_height * total_rows) 
    
    # Create image
    img = Image.new('RGB', (img_width, img_height), color='#0a0a0a')
    draw = ImageDraw.Draw(img)
    
    # Load fonts
    try:
        font_paths = [
            "arial.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/System/Library/Fonts/Helvetica.ttc",
            "C:\\Windows\\Fonts\\arial.ttf"
        ]
        
        title_font = None
        for font_path in font_paths:
            try:
                title_font = ImageFont.truetype(font_path, 55)
                subtitle_font = ImageFont.truetype(font_path, 30)
                tier_font = ImageFont.truetype(font_path, 27)
                name_font = ImageFont.truetype(font_path, 10)  # Small font for names
                break
            except:
                continue
        
        if title_font is None:
            raise Exception("No font found")
    except:
        
        title_font = ImageFont.load_default()
        subtitle_font = ImageFont.load_default()
        tier_font = ImageFont.load_default()
        name_font = ImageFont.load_default()
    
    # Draw header
    region_text = "All Regions" if region == 'ALL' else f"{region} Region"
    mode_text = f" - {mode}" if mode != 'ALL' else ""
    title = f"Meta Tier List"
    subtitle = f"{region_text}{mode_text}"
    
    # Draw header background
    draw.rectangle([0, 0, img_width, header_height], fill='#1e1e2e')
    
    # Title
    bbox = draw.textbbox((0, 0), title, font=title_font)
    text_width = bbox[2] - bbox[0]
    draw.text(((img_width - text_width) // 2, 25), title, font=title_font, fill='#ffffff')
    
    # Subtitle
    bbox = draw.textbbox((0, 0), subtitle, font=subtitle_font)
    text_width = bbox[2] - bbox[0]
    draw.text(((img_width - text_width) // 2, 115), subtitle, font=subtitle_font, fill='#c0c0c0')
    
    # Draw each tier
    y_offset = header_height
    
    for tier_index, tier in enumerate(active_tiers):
        brawlers = tier_lists[tier]
        
        # Get tier config colors
        tier_data = tier_config[tier]
        color = tier_data['color']
        bg_color = tier_data['bg']
        
        # Split brawlers into rows
        brawler_rows = []
        for i in range(0, len(brawlers), max_brawlers_per_row):
            brawler_rows.append(brawlers[i:i + max_brawlers_per_row])
        
        tier_total_height = tier_height * len(brawler_rows)
        
        # Draw tier background
        draw.rectangle(
            [(0, y_offset), (img_width, y_offset + tier_total_height)],
            fill='#282838'
        )
        
        # Draw tier label
        draw.rectangle(
            [(0, y_offset), (tier_box_width, y_offset + tier_total_height)],
            fill=color
        )
        
        bbox = draw.textbbox((0, 0), tier, font=tier_font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        draw.text(
            ((tier_box_width - text_width) // 2, y_offset + (tier_total_height - text_height) // 2),
            tier,
            fill=(0, 0, 0),
            font=tier_font
        )
        
        # Draw brawlers row by row
        current_row_y = y_offset
        for brawler_row in brawler_rows:
            x_offset = tier_box_width + spacing
            
            for brawler_data in brawler_row:
                brawler_name = brawler_data['name']
                
                # Try to load brawler image
                brawler_img_path = f"static/images/brawlers/{brawler_name.lower().replace(' ', '_').replace('-', '_')}.png"
                
                try:
                    if os.path.exists(brawler_img_path):
                        brawler_img = Image.open(brawler_img_path).convert('RGBA')
                        brawler_img.thumbnail((card_size - 10, card_size - 10), Image.Resampling.LANCZOS)
                        # Paste centered
                        paste_x = x_offset + (card_size - brawler_img.width) // 2
                        paste_y = current_row_y + 6 + (card_size - brawler_img.height) // 2
                        img.paste(brawler_img, (paste_x, paste_y), brawler_img)
                except Exception as e:
                    print(f"Error loading brawler image {brawler_name}: {e}")
                    # Draw placeholder
                    draw.rectangle(
                        [(x_offset + 5, current_row_y + 11), 
                         (x_offset + card_size - 5, current_row_y + 6 + card_size - 5)],
                        fill=(80, 80, 80)
                    )
                
                # Draw brawler name under image
                display_name = brawler_name if len(brawler_name) <= 9 else brawler_name[:7] + ".."
                name_bbox = draw.textbbox((0, 0), display_name, font=name_font)
                name_width = name_bbox[2] - name_bbox[0]
                draw.text(
                    (x_offset + (card_size - name_width) // 2, current_row_y + card_size + 4),
                    display_name,
                    fill=(200, 200, 200),
                    font=name_font
                )
                
                x_offset += card_size + spacing
            
            current_row_y += tier_height
        
        y_offset = current_row_y
        
        # **NEW: Draw separator line between tiers (except after last tier)**
        if tier_index < len(active_tiers) - 1:
            # Draw a horizontal line at y_offset
            draw.line(
                [(0, y_offset), (img_width, y_offset)],
                fill='#0a0a0a',  # Match background color for a dividing line
                width=3
            )
    
    return img


# Replace the brawler_detail_page function in website.py

@app.route('/brawler/<brawler_name>')
def brawler_detail_page(brawler_name):
    """Detailed brawler statistics page"""
    try:
        matches_df, teams_data, players_data, region_stats, mode_stats, all_brawlers = get_cached_data()

        if teams_data is None:
            teams_data = {}
        if players_data is None:
            players_data = {}

        user_settings = load_json(USER_SETTINGS_FILE)
        user_id = get_user_id()
        user_prefs = user_settings.get(user_id, {})
        view_mode = user_prefs.get('view_mode', 'season')

        # Use the appropriate data
        if view_mode == 'offseason':
            data = players_data
        else:
            data = teams_data
        
        
        # Initialize brawler stats
        brawler_stats = {
            'picks': 0,
            'wins': 0,
            'modes': defaultdict(lambda: {
                'picks': 0,
                'wins': 0,
                'maps': defaultdict(lambda: {'picks': 0, 'wins': 0})
            })
        }
        
        if view_mode == 'offseason':
            # OFF-SEASON MODE: Use players_data
            players_data = data
            
            for player_tag, player in players_data.items():
                if not isinstance(player, dict):
                    continue
                
                player_brawlers = player.get('brawlers', {})
                if not isinstance(player_brawlers, dict) or brawler_name not in player_brawlers:
                    continue
                
                player_brawler_data = player_brawlers.get(brawler_name, {})
                if isinstance(player_brawler_data, dict):
                    brawler_stats['picks'] += player_brawler_data.get('picks', 0)
                    brawler_stats['wins'] += player_brawler_data.get('wins', 0)
                
                player_modes = player.get('modes', {})
                if isinstance(player_modes, dict):
                    for mode, mode_data in player_modes.items():
                        if mode not in VALID_MODES or not isinstance(mode_data, dict):
                            continue
                        
                        mode_maps = mode_data.get('maps', {})
                        if isinstance(mode_maps, dict):
                            for map_name, map_data in mode_maps.items():
                                if not isinstance(map_data, dict):
                                    continue
                                
                                map_brawlers = map_data.get('brawlers', {})
                                if isinstance(map_brawlers, dict) and brawler_name in map_brawlers:
                                    map_brawler_data = map_brawlers[brawler_name]
                                    if isinstance(map_brawler_data, dict):
                                        brawler_stats['modes'][mode]['picks'] += map_brawler_data.get('picks', 0)
                                        brawler_stats['modes'][mode]['wins'] += map_brawler_data.get('wins', 0)
                                        brawler_stats['modes'][mode]['maps'][map_name]['picks'] += map_brawler_data.get('picks', 0)
                                        brawler_stats['modes'][mode]['maps'][map_name]['wins'] += map_brawler_data.get('wins', 0)
        else:
            # SEASON MODE: Use teams_data
            teams_data = data
            
            for team_name, team in teams_data.items():
                if not isinstance(team, dict):
                    continue
                
                team_brawlers = team.get('brawlers', {})
                if not isinstance(team_brawlers, dict) or brawler_name not in team_brawlers:
                    continue
                
                team_brawler_data = team_brawlers.get(brawler_name, {})
                if isinstance(team_brawler_data, dict):
                    brawler_stats['picks'] += team_brawler_data.get('picks', 0)
                    brawler_stats['wins'] += team_brawler_data.get('wins', 0)
                
                team_modes = team.get('modes', {})
                if isinstance(team_modes, dict):
                    for mode, mode_data in team_modes.items():
                        if mode not in VALID_MODES or not isinstance(mode_data, dict):
                            continue
                        
                        mode_maps = mode_data.get('maps', {})
                        if isinstance(mode_maps, dict):
                            for map_name, map_data in mode_maps.items():
                                if not isinstance(map_data, dict):
                                    continue
                                
                                map_brawlers = map_data.get('brawlers', {})
                                if isinstance(map_brawlers, dict) and brawler_name in map_brawlers:
                                    map_brawler_data = map_brawlers[brawler_name]
                                    if isinstance(map_brawler_data, dict):
                                        brawler_stats['modes'][mode]['picks'] += map_brawler_data.get('picks', 0)
                                        brawler_stats['modes'][mode]['wins'] += map_brawler_data.get('wins', 0)
                                        brawler_stats['modes'][mode]['maps'][map_name]['picks'] += map_brawler_data.get('picks', 0)
                                        brawler_stats['modes'][mode]['maps'][map_name]['wins'] += map_brawler_data.get('wins', 0)
        
        if brawler_stats['picks'] == 0:
            return f"Brawler {brawler_name} not found or no data available", 404
        
        # Calculate stats
        overall_winrate = (brawler_stats['wins'] / brawler_stats['picks'] * 100) if brawler_stats['picks'] > 0 else 0
        
        best_modes = []
        for mode, mode_data in brawler_stats['modes'].items():
            if mode not in VALID_MODES:
                continue
            if mode_data['picks'] >= 1:
                wr = (mode_data['wins'] / mode_data['picks'] * 100) if mode_data['picks'] > 0 else 0
                best_modes.append({'name': mode, 'picks': mode_data['picks'], 'wins': mode_data['wins'], 'win_rate': wr})
        best_modes.sort(key=lambda x: x['win_rate'], reverse=True)
        
        best_maps = []
        for mode, mode_data in brawler_stats['modes'].items():
            for map_name, map_stats in mode_data['maps'].items():
                if map_stats['picks'] >= 1:
                    wr = (map_stats['wins'] / map_stats['picks'] * 100) if map_stats['picks'] > 0 else 0
                    best_maps.append({
                        'name': map_name,
                        'mode': mode,
                        'picks': map_stats['picks'],
                        'wins': map_stats['wins'],
                        'win_rate': wr
                    })
        best_maps.sort(key=lambda x: x['win_rate'], reverse=True)
        
        # Use the Rust-calculated synergies from global cache
        # This works for BOTH season and offseason modes
        best_teammates, best_matchups, worst_matchups = get_brawler_synergies(brawler_name)

        print(f"🔍 DEBUG for {brawler_name}:")
        print(f"  View mode: {view_mode}")
        print(f"  Best teammates: {best_teammates}")
        print(f"  Best matchups: {best_matchups}")
        print(f"  Worst matchups: {worst_matchups}")
        
        return render_template('brawler_detail.html',
                             user=session.get('discord_tag', 'Guest'),
                             brawler_name=brawler_name,
                             stats=brawler_stats,
                             overall_winrate=overall_winrate,
                             best_modes=best_modes[:10],
                             best_maps=best_maps[:10],
                             best_teammates=best_teammates[:10],
                             best_matchups=best_matchups,
                             worst_matchups=worst_matchups)
    
    except Exception as e:
        print(f"❌ BRAWLER DETAIL ERROR: {e}")
        import traceback
        traceback.print_exc()
        return f"Error loading brawler data: {e}", 500


@app.route('/modes/<mode_name>')
def mode_detail_page(mode_name):
    """Detailed mode statistics page"""
    try:
        # Convert URL format back to display format
        mode_display = mode_name.replace('_', ' ').title()
        
        
        
        matches_df, teams_data, players_data, region_stats, mode_stats, all_brawlers = get_cached_data()

        if teams_data is None:
            teams_data = {}
        if players_data is None:
            players_data = {}

        user_settings = load_json(USER_SETTINGS_FILE)
        user_id = get_user_id()
        user_prefs = user_settings.get(user_id, {})
        view_mode = user_prefs.get('view_mode', 'season')

        # Use the appropriate data
        if view_mode == 'offseason':
            data = players_data
        else:
            data = teams_data



        # Get user's view mode
        user_settings = load_json(USER_SETTINGS_FILE)
        user_id = get_user_id()
        user_prefs = user_settings.get(user_id, {})
        view_mode = user_prefs.get('view_mode', 'season')



        # Collect stats for this mode
        mode_stats_data = {
            'total_games': 0,
            'brawlers': defaultdict(lambda: {'picks': 0, 'wins': 0}),
            'maps': defaultdict(lambda: {'games': 0})
        }
        
        if view_mode == 'offseason':
            # OFF-SEASON: Use players_data
            for tag, player in data.items():
                if not isinstance(player, dict):
                    continue
                
                player_modes = player.get('modes', {})
                if not isinstance(player_modes, dict):
                    continue
                
                for mode, mode_data in player_modes.items():
                    if mode.lower().replace(' ', '_') != mode_name.lower() or not isinstance(mode_data, dict):
                        continue
                    
                    mode_display = mode  # Use actual mode name from data
                    
                    # Count games
                    mode_stats_data['total_games'] += mode_data.get('matches', 0)
                    
                    # Track maps
                    mode_maps = mode_data.get('maps', {})
                    if isinstance(mode_maps, dict):
                        for map_name, map_data in mode_maps.items():
                            if isinstance(map_data, dict):
                                mode_stats_data['maps'][map_name]['games'] += map_data.get('matches', 0)
                                
                                # Collect brawler stats
                                map_brawlers = map_data.get('brawlers', {})
                                if isinstance(map_brawlers, dict):
                                    for brawler, brawler_data in map_brawlers.items():
                                        if isinstance(brawler_data, dict):
                                            mode_stats_data['brawlers'][brawler]['picks'] += brawler_data.get('picks', 0)
                                            mode_stats_data['brawlers'][brawler]['wins'] += brawler_data.get('wins', 0)
        else:
            # SEASON: Use teams_data
            for team_name, team in data.items():
                if not isinstance(team, dict):
                    continue
                
                team_modes = team.get('modes', {})
                if not isinstance(team_modes, dict):
                    continue
                
                for mode, mode_data in team_modes.items():
                    if mode.lower().replace(' ', '_') != mode_name.lower() or not isinstance(mode_data, dict):
                        continue
                    
                    mode_display = mode
                    
                    # Count games
                    mode_stats_data['total_games'] += mode_data.get('matches', 0)
                    
                    # Track maps
                    mode_maps = mode_data.get('maps', {})
                    if isinstance(mode_maps, dict):
                        for map_name, map_data in mode_maps.items():
                            if isinstance(map_data, dict):
                                mode_stats_data['maps'][map_name]['games'] += map_data.get('matches', 0)
                                
                                # Collect brawler stats
                                map_brawlers = map_data.get('brawlers', {})
                                if isinstance(map_brawlers, dict):
                                    for brawler, brawler_data in map_brawlers.items():
                                        if isinstance(brawler_data, dict):
                                            mode_stats_data['brawlers'][brawler]['picks'] += brawler_data.get('picks', 0)
                                            mode_stats_data['brawlers'][brawler]['wins'] += brawler_data.get('wins', 0)
        
        if mode_stats_data['total_games'] == 0:
            return f"Mode {mode_display} not found or no data available", 404
        
        
        # Calculate best brawlers
        best_brawlers = []
        for brawler, brawler_data in mode_stats_data['brawlers'].items():
            if brawler_data['picks'] >= 1:
                win_rate = (brawler_data['wins'] / brawler_data['picks'] * 100) if brawler_data['picks'] > 0 else 0
                best_brawlers.append({
                    'name': brawler,
                    'picks': brawler_data['picks'],
                    'wins': brawler_data['wins'],
                    'win_rate': win_rate
                })
        
        best_brawlers.sort(key=lambda x: x['win_rate'], reverse=True)
        
        # Get maps list
        maps_list = []
        for map_name, map_data in mode_stats_data['maps'].items():
            maps_list.append({
                'name': map_name,
                'picks': map_data['games']
            })
        
        maps_list.sort(key=lambda x: x['picks'], reverse=True)
        
        # Calculate total picks for meta score
        total_picks = sum(b['picks'] for b in best_brawlers)

        # Calculate best teams for this mode
        top_teams_mode = []
        if view_mode != 'offseason':
            for team_name, team in data.items():
                if not isinstance(team, dict):
                    continue
                team_modes = team.get('modes', {})
                if not isinstance(team_modes, dict):
                    continue
                for mode, mode_data in team_modes.items():
                    if mode.lower().replace(' ', '_') != mode_name.lower() or not isinstance(mode_data, dict):
                        continue
                    t_matches = mode_data.get('matches', 0)
                    t_wins = mode_data.get('wins', 0)
                    if t_matches >= 3:
                        t_wr = (t_wins / t_matches * 100) if t_matches > 0 else 0
                        top_teams_mode.append({
                            'name': team_name,
                            'matches': t_matches,
                            'wins': t_wins,
                            'losses': t_matches - t_wins,
                            'win_rate': t_wr
                        })
            top_teams_mode.sort(key=lambda x: x['win_rate'], reverse=True)
            top_teams_mode = top_teams_mode[:1]

        return render_template('mode_detail.html',
                            user=session.get('discord_tag', 'Guest'),
                            mode_name=mode_display,
                            total_games=mode_stats_data['total_games'],
                            total_maps=len(mode_stats_data['maps']),
                            total_brawlers=len(mode_stats_data['brawlers']),
                            best_brawlers=best_brawlers,
                            maps=maps_list,
                            total_picks=total_picks,
                            top_teams=top_teams_mode)
    
    except Exception as e:
        print(f"❌ MODE DETAIL ERROR: {e}")
        import traceback
        traceback.print_exc()
        return f"Error loading mode data: {e}", 500

@app.route('/player/<path:player_tag>')
def player_page(player_tag):
    """Display individual player statistics - adapts to view mode"""
    # Decode the URL-encoded tag
    player_tag = unquote(player_tag)
    
    # Normalize tag
    if not player_tag.startswith('#'):
        player_tag = '#' + player_tag
    player_tag = player_tag.upper().replace('0', 'O')
    
    matches_df, teams_data, players_data, region_stats, mode_stats, all_brawlers = get_cached_data()

    if teams_data is None:
        teams_data = {}
    if players_data is None:
        players_data = {}

    user_settings = load_json(USER_SETTINGS_FILE)
    user_id = get_user_id()
    user_prefs = user_settings.get(user_id, {})
    view_mode = user_prefs.get('view_mode', 'season')

    # Use the appropriate data
    if view_mode == 'offseason':
        data = players_data
    else:
        data = teams_data

    # Get user's view mode
    user_settings = load_json(USER_SETTINGS_FILE)
    user_id = get_user_id()
    user_prefs = user_settings.get(user_id, {})
    view_mode = user_prefs.get('view_mode', 'season')

    if view_mode == 'offseason':
        # OFF-SEASON MODE: Use players_data directly
        players_data = data

        if player_tag not in players_data:
            return f"Player not found: {player_tag}", 404
        
        player_data = players_data[player_tag]
        
        # Find favorite (most played) brawler
        favorite_brawler = None
        if player_data['brawlers']:
            favorite_brawler_name = max(
                player_data['brawlers'].items(),
                key=lambda x: x[1]['picks']
            )
            favorite_brawler = {
                'name': favorite_brawler_name[0],
                'picks': favorite_brawler_name[1]['picks'],
                'wins': favorite_brawler_name[1]['wins']
            }
        else:
            favorite_brawler = {
                'name': 'None',
                'picks': 0,
                'wins': 0
            }
        
        # NEW: Enrich teammates_seen with favorite brawler info
        teammates_with_brawlers = {}
        for teammate_tag, teammate_stats in player_data['teammates_seen'].items():
            teammate_info = {
                'name': teammate_stats['name'],
                'matches': teammate_stats['matches'],
                'wins': teammate_stats['wins']
            }
            
            # Get teammate's favorite brawler
            if teammate_tag in players_data:
                teammate_player = players_data[teammate_tag]
                if teammate_player.get('brawlers'):
                    fav_brawler = max(
                        teammate_player['brawlers'].items(),
                        key=lambda x: x[1]['picks']
                    )
                    teammate_info['favorite_brawler'] = fav_brawler[0]
                else:
                    teammate_info['favorite_brawler'] = 'default'
            else:
                teammate_info['favorite_brawler'] = 'default'
            
            teammates_with_brawlers[teammate_tag] = teammate_info
        
        # Build player object for template
        player = {
            'name': player_data['name'],
            'tag': player_tag,
            'region': player_data['region'],
            'matches': player_data['matches'],
            'wins': player_data['wins'],
            'losses': player_data['losses'],
            'star_player': player_data['star_player'],
            'favorite_brawler': favorite_brawler,
            'brawlers': player_data['brawlers'],
            'modes': player_data['modes'],
            'teammates_seen': teammates_with_brawlers,  # UPDATED
            'opponents_seen': player_data['opponents_seen']
        }
        
        return render_template('player_offseason.html',
                             user=session.get('discord_tag', 'Guest'),
                             player=player)
    
    else:
        # SEASON MODE: Search through teams (existing logic)
        teams_data = data
        
        player_data = None
        team_name = None
        
        for t_name, team in teams_data.items():
            if player_tag in team['players']:
                player_data = team['players'][player_tag]
                team_name = t_name
                break
        
        if not player_data:
            return f"Player not found: {player_tag}", 404
        
        # Find favorite (most played) brawler
        favorite_brawler = None
        if player_data['brawlers']:
            favorite_brawler_name = max(
                player_data['brawlers'].items(),
                key=lambda x: x[1]['picks']
            )
            favorite_brawler = {
                'name': favorite_brawler_name[0],
                'picks': favorite_brawler_name[1]['picks'],
                'wins': favorite_brawler_name[1]['wins']
            }
        else:
            favorite_brawler = {
                'name': 'None',
                'picks': 0,
                'wins': 0
            }
        
        # Build modes stats from player data or fall back to team modes
        player_modes = player_data.get('modes', {})
        modes_stats = {}
        total_brawler_picks = sum(b.get('picks', 0) for b in player_data['brawlers'].values())

        # If player has no modes data, use team-level modes
        if not player_modes and team_name and team_name in teams_data:
            player_modes = teams_data[team_name].get('modes', {})

        if isinstance(player_modes, dict):
            for mode_name, mode_data in player_modes.items():
                if not isinstance(mode_data, dict):
                    continue
                mode_maps = mode_data.get('maps', {})
                if not isinstance(mode_maps, dict):
                    continue
                mode_picks = 0
                mode_wins = 0
                for map_name, map_data in mode_maps.items():
                    if not isinstance(map_data, dict):
                        continue
                    for brawler, brawler_data in map_data.get('brawlers', {}).items():
                        mode_picks += brawler_data.get('picks', 0)
                        mode_wins += brawler_data.get('wins', 0)
                if mode_picks > 0:
                    modes_stats[mode_name] = {
                        'picks': mode_picks,
                        'wins': mode_wins,
                        'maps': len(mode_maps)
                    }

        # Compute additional stats
        losses = player_data['matches'] - player_data['wins']
        unique_brawlers = len([b for b, d in player_data['brawlers'].items() if d.get('picks', 0) > 0])
        avg_picks_per_brawler = round(total_brawler_picks / unique_brawlers, 1) if unique_brawlers > 0 else 0

        # Best brawler by win rate (min 2 picks)
        best_wr_brawler = None
        best_wr = 0
        for b_name, b_data in player_data['brawlers'].items():
            if b_data.get('picks', 0) >= 2:
                wr = (b_data['wins'] / b_data['picks'] * 100) if b_data['picks'] > 0 else 0
                if wr > best_wr:
                    best_wr = wr
                    best_wr_brawler = {'name': b_name, 'picks': b_data['picks'], 'wins': b_data['wins'], 'win_rate': wr}

        # Worst brawler by win rate (min 2 picks)
        worst_wr_brawler = None
        worst_wr = 100
        for b_name, b_data in player_data['brawlers'].items():
            if b_data.get('picks', 0) >= 2:
                wr = (b_data['wins'] / b_data['picks'] * 100) if b_data['picks'] > 0 else 0
                if wr < worst_wr:
                    worst_wr = wr
                    worst_wr_brawler = {'name': b_name, 'picks': b_data['picks'], 'wins': b_data['wins'], 'win_rate': wr}

        # Star player rate
        star_player_rate = round(player_data['star_player'] / player_data['matches'] * 100, 1) if player_data['matches'] > 0 else 0

        # Best mode (highest WR)
        best_mode = None
        best_mode_wr = 0
        for m_name, m_data in modes_stats.items():
            if m_data['picks'] >= 1:
                m_wr = (m_data['wins'] / m_data['picks'] * 100)
                if m_wr > best_mode_wr:
                    best_mode_wr = m_wr
                    best_mode = {'name': m_name, 'picks': m_data['picks'], 'wins': m_data['wins'], 'win_rate': round(m_wr, 1)}

        # Worst mode (lowest WR)
        worst_mode = None
        worst_mode_wr = 100
        for m_name, m_data in modes_stats.items():
            if m_data['picks'] >= 1:
                m_wr = (m_data['wins'] / m_data['picks'] * 100)
                if m_wr < worst_mode_wr:
                    worst_mode_wr = m_wr
                    worst_mode = {'name': m_name, 'picks': m_data['picks'], 'wins': m_data['wins'], 'win_rate': round(m_wr, 1)}

        # Collect all map stats
        all_maps = []
        for mode_name, mode_data in player_modes.items():
            if not isinstance(mode_data, dict):
                continue
            mode_maps = mode_data.get('maps', {})
            if not isinstance(mode_maps, dict):
                continue
            for map_name, map_data in mode_maps.items():
                if not isinstance(map_data, dict):
                    continue
                map_picks = sum(bd.get('picks', 0) for bd in map_data.get('brawlers', {}).values())
                map_wins = sum(bd.get('wins', 0) for bd in map_data.get('brawlers', {}).values())
                if map_picks > 0:
                    all_maps.append({
                        'name': map_name,
                        'mode': mode_name,
                        'picks': map_picks,
                        'wins': map_wins,
                        'win_rate': round(map_wins / map_picks * 100, 1)
                    })

        # Most played map
        most_played_map = max(all_maps, key=lambda m: m['picks']) if all_maps else None

        # Best map by WR
        maps_with_min = [m for m in all_maps if m['picks'] >= 1]
        best_map = max(maps_with_min, key=lambda m: m['win_rate']) if maps_with_min else None
        worst_map = min(maps_with_min, key=lambda m: m['win_rate']) if maps_with_min else None

        # Total modes and maps played
        total_modes = len(modes_stats)
        total_maps = sum(m['maps'] for m in modes_stats.values())

        # Pick concentration (what % of picks go to top 3 brawlers)
        sorted_brawler_picks = sorted(
            [d.get('picks', 0) for d in player_data['brawlers'].values()],
            reverse=True
        )
        top3_picks = sum(sorted_brawler_picks[:3])
        pick_concentration = round(top3_picks / total_brawler_picks * 100, 1) if total_brawler_picks > 0 else 0

        # Build liquipedia URL - use name after pipe symbol if present
        raw_name = player_data['name']
        if '|' in raw_name:
            liquipedia_name = raw_name.split('|', 1)[1].strip()
        else:
            liquipedia_name = raw_name
        liquipedia_name = liquipedia_name.replace(' ', '_')
        liquipedia_url = f"https://liquipedia.net/brawlstars/{liquipedia_name}"

        # Build player object for template
        player = {
            'name': player_data['name'],
            'tag': player_tag,
            'team_name': team_name,
            'region': teams_data[team_name]['region'],
            'matches': player_data['matches'],
            'wins': player_data['wins'],
            'losses': losses,
            'star_player': player_data['star_player'],
            'favorite_brawler': favorite_brawler,
            'brawlers': player_data['brawlers'],
            'modes': modes_stats,
            'unique_brawlers': unique_brawlers,
            'avg_picks_per_brawler': avg_picks_per_brawler,
            'best_wr_brawler': best_wr_brawler,
            'worst_wr_brawler': worst_wr_brawler,
            'liquipedia_url': liquipedia_url,
            'star_player_rate': star_player_rate,
            'best_mode': best_mode,
            'worst_mode': worst_mode,
            'most_played_map': most_played_map,
            'best_map': best_map,
            'worst_map': worst_map,
            'all_maps': sorted(all_maps, key=lambda m: m['picks'], reverse=True),
            'total_modes': total_modes,
            'total_maps': total_maps,
            'pick_concentration': pick_concentration
        }

        return render_template('player.html',
                             user=session.get('discord_tag', 'Guest'),
                             player=player)



_matches_df_cache = {
    'df': None,
    'file': None,
    'mtime': None
}

def get_cached_matches_df(matches_file):
    """Return a cached pandas DataFrame for the given matches file, reloading only when the file changes."""
    try:
        mtime = os.path.getmtime(matches_file)
    except OSError:
        return None
    c = _matches_df_cache
    if c['df'] is not None and c['file'] == matches_file and c['mtime'] == mtime:
        return c['df']
    try:
        df = pd.read_excel(matches_file)
        c['df'] = df
        c['file'] = matches_file
        c['mtime'] = mtime
        print(f"✅ Loaded {matches_file} into matches cache ({len(df)} rows)")
        return df
    except Exception as e:
        print(f"❌ Failed to load {matches_file}: {e}")
        return None

# Add this at the top with your other cache variables
_trios_cache = {
    'data': None,
    'timestamp': None,
    'cache_key': None
}

TRIOS_CACHE_DURATION = 300  # 5 minutes

def get_cached_trios():
    """Get cached trio data or recalculate"""
    current_cache_key = get_cache_key()
    current_time = time.time()
    
    # Check if cache is valid
    if (_trios_cache['data'] is not None and 
        _trios_cache['timestamp'] is not None and
        _trios_cache['cache_key'] == current_cache_key and
        current_time - _trios_cache['timestamp'] < TRIOS_CACHE_DURATION):
        
        return _trios_cache['data']
    
    # Cache is invalid, recalculate
    print("⚙ Calculating trios...")
    start_time = time.time()
    
    matches_df, teams_data, players_data, region_stats, mode_stats, all_brawlers = get_cached_data()

    if teams_data is None:
        teams_data = {}
    if players_data is None:
        players_data = {}

    user_settings = load_json(USER_SETTINGS_FILE)
    user_id = get_user_id()
    user_prefs = user_settings.get(user_id, {})
    view_mode = user_prefs.get('view_mode', 'season')

    # Use the appropriate data
    if view_mode == 'offseason':
        data = players_data
    else:
        data = teams_data

    # Load matches file
    matches_file = 'matches_off.xlsx'
    if not os.path.exists(matches_file):
        return None
    
    try:
        matches_df = pd.read_excel(matches_file)
    except Exception as e:
        print(f"Error loading matches: {e}")
        return None
    
    # Calculate trios
    trios_dict = {}
    
    def normalize_tag(tag):
        if not tag or tag == 'NAN':
            return None
        tag = str(tag).strip().upper().replace('0', 'O')
        if not tag.startswith('#'):
            tag = '#' + tag
        return tag
    
    for _, match in matches_df.iterrows():
        mode = str(match.get('mode', 'Unknown'))
        
        if mode not in VALID_MODES:
            continue
        
        for team_prefix in ['team1', 'team2']:
            team_tags = []
            for i in range(1, 4):
                tag = normalize_tag(match.get(f'{team_prefix}_player{i}_tag', ''))
                if tag and tag in players_data:
                    team_tags.append(tag)
            
            if len(team_tags) == 3:
                trio_key = tuple(sorted(team_tags))
                
                if trio_key not in trios_dict:
                    trios_dict[trio_key] = {'games': 0, 'wins': 0}
                
                trios_dict[trio_key]['games'] += 1
                
                winner_name = str(match.get('winner', '')).strip()
                if winner_name == team_prefix:
                    trios_dict[trio_key]['wins'] += 1
    
    # Update cache
    _trios_cache['data'] = (trios_dict, players_data)
    _trios_cache['timestamp'] = current_time
    _trios_cache['cache_key'] = current_cache_key
    
    elapsed = time.time() - start_time
    
    return _trios_cache['data']


@app.route('/possible-teams')
def possible_teams_page():
    """Show possible team combinations based on teammate frequency"""
    sort_by = request.args.get('sort', 'games')
    region = request.args.get('region', 'ALL').upper()
    
    # Get user's view mode
    user_settings = load_json(USER_SETTINGS_FILE)
    user_id = get_user_id()
    user_prefs = user_settings.get(user_id, {})
    view_mode = user_prefs.get('view_mode', 'season')
    
    if view_mode != 'offseason':
        return "This feature is only available in off-season mode", 404
    
    # Get cached trios
    cached_result = get_cached_trios()
    
    trios_dict, players_data = cached_result
    
    # Build trios list with calculated stats
    all_trios = []
    
    for trio_key, trio_stats in trios_dict.items():
        tag1, tag2, tag3 = trio_key
        
        # Skip if any player not in our region filter
        if region != 'ALL':
            if not all(players_data.get(tag, {}).get('region') == region for tag in trio_key):
                continue
        
        trio_games = trio_stats['games']
        trio_wins = trio_stats['wins']
        
        # Minimum 3 games together
        if trio_games >= 3:
            player1 = players_data.get(tag1)
            player2 = players_data.get(tag2)
            player3 = players_data.get(tag3)
            
            if not all([player1, player2, player3]):
                continue
            
            trio_wr = (trio_wins / trio_games * 100) if trio_games > 0 else 0
            player_region = player1['region']
            
            all_trios.append({
                'players': [player1['name'], player2['name'], player3['name']],
                'tags': [tag1, tag2, tag3],
                'region': player_region,
                'games_together': trio_games,
                'trio_winrate': trio_wr,
                'trio_wins': trio_wins,
            })
    
    # Sort trios
    if sort_by == 'games':
        all_trios.sort(key=lambda x: x['games_together'], reverse=True)
    else:  # winrate
        all_trios = [t for t in all_trios if t['games_together'] >= 3]
        all_trios.sort(key=lambda x: x['trio_winrate'], reverse=True)
    
    # Filter to ensure each player appears only once - LIMIT TO TOP 10
    used_players = set()
    unique_trios = []
    
    for trio in all_trios:
        if not any(tag in used_players for tag in trio['tags']):
            unique_trios.append(trio)
            used_players.update(trio['tags'])
            
            if len(unique_trios) >= 10:
                break
    
    return render_template('possible_teams.html',
                         user=session.get('discord_tag', 'Guest'),
                         trios=unique_trios,
                         sort_by=sort_by,
                         current_region=region,
                         zip=zip)



@app.route('/players')
def players_page():
    """Players overview page - only available in offseason mode"""
    matches_df, teams_data, players_data, region_stats, mode_stats, all_brawlers = get_cached_data()

    if teams_data is None:
        teams_data = {}
    if players_data is None:
        players_data = {}

    user_settings = load_json(USER_SETTINGS_FILE)
    user_id = get_user_id()
    user_prefs = user_settings.get(user_id, {})
    view_mode = user_prefs.get('view_mode', 'season')

    # Use the appropriate data
    if view_mode == 'offseason':
        data = players_data
    else:
        data = teams_data

    # Get user's view mode
    user_settings = load_json(USER_SETTINGS_FILE)
    user_id = get_user_id()
    user_prefs = user_settings.get(user_id, {})
    view_mode = user_prefs.get('view_mode', 'season')
    
    if view_mode != 'offseason':
        return "This feature is only available in off-season mode", 404
    
    players_data = data  # In offseason mode, data is players_data
    
    # Build players list with their stats
    players_list = []
    
    for player_tag, player_info in players_data.items():
        # Find most played brawler
        most_played_brawler = 'Shelly'  # Default
        if player_info.get('brawlers'):
            most_played = max(
                player_info['brawlers'].items(),
                key=lambda x: x[1]['picks']
            )
            most_played_brawler = most_played[0]
        
        # Calculate win rate
        win_rate = (player_info['wins'] / player_info['matches'] * 100) if player_info['matches'] > 0 else 0
        
        players_list.append({
            'name': player_info['name'],
            'tag': player_tag,
            'region': player_info['region'],
            'games': player_info['matches'],
            'wins': player_info['wins'],
            'win_rate': win_rate,
            'most_played_brawler': most_played_brawler
        })
    
    # Sort by total games (most active players first)
    players_list.sort(key=lambda x: x['games'], reverse=True)
    
    return render_template('players.html',
                         user=session.get('discord_tag', 'Guest'),
                         players=players_list)


@app.route('/maps/<map_name>')
def map_detail_page(map_name):
    """Detailed map statistics page"""
    try:
        # Convert URL format back to display format
        map_display = map_name.replace('_', ' ').title()
        
        
        matches_df, teams_data, players_data, region_stats, mode_stats, all_brawlers = get_cached_data()

        if teams_data is None:
            teams_data = {}
        if players_data is None:
            players_data = {}

        user_settings = load_json(USER_SETTINGS_FILE)
        user_id = get_user_id()
        user_prefs = user_settings.get(user_id, {})
        view_mode = user_prefs.get('view_mode', 'season')

        # Use the appropriate data
        if view_mode == 'offseason':
            data = players_data
        else:
            data = teams_data

        # Get user's view mode
        user_settings = load_json(USER_SETTINGS_FILE)
        user_id = get_user_id()
        user_prefs = user_settings.get(user_id, {})
        view_mode = user_prefs.get('view_mode', 'season')

        # Collect stats for this map
        map_stats = {
            'total_games': 0,
            'mode': None,
            'brawlers': defaultdict(lambda: {'picks': 0, 'wins': 0})
        }
        
        if view_mode == 'offseason':
            # OFF-SEASON: Use players_data
            for tag, player in data.items():
                if not isinstance(player, dict):
                    continue
                
                player_modes = player.get('modes', {})
                if not isinstance(player_modes, dict):
                    continue
                
                for mode, mode_data in player_modes.items():
                    if not isinstance(mode_data, dict):
                        continue
                    
                    mode_maps = mode_data.get('maps', {})
                    if not isinstance(mode_maps, dict):
                        continue
                    
                    for map_n, map_data_item in mode_maps.items():
                        if map_n.lower().replace(' ', '_').replace("'", '').replace('-', '_') != map_name.lower():
                            continue
                        
                        if not isinstance(map_data_item, dict):
                            continue
                        
                        map_display = map_n
                        map_stats['mode'] = mode
                        map_stats['total_games'] += map_data_item.get('matches', 0)
                        
                        map_brawlers = map_data_item.get('brawlers', {})
                        if isinstance(map_brawlers, dict):
                            for brawler, brawler_data in map_brawlers.items():
                                if isinstance(brawler_data, dict):
                                    map_stats['brawlers'][brawler]['picks'] += brawler_data.get('picks', 0)
                                    map_stats['brawlers'][brawler]['wins'] += brawler_data.get('wins', 0)
        else:
            # SEASON: Use teams_data
            for team_name, team in data.items():
                if not isinstance(team, dict):
                    continue
                
                team_modes = team.get('modes', {})
                if not isinstance(team_modes, dict):
                    continue
                
                for mode, mode_data in team_modes.items():
                    if not isinstance(mode_data, dict):
                        continue
                    
                    mode_maps = mode_data.get('maps', {})
                    if not isinstance(mode_maps, dict):
                        continue
                    
                    for map_n, map_data_item in mode_maps.items():
                        if map_n.lower().replace(' ', '_').replace("'", '').replace('-', '_') != map_name.lower():
                            continue
                        
                        if not isinstance(map_data_item, dict):
                            continue
                        
                        map_display = map_n
                        map_stats['mode'] = mode
                        map_stats['total_games'] += map_data_item.get('matches', 0)
                        
                        map_brawlers = map_data_item.get('brawlers', {})
                        if isinstance(map_brawlers, dict):
                            for brawler, brawler_data in map_brawlers.items():
                                if isinstance(brawler_data, dict):
                                    map_stats['brawlers'][brawler]['picks'] += brawler_data.get('picks', 0)
                                    map_stats['brawlers'][brawler]['wins'] += brawler_data.get('wins', 0)
        
        if map_stats['total_games'] == 0:
            return f"Map {map_display} not found or no data available", 404
        
        
        # Calculate best brawlers
        best_brawlers = []
        for brawler, brawler_data in map_stats['brawlers'].items():
            if brawler_data['picks'] >= 1:
                win_rate = (brawler_data['wins'] / brawler_data['picks'] * 100) if brawler_data['picks'] > 0 else 0
                best_brawlers.append({
                    'name': brawler,
                    'picks': brawler_data['picks'],
                    'wins': brawler_data['wins'],
                    'win_rate': win_rate
                })
        
        best_brawlers.sort(key=lambda x: x['win_rate'], reverse=True)
        
        # Calculate total picks
        total_picks = sum(b['picks'] for b in best_brawlers)
        total_wins = sum(b['wins'] for b in best_brawlers)
        avg_wr = (total_wins / total_picks * 100) if total_picks > 0 else 0

        # Top brawler by meta score
        top_brawler = None
        if best_brawlers:
            scored = sorted(best_brawlers, key=lambda b: b['win_rate'] * (b['picks'] / total_picks * 100) if total_picks > 0 else 0, reverse=True)
            top_brawler = scored[0]

        # Best team on this map
        best_team_map = None
        if view_mode != 'offseason':
            team_map_stats = []
            for team_name, team in data.items():
                if not isinstance(team, dict):
                    continue
                team_modes = team.get('modes', {})
                if not isinstance(team_modes, dict):
                    continue
                for mode, mode_data in team_modes.items():
                    if not isinstance(mode_data, dict):
                        continue
                    mode_maps = mode_data.get('maps', {})
                    if not isinstance(mode_maps, dict):
                        continue
                    for map_n, map_data_item in mode_maps.items():
                        if map_n.lower().replace(' ', '_').replace("'", '').replace('-', '_') != map_name.lower():
                            continue
                        if not isinstance(map_data_item, dict):
                            continue
                        t_matches = map_data_item.get('matches', 0)
                        t_wins = map_data_item.get('wins', 0)
                        if t_matches >= 2:
                            t_wr = (t_wins / t_matches * 100) if t_matches > 0 else 0
                            team_map_stats.append({
                                'name': team_name,
                                'matches': t_matches,
                                'wins': t_wins,
                                'losses': t_matches - t_wins,
                                'win_rate': t_wr
                            })
            if team_map_stats:
                team_map_stats.sort(key=lambda x: x['win_rate'], reverse=True)
                best_team_map = team_map_stats[0]

        return render_template('map_detail.html',
                            user=session.get('discord_tag', 'Guest'),
                            map_name=map_display,
                            mode_name=map_stats['mode'],
                            total_games=map_stats['total_games'],
                            total_brawlers=len(map_stats['brawlers']),
                            best_brawlers=best_brawlers,
                            total_picks=total_picks,
                            total_wins=total_wins,
                            avg_wr=avg_wr,
                            top_brawler=top_brawler,
                            best_team=best_team_map)
    
    except Exception as e:
        print(f"❌ MAP DETAIL ERROR: {e}")
        import traceback
        traceback.print_exc()
        return f"Error loading map data: {e}", 500

@app.route('/brawler/<brawler_name>/mode/<mode_name>')
def brawler_mode_page(brawler_name, mode_name):
    """Brawler performance in a specific mode"""
    try:
        matches_df, teams_data, players_data, region_stats, mode_stats, all_brawlers = get_cached_data()

        if teams_data is None:
            teams_data = {}
        if players_data is None:
            players_data = {}

        user_settings = load_json(USER_SETTINGS_FILE)
        user_id = get_user_id()
        user_prefs = user_settings.get(user_id, {})
        view_mode = user_prefs.get('view_mode', 'season')

        # Use the appropriate data
        if view_mode == 'offseason':
            data = players_data
        else:
            data = teams_data



        # Convert URL format to display format
        mode_display = mode_name.replace('_', ' ').title()
        
        # Get user's view mode
        user_settings = load_json(USER_SETTINGS_FILE)
        user_id = get_user_id()
        user_prefs = user_settings.get(user_id, {})
        view_mode = user_prefs.get('view_mode', 'season')
        
        # Initialize brawler mode stats
        brawler_mode_stats = {
            'picks': 0,
            'wins': 0
        }
        
        map_stats = defaultdict(lambda: {'picks': 0, 'wins': 0})
        teammates_stats = defaultdict(lambda: {'picks': 0, 'wins': 0})
        opponent_stats = defaultdict(lambda: {'picks': 0, 'wins': 0})
        
        if view_mode == 'offseason':
            # OFF-SEASON MODE: Use players_data
            players_data = data
            
            for player_tag, player in players_data.items():
                if not isinstance(player, dict):
                    continue
                
                player_modes = player.get('modes', {})
                if not isinstance(player_modes, dict):
                    continue
                
                for mode, mode_data in player_modes.items():
                    if mode.lower().replace(' ', '_') != mode_name.lower() or not isinstance(mode_data, dict):
                        continue
                    
                    mode_display = mode
                    
                    mode_maps = mode_data.get('maps', {})
                    if isinstance(mode_maps, dict):
                        for map_name_iter, map_data in mode_maps.items():
                            if not isinstance(map_data, dict):
                                continue
                            
                            map_brawlers = map_data.get('brawlers', {})
                            if isinstance(map_brawlers, dict) and brawler_name in map_brawlers:
                                map_brawler_data = map_brawlers[brawler_name]
                                if isinstance(map_brawler_data, dict):
                                    picks = map_brawler_data.get('picks', 0)
                                    wins = map_brawler_data.get('wins', 0)
                                    
                                    brawler_mode_stats['picks'] += picks
                                    brawler_mode_stats['wins'] += wins
                                    map_stats[map_name_iter]['picks'] += picks
                                    map_stats[map_name_iter]['wins'] += wins
        else:
            # SEASON MODE: Use teams_data
            teams_data = data
            
            for team_name, team in teams_data.items():
                if not isinstance(team, dict):
                    continue
                
                team_modes = team.get('modes', {})
                if not isinstance(team_modes, dict):
                    continue
                
                for mode, mode_data in team_modes.items():
                    if mode.lower().replace(' ', '_') != mode_name.lower() or not isinstance(mode_data, dict):
                        continue
                    
                    mode_display = mode
                    
                    mode_maps = mode_data.get('maps', {})
                    if isinstance(mode_maps, dict):
                        for map_name_iter, map_data in mode_maps.items():
                            if not isinstance(map_data, dict):
                                continue
                            
                            map_brawlers = map_data.get('brawlers', {})
                            if isinstance(map_brawlers, dict) and brawler_name in map_brawlers:
                                map_brawler_data = map_brawlers[brawler_name]
                                if isinstance(map_brawler_data, dict):
                                    picks = map_brawler_data.get('picks', 0)
                                    wins = map_brawler_data.get('wins', 0)
                                    
                                    brawler_mode_stats['picks'] += picks
                                    brawler_mode_stats['wins'] += wins
                                    map_stats[map_name_iter]['picks'] += picks
                                    map_stats[map_name_iter]['wins'] += wins
        
        if brawler_mode_stats['picks'] == 0:
            return f"No data available for {brawler_name} in {mode_display}", 404
        
        
        # Convert map stats to list
        maps = []
        for map_name, data_item in map_stats.items():
            if data_item['picks'] >= 1:
                win_rate = (data_item['wins'] / data_item['picks'] * 100) if data_item['picks'] > 0 else 0
                maps.append({
                    'name': map_name,
                    'picks': data_item['picks'],
                    'wins': data_item['wins'],
                    'win_rate': win_rate
                })
        maps.sort(key=lambda x: x['win_rate'], reverse=True)
        
        overall_winrate = (brawler_mode_stats['wins'] / brawler_mode_stats['picks'] * 100) if brawler_mode_stats['picks'] > 0 else 0

        # NEW CODE:
        if view_mode == 'offseason':
            best_teammates, best_matchups, worst_matchups = get_brawler_synergies_filtered(
                brawler_name,
                mode_filter=mode_name
            )
        else:
            # Season mode - calculate from matches.xlsx
            best_teammates, best_matchups, worst_matchups = calculate_synergies_for_season_filtered(
                brawler_name,
                mode_filter=mode_name
            )

        # Calculate "used by" teams for this brawler in this mode
        used_by_teams = []
        if view_mode != 'offseason':
            team_brawler_stats = defaultdict(lambda: {'picks': 0, 'wins': 0})
            for team_name, team in data.items():
                if not isinstance(team, dict):
                    continue
                team_modes = team.get('modes', {})
                if not isinstance(team_modes, dict):
                    continue
                for mode, mode_data in team_modes.items():
                    if mode.lower().replace(' ', '_') != mode_name.lower() or not isinstance(mode_data, dict):
                        continue
                    mode_maps = mode_data.get('maps', {})
                    if not isinstance(mode_maps, dict):
                        continue
                    for mn, md in mode_maps.items():
                        if not isinstance(md, dict):
                            continue
                        mb = md.get('brawlers', {})
                        if isinstance(mb, dict) and brawler_name in mb:
                            bd = mb[brawler_name]
                            if isinstance(bd, dict):
                                team_brawler_stats[team_name]['picks'] += bd.get('picks', 0)
                                team_brawler_stats[team_name]['wins'] += bd.get('wins', 0)
            for tn, ts in team_brawler_stats.items():
                if ts['picks'] >= 1:
                    wr = (ts['wins'] / ts['picks'] * 100) if ts['picks'] > 0 else 0
                    used_by_teams.append({
                        'name': tn,
                        'picks': ts['picks'],
                        'wins': ts['wins'],
                        'win_rate': wr
                    })
            used_by_teams.sort(key=lambda x: x['picks'], reverse=True)

        return render_template('brawler_mode.html',
                             user=session.get('discord_tag', 'Guest'),
                             brawler_name=brawler_name,
                             mode_name=mode_display,
                             stats=brawler_mode_stats,
                             overall_winrate=overall_winrate,
                             maps=maps,
                             best_teammates=best_teammates[:10],
                             best_matchups=best_matchups,
                             worst_matchups=worst_matchups,
                             used_by_teams=used_by_teams)

    except Exception as e:
        print(f"❌ BRAWLER-MODE ERROR: {e}")
        import traceback
        traceback.print_exc()
        return f"Error loading brawler-mode data: {e}", 500


@app.route('/brawler/<brawler_name>/map/<map_name>')
def brawler_map_page(brawler_name, map_name):
    """Brawler performance on a specific map"""
    try:
        matches_df, teams_data, players_data, region_stats, mode_stats, all_brawlers = get_cached_data()

        if teams_data is None:
            teams_data = {}
        if players_data is None:
            players_data = {}

        user_settings = load_json(USER_SETTINGS_FILE)
        user_id = get_user_id()
        user_prefs = user_settings.get(user_id, {})
        view_mode = user_prefs.get('view_mode', 'season')

        # Use the appropriate data
        if view_mode == 'offseason':
            data = players_data
        else:
            data = teams_data



        # Convert URL format to display format
        map_display = map_name.replace('_', ' ').title()
        
        # Get user's view mode
        user_settings = load_json(USER_SETTINGS_FILE)
        user_id = get_user_id()
        user_prefs = user_settings.get(user_id, {})
        view_mode = user_prefs.get('view_mode', 'season')
        
        # Initialize brawler map stats
        brawler_map_stats = {
            'picks': 0,
            'wins': 0,
            'mode': None
        }
        
        if view_mode == 'offseason':
            # OFF-SEASON MODE: Use players_data
            players_data = data
            
            for player_tag, player in players_data.items():
                if not isinstance(player, dict):
                    continue
                
                player_modes = player.get('modes', {})
                if not isinstance(player_modes, dict):
                    continue
                
                for mode, mode_data in player_modes.items():
                    if not isinstance(mode_data, dict):
                        continue
                    
                    mode_maps = mode_data.get('maps', {})
                    if not isinstance(mode_maps, dict):
                        continue
                    
                    for map_n, map_data in mode_maps.items():
                        if map_n.lower().replace(' ', '_').replace("'", '').replace('-', '_') != map_name.lower():
                            continue
                        
                        if not isinstance(map_data, dict):
                            continue
                        
                        map_display = map_n
                        brawler_map_stats['mode'] = mode
                        
                        map_brawlers = map_data.get('brawlers', {})
                        if isinstance(map_brawlers, dict) and brawler_name in map_brawlers:
                            map_brawler_data = map_brawlers[brawler_name]
                            if isinstance(map_brawler_data, dict):
                                brawler_map_stats['picks'] += map_brawler_data.get('picks', 0)
                                brawler_map_stats['wins'] += map_brawler_data.get('wins', 0)
        else:
            # SEASON MODE: Use teams_data
            teams_data = data
            
            for team_name, team in teams_data.items():
                if not isinstance(team, dict):
                    continue
                
                team_modes = team.get('modes', {})
                if not isinstance(team_modes, dict):
                    continue
                
                for mode, mode_data in team_modes.items():
                    if not isinstance(mode_data, dict):
                        continue
                    
                    mode_maps = mode_data.get('maps', {})
                    if not isinstance(mode_maps, dict):
                        continue
                    
                    for map_n, map_data in mode_maps.items():
                        if map_n.lower().replace(' ', '_').replace("'", '').replace('-', '_') != map_name.lower():
                            continue
                        
                        if not isinstance(map_data, dict):
                            continue
                        
                        map_display = map_n
                        brawler_map_stats['mode'] = mode
                        
                        map_brawlers = map_data.get('brawlers', {})
                        if isinstance(map_brawlers, dict) and brawler_name in map_brawlers:
                            map_brawler_data = map_brawlers[brawler_name]
                            if isinstance(map_brawler_data, dict):
                                brawler_map_stats['picks'] += map_brawler_data.get('picks', 0)
                                brawler_map_stats['wins'] += map_brawler_data.get('wins', 0)
        
        if brawler_map_stats['picks'] == 0:
            return f"No data available for {brawler_name} on {map_display}", 404
        
        
        overall_winrate = (brawler_map_stats['wins'] / brawler_map_stats['picks'] * 100) if brawler_map_stats['picks'] > 0 else 0

        # NEW CODE:
        if view_mode == 'offseason':
            best_teammates, best_matchups, worst_matchups = get_brawler_synergies_filtered(
                brawler_name,
                map_filter=map_name
            )
        else:
            # Season mode - calculate from matches.xlsx
            best_teammates, best_matchups, worst_matchups = calculate_synergies_for_season_filtered(
                brawler_name,
                map_filter=map_name
            )
        
        # Compute top synergy brawler (best teammate by win rate with min picks)
        top_brawler = None
        if best_teammates:
            top_brawler = best_teammates[0]  # already sorted by win rate

        # Compute avg win rate across all brawlers on this map
        avg_wr = overall_winrate  # fallback to brawler's own WR

        # Compute best team using this brawler on this map
        best_team = None
        used_by_teams = []
        if view_mode != 'offseason':
            team_brawler_stats = defaultdict(lambda: {'picks': 0, 'wins': 0})
            for team_name, team in data.items():
                if not isinstance(team, dict):
                    continue
                team_modes = team.get('modes', {})
                if not isinstance(team_modes, dict):
                    continue
                for mode, mode_data in team_modes.items():
                    if not isinstance(mode_data, dict):
                        continue
                    mode_maps = mode_data.get('maps', {})
                    if not isinstance(mode_maps, dict):
                        continue
                    for mn, md in mode_maps.items():
                        if mn.lower().replace(' ', '_').replace("'", '').replace('-', '_') != map_name.lower():
                            continue
                        if not isinstance(md, dict):
                            continue
                        mb = md.get('brawlers', {})
                        if isinstance(mb, dict) and brawler_name in mb:
                            bd = mb[brawler_name]
                            if isinstance(bd, dict):
                                team_brawler_stats[team_name]['picks'] += bd.get('picks', 0)
                                team_brawler_stats[team_name]['wins'] += bd.get('wins', 0)
            for tn, ts in team_brawler_stats.items():
                if ts['picks'] >= 1:
                    wr = (ts['wins'] / ts['picks'] * 100) if ts['picks'] > 0 else 0
                    used_by_teams.append({
                        'name': tn,
                        'picks': ts['picks'],
                        'wins': ts['wins'],
                        'win_rate': wr
                    })
            used_by_teams.sort(key=lambda x: x['picks'], reverse=True)
            # Best team = highest WR with at least 2 picks
            team_candidates = [t for t in used_by_teams if t['picks'] >= 2]
            if team_candidates:
                best_team = max(team_candidates, key=lambda x: x['win_rate'])

        return render_template('brawler_map.html',
                             user=session.get('discord_tag', 'Guest'),
                             brawler_name=brawler_name,
                             map_name=map_display,
                             mode_name=brawler_map_stats['mode'],
                             stats=brawler_map_stats,
                             overall_winrate=overall_winrate,
                             best_teammates=best_teammates[:10],
                             best_matchups=best_matchups,
                             worst_matchups=worst_matchups,
                             used_by_teams=used_by_teams,
                             top_brawler=top_brawler,
                             best_team=best_team,
                             avg_wr=overall_winrate)

    except Exception as e:
        print(f"❌ BRAWLER-MAP ERROR: {e}")
        import traceback
        traceback.print_exc()
        return f"Error loading brawler-map data: {e}", 500


@app.route('/modes')
def modes_overview():
    """Overview page for all game modes"""
    matches_df, teams_data, players_data, region_stats, mode_stats, all_brawlers = get_cached_data()

    if teams_data is None:
        teams_data = {}
    if players_data is None:
        players_data = {}

    user_settings = load_json(USER_SETTINGS_FILE)
    user_id = get_user_id()
    user_prefs = user_settings.get(user_id, {})
    view_mode = user_prefs.get('view_mode', 'season')

    # Use the appropriate data
    if view_mode == 'offseason':
        data = players_data
    else:
        data = teams_data
    
    
    # Collect comprehensive mode statistics
    modes_data = defaultdict(lambda: {
        'total_games': 0,
        'maps': set(),
        'brawlers': defaultdict(lambda: {'picks': 0, 'wins': 0})
    })

    # Track map play counts across all modes
    map_play_counts = defaultdict(lambda: {'games': 0, 'mode': ''})

    # Track best team per mode
    team_mode_stats = defaultdict(lambda: defaultdict(lambda: {'matches': 0, 'wins': 0}))

    # FIXED: Iterate over 'data' instead of 'teams_data'
    for entity_name, entity in data.items():
        if not isinstance(entity, dict):
            continue
        
        entity_modes = entity.get('modes', {})
        if not isinstance(entity_modes, dict):
            continue
        
        for mode_name, mode_data in entity_modes.items():
            if mode_name not in VALID_MODES:
                continue
            
            if not isinstance(mode_data, dict):
                continue
            
            # Count games
            mode_matches = mode_data.get('matches', 0)
            modes_data[mode_name]['total_games'] += mode_matches

            # Track team performance per mode
            mode_wins = mode_data.get('wins', 0)
            team_mode_stats[mode_name][entity_name]['matches'] += mode_matches
            team_mode_stats[mode_name][entity_name]['wins'] += mode_wins

            # Track maps
            mode_maps = mode_data.get('maps', {})
            if isinstance(mode_maps, dict):
                for map_name in mode_maps.keys():
                    modes_data[mode_name]['maps'].add(map_name)
                    # Track map play counts
                    map_key = f"{mode_name}|{map_name}"
                    if not map_play_counts[map_key]['mode']:
                        map_play_counts[map_key]['mode'] = mode_name
                        map_play_counts[map_key]['name'] = map_name

                # Track brawler stats
                for map_name, map_data in mode_maps.items():
                    if not isinstance(map_data, dict):
                        continue

                    # Count map games using matches count
                    map_key = f"{mode_name}|{map_name}"
                    map_play_counts[map_key]['games'] += map_data.get('matches', 0)

                    map_brawlers = map_data.get('brawlers', {})
                    if isinstance(map_brawlers, dict):
                        for brawler, brawler_data in map_brawlers.items():
                            if isinstance(brawler_data, dict):
                                modes_data[mode_name]['brawlers'][brawler]['picks'] += brawler_data.get('picks', 0)
                                modes_data[mode_name]['brawlers'][brawler]['wins'] += brawler_data.get('wins', 0)
    
    # Build modes list with stats
    modes_list = []
    for mode_name, mode_info in modes_data.items():
        # Find top brawler for this mode using meta score (win_rate * pick_rate)
        top_brawler = None
        if mode_info['brawlers']:
            # Calculate total picks for this mode
            total_picks = sum(b['picks'] for b in mode_info['brawlers'].values())
            
            # Calculate meta score for each brawler
            brawler_scores = []
            for brawler_name, brawler_data in mode_info['brawlers'].items():
                if brawler_data['picks'] >= 5:  # Minimum 5 picks
                    win_rate = (brawler_data['wins'] / brawler_data['picks'] * 100)
                    pick_rate = (brawler_data['picks'] / total_picks * 100) if total_picks > 0 else 0
                    meta_score = win_rate * pick_rate  # win_rate * pick_rate
                    brawler_scores.append({
                        'name': brawler_name,
                        'win_rate': win_rate,
                        'picks': brawler_data['picks'],
                        'meta_score': meta_score
                    })
            
            # Get brawler with highest meta score
            if brawler_scores:
                top_brawler_data = max(brawler_scores, key=lambda x: x['meta_score'])
                top_brawler = {
                    'name': top_brawler_data['name'],
                    'win_rate': top_brawler_data['win_rate'],
                    'picks': top_brawler_data['picks']
                }
        
        modes_list.append({
            'name': mode_name,
            'total_games': mode_info['total_games'],
            'total_maps': len(mode_info['maps']),
            'total_brawlers': len(mode_info['brawlers']),
            'top_brawler': top_brawler
        })
    
    # Sort by total games
    modes_list.sort(key=lambda x: x['total_games'], reverse=True)

    # Build best team per mode
    best_teams_per_mode = {}
    for mode_name, teams in team_mode_stats.items():
        best_team = None
        best_wr = 0
        for team_name, stats in teams.items():
            if stats['matches'] >= 3:  # Minimum 3 matches
                wr = (stats['wins'] / stats['matches'] * 100) if stats['matches'] > 0 else 0
                if wr > best_wr:
                    best_wr = wr
                    best_team = {
                        'name': team_name,
                        'wins': stats['wins'],
                        'matches': stats['matches'],
                        'win_rate': wr
                    }
        if best_team:
            best_teams_per_mode[mode_name] = best_team

    # Build most played maps list
    maps_list = []
    for map_key, map_info in map_play_counts.items():
        maps_list.append({
            'name': map_info.get('name', ''),
            'mode': map_info.get('mode', ''),
            'games': map_info['games']
        })
    maps_list.sort(key=lambda x: x['games'], reverse=True)
    top_maps = maps_list[:5]

    # Attach best team to each mode in modes_list
    for mode in modes_list:
        mode['best_team'] = best_teams_per_mode.get(mode['name'])

    return render_template('modes_overview.html',
                         user=session.get('discord_tag', 'Guest'),
                         modes=modes_list,
                         top_maps=top_maps)

@app.route('/team/<team_name>/brawler/<brawler_name>')
def team_brawler_page(team_name, brawler_name):
    matches_df, teams_data, players_data, region_stats, mode_stats, all_brawlers = get_cached_data()

    if teams_data is None:
        teams_data = {}
    if players_data is None:
        players_data = {}

    user_settings = load_json(USER_SETTINGS_FILE)
    user_id = get_user_id()
    user_prefs = user_settings.get(user_id, {})
    view_mode = user_prefs.get('view_mode', 'season')

    # Use the appropriate data
    if view_mode == 'offseason':
        data = players_data
    else:
        data = teams_data

    if team_name not in teams_data:
        return "Team not found", 404
    
    team = teams_data[team_name]
    
    if brawler_name not in team['brawlers']:
        return "Brawler not found", 404
    
    brawler_data = team['brawlers'][brawler_name]
    
    # Get mode stats for this brawler
    mode_stats = {}
    for mode, mode_data in team['modes'].items():
        for map_name, map_data in mode_data['maps'].items():
            if brawler_name in map_data['brawlers']:
                if mode not in mode_stats:
                    mode_stats[mode] = {'picks': 0, 'wins': 0}
                mode_stats[mode]['picks'] += map_data['brawlers'][brawler_name]['picks']
                mode_stats[mode]['wins'] += map_data['brawlers'][brawler_name]['wins']
    
    # Get player stats for this brawler (this team only)
    player_stats = {}
    for player_tag, player_data in team['players'].items():
        if brawler_name in player_data['brawlers']:
            player_stats[player_tag] = {
                'name': player_data['name'],
                'picks': player_data['brawlers'][brawler_name]['picks'],
                'wins': player_data['brawlers'][brawler_name]['wins']
            }

    # Brawlers that counter this brawler globally (from Rust synergy data)
    _, _, worst_matchups_raw = get_brawler_synergies(brawler_name)
    worst_matchups = sorted(worst_matchups_raw, key=lambda x: x['win_rate'])

    # This team's overall h2h record vs each opponent team (from Rust h2h data)
    team_h2h = _h2h_data.get(team_name, {})
    vs_teams = dict(sorted(team_h2h.items(), key=lambda x: x[1].get('matches', 0), reverse=True))

    return render_template('team_brawler.html',
                         user=session.get('discord_tag', 'Guest'),
                         team_name=team_name,
                         team=team,
                         brawler_name=brawler_name,
                         brawler_data=brawler_data,
                         mode_stats=mode_stats,
                         player_stats=player_stats,
                         worst_matchups=worst_matchups,
                         vs_teams=vs_teams)

@app.route('/auth')
def auth():
    token = request.args.get('token')
    if not token:
        return render_template('login.html', error="No token provided")
    
    tokens = load_json(TOKENS_FILE)
    
    if token not in tokens:
        return render_template('login.html', error="Invalid token")
    
    token_data = tokens[token]
    
    if token_data.get('used', False):
        return render_template('login.html', error="Token already used")
    
    # Check if user is authorized
    if not is_user_authorized(token_data['discord_id']):
        return render_template('login.html', error="User not authorized")
    
    # Mark token as used
    tokens[token]['used'] = True
    save_json(TOKENS_FILE, tokens)
    
    # Create session
    session['discord_id'] = token_data['discord_id']
    session['discord_tag'] = token_data['discord_tag']
    
    return redirect('/dashboard')


def ensure_roster_files_exist():
    """Ensure team_rosters.json and tracked_players.json exist and are up to date"""
    import os
    
    def should_regenerate(json_file, source_file):
        """Check if JSON file needs regeneration"""
        if not os.path.exists(json_file):
            return True
        if not os.path.exists(source_file):
            return False
        # Regenerate if source is newer than JSON
        return os.path.getmtime(source_file) > os.path.getmtime(json_file)
    
    # Check team_rosters.json
    if should_regenerate('team_rosters.json', 'teams.xlsx'):
        try:
            teams_df = pd.read_excel('teams.xlsx')
            rosters = {}
            
            for _, row in teams_df.iterrows():
                team_name = row['Team Name']
                roster = []
                
                for i in range(1, 4):
                    tag_col = f'Player {i} ID'
                    if tag_col in teams_df.columns and pd.notna(row.get(tag_col)):
                        tag = str(row[tag_col]).strip().upper().replace('0', 'O')
                        if not tag.startswith('#'):
                            tag = '#' + tag
                        roster.append(tag)
                
                if roster:
                    rosters[team_name] = roster
            
            with open('team_rosters.json', 'w') as f:
                json.dump(rosters, f, indent=2)
            
        except Exception as e:
            print(f"❌ Error creating team_rosters.json: {e}")
            with open('team_rosters.json', 'w') as f:
                json.dump({}, f)
    
    # Check tracked_players.json
    if should_regenerate('tracked_players.json', 'players_off.xlsx'):
        try:
            players_df = pd.read_excel('players_off.xlsx')
            tracked = {}
            
            for _, row in players_df.iterrows():
                tag = str(row['Player ID']).strip().upper().replace('0', 'O')
                if not tag.startswith('#'):
                    tag = '#' + tag
                
                region = str(row.get('Region', 'NA')).strip().upper()
                if region in ['NAN', 'NONE', '', 'NULL'] or pd.isna(row.get('Region')):
                    region = 'NA'
                
                tracked[tag] = {
                    'name': str(row['Player Name']).strip(),
                    'region': region
                }
            
            with open('tracked_players.json', 'w') as f:
                json.dump(tracked, f, indent=2)
            
            print(f"✓ Created tracked_players.json with {len(tracked)} players")
        except Exception as e:
            print(f"❌ Error creating tracked_players.json: {e}")
            with open('tracked_players.json', 'w') as f:
                json.dump({}, f)


if __name__ == '__main__':
    sync_from_gcs()
    ensure_roster_files_exist()
    warm_cache_at_startup()
    # Pre-load the matches DataFrame so h2h page is instant on first visit
    for _mf in ['matches.xlsx', 'matches_off.xlsx']:
        if os.path.exists(_mf):
            get_cached_matches_df(_mf)

    debug = os.environ.get('FLASK_DEBUG', 'true').lower() == 'true'
    app.run(host='0.0.0.0', port=8080, debug=debug, use_reloader=False)