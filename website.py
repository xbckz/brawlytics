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




from bot2 import (
    load_bot_mode as bot_load_mode
)

app = Flask(__name__)
app.secret_key = os.urandom(24)

# File paths
TOKENS_FILE = 'data/tokens.json'
AUTHORIZED_USERS_FILE = 'data/authorized_users.json'
USER_SETTINGS_FILE = 'data/user_settings.json'
MATCHES_FILE = 'matches.xlsx'
TEAMS_FILE = 'teams.xlsx'


_brawler_synergies = {}


CONFIG = {
    'REGIONS': ['NA', 'EU', 'LATAM', 'EA', 'SEA'],
    'MODES': ['Gem Grab', 'Brawl Ball', 'Heist', 'Bounty', 'Knockout', 'Hot Zone']
}

VALID_MODES = {'Gem Grab', 'Brawl Ball', 'Heist', 'Bounty', 'Knockout', 'Hot Zone'}

# Theme presets
THEMES = {
    'red': {'primary': '#ef4444', 'bg': '#0a0a0a', 'card': '#111111', 'dark': '#1a1a1a'},
    'brawl': {'primary': '#e94560', 'bg': '#1a1a2e', 'card': '#16213e', 'dark': '#0f3460'},
    'purple': {'primary': '#8b5cf6', 'bg': '#1e1b29', 'card': '#2d2438', 'dark': '#1a1625'},
    'blue': {'primary': '#3b82f6', 'bg': '#0f172a', 'card': '#1e293b', 'dark': '#0f172a'},
    'green': {'primary': '#10b981', 'bg': '#064e3b', 'card': '#065f46', 'dark': '#022c22'},
    'orange': {'primary': '#f97316', 'bg': '#1c1917', 'card': '#292524', 'dark': '#1c1917'},
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


_trios_cache = {
    'data': None,
    'timestamp': None,
    'cache_key': None
}

TRIOS_CACHE_DURATION = 300

def get_cache_key():
    """Generate a cache key based on user settings"""
    user_settings = load_json(USER_SETTINGS_FILE)
    user_id = str(session.get('discord_id', 'test_user'))
    user_prefs = user_settings.get(user_id, {})
    
    # Create a hash of relevant settings that affect data
    view_mode = user_prefs.get('view_mode', 'season')
    cache_key = f"{view_mode}_{user_prefs.get('date_range', '30d')}_{user_prefs.get('start_date', '')}_{user_prefs.get('end_date', '')}"
    return cache_key

def get_cached_data():
    """Get cached data if valid, otherwise reload"""
    current_cache_key = get_cache_key()
    current_time = time.time()
    
    # Check if cache is valid
    if (_cache['data'] is not None and 
        _cache['timestamp'] is not None and
        _cache['user_settings_hash'] == current_cache_key and
        current_time - _cache['timestamp'] < CACHE_DURATION):
        
        print(f"Using cached data (age: {int(current_time - _cache['timestamp'])}s)")
        return _cache['data']
    
    # Cache is invalid, reload data
    print("Loading fresh data...")
    start_time = time.time()
    
    data = load_matches_data()
    
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

def get_user_theme():
    """Get current user's theme or default"""
    if 'discord_id' not in session:
        return THEMES['red']
    
    settings = load_json(USER_SETTINGS_FILE)
    user_id = str(session['discord_id'])
    
    if user_id in settings:
        theme_name = settings[user_id].get('theme', 'red')
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
    """Load current bot mode"""
    mode_file = 'data/bot_mode.json'
    if os.path.exists(mode_file):
        with open(mode_file, 'r') as f:
            data = json.load(f)
            return data.get('mode', 'season')
    return 'season'

def get_config_for_mode():
    """Get configuration based on current mode"""
    mode = load_bot_mode()
    
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


def save_bot_mode(mode):
    """Save bot mode"""
    mode_file = 'data/bot_mode.json'
    os.makedirs(os.path.dirname(mode_file), exist_ok=True)
    with open(mode_file, 'w') as f:
        json.dump({
            'mode': mode,
            'updated_at': datetime.now().isoformat()
        }, f, indent=2)



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
    """Load matches data using Rust processor"""
    global _brawler_synergies
    
    user_id = str(session.get('discord_id', 'test_user'))
    
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
            return None, {}, {}, {}, set()
        
        # Extract synergies
        _brawler_synergies = result.get('brawler_matchups', {})
        
       
        
        # Print structure of first brawler
        if _brawler_synergies:
            first_brawler = list(_brawler_synergies.keys())[0]
            
        
        teams_data = result.get('teams_data')
        players_data = result.get('players_data')
        data = teams_data if teams_data is not None else players_data
        
        if data is None:
            print("⚠️ No data returned from Rust")
            return None, {}, {}, {}, set()
        
        return (
            None,
            data,
            result.get('region_stats', {}),
            result.get('mode_stats', {}),
            set(result.get('all_brawlers', []))
        )
        
    except Exception as e:
        print(f"❌ PYTHON ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, {}, {}, {}, set()


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



@app.context_processor
def inject_theme():
    """Make theme and view mode available to all templates"""
    user_settings = load_json(USER_SETTINGS_FILE)
    user_id = str(session.get('discord_id', 'test_user'))
    user_prefs = user_settings.get(user_id, {})
    view_mode = user_prefs.get('view_mode', 'season')
    
    return {
        'theme': get_user_theme(),
        'view_mode': view_mode
    }

@app.before_request
def require_auth():
    """Require authentication for all pages except auth"""
    if request.endpoint not in ['auth', 'index', 'static'] and 'discord_id' not in session:
        return redirect('/auth')

@app.route('/')
def index():
    if 'discord_id' in session:
        return redirect('/dashboard')
    return redirect('/auth')  # Force login in production


@app.route('/dashboard')
def dashboard():
    try:
        matches_df, data, region_stats, mode_stats, all_brawlers = get_cached_data()
        
        if data is None or not data:
            print("❌ No data returned")
            return "Error loading data - no data returned", 500
        
        # Get user's view mode
        user_settings = load_json(USER_SETTINGS_FILE)
        user_id = str(session.get('discord_id', 'test_user'))
        user_prefs = user_settings.get(user_id, {})
        view_mode = user_prefs.get('view_mode', 'season')
        
        
        if view_mode == 'offseason':
            
            
            # Check data structure
            if not isinstance(data, dict):
                print(f"❌ Expected dict, got {type(data)}")
                return "Error: Invalid data structure for offseason mode", 500
            
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
                return f"Error sorting players: {e}", 500
            
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
                                 user=session.get('discord_tag', 'Unknown'),
                                 total_matches=total_matches,
                                 total_players=len(data),
                                 total_brawlers=len(all_brawlers),
                                 top_players=top_items,
                                 region_stats=region_stats)
        
        else:  # Season mode
          
            
            if not isinstance(data, dict):
                print(f"❌ Expected dict, got {type(data)}")
                return "Error: Invalid data structure for season mode", 500
            
            try:
                top_teams = sorted(
                    data.items(),
                    key=lambda x: (x[1]['wins'] / x[1]['matches']) if x[1].get('matches', 0) > 0 else 0,
                    reverse=True
                )[:10]
            except Exception as e:
                print(f"❌ Error sorting teams: {e}")
                import traceback
                traceback.print_exc()
                return f"Error sorting teams: {e}", 500
            
            # Calculate total matches
            total_matches = sum(team.get('matches', 0) for team in data.values())
            
            return render_template('dashboard.html',
                                 user=session.get('discord_tag', 'Unknown'),
                                 total_matches=total_matches,
                                 total_teams=len(data),
                                 total_brawlers=len(all_brawlers),
                                 top_teams=top_teams)
    
    except Exception as e:
        print(f"\n❌ DASHBOARD ERROR: {e}")
        import traceback
        traceback.print_exc()
        return f"Dashboard error: {e}", 500


@app.route('/region/<region_name>')
def region_page(region_name):
    try:
        region_name = region_name.upper()
        matches_df, data, region_stats, mode_stats, all_brawlers = get_cached_data()
        
        
        
        if data is None or not data:
            print("❌ No data")
            return "Error loading data", 500
        
        # Get user's view mode
        user_settings = load_json(USER_SETTINGS_FILE)
        user_id = str(session.get('discord_id', 'test_user'))
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
                                 user=session.get('discord_tag', 'Unknown'),
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
            
            
            
            top_teams = sorted(
                region_teams.items(),
                key=lambda x: (x[1]['wins'] / x[1]['matches']) if x[1].get('matches', 0) > 0 else 0,
                reverse=True
            )[:20]
            
            total_matches = sum(t.get('matches', 0) for t in region_teams.values() if isinstance(t, dict))
            
            return render_template('region.html',
                                 user=session.get('discord_tag', 'Unknown'),
                                 region=title,
                                 region_code=region_name,
                                 total_matches=total_matches,
                                 total_teams=len(region_teams),
                                 top_teams=top_teams,
                                 teams_data=data)
    
    except Exception as e:
        print(f"❌ REGION PAGE ERROR: {e}")
        import traceback
        traceback.print_exc()
        return f"Region page error: {e}", 500

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
    _, teams_data, _, _, _ = get_cached_data()
    
    if team_name not in teams_data:
        return "Team not found", 404
    
    team = teams_data[team_name]
    
    return render_template('team.html',
                         user=session['discord_tag'],
                         team_name=team_name,
                         team=team)

@app.route('/team/<team_name>/mode/<mode>')
def team_mode_page(team_name, mode):
    _, teams_data, _, _, _ = get_cached_data()
    
    if team_name not in teams_data:
        return "Team not found", 404
    
    team = teams_data[team_name]
    
    if mode not in team['modes']:
        return "Mode not found", 404
    
    mode_data = team['modes'][mode]
    
    return render_template('team_mode.html',
                         user=session['discord_tag'],
                         team_name=team_name,
                         team=team,
                         mode=mode,
                         mode_data=mode_data)

@app.route('/team/<team_name>/mode/<mode>/map/<map_name>')
def team_map_page(team_name, mode, map_name):
    _, teams_data, _, _, _ = get_cached_data()
    
    if team_name not in teams_data:
        return "Team not found", 404
    
    team = teams_data[team_name]
    
    if mode not in team['modes'] or map_name not in team['modes'][mode]['maps']:
        return "Map not found", 404
    
    map_data = team['modes'][mode]['maps'][map_name]
    
    return render_template('team_map.html',
                         user=session['discord_tag'],
                         team_name=team_name,
                         team=team,
                         mode=mode,
                         map_name=map_name,
                         map_data=map_data)



@app.route('/about')
def about_page():
    return render_template('about.html',
                         user=session.get('discord_tag', 'Unknown'))


@app.route('/settings', methods=['GET', 'POST'])
def settings():
    if request.method == 'POST':
        user_settings = load_json(USER_SETTINGS_FILE)
        user_id = str(session['discord_id'])
        
        theme = request.form.get('theme', 'red')
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
        
        # Save all settings
        user_settings[user_id] = {
            'theme': theme if theme in THEMES else 'red',
            'date_range': date_range,
            'start_date': start_date_rfc3339,
            'end_date': end_date_rfc3339,
            'view_mode': view_mode
        }
        
        
        
        save_json(USER_SETTINGS_FILE, user_settings)
        
        # IMPORTANT: Clear cache when settings change
        clear_cache()
        
        session.modified = True
        
        return redirect('/settings')
    
    # GET request - load current settings
    current_settings = load_json(USER_SETTINGS_FILE)
    user_id = str(session['discord_id'])
    user_prefs = current_settings.get(user_id, {})
    
    current_theme = user_prefs.get('theme', 'red')
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
                         user=session['discord_tag'],
                         themes=THEMES,
                         current_theme=current_theme,
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
    
    _, teams_data, _, _, _ = get_cached_data()
    
    # Collect brawler stats based on filters
    # Use the SAME data source as load_matches_data already processed
    brawler_stats = defaultdict(lambda: {
        'picks': 0,
        'wins': 0
    })
    
    total_picks = 0
    
    for team_name, team in teams_data.items():
        team_region = team['region']
        
        # Filter by region
        if region != 'ALL' and team_region != region:
            continue
        
        for mode_name, mode_data in team['modes'].items():
            if mode_name not in VALID_MODES:
                continue
            
            # Filter by mode
            if mode != 'ALL' and mode_name != mode:
                continue
                
            for map_name, map_data in mode_data['maps'].items():
                for brawler, brawler_data in map_data['brawlers'].items():
                    # This data is ALREADY deduplicated by load_matches_data
                    brawler_stats[brawler]['picks'] += brawler_data['picks']
                    brawler_stats[brawler]['wins'] += brawler_data['wins']
                    total_picks += brawler_data['picks']
    
    
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
        for mode_name in team['modes'].keys():
            if mode_name in VALID_MODES:
                all_modes.add(mode_name)
    
    return render_template('meta.html',
                         user=session['discord_tag'],
                         meta_brawlers=meta_brawlers,
                         total_picks=total_picks,
                         modes=sorted(all_modes),
                         current_region=region,
                         current_mode=mode)

@app.route('/api/meta/generate')
def generate_meta_tier_list():
    """Generate tier list image based on filters"""
    try:
        region = request.args.get('region', 'ALL').upper()
        mode = request.args.get('mode', 'ALL')
        
        
        
        _, teams_data, _, _, _ = get_cached_data()
        
        if not teams_data:
            print("No teams data available")
            return "No data available", 404
    except:
        print("!")
        
    # Collect brawler stats based on filters
    brawler_stats = defaultdict(lambda: {'picks': 0, 'wins': 0})
    total_picks = 0
    
    for team_name, team in teams_data.items():
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

@app.route('/brawlers')
def brawlers_page():
    """Main brawlers overview page"""
    _, teams_data, _, _, all_brawlers = get_cached_data()
    
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
        for mode, mode_data in team['modes'].items():
            if mode in ['Unknown', 'nan', '', 'None']:
                continue
                
            for map_name, map_data in mode_data['maps'].items():
                for brawler, brawler_data in map_data['brawlers'].items():
                    stats = brawler_stats[brawler]
                    stats['picks'] += brawler_data['picks']
                    stats['wins'] += brawler_data['wins']
                    stats['modes'][mode]['picks'] += brawler_data['picks']
                    stats['modes'][mode]['wins'] += brawler_data['wins']
                    stats['maps'][map_name]['picks'] += brawler_data['picks']
                    stats['maps'][map_name]['wins'] += brawler_data['wins']
                    total_picks += brawler_data['picks']
    
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
                         user=session['discord_tag'],
                         brawlers=brawlers_list,
                         total_picks=total_picks)


# Replace the brawler_detail_page function in website.py

@app.route('/brawler/<brawler_name>')
def brawler_detail_page(brawler_name):
    """Detailed brawler statistics page"""
    try:
        matches_df, data, region_stats, _, _ = get_cached_data()
        
      
        
        if data is None or not data:
            return "Error loading data", 500
        
        # Get user's view mode
        user_settings = load_json(USER_SETTINGS_FILE)
        user_id = str(session.get('discord_id', 'test_user'))
        user_prefs = user_settings.get(user_id, {})
        view_mode = user_prefs.get('view_mode', 'season')
        
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
        
        # NEW: Get synergies from global cache
        best_teammates, best_matchups, worst_matchups = get_brawler_synergies(brawler_name)
        
        
        return render_template('brawler_detail.html',
                             user=session.get('discord_tag', 'Unknown'),
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
        return f"Brawler detail error: {e}", 500



@app.route('/modes/<mode_name>')
def mode_detail_page(mode_name):
    """Detailed mode statistics page"""
    try:
        # Convert URL format back to display format
        mode_display = mode_name.replace('_', ' ').title()
        
        
        
        matches_df, data, _, mode_stats, _ = get_cached_data()
        
        if data is None or not data:
            return "Error loading data", 500
        
        # Get user's view mode
        user_settings = load_json(USER_SETTINGS_FILE)
        user_id = str(session.get('discord_id', 'test_user'))
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
        
        
        return render_template('mode_detail.html',
                            user=session.get('discord_tag', 'Unknown'),
                            mode_name=mode_display,
                            total_games=mode_stats_data['total_games'],
                            total_maps=len(mode_stats_data['maps']),
                            total_brawlers=len(mode_stats_data['brawlers']),
                            best_brawlers=best_brawlers,
                            maps=maps_list,
                            total_picks=total_picks)
    
    except Exception as e:
        print(f"❌ MODE DETAIL ERROR: {e}")
        import traceback
        traceback.print_exc()
        return f"Mode detail error: {e}", 500

@app.route('/player/<path:player_tag>')
def player_page(player_tag):
    """Display individual player statistics - adapts to view mode"""
    # Decode the URL-encoded tag
    player_tag = unquote(player_tag)
    
    # Normalize tag
    if not player_tag.startswith('#'):
        player_tag = '#' + player_tag
    player_tag = player_tag.upper().replace('0', 'O')
    
    _, data, _, _, _ = get_cached_data()
    
    # Get user's view mode
    user_settings = load_json(USER_SETTINGS_FILE)
    user_id = str(session.get('discord_id', 'test_user'))
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
                             user=session['discord_tag'],
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
        
        # Build player object for template
        player = {
            'name': player_data['name'],
            'tag': player_tag,
            'team_name': team_name,
            'region': teams_data[team_name]['region'],
            'matches': player_data['matches'],
            'wins': player_data['wins'],
            'star_player': player_data['star_player'],
            'favorite_brawler': favorite_brawler,
            'brawlers': player_data['brawlers']
        }
        
        return render_template('player.html',
                             user=session['discord_tag'],
                             player=player)



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
    
    _, data, _, _, _ = get_cached_data()
    players_data = data
    
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
    user_id = str(session.get('discord_id', 'test_user'))
    user_prefs = user_settings.get(user_id, {})
    view_mode = user_prefs.get('view_mode', 'season')
    
    if view_mode != 'offseason':
        return "This feature is only available in off-season mode", 404
    
    # Get cached trios
    cached_result = get_cached_trios()
    if cached_result is None:
        return "Error loading trio data", 500
    
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
                         user=session['discord_tag'],
                         trios=unique_trios,
                         sort_by=sort_by,
                         current_region=region,
                         zip=zip)



@app.route('/players')
def players_page():
    """Players overview page - only available in offseason mode"""
    _, data, _, _, _ = get_cached_data()
    
    # Get user's view mode
    user_settings = load_json(USER_SETTINGS_FILE)
    user_id = str(session.get('discord_id', 'test_user'))
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
                         user=session['discord_tag'],
                         players=players_list)


@app.route('/maps/<map_name>')
def map_detail_page(map_name):
    """Detailed map statistics page"""
    try:
        # Convert URL format back to display format
        map_display = map_name.replace('_', ' ').title()
        
        
        matches_df, data, _, _, _ = get_cached_data()
        
        if data is None or not data:
            return "Error loading data", 500
        
        # Get user's view mode
        user_settings = load_json(USER_SETTINGS_FILE)
        user_id = str(session.get('discord_id', 'test_user'))
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
        
        
        return render_template('map_detail.html',
                            user=session.get('discord_tag', 'Unknown'),
                            map_name=map_display,
                            mode_name=map_stats['mode'],
                            total_games=map_stats['total_games'],
                            total_brawlers=len(map_stats['brawlers']),
                            best_brawlers=best_brawlers,
                            total_picks=total_picks)
    
    except Exception as e:
        print(f"❌ MAP DETAIL ERROR: {e}")
        import traceback
        traceback.print_exc()
        return f"Map detail error: {e}", 500

@app.route('/brawler/<brawler_name>/mode/<mode_name>')
def brawler_mode_page(brawler_name, mode_name):
    """Brawler performance in a specific mode"""
    try:
        matches_df, data, region_stats, _, _ = get_cached_data()
        
        
        if data is None or not data:
            return "Error loading data", 500
        
        # Convert URL format to display format
        mode_display = mode_name.replace('_', ' ').title()
        
        # Get user's view mode
        user_settings = load_json(USER_SETTINGS_FILE)
        user_id = str(session.get('discord_id', 'test_user'))
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
        
        # Get MODE-SPECIFIC synergies
        best_teammates, best_matchups, worst_matchups = get_brawler_synergies_filtered(
            brawler_name, 
            mode_filter=mode_name
        )
        
        return render_template('brawler_mode.html',
                             user=session.get('discord_tag', 'Unknown'),
                             brawler_name=brawler_name,
                             mode_name=mode_display,
                             stats=brawler_mode_stats,
                             overall_winrate=overall_winrate,
                             maps=maps,
                             best_teammates=best_teammates[:10],
                             best_matchups=best_matchups,
                             worst_matchups=worst_matchups)
    
    except Exception as e:
        print(f"❌ BRAWLER-MODE ERROR: {e}")
        import traceback
        traceback.print_exc()
        return f"Brawler-mode error: {e}", 500


@app.route('/brawler/<brawler_name>/map/<map_name>')
def brawler_map_page(brawler_name, map_name):
    """Brawler performance on a specific map"""
    try:
        matches_df, data, region_stats, _, _ = get_cached_data()
        
        
        if data is None or not data:
            return "Error loading data", 500
        
        # Convert URL format to display format
        map_display = map_name.replace('_', ' ').title()
        
        # Get user's view mode
        user_settings = load_json(USER_SETTINGS_FILE)
        user_id = str(session.get('discord_id', 'test_user'))
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
        
        best_teammates, best_matchups, worst_matchups = get_brawler_synergies_filtered(
            brawler_name,
            map_filter=map_name
        )
        
        return render_template('brawler_map.html',
                             user=session.get('discord_tag', 'Unknown'),
                             brawler_name=brawler_name,
                             map_name=map_display,
                             mode_name=brawler_map_stats['mode'],
                             stats=brawler_map_stats,
                             overall_winrate=overall_winrate,
                             best_teammates=best_teammates[:10],
                             best_matchups=best_matchups,
                             worst_matchups=worst_matchups)
    
    except Exception as e:
        print(f"❌ BRAWLER-MAP ERROR: {e}")
        import traceback
        traceback.print_exc()
        return f"Brawler-map error: {e}", 500



@app.route('/modes')
def modes_overview():
    """Overview page for all game modes"""
    _, teams_data, _, mode_stats, _ = get_cached_data()
    
    if not teams_data:
        return "Error loading data", 500
    
    # Collect comprehensive mode statistics
    modes_data = defaultdict(lambda: {
        'total_games': 0,
        'maps': set(),
        'brawlers': defaultdict(lambda: {'picks': 0, 'wins': 0})
    })
    
    for team_name, team in teams_data.items():
        for mode_name, mode_data in team['modes'].items():
            if mode_name not in VALID_MODES:
                continue
            
            # Count games
            modes_data[mode_name]['total_games'] += mode_data.get('matches', 0)
            
            # Track maps
            for map_name in mode_data.get('maps', {}).keys():
                modes_data[mode_name]['maps'].add(map_name)
            
            # Track brawler stats
            for map_name, map_data in mode_data.get('maps', {}).items():
                for brawler, brawler_data in map_data.get('brawlers', {}).items():
                    modes_data[mode_name]['brawlers'][brawler]['picks'] += brawler_data['picks']
                    modes_data[mode_name]['brawlers'][brawler]['wins'] += brawler_data['wins']
    
    # Build modes list with stats
    modes_list = []
    for mode_name, data in modes_data.items():
        # Find top brawler for this mode using meta score (win_rate * pick_rate)
        top_brawler = None
        if data['brawlers']:
            # Calculate total picks for this mode
            total_picks = sum(b['picks'] for b in data['brawlers'].values())
            
            # Calculate meta score for each brawler
            brawler_scores = []
            for brawler_name, brawler_data in data['brawlers'].items():
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
            'total_games': data['total_games'],
            'total_maps': len(data['maps']),
            'total_brawlers': len(data['brawlers']),
            'top_brawler': top_brawler
        })
    
    # Sort by total games
    modes_list.sort(key=lambda x: x['total_games'], reverse=True)
    
    return render_template('modes_overview.html',
                         user=session['discord_tag'],
                         modes=modes_list)



@app.route('/team/<team_name>/brawler/<brawler_name>')
def team_brawler_page(team_name, brawler_name):
    _, teams_data, _, _, _ = get_cached_data()
    
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
    
    # Get player stats for this brawler
    player_stats = {}
    for player_tag, player_data in team['players'].items():
        if brawler_name in player_data['brawlers']:
            player_stats[player_tag] = {
                'name': player_data['name'],
                'picks': player_data['brawlers'][brawler_name]['picks'],
                'wins': player_data['brawlers'][brawler_name]['wins']
            }
    
    return render_template('team_brawler.html',
                         user=session['discord_tag'],
                         team_name=team_name,
                         team=team,
                         brawler_name=brawler_name,
                         brawler_data=brawler_data,
                         mode_stats=mode_stats,
                         player_stats=player_stats)

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
    ensure_roster_files_exist()
    
    app.run(host='0.0.0.0', port=8080, debug=True)