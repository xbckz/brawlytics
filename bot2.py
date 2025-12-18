"""
Brawl Stars Match Analyzer - Discord Bot with Excel Data Reading
Install: pip install discord.py pandas openpyxl python-dotenv

DATA SOURCE: Reads from 'matches.xlsx' in the same folder as this script
The Excel file should have columns like:
- team1_name, team1_region, team2_name, team2_region
- team1_player1, team1_player1_tag, team1_player1_brawler (and player 2, 3)
- team2_player1, team2_player1_tag, team2_player1_brawler (and player 2, 3)
- winner, mode, map, star_player_tag

IMAGES: Place brawler and map images in these folders:
- ./static/images/brawlers/  (e.g., spike.png, colt.png)
- ./static/images/maps/      (e.g., gem_grab_undermine.png)
File names should be lowercase with spaces replaced by underscores
"""

import discord
from discord.ext import commands, tasks
from discord.ui import Button, View, Select
import pandas as pd
import os
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv
import subprocess
import sys
from PIL import Image, ImageDraw, ImageFont
import io

from config import WEB_SERVER_URL
import secrets
import json

from schedule_commands import setup_schedule

from functools import lru_cache
import numpy as np

from storage_helper import (
    save_tokens, load_tokens,
    save_authorized_users, load_authorized_users,
    save_matches
)

load_dotenv()

# Configuration
CONFIG = {
    'DISCORD_TOKEN': os.getenv('DISCORD_TOKEN', 'YOUR_DISCORD_BOT_TOKEN'),
    'MATCHES_FILE': 'matches.xlsx',
    'CHECK_INTERVAL_MINUTES': 5,
    'REGIONS': ['NA', 'EU', 'LATAM', 'EA', 'SEA'],
    'MODES': ['Gem Grab', 'Brawl Ball', 'Heist', 'Bounty', 'Knockout', 'Hot Zone'],
    'BRAWLER_IMAGES_DIR': './static/images/brawlers/',
    'MAP_IMAGES_DIR': './static/images/maps/'
}


VALID_MODES = {'Gem Grab', 'Brawl Ball', 'Heist', 'Bounty', 'Knockout', 'Hot Zone'}

TOKENS_FILE = 'data/tokens.json'
AUTHORIZED_USERS_FILE = 'data/authorized_users.json'

teams_data = {}
region_stats = {}

# OFF-SEASON MODE DATA  
players_data = {}  # Structure: {player_tag: {name, region, stats...}}
cached_trios = {}

matches_df = None
original_matches_df = None
filter_start_date = None
filter_end_date = None

load_process = None

# Bot setup
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)

BOT_MODE_FILE = 'data/bot_mode.json'


@lru_cache(maxsize=10000)
def normalize_tag(tag):
    """Cached tag normalization"""
    if not tag or tag == 'NAN':
        return None
    tag = str(tag).strip().upper().replace('0', 'O')
    if not tag.startswith('#'):
        tag = '#' + tag
    return tag


def load_bot_mode():
    """Load current bot mode (season or offseason)"""
    if not os.path.exists(BOT_MODE_FILE):
        os.makedirs(os.path.dirname(BOT_MODE_FILE), exist_ok=True)
        save_bot_mode('season')  # Default to season mode
        return 'season'
    
    try:
        with open(BOT_MODE_FILE, 'r') as f:
            data = json.load(f)
            return data.get('mode', 'season')
    except:
        return 'season'

def save_bot_mode(mode):
    """Save bot mode"""
    os.makedirs(os.path.dirname(BOT_MODE_FILE), exist_ok=True)
    with open(BOT_MODE_FILE, 'w') as f:
        json.dump({
            'mode': mode,
            'updated_at': datetime.now().isoformat()
        }, f, indent=2)

def get_config_for_mode():
    """Get file paths based on current mode"""
    mode = load_bot_mode()
    
    if mode == 'offseason':
        return {
            'MATCHES_FILE': 'matches_off.xlsx',
            'TEAMS_FILE': 'players_off.xlsx',  # Changed: uses players file for off-season
            'MODE_NAME': 'Off Season',
            'MODE_EMOJI': '🏖️',
            'IS_PLAYER_MODE': True
        }
    else:  # season
        return {
            'MATCHES_FILE': 'matches.xlsx',
            'TEAMS_FILE': 'teams.xlsx',
            'MODE_NAME': 'Season',
            'MODE_EMOJI': '🏆',
            'IS_PLAYER_MODE': False
        }

# Update the CONFIG dictionary to be dynamic
def get_matches_file():
    """Get the appropriate matches file for current mode"""
    mode_config = get_config_for_mode()
    return mode_config['MATCHES_FILE']

def get_teams_file():
    """Get the appropriate teams file for current mode"""
    mode_config = get_config_for_mode()
    return mode_config['TEAMS_FILE']

def is_admin():
    """Decorator to check if user is the admin"""
    async def predicate(ctx):
        return str(ctx.author.id) == "606751416261935123"
    return commands.check(predicate)

def is_authorized():
    """Decorator to check if user is authorized (paid subscriber)"""
    async def predicate(ctx):
        user_id = str(ctx.author.id)
        
        # Admin always has access
        if user_id == "606751416261935123":
            return True
        
        # Check if user is in authorized list
        if not is_user_authorized(user_id):
            embed = discord.Embed(
                title="❌ Access Denied",
                description="You need to be authorized to use this bot.",
                color=discord.Color.red()
            )
            embed.add_field(
                name="How to get access",
                value="Contact @xiaku to get authorized access.",
                inline=False
            )
            await ctx.send(embed=embed, delete_after=10)
            return False
        
        # Check if subscription has expired
        authorized = load_json(AUTHORIZED_USERS_FILE)
        user_data = authorized.get(user_id)
        
        if user_data and user_data.get('expires_at'):
            expiration_date = pd.to_datetime(user_data['expires_at'])
            if pd.Timestamp.now() > expiration_date:
                embed = discord.Embed(
                    title="⚠️ Subscription Expired",
                    description="Your subscription has expired.",
                    color=discord.Color.orange()
                )
                embed.add_field(
                    name="Expired on",
                    value=expiration_date.strftime('%Y-%m-%d'),
                    inline=True
                )
                embed.add_field(
                    name="Renew access",
                    value="Contact the @xiaku to renew your subscription.",
                    inline=False
                )
                await ctx.send(embed=embed, delete_after=15)
                return False
        
        return True
    
    return commands.check(predicate)

def load_json(filepath):
    """Load JSON file, create if doesn't exist"""
    if not os.path.exists(filepath):
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w') as f:
            json.dump({}, f)
        return {}
    
    with open(filepath, 'r') as f:
        return json.load(f)

def save_json(filepath, data):
    """Save data to JSON file"""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)

def is_user_authorized(discord_id):
    """Check if user is authorized (paid subscriber)"""
    authorized = load_json(AUTHORIZED_USERS_FILE)
    return str(discord_id) in authorized

def generate_access_token(discord_id, discord_tag):
    """Generate a unique access token for user"""
    token = secrets.token_urlsafe(32)
    
    tokens = load_json(TOKENS_FILE)
    tokens[token] = {
        'discord_id': str(discord_id),
        'discord_tag': discord_tag,
        'created_at': datetime.now().isoformat(),
        'used': False
    }
    save_json(TOKENS_FILE, tokens)
    
    return token


def get_brawler_image(brawler_name):
    """Get the image file for a brawler if it exists"""
    if not os.path.exists(CONFIG['BRAWLER_IMAGES_DIR']):
        return None
    
    filename = brawler_name.lower().replace(' ', '_').replace('-', '_')
    
    for ext in ['.png', '.jpg', '.jpeg', '.webp']:
        filepath = os.path.join(CONFIG['BRAWLER_IMAGES_DIR'], f"{filename}{ext}")
        if os.path.exists(filepath):
            return filepath
    
    return None

def generate_player_stats_image(team_name, player_data, team):
    """Generate a visual player stats card with brawler icons and color-coded stats"""
    
    # Get brawler stats sorted by picks
    brawler_stats = sorted(
        player_data['brawlers'].items(),
        key=lambda x: x[1]['picks'],
        reverse=True
    )
    
    if not brawler_stats:
        return None
    
    # Image settings
    BRAWLER_SIZE = 70
    PADDING = 15
    HEADER_HEIGHT = 150
    ROW_HEIGHT = BRAWLER_SIZE + 50
    COLS = 5  # Brawlers per row
    
    rows = (len(brawler_stats) + COLS - 1) // COLS
    
    img_width = (BRAWLER_SIZE + PADDING) * COLS + PADDING * 2
    img_height = HEADER_HEIGHT + (ROW_HEIGHT * rows) + PADDING * 2
    
    # Create image
    img = Image.new('RGB', (img_width, img_height), color=(25, 25, 35))
    draw = ImageDraw.Draw(img)
    
    # Load fonts
    try:
        title_font = ImageFont.truetype("arial.ttf", 32)
        subtitle_font = ImageFont.truetype("arial.ttf", 18)
        stat_font = ImageFont.truetype("arial.ttf", 16)
        small_font = ImageFont.truetype("arial.ttf", 12)
    except:
        title_font = ImageFont.load_default()
        subtitle_font = ImageFont.load_default()
        stat_font = ImageFont.load_default()
        small_font = ImageFont.load_default()
    
    # Calculate overall stats
    p_wr = (player_data['wins'] / player_data['matches'] * 100) if player_data['matches'] > 0 else 0
    total_stars = sum(p['star_player'] for p in team['players'].values())
    star_rate = (player_data['star_player'] / total_stars * 100) if total_stars > 0 else 0
    total_picks = sum(d['picks'] for d in player_data['brawlers'].values())
    
    # Draw header background
    draw.rectangle([(0, 0), (img_width, HEADER_HEIGHT)], fill=(35, 35, 45))
    
    # Draw player name
    name_bbox = draw.textbbox((0, 0), player_data['name'], font=title_font)
    name_width = name_bbox[2] - name_bbox[0]
    draw.text(((img_width - name_width) // 2, 20), player_data['name'], fill=(255, 255, 255), font=title_font)
    
    # Draw team name
    team_text = f"{team_name} • {team['region']}"
    team_bbox = draw.textbbox((0, 0), team_text, font=subtitle_font)
    team_width = team_bbox[2] - team_bbox[0]
    draw.text(((img_width - team_width) // 2, 60), team_text, fill=(180, 180, 200), font=subtitle_font)
    
    # Draw overall stats
    stats_y = 95
    stats_text = f"Matches: {player_data['matches']}  •  Win Rate: {p_wr:.1f}%  •  Star Player: {star_rate:.1f}%"
    stats_bbox = draw.textbbox((0, 0), stats_text, font=small_font)
    stats_width = stats_bbox[2] - stats_bbox[0]
    draw.text(((img_width - stats_width) // 2, stats_y), stats_text, fill=(150, 200, 255), font=small_font)
    
    # Draw divider line
    draw.line([(PADDING, HEADER_HEIGHT - 10), (img_width - PADDING, HEADER_HEIGHT - 10)], fill=(60, 60, 80), width=2)
    
    # Helper function to get color based on win rate
    def get_wr_color(wr):
        if wr >= 65:
            return (34, 197, 94)    # #22c55e - S tier (green)
        elif wr >= 57.5:
            return (132, 204, 22)   # #84cc16 - A tier (lime)
        elif wr >= 50:
            return (234, 179, 8)    # #eab308 - B tier (yellow)
        elif wr >= 42.5:
            return (249, 115, 22)   # #f97316 - C tier (orange)
        elif wr >= 35:
            return (239, 68, 68)    # #ef4444 - D tier (red)
        else:
            return (153, 27, 27)    # #991b1b - F tier (dark red)
    
    # Draw brawlers in grid
    y_offset = HEADER_HEIGHT + PADDING
    
    for idx, (brawler, data) in enumerate(brawler_stats):
        row = idx // COLS
        col = idx % COLS
        
        x = PADDING + col * (BRAWLER_SIZE + PADDING)
        y = y_offset + row * ROW_HEIGHT
        
        # Calculate stats
        b_wr = (data['wins'] / data['picks'] * 100) if data['picks'] > 0 else 0
        pick_rate = (data['picks'] / total_picks * 100) if total_picks > 0 else 0
        
        # Get win rate color
        wr_color = get_wr_color(b_wr)
        
        # Draw background box with win rate color border
        box_padding = 3
        draw.rectangle(
            [(x - box_padding, y - box_padding), 
             (x + BRAWLER_SIZE + box_padding, y + BRAWLER_SIZE + 35)],
            fill=(40, 40, 50),
            outline=wr_color,
            width=3
        )
        
        # Try to load and draw brawler image
        brawler_img_path = get_brawler_image(brawler)
        if brawler_img_path and os.path.exists(brawler_img_path):
            try:
                brawler_img = Image.open(brawler_img_path)
                brawler_img = brawler_img.resize((BRAWLER_SIZE, BRAWLER_SIZE), Image.Resampling.LANCZOS)
                img.paste(brawler_img, (x, y))
            except:
                # Draw placeholder
                draw.rectangle(
                    [(x, y), (x + BRAWLER_SIZE, y + BRAWLER_SIZE)],
                    fill=(60, 60, 70)
                )
        else:
            # Draw placeholder
            draw.rectangle(
                [(x, y), (x + BRAWLER_SIZE, y + BRAWLER_SIZE)],
                fill=(60, 60, 70)
            )
        
        # Draw brawler name (truncated if needed)
        name_display = brawler if len(brawler) <= 10 else brawler[:8] + ".."
        name_bbox = draw.textbbox((0, 0), name_display, font=small_font)
        name_width = name_bbox[2] - name_bbox[0]
        draw.text(
            (x + (BRAWLER_SIZE - name_width) // 2, y + BRAWLER_SIZE + 3),
            name_display,
            fill=(255, 255, 255),
            font=small_font
        )
        
        # Draw stats below name
        stats = f"{data['picks']} • {b_wr:.0f}%"
        stats_bbox = draw.textbbox((0, 0), stats, font=small_font)
        stats_width = stats_bbox[2] - stats_bbox[0]
        draw.text(
            (x + (BRAWLER_SIZE - stats_width) // 2, y + BRAWLER_SIZE + 18),
            stats,
            fill=wr_color,
            font=small_font
        )
    
    # Add legend at bottom
    legend_y = img_height - 25
    legend_text = "Color: Win Rate  •  Format: Picks • WR%"
    legend_bbox = draw.textbbox((0, 0), legend_text, font=small_font)
    legend_width = legend_bbox[2] - legend_bbox[0]
    draw.text(
        ((img_width - legend_width) // 2, legend_y),
        legend_text,
        fill=(120, 120, 140),
        font=small_font
    )
    
    # Save to BytesIO
    img_bytes = io.BytesIO()
    img.save(img_bytes, format='PNG')
    img_bytes.seek(0)
    
    return img_bytes


def generate_meta_tier_image(region='ALL', mode=None):
    """
    Generate a tier list image for meta brawlers
    Works for both season and off-season modes
    Returns: BytesIO object containing the PNG image
    """
    mode_type = load_bot_mode()
    
    # Collect brawler stats
    brawler_picks = defaultdict(int)
    brawler_wins = defaultdict(int)
    
    if mode_type == 'offseason':
        # OFF-SEASON MODE: Filter by region if specified
        if region != 'ALL':
            # Filter to only players from this region
            for player_tag, player_data in players_data.items():
                if player_data['region'] != region:
                    continue  # Skip players not in this region
                
                if mode and mode != 'ALL':
                    if mode not in VALID_MODES:
                        return None
                    # Specific mode - get from player's mode stats
                    if mode in player_data['modes']:
                        for map_name, map_stats in player_data['modes'][mode]['maps'].items():
                            for brawler, data in map_stats['brawlers'].items():
                                brawler_picks[brawler] += data['picks']
                                brawler_wins[brawler] += data['wins']
                else:
                    # All modes - get from player's overall brawler stats
                    for brawler, data in player_data['brawlers'].items():
                        brawler_picks[brawler] += data['picks']
                        brawler_wins[brawler] += data['wins']
        else:
            # ALL regions - use global stats
            global_stats = region_stats.get('_global_brawlers', {})
            mode_map_data = region_stats.get('_mode_map_stats', {})
            
            if mode and mode != 'ALL':
                # Filter by specific mode
                if mode in mode_map_data:
                    for map_name, map_stats in mode_map_data[mode].items():
                        for brawler, data in map_stats['brawlers'].items():
                            brawler_picks[brawler] += data['picks']
                            brawler_wins[brawler] += data['wins']
            else:
                # All modes
                for brawler, data in global_stats.items():
                    brawler_picks[brawler] = data['picks']
                    brawler_wins[brawler] = data['wins']
    
    else:
        # SEASON MODE: Use team data
        relevant_teams = teams_data.items()
        if region != 'ALL':
            relevant_teams = [(name, data) for name, data in teams_data.items() if data['region'] == region]
        
        # Aggregate brawler data
        for team_name, team_data in relevant_teams:
            for mode_name, mode_data in team_data['modes'].items():
                if mode_name not in VALID_MODES:
                    continue
                
                # If specific mode requested, filter
                if mode and mode != mode_name:
                    continue
                
                for map_name, map_data in mode_data['maps'].items():
                    for brawler, brawler_data in map_data['brawlers'].items():
                        brawler_picks[brawler] += brawler_data['picks']
                        brawler_wins[brawler] += brawler_data['wins']
    
    # Calculate meta scores (WR × Pick Rate)
    total_picks = sum(brawler_picks.values())
    if total_picks == 0:
        return None
    
    meta_scores = []
    for brawler in brawler_picks:
        if brawler_picks[brawler] >= 3:  # Minimum picks
            pick_rate = (brawler_picks[brawler] / total_picks) * 100
            win_rate = (brawler_wins[brawler] / brawler_picks[brawler]) * 100
            meta_score = (win_rate * pick_rate) / 100  # Normalized score
            
            meta_scores.append({
                'brawler': brawler,
                'score': meta_score,
                'pick_rate': pick_rate,
                'win_rate': win_rate,
                'picks': brawler_picks[brawler]
            })
    
    if not meta_scores:
        return None
    
    # Sort by meta score
    meta_scores.sort(key=lambda x: x['score'], reverse=True)
    
    # Determine tier thresholds based on score distribution (MORE LENIENT)
    max_score = meta_scores[0]['score']
    min_score = meta_scores[-1]['score']
    score_range = max_score - min_score
    
    
    tiers = assign_brawlers_to_tiers_unified(meta_scores)
    if not tiers:
        return None

    
    
    # Image settings
    BRAWLER_IMG_SIZE = 80
    PADDING = 20
    TIER_LABEL_WIDTH = 80
    HEADER_HEIGHT = 180  # Even more space for bigger fonts
    ROW_HEIGHT = BRAWLER_IMG_SIZE + PADDING * 2
    MAX_BRAWLERS_PER_ROW = 14  # Maximum brawlers per row
    
    # Calculate total rows needed
    total_rows = 0
    for tier in tiers.values():
        if tier['brawlers']:
            rows_for_tier = (len(tier['brawlers']) + MAX_BRAWLERS_PER_ROW - 1) // MAX_BRAWLERS_PER_ROW
            total_rows += rows_for_tier
    
    # Calculate image dimensions
    img_width = TIER_LABEL_WIDTH + (BRAWLER_IMG_SIZE + PADDING) * MAX_BRAWLERS_PER_ROW + PADDING
    img_height = HEADER_HEIGHT + (ROW_HEIGHT * total_rows) + PADDING + 50  # Extra space for legend
    
    # Create image
    img = Image.new('RGB', (img_width, img_height), color=(30, 30, 40))
    draw = ImageDraw.Draw(img)
    
    # Try to load fonts (MUCH LARGER SIZES)
    try:
        title_font = ImageFont.truetype("arial.ttf", 55)
        subtitle_font = ImageFont.truetype("arial.ttf", 30)  # Much bigger
        tier_font = ImageFont.truetype("arial.ttf", 27)
        brawler_font = ImageFont.truetype("arial.ttf", 14)
        stat_font = ImageFont.truetype("arial.ttf", 11)
    except:
        title_font = ImageFont.load_default()
        subtitle_font = ImageFont.load_default()
        tier_font = ImageFont.load_default()
        brawler_font = ImageFont.load_default()
        stat_font = ImageFont.load_default()
    
    # Draw header
    if mode_type == 'offseason':
        # Fix the region text to show actual region name
        region_text = "Global Stats" if region == 'ALL' else f"{region} Region"
    else:
        region_text = "All Regions" if region == 'ALL' else f"{region} Region"
    
    mode_text = f" - {mode}" if mode else ""
    title = f"Meta Tier List"
    subtitle = f"{region_text}{mode_text}"
    
    # Title
    title_bbox = draw.textbbox((0, 0), title, font=title_font)
    title_width = title_bbox[2] - title_bbox[0]
    draw.text(((img_width - title_width) // 2, 25), title, fill=(255, 255, 255), font=title_font)
    
    # Subtitle
    subtitle_bbox = draw.textbbox((0, 0), subtitle, font=subtitle_font)
    subtitle_width = subtitle_bbox[2] - subtitle_bbox[0]
    draw.text(((img_width - subtitle_width) // 2, 115), subtitle, fill=(200, 200, 200), font=subtitle_font)
    
    # Draw tiers
    y_offset = HEADER_HEIGHT
    
    for tier_name, tier_data in tiers.items():
        if not tier_data['brawlers']:
            continue
        
        # Split brawlers into rows of MAX_BRAWLERS_PER_ROW
        brawler_rows = []
        for i in range(0, len(tier_data['brawlers']), MAX_BRAWLERS_PER_ROW):
            brawler_rows.append(tier_data['brawlers'][i:i + MAX_BRAWLERS_PER_ROW])
        
        tier_total_height = ROW_HEIGHT * len(brawler_rows)
        
        # Draw tier background for all rows
        draw.rectangle(
            [(0, y_offset), (img_width, y_offset + tier_total_height)],
            fill=(40, 40, 50),
            outline=(60, 60, 70),
            width=2
        )
        
        # Draw tier label (spans all rows)
        draw.rectangle(
            [(0, y_offset), (TIER_LABEL_WIDTH, y_offset + tier_total_height)],
            fill=tier_data['color']
        )
        
        tier_bbox = draw.textbbox((0, 0), tier_name, font=tier_font)
        tier_text_width = tier_bbox[2] - tier_bbox[0]
        tier_text_height = tier_bbox[3] - tier_bbox[1]
        draw.text(
            ((TIER_LABEL_WIDTH - tier_text_width) // 2, y_offset + (tier_total_height - tier_text_height) // 2),
            tier_name,
            fill=(0, 0, 0),
            font=tier_font
        )
        
        # Draw brawlers row by row
        current_row_y = y_offset
        for brawler_row in brawler_rows:
            x_offset = TIER_LABEL_WIDTH + PADDING
            
            for brawler_data in brawler_row:
                brawler_name = brawler_data['brawler']
                
                # Try to load brawler image
                brawler_img_path = get_brawler_image(brawler_name)
                
                if brawler_img_path and os.path.exists(brawler_img_path):
                    try:
                        brawler_img = Image.open(brawler_img_path)
                        brawler_img = brawler_img.resize((BRAWLER_IMG_SIZE, BRAWLER_IMG_SIZE), Image.Resampling.LANCZOS)
                        img.paste(brawler_img, (x_offset, current_row_y + PADDING))
                    except:
                        # Draw placeholder if image fails
                        draw.rectangle(
                            [(x_offset, current_row_y + PADDING), 
                             (x_offset + BRAWLER_IMG_SIZE, current_row_y + PADDING + BRAWLER_IMG_SIZE)],
                            fill=(80, 80, 80),
                            outline=(120, 120, 120),
                            width=2
                        )
                else:
                    # Draw placeholder rectangle
                    draw.rectangle(
                        [(x_offset, current_row_y + PADDING), 
                         (x_offset + BRAWLER_IMG_SIZE, current_row_y + PADDING + BRAWLER_IMG_SIZE)],
                        fill=(80, 80, 80),
                        outline=(120, 120, 120),
                        width=2
                    )
                
                # Draw brawler name (truncate if too long)
                name_display = brawler_name if len(brawler_name) <= 10 else brawler_name[:8] + ".."
                name_bbox = draw.textbbox((0, 0), name_display, font=brawler_font)
                name_width = name_bbox[2] - name_bbox[0]
                draw.text(
                    (x_offset + (BRAWLER_IMG_SIZE - name_width) // 2, current_row_y + PADDING + BRAWLER_IMG_SIZE + 3),
                    name_display,
                    fill=(255, 255, 255),
                    font=brawler_font
                )
                
                x_offset += BRAWLER_IMG_SIZE + PADDING
            
            current_row_y += ROW_HEIGHT
        
        y_offset = current_row_y
    
    # Add legend at bottom
    legend_y = y_offset + PADDING
    legend_text = "Stats: Win Rate | Pick Rate  •  Score = WR x Pick Rate"
    legend_bbox = draw.textbbox((0, 0), legend_text, font=stat_font)
    legend_width = legend_bbox[2] - legend_bbox[0]
    
    draw.text(
        ((img_width - legend_width) // 2, legend_y),
        legend_text,
        fill=(150, 150, 150),
        font=stat_font
    )
    
    # Save to BytesIO
    img_bytes = io.BytesIO()
    img.save(img_bytes, format='PNG')
    img_bytes.seek(0)
    
    return img_bytes

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





def generate_mode_stats_image_offseason(mode: str, sort_by: str = 'meta'):
    """
    Generate a comprehensive stats image for a mode in off-season
    Dynamic layout that adapts to number of brawlers - fills screen optimally
    Pattern: 2 rows with mode image in center, then 3-column rows below (max 8 rows)
    
    sort_by options: 'meta' (WR × Pick), 'picks', 'winrate'
    """
    mode_map_data = region_stats.get('_mode_map_stats', {})
    mode_data = mode_map_data.get(mode, {})
    
    if not mode_data:
        return None
    
    # Aggregate brawler stats across all maps
    brawler_picks = defaultdict(int)
    brawler_wins = defaultdict(int)
    total_matches = sum(map_stats['matches'] for map_stats in mode_data.values())
    
    for map_name, map_stats in mode_data.items():
        for brawler, data in map_stats['brawlers'].items():
            brawler_picks[brawler] += data['picks']
            brawler_wins[brawler] += data['wins']
    
    if not brawler_picks:
        return None
    
    # Sort by meta score (WR × Pick Rate)
    total_picks = sum(brawler_picks.values())
    meta_scores = []
    
    for brawler in brawler_picks:
        if brawler_picks[brawler] >= 1:
            pick_rate = (brawler_picks[brawler] / total_picks) * 100
            win_rate = (brawler_wins[brawler] / brawler_picks[brawler]) * 100
            meta_score = (win_rate * pick_rate) / 100
            
            meta_scores.append({
                'brawler': brawler,
                'score': meta_score,
                'pick_rate': pick_rate,
                'win_rate': win_rate,
                'picks': brawler_picks[brawler],
                'wins': brawler_wins[brawler]
            })
    
    # Sort based on selected criteria
    if sort_by == 'picks':
        meta_scores.sort(key=lambda x: x['picks'], reverse=True)
    elif sort_by == 'winrate':
        meta_scores.sort(key=lambda x: x['win_rate'], reverse=True)
    else:  # 'meta' - default
        meta_scores.sort(key=lambda x: x['score'], reverse=True)
    
    # Rest of the function stays the same...
    total_brawlers = len(meta_scores)
    
    # Image settings - optimized for 9:16 ratio
    BRAWLER_SIZE = 70
    STAT_WIDTH = 125
    ROW_HEIGHT = 80
    COLUMN_PADDING = 15
    PADDING = 12
    HEADER_HEIGHT = 100  # Smaller header
    MODE_IMAGE_SIZE = 180
    SIDE_MARGIN = 20
    FOOTER_HEIGHT = 60
    MAX_BOTTOM_ROWS = 8  # Limit bottom rows to 8
    
    # ... (rest of the image generation code remains the same)
    
    # Layout: 2 rows with mode (4 brawlers), then 3-column rows below
    TOP_ROWS = 2  # Rows with mode image in center
    BOTTOM_COLS = 3  # 3 columns for rows below mode
    
    # First 4 brawlers go around the mode image (2 on left, 2 on right)
    brawlers_top_section = min(4, total_brawlers)
    remaining_brawlers = max(0, total_brawlers - brawlers_top_section)
    
    # Calculate how many rows needed for bottom section (3 columns), capped at 8 rows
    bottom_rows = min((remaining_brawlers + BOTTOM_COLS - 1) // BOTTOM_COLS, MAX_BOTTOM_ROWS)
    
    # Maximum brawlers to display: 4 top + (8 rows × 3 columns) = 28 total
    max_brawlers_to_display = brawlers_top_section + (bottom_rows * BOTTOM_COLS)
    
    # Calculate total height
    top_section_height = TOP_ROWS * ROW_HEIGHT + 20
    bottom_section_height = bottom_rows * ROW_HEIGHT + (20 if bottom_rows > 0 else 0)
    
    img_height = HEADER_HEIGHT + top_section_height + bottom_section_height + FOOTER_HEIGHT
    
    # 9:16 aspect ratio - calculate width based on height
    img_width = int(img_height * 9 / 16)
    
    # Ensure minimum width for readability
    img_width = max(img_width, 720)
    
    # Create image with BLACK background
    img = Image.new('RGB', (img_width, img_height), color=(0, 0, 0))
    draw = ImageDraw.Draw(img)
    
    # Load fonts
    try:
        title_font = ImageFont.truetype("arial.ttf", 50)  # Smaller title
        subtitle_font = ImageFont.truetype("arial.ttf", 20)
        stat_font = ImageFont.truetype("arial.ttf", 21)
        small_font = ImageFont.truetype("arial.ttf", 19)
        tiny_font = ImageFont.truetype("arial.ttf", 17)
        rank_font = ImageFont.truetype("arial.ttf", 16)  # Font for rank numbers
    except:
        title_font = ImageFont.load_default()
        subtitle_font = ImageFont.load_default()
        stat_font = ImageFont.load_default()
        small_font = ImageFont.load_default()
        tiny_font = ImageFont.load_default()
        rank_font = ImageFont.load_default()
    
    # === HEADER ===
    header_overlay = Image.new('RGBA', (img_width, HEADER_HEIGHT), (15, 15, 15, 240))
    img.paste(header_overlay, (0, 0), header_overlay)
    
    # Mode title with shadow
    title_bbox = draw.textbbox((0, 0), mode, font=title_font)
    title_width = title_bbox[2] - title_bbox[0]
    title_x = (img_width - title_width) // 2
    draw.text((title_x + 2, 18), mode, fill=(0, 0, 0, 180), font=title_font)
    draw.text((title_x, 16), mode, fill=(255, 255, 255), font=title_font)
    
    # Stats bar
    stats_y = 70
    stats_text = f"{total_matches} Matches • {total_picks} Picks • {len(meta_scores)} Brawlers"
    stats_bbox = draw.textbbox((0, 0), stats_text, font=tiny_font)
    stats_width = stats_bbox[2] - stats_bbox[0]
    draw.text(((img_width - stats_width) // 2, stats_y), stats_text, fill=(150, 200, 255), font=tiny_font)

    # === RED LINE SEPARATOR AFTER HEADER ===
    header_line_y = HEADER_HEIGHT - 6

    # Draw glow layers
    for i in range(5, 0, -1):
        glow_alpha = int(30 * (6 - i) / 5)
        glow_overlay = Image.new('RGBA', (img_width, img_height), (0, 0, 0, 0))
        glow_draw = ImageDraw.Draw(glow_overlay)
        glow_draw.rectangle(
            [(0, header_line_y - i), (img_width, header_line_y + 3 + i)],
            fill=(220, 50, 50, glow_alpha)
        )
        img.paste(Image.alpha_composite(img.convert('RGBA'), glow_overlay).convert('RGB'))

    # Draw main red line
   
    draw.rectangle([(0, header_line_y), (img_width, header_line_y + 3)], 
                fill=(220, 50, 50))
    
    # Helper function for WR color
    def get_wr_color(wr):
        if wr >= 70:
            return (50, 255, 100)
        elif wr >= 60:
            return (120, 255, 120)
        elif wr >= 50:
            return (255, 230, 100)
        elif wr >= 40:
            return (255, 170, 80)
        else:
            return (255, 80, 80)
    
    def draw_brawler_card(x, y, data, global_idx):
        """Helper function to draw a brawler card"""
        brawler = data['brawler']
        wr = data['win_rate']
        pr = data['pick_rate']
        picks = data['picks']
        wr_color = get_wr_color(wr)
        
        card_width = BRAWLER_SIZE + STAT_WIDTH + PADDING
        
        # Background card
        draw.rectangle(
            [(x - 4, y), (x + card_width + 4, y + BRAWLER_SIZE + 8)],
            fill=(25, 25, 30)
        )
        
        # Brawler image
        brawler_img_path = get_brawler_image(brawler)
        if brawler_img_path and os.path.exists(brawler_img_path):
            try:
                brawler_img = Image.open(brawler_img_path).convert('RGBA')
                brawler_img = brawler_img.resize((BRAWLER_SIZE, BRAWLER_SIZE), Image.Resampling.LANCZOS)
                img.paste(brawler_img, (x, y + 4), brawler_img)
            except:
                draw.rectangle([(x, y + 4), (x + BRAWLER_SIZE, y + BRAWLER_SIZE + 4)], 
                             fill=(40, 40, 45))
        else:
            draw.rectangle([(x, y + 4), (x + BRAWLER_SIZE, y + BRAWLER_SIZE + 4)], 
                         fill=(40, 40, 45))
        
        # Stats next to brawler
        stats_x = x + BRAWLER_SIZE + PADDING
        
        # Rank number and brawler name
        rank_text = f"#{global_idx + 1}"
        
        # Special colors for top 5
        if global_idx < 5:
            rank_colors = [
                (255, 215, 0), (192, 192, 192), (205, 127, 50),
                (100, 149, 237), (147, 112, 219)
            ]
            rank_color = rank_colors[global_idx]
        else:
            rank_color = (120, 120, 140)  # Gray for ranks 6+
        
        # Draw rank number
        draw.text((stats_x, y + 8), rank_text, fill=rank_color, font=rank_font)
        
        # Get rank text width to position brawler name next to it
        rank_bbox = draw.textbbox((0, 0), rank_text, font=rank_font)
        rank_width = rank_bbox[2] - rank_bbox[0]
        
        # Brawler name next to rank
        name_display = brawler if len(brawler) <= 9 else brawler[:7] + ".."
        draw.text((stats_x + rank_width + 6, y + 8), name_display, fill=(255, 255, 255), font=small_font)
        
        # Picks
        draw.text((stats_x, y + 35), f"{picks} picks", fill=(180, 180, 200), font=tiny_font)
        
        # WR and PR (draw separately for different colors)
        draw.text((stats_x, y + 54), f"{wr:.1f}%", fill=wr_color, font=tiny_font)
        wr_text_bbox = draw.textbbox((stats_x, y + 54), f"{wr:.1f}%", font=tiny_font)
        wr_text_width = wr_text_bbox[2] - wr_text_bbox[0]
        draw.text((stats_x + wr_text_width, y + 54), f" • {pr:.1f}%", fill=(255, 255, 255), font=tiny_font)
    
    # === CALCULATE COLUMN POSITIONS (shared by top and bottom) ===
    card_width = BRAWLER_SIZE + STAT_WIDTH + PADDING
    total_content_width = card_width * BOTTOM_COLS + COLUMN_PADDING * (BOTTOM_COLS - 1)
    left_edge = (img_width - total_content_width) // 2
    
    column_x_positions = [
        left_edge + i * (card_width + COLUMN_PADDING)
        for i in range(BOTTOM_COLS)
    ]
    
    # === TOP SECTION: MODE IMAGE WITH BRAWLERS ON SIDES ===
    content_start_y = HEADER_HEIGHT + 20
    
    # Mode image centered
    mode_img_x = (img_width - MODE_IMAGE_SIZE) // 2
    mode_img_y = content_start_y
    
    # Get mode image from modes folder
    mode_image_path = None
    modes_dir = './static/images/modes/'
    
    if os.path.exists(modes_dir):
        mode_clean = mode.lower().replace(' ', '_').replace('-', '_')
        for ext in ['.png', '.jpg', '.jpeg', '.webp']:
            filepath = os.path.join(modes_dir, f"{mode_clean}{ext}")
            if os.path.exists(filepath):
                mode_image_path = filepath
                break
    
    # Draw mode image with aspect ratio preserved
    if mode_image_path and os.path.exists(mode_image_path):
        try:
            mode_img = Image.open(mode_image_path).convert('RGBA')
            
            # Preserve aspect ratio - fit within MODE_IMAGE_SIZE box
            original_width, original_height = mode_img.size
            aspect_ratio = original_width / original_height
            
            if aspect_ratio > 1:
                # Width is larger
                new_width = MODE_IMAGE_SIZE
                new_height = int(MODE_IMAGE_SIZE / aspect_ratio)
            else:
                # Height is larger or square
                new_height = MODE_IMAGE_SIZE
                new_width = int(MODE_IMAGE_SIZE * aspect_ratio)
            
            mode_img = mode_img.resize((new_width, new_height), Image.Resampling.LANCZOS)
            
            # Center the resized image
            paste_x = mode_img_x + (MODE_IMAGE_SIZE - new_width) // 2
            paste_y = mode_img_y + (MODE_IMAGE_SIZE - new_height) // 2
            
            img.paste(mode_img, (paste_x, paste_y), mode_img)
        except Exception as e:
            print(f"Failed to load mode image: {e}")
    
    brawler_idx = 0
    
    # Left side - 2 brawlers (aligned with column 0)
    left_x = column_x_positions[0]
    for row in range(TOP_ROWS):
        if brawler_idx < total_brawlers:
            row_y = content_start_y + row * ROW_HEIGHT
            draw_brawler_card(left_x, row_y, meta_scores[brawler_idx], brawler_idx)
            brawler_idx += 1
    
    # Right side - 2 brawlers (aligned with column 2)
    right_x = column_x_positions[2]
    for row in range(TOP_ROWS):
        if brawler_idx < total_brawlers:
            row_y = content_start_y + row * ROW_HEIGHT
            draw_brawler_card(right_x, row_y, meta_scores[brawler_idx], brawler_idx)
            brawler_idx += 1
    
    # === BOTTOM SECTION: 3-COLUMN GRID (MAX 8 ROWS) ===
    if remaining_brawlers > 0 and brawler_idx < max_brawlers_to_display:
        bottom_start_y = content_start_y + top_section_height
        
        # Draw remaining brawlers in 3-column grid (up to max)
        for row in range(bottom_rows):
            for col in range(BOTTOM_COLS):
                if brawler_idx < total_brawlers and brawler_idx < max_brawlers_to_display:
                    row_y = bottom_start_y + row * ROW_HEIGHT
                    draw_brawler_card(column_x_positions[col], row_y, meta_scores[brawler_idx], brawler_idx)
                    brawler_idx += 1
    
    # === FOOTER ===
    footer_y = img_height - 50
    
    
    
    sort_text = {
        'meta': 'Meta Score (WR × Pick Rate)',
        'picks': 'Pick Rate',
        'winrate': 'Win Rate'
    }.get(sort_by, 'Meta Score')

    legend_text = f"Sorted by {sort_text}"
    legend_bbox = draw.textbbox((0, 0), legend_text, font=tiny_font)
    legend_width = legend_bbox[2] - legend_bbox[0]
    draw.text(
        ((img_width - legend_width) // 2, footer_y + 5),
        legend_text,
        fill=(120, 140, 180),
        font=tiny_font
    )
    
    # Save to BytesIO
    img_bytes = io.BytesIO()
    img.save(img_bytes, format='PNG', quality=95)
    img_bytes.seek(0)
    
    return img_bytes




def get_map_image(mode, map_name):
    """Get the image file for a map if it exists"""
    maps_dir = './static/images/maps/'
    
    if not os.path.exists(maps_dir):
        return None
    
    mode_clean = mode.lower().replace(' ', '_')
    map_clean = map_name.lower().replace(' ', '_').replace('-', '_')
    
    for name in [f"{mode_clean}_{map_clean}", map_clean]:
        for ext in ['.png', '.jpg', '.jpeg', '.webp']:
            filepath = os.path.join(maps_dir, f"{name}{ext}")
            if os.path.exists(filepath):
                return filepath
    
    return None

def apply_date_filter(start_date=None, end_date=None):
    """Apply date filter to matches data"""
    global matches_df, filter_start_date, filter_end_date, original_matches_df
    
    # Store original data if not already stored
    if original_matches_df is None:
        if matches_df is None:
            return False, "No data loaded"
        original_matches_df = matches_df.copy()
    
    # Reset to original data
    matches_df = original_matches_df.copy()
    
    if 'battle_time' not in matches_df.columns:
        return False, "No battle_time column found in data"
    
    # Convert to datetime FIRST
    try:
        matches_df['battle_time'] = pd.to_datetime(matches_df['battle_time'], utc=True)
    except Exception as e:
        return False, f"Error converting dates: {e}"
    
    # Apply filters
    if start_date:
        try:
            matches_df = matches_df[matches_df['battle_time'] >= start_date]
            filter_start_date = start_date
        except Exception as e:
            return False, f"Error filtering start date: {e}"
    
    if end_date:
        try:
            # Set end date to end of day (23:59:59)
            end_date = end_date.replace(hour=23, minute=59, second=59)
            matches_df = matches_df[matches_df['battle_time'] <= end_date]
            filter_end_date = end_date
        except Exception as e:
            return False, f"Error filtering end date: {e}"
    
    if len(matches_df) == 0:
        matches_df = original_matches_df.copy()
        filter_start_date = None
        filter_end_date = None
        return False, "No matches found in that date range"
    
    # Recalculate all stats with filtered data
    try:
        calculate_all_stats()
    except Exception as e:
        matches_df = original_matches_df.copy()
        filter_start_date = None
        filter_end_date = None
        return False, f"Error recalculating stats: {e}"
    
    return True, f"Filtered to {len(matches_df)} matches"


def load_matches_data():
    """Load matches from Excel file (last 30 days only)"""
    global matches_df, teams_data, region_stats, players_data
    
    matches_file = get_matches_file()
    
    if not os.path.exists(matches_file):
        print(f"{matches_file} not found!")
        return False
    
    try:
        df = pd.read_excel(matches_file)
        
        # Filter to last 30 days
        if 'battle_time' in df.columns:
            df['battle_time'] = pd.to_datetime(df['battle_time'], utc=True)
            cutoff_date = pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=30)
            df = df[df['battle_time'] >= cutoff_date]
            print(f"Filtered to matches after {cutoff_date.strftime('%Y-%m-%d')}")
        else:
            print("Warning: 'battle_time' column not found - using all matches")
        
        matches_df = df
        print(f"Loaded {len(matches_df)} matches from {matches_file}")
        
        # Call appropriate stats calculation based on mode
        mode = load_bot_mode()
        if mode == 'offseason':
            calculate_all_stats_offseason()
        else:
            calculate_all_stats()
        
        return True
    except Exception as e:
        print(f"Error loading Excel: {e}")
        return False


def cache_trios():
    """Cache trio statistics from matches_df"""
    global cached_trios
    cached_trios = {}
    
    for _, match in matches_df.iterrows():
        for team_prefix in ['team1', 'team2']:
            team_tags = []
            for i in range(1, 4):
                tag_raw = match.get(f'{team_prefix}_player{i}_tag', '')
                if pd.notna(tag_raw):
                    tag = str(tag_raw).strip().upper().replace('0', 'O')
                    if tag and tag != 'NAN':
                        if not tag.startswith('#'):
                            tag = '#' + tag
                        team_tags.append(tag)
            
            if len(team_tags) == 3:
                trio_key = tuple(sorted(team_tags))
                if trio_key not in cached_trios:
                    cached_trios[trio_key] = {'games': 0, 'wins': 0}
                cached_trios[trio_key]['games'] += 1
                
                winner = match.get('winner', '')
                if pd.notna(winner):
                    winner_name = str(winner).strip()
                    if winner_name in ['team1', 'team2']:
                        if winner_name == team_prefix:
                            cached_trios[trio_key]['wins'] += 1
                    else:
                        team_name_raw = match.get(f'{team_prefix}_name', '')
                        if pd.notna(team_name_raw):
                            if str(team_name_raw).strip() == winner_name:
                                cached_trios[trio_key]['wins'] += 1


def calculate_all_stats_offseason():
    global players_data, region_stats
    
    tracked_players = load_tracked_players()
    
    if not tracked_players:
        print("⚠️ No tracked players found in players_off.xlsx")
        players_data = {}
        region_stats = {}
        return
    
    print(f"📋 Tracking {len(tracked_players)} players")
    
    # Initialize data structures
    players_data = {}
    region_stats = defaultdict(lambda: {
        'total_matches': 0,
        'players': set()
    })
    
    global_brawler_stats = defaultdict(lambda: {'picks': 0, 'wins': 0})
    mode_map_stats = defaultdict(lambda: defaultdict(lambda: {
        'matches': 0,
        'wins': 0,
        'brawlers': defaultdict(lambda: {'picks': 0, 'wins': 0})
    }))
    
    # OPTIMIZATION 1: Pre-build tracked players set for O(1) lookup
    tracked_tags_set = set(tracked_players.keys())
    
    # OPTIMIZATION 2: Pre-filter matches to only those with tracked players
    print("🔍 Pre-filtering relevant matches...")
    relevant_matches = []
    
    for idx, match in matches_df.iterrows():
        # Quick check: does this match contain any tracked players?
        has_tracked = False
        for team_prefix in ['team1', 'team2']:
            for i in range(1, 4):
                tag = normalize_tag(match.get(f'{team_prefix}_player{i}_tag', ''))
                if tag and tag in tracked_tags_set:
                    has_tracked = True
                    break
            if has_tracked:
                break
        
        if has_tracked:
            relevant_matches.append((idx, match))
    
    print(f"✓ Filtered to {len(relevant_matches)} relevant matches (from {len(matches_df)} total)")
    
    # OPTIMIZATION 3: Batch initialize all tracked players
    for player_tag, player_info in tracked_players.items():
        players_data[player_tag] = {
            'name': player_info['name'],  # This comes from tracked_players
            'region': player_info.get('region', 'Unknown'),
            'potential_team': player_info.get('potential_team', ''),
            'notes': player_info.get('notes', ''),
            'matches': 0,
            'wins': 0,
            'losses': 0,
            'brawlers': defaultdict(lambda: {'picks': 0, 'wins': 0}),
            'star_player': 0,
            'modes': defaultdict(lambda: {
                'matches': 0,
                'wins': 0,
                'maps': defaultdict(lambda: {
                    'matches': 0,
                    'wins': 0,
                    'brawlers': defaultdict(lambda: {'picks': 0, 'wins': 0})
                })
            }),
            'teammates_seen': defaultdict(lambda: {'name': '', 'matches': 0}),
            'opponents_seen': defaultdict(lambda: {'name': '', 'matches': 0})
        }
    
    # OPTIMIZATION 4: Series tracking with pre-allocated dictionaries
    series_tracking_brawlers = {}
    series_tracking_outcomes = {}
    matches_counted = set()
    
    # OPTIMIZATION 5: Process matches in batch
    print("⚙️ Processing matches...")
    
    for match_idx, match in relevant_matches:
        match_id = match.get('battle_time', str(match_idx))
        
        mode = str(match.get('mode', 'Unknown'))
        map_name = str(match.get('map', 'Unknown'))
        
        if mode == 'nan' or pd.isna(mode):
            mode = 'Unknown'
        if map_name == 'nan' or pd.isna(map_name):
            map_name = 'Unknown'

        if mode not in VALID_MODES:
            continue
        
        # OPTIMIZATION 6: Extract all player data once
        match_players = {}  # team_prefix -> player_index -> {tag, name, brawler}
        all_player_tags = []
        all_brawlers = []
        
        for team_prefix in ['team1', 'team2']:
            match_players[team_prefix] = {}
            for i in range(1, 4):
                tag = normalize_tag(match.get(f'{team_prefix}_player{i}_tag', ''))
                name = str(match.get(f'{team_prefix}_player{i}', ''))
                brawler = str(match.get(f'{team_prefix}_player{i}_brawler', ''))
                
                if tag:
                    match_players[team_prefix][i] = {
                        'tag': tag,
                        'name': name,  # Get name from match data
                        'brawler': brawler
                    }
                    all_player_tags.append(tag)
                    if brawler and brawler != 'nan':
                        all_brawlers.append(brawler)
        
        # Create series ID
        all_player_tags.sort()
        all_brawlers.sort()
        
        battle_time = match.get('battle_time')
        if pd.notna(battle_time):
            time_rounded = pd.Timestamp(battle_time).floor('30min')
        else:
            time_rounded = match_id
        
        series_id = f"{tuple(all_player_tags)}_{tuple(all_brawlers)}_{mode}_{map_name}_{time_rounded}"
        
        if series_id not in series_tracking_brawlers:
            series_tracking_brawlers[series_id] = {}
        if series_id not in series_tracking_outcomes:
            series_tracking_outcomes[series_id] = {
                'mode': mode,
                'map': map_name,
                'players': {}
            }
        
        # Get winner once
        winner_name = str(match.get('winner', '')).strip()
        
        # Count match for mode/map stats (once per match)
        if match_id not in matches_counted:
            matches_counted.add(match_id)
            mode_map_stats[mode][map_name]['matches'] += 1
            if winner_name == 'team1':
                mode_map_stats[mode][map_name]['wins'] += 1
        
        # OPTIMIZATION 7: Process each team's tracked players
        for team_prefix in ['team1', 'team2']:
            is_winner = (winner_name == team_prefix)
            
            team_players = match_players.get(team_prefix, {})
            
            for i, player_info in team_players.items():
                player_tag = player_info['tag']
                
                # Skip if not tracked
                if player_tag not in tracked_tags_set:
                    continue
                
                player_name = player_info['name']
                brawler = player_info['brawler']
                
                if player_name == 'nan' or not player_name:
                    continue
                
                # Get player reference (already initialized)
                player = players_data[player_tag]
                
                # UPDATE NAME FROM MATCH DATA (in case it changed)
                if player_name and player_name != 'nan':
                    player['name'] = player_name
                
                # Update match counts
                player['matches'] += 1
                player['modes'][mode]['matches'] += 1
                player['modes'][mode]['maps'][map_name]['matches'] += 1
                
                # Region stats
                region = player['region']
                region_stats[region]['total_matches'] += 1
                region_stats[region]['players'].add(player_tag)
                
                # Update wins/losses
                if is_winner:
                    player['wins'] += 1
                    player['modes'][mode]['wins'] += 1
                    player['modes'][mode]['maps'][map_name]['wins'] += 1
                else:
                    player['losses'] += 1
                
                # Track series outcomes
                if player_tag not in series_tracking_outcomes[series_id]['players']:
                    series_tracking_outcomes[series_id]['players'][player_tag] = {
                        'wins': 0,
                        'losses': 0,
                        'brawlers': set()
                    }
                
                if is_winner:
                    series_tracking_outcomes[series_id]['players'][player_tag]['wins'] += 1
                else:
                    series_tracking_outcomes[series_id]['players'][player_tag]['losses'] += 1
                
                series_tracking_outcomes[series_id]['players'][player_tag]['brawlers'].add(brawler)
                
                # Track brawler picks per SERIES (only once)
                if player_tag not in series_tracking_brawlers[series_id]:
                    series_tracking_brawlers[series_id][player_tag] = set()
                
                if brawler not in series_tracking_brawlers[series_id][player_tag]:
                    series_tracking_brawlers[series_id][player_tag].add(brawler)
                    
                    # Count pick once per series
                    global_brawler_stats[brawler]['picks'] += 1
                    mode_map_stats[mode][map_name]['brawlers'][brawler]['picks'] += 1
                    player['brawlers'][brawler]['picks'] += 1
                    player['modes'][mode]['maps'][map_name]['brawlers'][brawler]['picks'] += 1
                
                # Star player tracking
                star_player_tag = normalize_tag(match.get('star_player_tag', ''))
                if star_player_tag and star_player_tag == player_tag:
                    player['star_player'] += 1
                
                # OPTIMIZATION 8: Batch teammate/opponent tracking
                # Track teammates
                for j in range(1, 4):
                    if j == i:
                        continue
                    teammate_info = team_players.get(j)
                    if teammate_info and teammate_info['name'] != 'nan':
                        teammate_tag = teammate_info['tag']
                        teammate_name = teammate_info['name']  # Get from match data
                        player['teammates_seen'][teammate_tag]['name'] = teammate_name
                        player['teammates_seen'][teammate_tag]['matches'] += 1
                
                # Track opponents
                opponent_prefix = 'team2' if team_prefix == 'team1' else 'team1'
                opponent_players = match_players.get(opponent_prefix, {})
                
                for opponent_info in opponent_players.values():
                    if opponent_info['name'] != 'nan':
                        opponent_name = opponent_info['name']  # Get from match data
                        player['opponents_seen'][opponent_info['tag']]['name'] = opponent_name
                        player['opponents_seen'][opponent_info['tag']]['matches'] += 1
    
    
    
    for series_id, series_info in series_tracking_outcomes.items():
        mode = series_info['mode']
        map_name = series_info['map']
        
        for player_tag, outcome in series_info['players'].items():
            if player_tag not in players_data:
                continue
            
            player = players_data[player_tag]
            
            # Determine if player won the series
            series_won = outcome['wins'] > outcome['losses']
            
            # If they won the series, count wins for all brawlers used
            if series_won:
                for brawler in outcome['brawlers']:
                    global_brawler_stats[brawler]['wins'] += 1
                    mode_map_stats[mode][map_name]['brawlers'][brawler]['wins'] += 1
                    player['brawlers'][brawler]['wins'] += 1
                    player['modes'][mode]['maps'][map_name]['brawlers'][brawler]['wins'] += 1
    
    # Store global stats
    region_stats['_global_brawlers'] = dict(global_brawler_stats)
    region_stats['_mode_map_stats'] = {mode: dict(maps) for mode, maps in mode_map_stats.items()}
    
    # Convert sets to lists
    for region in region_stats:
        if region.startswith('_'):
            continue
        region_stats[region]['players'] = list(region_stats[region]['players'])

    # Cache trios
    cache_trios()
    
    print(f"✅ Calculated stats for {len(players_data)} tracked players")
    print(f"✅ Global stats: {len(global_brawler_stats)} brawlers across {len(mode_map_stats)} modes")


def load_tracked_players():
    """Load player information from players_off.xlsx"""
    players_file = 'players_off.xlsx'
    
    if not os.path.exists(players_file):
        print(f"❌ {players_file} not found")
        return {}
    
    tracked = {}
    
    try:
        df = pd.read_excel(players_file)
        
        for _, row in df.iterrows():
            tag = str(row['Player ID']).strip().upper().replace('0', 'O')
            if not tag.startswith('#'):
                tag = '#' + tag
            
            # Fix region - default to 'NA' if missing or invalid
            region = str(row.get('Region', 'NA')).strip().upper()
            if region in ['NAN', 'NONE', '', 'NULL'] or pd.isna(row.get('Region')):
                region = 'NA'
            
            tracked[tag] = {
                'name': str(row['Player Name']).strip(),
                'region': region,
            }
        
        return tracked
        
    except Exception as e:
        print(f"❌ Error loading tracked players: {e}")
        return {}



# NEW FUNCTION: Load rosters for off-season mode
def load_team_rosters_offseason():
    """Load valid player tags from players_off.xlsx"""
    valid_players = {}
    mode_config = get_config_for_mode()
    players_file = mode_config['TEAMS_FILE']
    
    if not os.path.exists(players_file):
        print(f"Warning: {players_file} not found - all players will be included")
        return None
    
    try:
        players_df = pd.read_excel(players_file)
        
        for _, row in players_df.iterrows():
            tag_col = 'Player ID'
            potential_team_col = 'Potential Team'
            
            if tag_col in players_df.columns and pd.notna(row.get(tag_col)):
                tag = str(row[tag_col]).strip().upper().replace('0', 'O')
                if not tag.startswith('#'):
                    tag = '#' + tag
                
                # Use potential team or player name as team key
                if potential_team_col in players_df.columns and pd.notna(row.get(potential_team_col)):
                    team_key = str(row[potential_team_col]).strip()
                else:
                    team_key = str(row['Player Name']).strip()
                
                if team_key not in valid_players:
                    valid_players[team_key] = set()
                valid_players[team_key].add(tag)
        
        print(f"Loaded {sum(len(tags) for tags in valid_players.values())} tracked players from {players_file} (off season mode)")
        return valid_players
        
    except Exception as e:
        print(f"Error loading player rosters: {e}")
        return None

def calculate_all_stats():
    """Calculate comprehensive statistics from matches"""
    global teams_data, region_stats
    
    valid_rosters = load_team_rosters()

    # Region name mapping (matches file -> bot display)
    region_mapping = {
        'APAC': 'EA',  # Map APAC in Excel to EA in bot
    }

    teams_data = {}
    region_stats = defaultdict(lambda: {
        'total_matches': 0,
        'teams': set()
    })
    
    match_brawler_tracking = {}
    
    series_tracking_brawlers = {}  # Track brawler picks per series

    for _, match in matches_df.iterrows():
        match_id = match.get('battle_time', str(_))
        
        # Create series ID based on: teams + mode + map + both team comps
        team1 = match['team1_name']
        team2 = match['team2_name']
        teams_sorted = tuple(sorted([team1, team2]))
        mode = str(match['mode'])
        map_name = str(match['map'])
        
        # Get both team compositions (sorted brawler lists)
        team1_comp = sorted([
            str(match['team1_player1_brawler']),
            str(match['team1_player2_brawler']),
            str(match['team1_player3_brawler'])
        ])
        team2_comp = sorted([
            str(match['team2_player1_brawler']),
            str(match['team2_player2_brawler']),
            str(match['team2_player3_brawler'])
        ])
        
        # Sort both comps so order doesn't matter (Team1 vs Team2 or Team2 vs Team1)
        comps_sorted = tuple(sorted([tuple(team1_comp), tuple(team2_comp)]))
        
        # Round time to nearest 30 minutes as backup (in case of comp swaps mid-series)
        battle_time = match.get('battle_time')
        if pd.notna(battle_time):
            time_rounded = pd.Timestamp(battle_time).floor('30min')
        else:
            time_rounded = match_id
        
        # Series ID: same teams + mode + map + comps + time window
        series_id = f"{teams_sorted}_{mode}_{map_name}_{comps_sorted}_{time_rounded}"
        
        if series_id not in series_tracking_brawlers:
            series_tracking_brawlers[series_id] = {}
        
        for team_prefix in ['team1', 'team2']:
            team_name = match[f'{team_prefix}_name']
            
            
            team_region = str(match[f'{team_prefix}_region']).strip().upper()
            
            if team_region in ['NAN', 'NONE', '', 'UNKNOWN'] or pd.isna(match[f'{team_prefix}_region']):
                team_region = 'NA'
            
            # Apply region mapping
            team_region = region_mapping.get(team_region, team_region)
            
            if team_region not in CONFIG['REGIONS']:
                print(f"Invalid region '{team_region}' for team '{team_name}', setting to NA")
                team_region = 'NA'
            
            if team_name not in teams_data:
                teams_data[team_name] = {
                    'region': team_region,
                    'matches': 0,
                    'wins': 0,
                    'losses': 0,
                    'players': defaultdict(lambda: {
                        'matches': 0,
                        'wins': 0,
                        'brawlers': defaultdict(lambda: {'picks': 0, 'wins': 0}),
                        'star_player': 0
                    }),
                    'brawlers': defaultdict(lambda: {'picks': 0, 'wins': 0}),
                    'modes': defaultdict(lambda: {
                        'matches': 0,
                        'wins': 0,
                        'maps': defaultdict(lambda: {
                            'matches': 0,
                            'wins': 0,
                            'brawlers': defaultdict(lambda: {'picks': 0, 'wins': 0})
                        })
                    })
                }
            
            team = teams_data[team_name]
            team['matches'] += 1

            # Strip whitespace from winner name to match team_name
            winner_name = str(match['winner']).strip()
            is_winner = (winner_name == team_name)
            if is_winner:
                team['wins'] += 1
            else:
                team['losses'] += 1
            
            mode = str(match['mode'])
            map_name = str(match['map'])
            
            if pd.isna(match['mode']) or mode == 'nan':
                mode = 'Unknown'
            if pd.isna(match['map']) or map_name == 'nan':
                map_name = 'Unknown'
            
            team['modes'][mode]['matches'] += 1
            team['modes'][mode]['maps'][map_name]['matches'] += 1
            if is_winner:
                team['modes'][mode]['wins'] += 1
                team['modes'][mode]['maps'][map_name]['wins'] += 1
            
            if match_id not in match_brawler_tracking:
                match_brawler_tracking[match_id] = {}
            if team_name not in match_brawler_tracking[match_id]:
                match_brawler_tracking[match_id][team_name] = set()
            
            # Get star player tag once per team (MOVED OUTSIDE THE LOOP)
            star_player_tag = str(match.get('star_player_tag', '')).strip().upper().replace('0', 'O')
            
            for i in range(1, 4):
                player_name = str(match[f'{team_prefix}_player{i}'])
                player_tag = str(match[f'{team_prefix}_player{i}_tag']).strip().upper().replace('0', 'O')
                brawler = str(match[f'{team_prefix}_player{i}_brawler'])
                
                if pd.isna(match[f'{team_prefix}_player{i}']) or player_name == 'nan':
                    continue
                
                # Skip players not in the official roster
                if valid_rosters and team_name in valid_rosters:
                    if player_tag not in valid_rosters[team_name]:
                        continue
                
                player = team['players'][player_tag]
                player['name'] = player_name
                player['matches'] += 1
                
                if is_winner:
                    player['wins'] += 1
                
                # Track brawler picks per SERIES (based on comp + time)
                if team_name not in series_tracking_brawlers[series_id]:
                    series_tracking_brawlers[series_id][team_name] = set()

                brawler_key = f"{player_tag}_{brawler}"
                if brawler_key not in series_tracking_brawlers[series_id][team_name]:
                    series_tracking_brawlers[series_id][team_name].add(brawler_key)
                    
                    player['brawlers'][brawler]['picks'] += 1
                    team['brawlers'][brawler]['picks'] += 1
                    team['modes'][mode]['maps'][map_name]['brawlers'][brawler]['picks'] += 1
                    
                    # Only count win ONCE per series if they won
                    if is_winner:
                        player['brawlers'][brawler]['wins'] += 1
                        team['brawlers'][brawler]['wins'] += 1
                        team['modes'][mode]['maps'][map_name]['brawlers'][brawler]['wins'] += 1
                
                # Check if this player was the star player (FIXED COMPARISON)
                if star_player_tag and star_player_tag != 'NAN' and star_player_tag == player_tag:
                    player['star_player'] += 1
            
            region_stats[team_region]['total_matches'] += 1
            region_stats[team_region]['teams'].add(team_name)
    
    for region in region_stats:
        region_stats[region]['teams'] = list(region_stats[region]['teams'])
# ==================== VIEWS ====================

class WelcomeView(View):
    """Welcome screen with region selection"""
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="🌍 ALL REGIONS", style=discord.ButtonStyle.primary, row=0)
    async def all_regions_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = AllRegionsView()
        embed = view.create_all_regions_embed()
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    
    @discord.ui.button(label="🇺🇸 NA", style=discord.ButtonStyle.secondary, row=1)
    async def na_button(self, interaction: discord.Interaction, button: Button):
        await self.show_region(interaction, 'NA')
    
    @discord.ui.button(label="🇪🇺 EU", style=discord.ButtonStyle.secondary, row=1)
    async def eu_button(self, interaction: discord.Interaction, button: Button):
        await self.show_region(interaction, 'EU')
    
    @discord.ui.button(label="🇧🇷 LATAM", style=discord.ButtonStyle.secondary, row=1)
    async def latam_button(self, interaction: discord.Interaction, button: Button):
        await self.show_region(interaction, 'LATAM')
    
    @discord.ui.button(label="🌏 EA", style=discord.ButtonStyle.secondary, row=1)
    async def ea_button(self, interaction: discord.Interaction, button: Button):
        await self.show_region(interaction, 'EA')
    
    @discord.ui.button(label="🌏 SEA", style=discord.ButtonStyle.secondary, row=1)
    async def oce_button(self, interaction: discord.Interaction, button: Button):
        await self.show_region(interaction, 'SEA')

    @discord.ui.button(label="CURRENT META", style=discord.ButtonStyle.red, row=0)
    async def meta_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        
        # Calculate dynamic stats
        total_brawlers = len(set(
            brawler 
            for team_data in teams_data.values() 
            for brawler in team_data['brawlers'].keys()
        ))
        
        # Calculate total games analyzed (count all match entries)
        games_analyzed = len(matches_df) if matches_df is not None else 0
        
        # Calculate last update time
        if matches_df is not None and 'battle_time' in matches_df.columns:
            latest_match = matches_df['battle_time'].max()
            if pd.notna(latest_match):
                time_diff = pd.Timestamp.now(tz='UTC') - pd.to_datetime(latest_match, utc=True)
                hours = int(time_diff.total_seconds() / 3600)
                if hours < 1:
                    minutes = int(time_diff.total_seconds() / 60)
                    last_update = f"{minutes} min ago"
                elif hours < 24:
                    last_update = f"{hours}h ago"
                else:
                    days = int(time_diff.total_seconds() / 86400)
                    last_update = f"{days}d ago"
            else:
                last_update = "Unknown"
        else:
            last_update = "Unknown"
        
        embed = discord.Embed(
            title="Current Meta Analysis",
            description="Select a region below to view detailed meta statistics and tier rankings.",
            color=discord.Color.red()
        )
        
        embed.add_field(name="Brawlers Tracked", value=f"{total_brawlers}", inline=True)
        embed.add_field(name="Matches Analyzed", value=f"{games_analyzed:,}", inline=True)
        embed.add_field(name="Latest Match", value=f"{last_update}", inline=True)
        
        
        
        view = MetaView()
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    
    async def show_region(self, interaction: discord.Interaction, region: str):
        await interaction.response.defer()
        view = RegionView(region)
        embed = view.create_region_embed()
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)

# Add these new view classes to your bot.py file (after the WelcomeView class)

class MetaView(View):
    """View for meta analysis with region selection"""
    def __init__(self):
        super().__init__(timeout=300)
    
    @discord.ui.button(label="🌍 ALL REGIONS", style=discord.ButtonStyle.primary, row=0)
    async def all_regions_button(self, interaction: discord.Interaction, button: Button):
        view = MetaDetailView(region='ALL')
        await view.send_meta_image(interaction)
    
    @discord.ui.button(label="🇺🇸 NA", style=discord.ButtonStyle.secondary, row=1)
    async def na_button(self, interaction: discord.Interaction, button: Button):
        view = MetaDetailView(region='NA')
        await view.send_meta_image(interaction)
    
    @discord.ui.button(label="🇪🇺 EU", style=discord.ButtonStyle.secondary, row=1)
    async def eu_button(self, interaction: discord.Interaction, button: Button):
        view = MetaDetailView(region='EU')
        await view.send_meta_image(interaction)
    
    @discord.ui.button(label="🇧🇷 LATAM", style=discord.ButtonStyle.secondary, row=1)
    async def latam_button(self, interaction: discord.Interaction, button: Button):
        view = MetaDetailView(region='LATAM')
        await view.send_meta_image(interaction)
    
    @discord.ui.button(label="🌏 EA", style=discord.ButtonStyle.secondary, row=1)
    async def ea_button(self, interaction: discord.Interaction, button: Button):
        view = MetaDetailView(region='EA')
        await view.send_meta_image(interaction)
    
    @discord.ui.button(label="🌏 SEA", style=discord.ButtonStyle.secondary, row=1)
    async def sea_button(self, interaction: discord.Interaction, button: Button):
        view = MetaDetailView(region='SEA')
        await view.send_meta_image(interaction)
    
    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=2)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        
        # Check current mode and return to appropriate welcome screen
        mode = load_bot_mode()
        
        if mode == 'offseason':
            view = WelcomeViewOffseason()
        else:
            view = WelcomeView()
        
        embed = create_welcome_embed()
        await interaction.edit_original_response(embed=embed, view=view, attachments=[])

class MetaModeSelectView(View):
    """Dropdown to select a mode for meta analysis"""
    def __init__(self, region: str = 'ALL'):
        super().__init__(timeout=300)
        self.region = region
        
        # Collect all modes
        all_modes = set()
        relevant_teams = teams_data.items()
        if region != 'ALL':
            relevant_teams = [(name, data) for name, data in teams_data.items() if data['region'] == region]
        
        for team_name, team_data in relevant_teams:
            for mode in team_data['modes'].keys():
                if mode in VALID_MODES:  # **NEW: Only include valid modes**
                    all_modes.add(mode)
        
        sorted_modes = sorted(all_modes)
        
        if sorted_modes:
            options = [
                discord.SelectOption(label=mode, value=mode)
                for mode in sorted_modes
            ]
            
            select = Select(placeholder="Choose a game mode...", options=options)
            select.callback = self.select_callback
            self.add_item(select)
        
        # Add back button
        back_btn = Button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=1)
        back_btn.callback = self.back_callback
        self.add_item(back_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        mode = interaction.data['values'][0]
        view = MetaModeDetailView(self.region, mode)
        await view.generate_button.callback(interaction)
    
    async def back_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        view = MetaDetailView(self.region)
        await view.send_meta_image(interaction)


class MetaDetailView(View):
    """Detailed meta analysis with tier list image"""
    def __init__(self, region: str = 'ALL'):
        super().__init__(timeout=300)
        self.region = region
    
    async def send_meta_image(self, interaction, mode=None):
        """Generate and send meta tier list image"""
        await interaction.response.defer()
        
        # Generate image
        img_bytes = generate_meta_tier_image(self.region, mode)
        
        if img_bytes is None:
            await interaction.followup.send("❌ Not enough data to generate meta tier list.", ephemeral=True)
            return
        
        region_title = "All Regions" if self.region == 'ALL' else f"{self.region} Region"
        mode_text = f" - {mode}" if mode else ""
        
        file = discord.File(img_bytes, filename=f"meta_tier_{self.region}_{mode or 'overall'}.png")
        
        embed = discord.Embed(
            title=f"Meta Tier List",
            description=f"**{region_title}{mode_text}**\n\nBrawlers ranked by meta score (Win Rate x Pick Rate)",
            color=discord.Color.red(),
            timestamp=datetime.now()
        )
        embed.set_image(url=f"attachment://meta_tier_{self.region}_{mode or 'overall'}.png")
        embed.set_footer(text="Tiers are calculated based on competitive stats, only ever used brawlers are included")
        
        await interaction.followup.send(embed=embed, file=file, view=self, ephemeral=True)
    
    @discord.ui.button(label="By Mode", style=discord.ButtonStyle.primary, row=0)
    async def modes_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        
        # Check current bot mode
        bot_mode = load_bot_mode()
        
        if bot_mode == 'offseason':
            # Use offseason mode select view
            view = OffseasonMetaModeSelectView(self.region)
        else:
            # Use season mode select view
            view = MetaModeSelectView(self.region)
        
        await interaction.followup.send("Select a mode to view meta:", view=view, ephemeral=True)
    
    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=0)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = MetaView()
        
        # Check mode for proper content message
        mode = load_bot_mode()
        if mode == 'offseason':
            content = "**Current Meta Analysis**\n\nGlobal meta statistics:"
        else:
            content = "**Current Meta Analysis**\n\nSelect a region:"
        
        await interaction.edit_original_response(content=content, embed=None, view=view, attachments=[])


class OffseasonMetaModeSelectView(View):
    """Mode selection for meta analysis in offseason"""
    def __init__(self, region: str = 'ALL'):
        super().__init__(timeout=300)
        self.region = region
        
        # Get modes from global stats
        mode_map_data = region_stats.get('_mode_map_stats', {})
        sorted_modes = sorted(mode_map_data.keys())
        
        if sorted_modes:
            options = [
                discord.SelectOption(label=mode, value=mode)
                for mode in sorted_modes if mode not in ['Unknown', 'nan']
            ]
            
            if options:
                select = Select(placeholder="Choose a game mode...", options=options)
                select.callback = self.select_callback
                self.add_item(select)
        
        # Add back button
        back_btn = Button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=1)
        back_btn.callback = self.back_callback
        self.add_item(back_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        mode = interaction.data['values'][0]
        
        # Generate and send meta tier image for this mode
        await interaction.response.defer()
        
        img_bytes = generate_meta_tier_image(self.region, mode)
        
        if img_bytes is None:
            await interaction.followup.send("❌ Not enough data to generate meta tier list for this mode.", ephemeral=True)
            return
        
        region_title = "All Regions" if self.region == 'ALL' else f"{self.region} Region"
        
        file = discord.File(img_bytes, filename=f"meta_tier_{self.region}_{mode}.png")
        
        embed = discord.Embed(
            title=f"Meta Tier List",
            description=f"**{region_title} - {mode}**\n\nBrawlers ranked by meta score (Win Rate x Pick Rate)",
            color=discord.Color.red(),
            timestamp=datetime.now()
        )
        embed.set_image(url=f"attachment://meta_tier_{self.region}_{mode}.png")
        embed.set_footer(text="Tiers are calculated based on competitive stats, only ever used brawlers are included")
        
        # Create the detail view with back button
        view = OffseasonMetaModeDetailView(self.region, mode)
        await interaction.followup.send(embed=embed, file=file, view=view, ephemeral=True)
    
    async def back_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        view = MetaDetailView(self.region)
        await view.send_meta_image(interaction)


class OffseasonMetaModeDetailView(View):
    """Meta analysis image for a specific mode in offseason - just has a back button"""
    def __init__(self, region: str, mode: str):
        super().__init__(timeout=300)
        self.region = region
        self.mode = mode
    
    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=0)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = OffseasonMetaModeSelectView(self.region)
        await interaction.edit_original_response(content="Select a mode to view meta:", embed=None, view=view, attachments=[])


class MetaModeDetailView(View):
    """Meta analysis image for a specific mode"""
    def __init__(self, region: str, mode: str):
        super().__init__(timeout=300)
        self.region = region
        self.mode = mode
    
    async def send_meta_image(self, interaction):
        """Generate and send meta tier list image for this mode"""
        await interaction.response.defer()
        
        # Generate image
        img_bytes = generate_meta_tier_image(self.region, self.mode)
        
        if img_bytes is None:
            await interaction.followup.send("❌ Not enough data to generate meta tier list for this mode.", ephemeral=True)
            return
        
        region_title = "All Regions" if self.region == 'ALL' else f"{self.region} Region"
        
        file = discord.File(img_bytes, filename=f"meta_tier_{self.region}_{self.mode}.png")
        
        embed = discord.Embed(
            title=f"Meta Tier List",
            description=f"**{region_title} - {self.mode}**\n\nBrawlers ranked by meta score (Win Rate x Pick Rate)",
            color=discord.Color.red(),
            timestamp=datetime.now()
        )
        embed.set_image(url=f"attachment://meta_tier_{self.region}_{self.mode}.png")
        embed.set_footer(text="Tiers are calculated based on competitive stats, only ever used brawlers are included")
        
        await interaction.followup.send(embed=embed, file=file, view=self, ephemeral=True)
    
    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=0)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = MetaModeSelectView(self.region)
        await interaction.edit_original_response(content="Select a mode to view meta:", embed=None, view=view, attachments=[])
    
    
class MetaModeSelectView(View):
    """Dropdown to select a mode for meta analysis"""
    def __init__(self, region: str = 'ALL'):
        super().__init__(timeout=300)
        self.region = region
        
        # Collect all modes
        all_modes = set()
        relevant_teams = teams_data.items()
        if region != 'ALL':
            relevant_teams = [(name, data) for name, data in teams_data.items() if data['region'] == region]
        
        for team_name, team_data in relevant_teams:
            for mode in team_data['modes'].keys():
                if mode in VALID_MODES:  # **NEW: Only include valid modes**
                    all_modes.add(mode)
        
        sorted_modes = sorted(all_modes)
        
        if sorted_modes:
            options = [
                discord.SelectOption(label=mode, value=mode)
                for mode in sorted_modes
            ]
            
            select = Select(placeholder="Choose a game mode...", options=options)
            select.callback = self.select_callback
            self.add_item(select)
        
        # Add back button
        back_btn = Button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=1)
        back_btn.callback = self.back_callback
        self.add_item(back_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        mode = interaction.data['values'][0]
        
        # Generate and send the meta image directly
        await interaction.response.defer()
        
        img_bytes = generate_meta_tier_image(self.region, mode)
        
        if img_bytes is None:
            await interaction.followup.send("❌ Not enough data to generate meta tier list for this mode.", ephemeral=True)
            return
        
        region_title = "All Regions" if self.region == 'ALL' else f"{self.region} Region"
        
        file = discord.File(img_bytes, filename=f"meta_tier_{self.region}_{mode}.png")
        
        embed = discord.Embed(
            title=f"Meta Tier List",
            description=f"**{region_title} - {mode}**\n\nBrawlers ranked by meta score (Win Rate x Pick Rate)",
            color=discord.Color.red(),
            timestamp=datetime.now()
        )
        embed.set_image(url=f"attachment://meta_tier_{self.region}_{mode}.png")
        embed.set_footer(text="Tiers are calculated based on competitive stats, only ever used brawlers are included")
        
        # Create the detail view with back button
        view = MetaModeDetailView(self.region, mode)
        await interaction.followup.send(embed=embed, file=file, view=view, ephemeral=True)
    
    async def back_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        view = MetaDetailView(self.region)
        await view.send_meta_image(interaction)


class MetaModeDetailView(View):
    """Meta analysis image for a specific mode - just has a back button"""
    def __init__(self, region: str, mode: str):
        super().__init__(timeout=300)
        self.region = region
        self.mode = mode
    
    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=0)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = MetaModeSelectView(self.region)
        await interaction.edit_original_response(content="Select a mode to view meta:", embed=None, view=view, attachments=[])

class AllRegionsView(View):
    """View showing statistics for all regions"""
    def __init__(self):
        super().__init__(timeout=300)
    
    def create_all_regions_embed(self):
        embed = discord.Embed(
            title="🌐 All Regions Overview",
            description="Statistics across all competitive regions",
            color=discord.Color.red(),
            timestamp=datetime.now()
        )
        
        total_matches = len(matches_df) if matches_df is not None else 0
        total_teams = len(teams_data)
        
        embed.add_field(name="Total Matches", value=f"**{total_matches}**", inline=True)
        embed.add_field(name="Total Teams", value=f"**{total_teams}**", inline=True)
        embed.add_field(name="Regions", value=f"**{len(region_stats)}\n\n**", inline=True)
        
        valid_regions = [r for r in region_stats.keys() if isinstance(r, str) and r in CONFIG['REGIONS']]
        region_text = []
        for region in sorted(valid_regions):
            stats = region_stats[region]
            team_count = len(stats['teams'])
            matches = stats['total_matches']
            region_text.append(f"**{region}**: {team_count} teams • {matches} matches")
        
        embed.add_field(
            name="Regional Breakdown",
            value="\n".join(region_text) if region_text else "No data",
            inline=False
        )
        
        top_teams = sorted(
            teams_data.items(),
            key=lambda x: (x[1]['wins'] / x[1]['matches']) if x[1]['matches'] > 0 else 0,
            reverse=True
        )[:10]
        
        leaderboard = []
        for i, (team_name, data) in enumerate(top_teams, 1):
            wr = (data['wins'] / data['matches'] * 100) if data['matches'] > 0 else 0
            leaderboard.append(
                f"**{i}.** {team_name} ({data['region']})\n"
                f"     └ {data['wins']}-{data['losses']} • {wr:.1f}% WR"
            )
        
        embed.add_field(
            name="\u200b\n🏆 Top Teams (by Win Rate)",
            value="\n".join(leaderboard) if leaderboard else "No data",
            inline=False
        )
        
        return embed
    
    @discord.ui.button(label="View Modes & Maps", style=discord.ButtonStyle.primary, row=0)
    async def modes_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = AllRegionsModeSelectView()
        await interaction.followup.send("Select a game mode to view regional statistics:", view=view, ephemeral=True)
    
    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=0)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = WelcomeView()
        embed = create_welcome_embed()
        await interaction.edit_original_response(embed=embed, view=view)


class AllRegionsModeSelectView(View):
    """Dropdown to select a mode for all-region statistics"""
    def __init__(self):
        super().__init__(timeout=300)
        
        all_modes = set()
        for team_data in teams_data.values():
            for mode in team_data['modes'].keys():
                if mode not in ['Unknown', 'nan', '', 'None']:
                    all_modes.add(mode)
            
        
        sorted_modes = sorted(all_modes)
        
        if sorted_modes:
            options = [
                discord.SelectOption(label=mode, value=mode)
                for mode in sorted_modes
            ]
            
            select = Select(placeholder="Choose a game mode...", options=options)
            select.callback = self.select_callback
            self.add_item(select)
        
        # Add back button
        back_btn = Button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=1)
        back_btn.callback = self.back_callback
        self.add_item(back_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        mode = interaction.data['values'][0]
        view = AllRegionsModeDetailView(mode)
        embed = view.create_mode_embed()
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    
    async def back_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        view = AllRegionsView()
        embed = view.create_all_regions_embed()
        await interaction.edit_original_response(embed=embed, view=view)


class AllRegionsModeDetailView(View):
    """View showing mode statistics across all regions"""
    def __init__(self, mode: str):
        super().__init__(timeout=300)
        self.mode = mode
    
    def create_mode_embed(self):
        embed = discord.Embed(
            title=f"{self.mode} - All Regions",
            description="Statistics across all regions for this mode",
            color=discord.Color.red()
        )
        
        brawler_picks = defaultdict(int)
        brawler_wins = defaultdict(int)
        total_matches = 0
        
        for team_data in teams_data.values():
            if self.mode in team_data['modes']:
                mode_data = team_data['modes'][self.mode]
                total_matches += mode_data['matches']
                
                for map_name, map_data in mode_data['maps'].items():
                    for brawler, brawler_data in map_data['brawlers'].items():
                        brawler_picks[brawler] += brawler_data['picks']
                        brawler_wins[brawler] += brawler_data['wins']
        
        embed.add_field(name="⚔️ Total Matches", value=f"**{total_matches}**", inline=True)
        
        sorted_by_picks = sorted(brawler_picks.items(), key=lambda x: x[1], reverse=True)[:15]
        picks_text = []
        total_picks = sum(brawler_picks.values())
        
        for brawler, picks in sorted_by_picks:
            pick_rate = (picks / total_picks * 100) if total_picks > 0 else 0
            wr = (brawler_wins[brawler] / picks * 100) if picks > 0 else 0
            picks_text.append(f"**{brawler}**: {picks} ({pick_rate:.1f}%) • {wr:.1f}% WR")
        
        embed.add_field(
            name="\u200b\nMost Picked Brawlers",
            value="\n".join(picks_text) if picks_text else "No data",
            inline=False
        )
        
        filtered_brawlers = [(b, brawler_wins[b] / brawler_picks[b]) for b in brawler_picks if brawler_picks[b] >= 1]
        sorted_by_wr = sorted(filtered_brawlers, key=lambda x: x[1], reverse=True)[:15]
        
        wr_text = []
        for brawler, wr in sorted_by_wr:
            picks = brawler_picks[brawler]
            pick_rate = (picks / total_picks * 100) if total_picks > 0 else 0
            wr_text.append(f"**{brawler}**: {wr*100:.1f}% WR • {picks} picks ({pick_rate:.1f}%)")
        
        embed.add_field(
            name="\u200b\n🏆 Highest Win Rate",
            value="\n".join(wr_text) if wr_text else "No data",
            inline=False
        )
        
        return embed
    
    @discord.ui.button(label="View Maps", style=discord.ButtonStyle.primary, row=0)
    async def maps_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = AllRegionsMapSelectView(self.mode)
        await interaction.followup.send("Select a map:", view=view, ephemeral=True)
    
    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=0)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = AllRegionsModeSelectView()
        await interaction.edit_original_response(content="Select a game mode to view regional statistics:", embed=None, view=view)


class AllRegionsMapSelectView(View):
    """Dropdown to select a map for all-region statistics"""
    def __init__(self, mode: str):
        super().__init__(timeout=300)
        self.mode = mode
        
        all_maps = defaultdict(int)
        for team_data in teams_data.values():
            if mode in team_data['modes']:
                for map_name, map_data in team_data['modes'][mode]['maps'].items():
                    all_maps[map_name] += map_data['matches']
        
        sorted_maps = sorted(all_maps.items(), key=lambda x: x[1], reverse=True)
        
        if sorted_maps:
            options = [
                discord.SelectOption(
                    label=map_name[:100],
                    description=f"{matches} matches",
                    value=map_name[:100]
                )
                for map_name, matches in sorted_maps[:25]
            ]
            
            select = Select(placeholder="Choose a map...", options=options)
            select.callback = self.select_callback
            self.add_item(select)
        
        # Add back button
        back_btn = Button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=1)
        back_btn.callback = self.back_callback
        self.add_item(back_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        map_name = interaction.data['values'][0]
        view = AllRegionsMapDetailView(self.mode, map_name)
        embed = view.create_map_embed()
        
        map_image = get_map_image(self.mode, map_name)
        if map_image:
            file = discord.File(map_image, filename="map.png")
            await interaction.response.send_message(embed=embed, view=view, file=file, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    
    async def back_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        view = AllRegionsModeDetailView(self.mode)
        embed = view.create_mode_embed()
        await interaction.edit_original_response(embed=embed, view=view, attachments=[])


class AllRegionsMapDetailView(View):
    """View showing map statistics across all regions with sortable brawlers"""
    def __init__(self, mode: str, map_name: str, sort_by: str = 'picks'):
        super().__init__(timeout=300)
        self.mode = mode
        self.map_name = map_name
        self.sort_by = sort_by
    
    def create_map_embed(self):
        sort_text = 'Pick Rate' if self.sort_by == 'picks' else ('Win Rate' if self.sort_by == 'winrate' else 'Best Pick (WR × Pick)')
        embed = discord.Embed(
            title=f"{self.map_name}",
            description=f"**{self.mode}** - All Regions\n**Sorted by:** {sort_text} ",
            color=discord.Color.red()
        )
        
        map_image = get_map_image(self.mode, self.map_name)
        if map_image:
            embed.set_image(url="attachment://map.png")
        
        brawler_picks = defaultdict(int)
        brawler_wins = defaultdict(int)
        total_matches = 0
        
        for team_data in teams_data.values():
            if self.mode in team_data['modes']:
                if self.map_name in team_data['modes'][self.mode]['maps']:
                    map_data = team_data['modes'][self.mode]['maps'][self.map_name]
                    total_matches += map_data['matches']
                    
                    for brawler, brawler_data in map_data['brawlers'].items():
                        brawler_picks[brawler] += brawler_data['picks']
                        brawler_wins[brawler] += brawler_data['wins']
        
        embed.add_field(name="⚔️ Matches", value=f"**{total_matches}**", inline=True)
        
        total_picks = sum(brawler_picks.values())

        if self.sort_by == 'picks':
            sorted_brawlers = sorted(brawler_picks.items(), key=lambda x: x[1], reverse=True)
        elif self.sort_by == 'winrate':
            sorted_brawlers = sorted(
                [(b, brawler_wins[b] / brawler_picks[b]) for b in brawler_picks if brawler_picks[b] >= 1],
                key=lambda x: x[1],
                reverse=True
            )
        else:  # value = pick_rate * win_rate
            brawler_values = []
            for brawler in brawler_picks:
                if brawler_picks[brawler] >= 1:
                    pick_rate_pct = (brawler_picks[brawler] / total_picks) * 100
                    win_rate_pct = (brawler_wins[brawler] / brawler_picks[brawler]) * 100
                    value_score = win_rate_pct * pick_rate_pct
                    brawler_values.append((brawler, value_score))
            sorted_brawlers = sorted(brawler_values, key=lambda x: x[1], reverse=True)
        
        brawler_text = []
        
        for item in sorted_brawlers:
            if self.sort_by == 'picks':
                brawler, picks = item
            elif self.sort_by == 'winrate':
                brawler, _ = item
            else:  # value sort
                brawler, _ = item
            
            # Always get actual picks and wins for display
            picks = brawler_picks[brawler]
            wr = (brawler_wins[brawler] / picks * 100) if picks > 0 else 0
            pick_rate = (picks / total_picks * 100) if total_picks > 0 else 0
            
            brawler_text.append(f"**{brawler}**: {picks} picks ({pick_rate:.1f}%) • {wr:.1f}% WR")
        
        all_brawlers = "\n".join(brawler_text) if brawler_text else "No data"
        
        if len(all_brawlers) > 1024:
            current_chunk = []
            current_length = 0
            field_num = 0
            
            for line in brawler_text:
                line_length = len(line) + 1
                if current_length + line_length > 1024:
                    field_name = "\u200b\nBrawler Picks & Win Rates" if field_num == 0 else "\u200b"
                    embed.add_field(name=field_name, value="\n".join(current_chunk), inline=False)
                    current_chunk = [line]
                    current_length = line_length
                    field_num += 1
                else:
                    current_chunk.append(line)
                    current_length += line_length
            
            if current_chunk:
                field_name = "\u200b\nBrawler Picks & Win Rates" if field_num == 0 else "\u200b"
                embed.add_field(name=field_name, value="\n".join(current_chunk), inline=False)
        else:
            embed.add_field(
                name="\u200b\nBrawler Picks & Win Rates",
                value="\n" + all_brawlers,
                inline=False
            )
        
        return embed
    
    @discord.ui.button(label="Sort by Pick Rate", style=discord.ButtonStyle.primary, row=0)
    async def sort_picks_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        self.sort_by = 'picks'
        embed = self.create_map_embed()
        
        map_image = get_map_image(self.mode, self.map_name)
        if map_image:
            file = discord.File(map_image, filename="map.png")
            await interaction.edit_original_response(embed=embed, view=self, attachments=[file])
        else:
            await interaction.edit_original_response(embed=embed, view=self)
    
    @discord.ui.button(label="Sort by Win Rate", style=discord.ButtonStyle.primary, row=0)
    async def sort_wr_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        self.sort_by = 'winrate'
        embed = self.create_map_embed()
        
        map_image = get_map_image(self.mode, self.map_name)
        if map_image:
            file = discord.File(map_image, filename="map.png")
            await interaction.edit_original_response(embed=embed, view=self, attachments=[file])
        else:
            await interaction.edit_original_response(embed=embed, view=self)

    @discord.ui.button(label="Sort by Best Pick", style=discord.ButtonStyle.success, row=1)
    async def sort_value_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        self.sort_by = 'value'
        embed = self.create_map_embed()
        
        map_image = get_map_image(self.mode, self.map_name)
        if map_image:
            file = discord.File(map_image, filename="map.png")
            await interaction.edit_original_response(embed=embed, view=self, attachments=[file])
        else:
            await interaction.edit_original_response(embed=embed, view=self)
    
    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = AllRegionsMapSelectView(self.mode)
        await interaction.edit_original_response(content="Select a map:", embed=None, view=view, attachments=[])

class RegionView(View):
    """View for a specific region"""
    def __init__(self, region: str):
        super().__init__(timeout=300)
        self.region = region
    
    def create_region_embed(self):
        stats = region_stats.get(self.region, {})
        region_teams = {name: data for name, data in teams_data.items() if data['region'] == self.region}
        
        embed = discord.Embed(
            title=f"🌐 {self.region} Region Statistics",
            description=f"Competitive statistics for {self.region} region teams",
            color=discord.Color.red(),
            timestamp=datetime.now()
        )
        
        total_matches = stats.get('total_matches', 0) // 2
        team_count = len(region_teams)
        
        embed.add_field(name="⚔️ Total Matches", value=f"**{total_matches}**", inline=True)
        embed.add_field(name="Teams", value=f"**{team_count}**", inline=True)
        
        total_wins = sum(t['wins'] for t in region_teams.values())
        total_games = sum(t['matches'] for t in region_teams.values())
        overall_wr = (total_wins / total_games * 100) if total_games > 0 else 0
        embed.add_field(name="Avg Win Rate", value=f"**{overall_wr:.1f}%\n\n**", inline=True)
        
        sorted_teams = sorted(
            region_teams.items(),
            key=lambda x: (x[1]['wins'] / x[1]['matches']) if x[1]['matches'] > 0 else 0,
            reverse=True
        )
        
        leaderboard = []
        for i, (team_name, data) in enumerate(sorted_teams, 1):
            wr = (data['wins'] / data['matches'] * 100) if data['matches'] > 0 else 0
            leaderboard.append(
                f"**{i}.** {team_name}\n"
                f"     └ {data['wins']}-{data['losses']} • {wr:.1f}% WR"
            )
        
        embed.add_field(
            name=f"🏆 {self.region} Leaderboard",
            value="\n".join(leaderboard) if leaderboard else "No teams",
            inline=False
        )
        
        return embed
    
    @discord.ui.button(label="View Teams", style=discord.ButtonStyle.primary, row=0)
    async def teams_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = TeamSelectView(self.region)
        await interaction.followup.send("Select a team to view detailed stats:", view=view, ephemeral=True)
    
    @discord.ui.button(label="View Modes & Maps", style=discord.ButtonStyle.primary, row=0)
    async def modes_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = RegionModeSelectView(self.region)
        await interaction.followup.send("Select a game mode to view regional statistics:", view=view, ephemeral=True)
    
    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = WelcomeView()
        embed = create_welcome_embed()
        await interaction.edit_original_response(embed=embed, view=view)


class RegionModeSelectView(View):
    """Dropdown to select a mode for region-specific statistics"""
    def __init__(self, region: str):
        super().__init__(timeout=300)
        self.region = region
        
        all_modes = set()
        for team_name, team_data in teams_data.items():
            if team_data['region'] == region:
                for mode in team_data['modes'].keys():
                    if mode in VALID_MODES:  # **NEW: Only valid modes**
                        all_modes.add(mode)
        
        sorted_modes = sorted(all_modes)
        
        if sorted_modes:
            options = [
                discord.SelectOption(label=mode, value=mode)
                for mode in sorted_modes
            ]
            
            select = Select(placeholder="Choose a game mode...", options=options)
            select.callback = self.select_callback
            self.add_item(select)
        
        # Add back button
        back_btn = Button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=1)
        back_btn.callback = self.back_callback
        self.add_item(back_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        mode = interaction.data['values'][0]
        view = RegionModeDetailView(self.region, mode)
        embed = view.create_mode_embed()
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    
    async def back_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        view = RegionView(self.region)
        embed = view.create_region_embed()
        await interaction.edit_original_response(embed=embed, view=view)


class RegionModeDetailView(View):
    """View showing mode statistics for a specific region"""
    def __init__(self, region: str, mode: str):
        super().__init__(timeout=300)
        self.region = region
        self.mode = mode
    
    def create_mode_embed(self):
        embed = discord.Embed(
            title=f"{self.mode} - {self.region} Region",
            description=f"Statistics for {self.region} teams in this mode",
            color=discord.Color.red()
        )
        
        brawler_picks = defaultdict(int)
        brawler_wins = defaultdict(int)
        total_matches = 0
        
        for team_name, team_data in teams_data.items():
            if team_data['region'] == self.region and self.mode in team_data['modes']:
                mode_data = team_data['modes'][self.mode]
                total_matches += mode_data['matches']
                
                for map_name, map_data in mode_data['maps'].items():
                    for brawler, brawler_data in map_data['brawlers'].items():
                        brawler_picks[brawler] += brawler_data['picks']
                        brawler_wins[brawler] += brawler_data['wins']
        
        embed.add_field(name="Total Matches", value=f"**{total_matches}**", inline=True)
        
        sorted_by_picks = sorted(brawler_picks.items(), key=lambda x: x[1], reverse=True)[:15]
        picks_text = []
        total_picks = sum(brawler_picks.values())
        
        for brawler, picks in sorted_by_picks:
            pick_rate = (picks / total_picks * 100) if total_picks > 0 else 0
            wr = (brawler_wins[brawler] / picks * 100) if picks > 0 else 0
            picks_text.append(f"**{brawler}**: {picks} ({pick_rate:.1f}%) • {wr:.1f}% WR")
        
        embed.add_field(
            name="\u200b\nMost Picked Brawlers",
            value="\n".join(picks_text) if picks_text else "No data",
            inline=False
        )
        
        filtered_brawlers = [(b, brawler_wins[b] / brawler_picks[b]) for b in brawler_picks if brawler_picks[b] >= 1]
        sorted_by_wr = sorted(filtered_brawlers, key=lambda x: x[1], reverse=True)[:15]
        
        wr_text = []
        for brawler, wr in sorted_by_wr:
            picks = brawler_picks[brawler]
            pick_rate = (picks / total_picks * 100) if total_picks > 0 else 0
            wr_text.append(f"**{brawler}**: {wr*100:.1f}% WR • {picks} picks ({pick_rate:.1f}%)")
        
        embed.add_field(
            name="\u200b\n🏆 Highest Win Rate",
            value="\n".join(wr_text) if wr_text else "No data",
            inline=False
        )
        
        return embed
    
    @discord.ui.button(label="View Maps", style=discord.ButtonStyle.primary, row=0)
    async def maps_button(self, interaction: discord.Interaction, button: Button):
        view = RegionMapSelectView(self.region, self.mode)
        
        if not view.children:
            await interaction.response.send_message("❌ No maps available for this mode in this region.", ephemeral=True)
            return
            
        await interaction.response.send_message("Select a map:", view=view, ephemeral=True)
    
    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=0)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = RegionModeSelectView(self.region)
        await interaction.edit_original_response(content="Select a game mode to view regional statistics:", embed=None, view=view)


class WelcomeViewOffseason(View):
    """Off-season welcome screen with region selection"""
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="🌍 ALL REGIONS", style=discord.ButtonStyle.primary, row=0)
    async def all_players_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = OffseasonAllPlayersView()
        embed = view.create_all_players_embed()
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    
    @discord.ui.button(label="CURRENT META", style=discord.ButtonStyle.red, row=0)
    async def meta_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        
        global_brawlers = region_stats.get('_global_brawlers', {})
        total_brawlers = len(global_brawlers)
        games_analyzed = len(matches_df) if matches_df is not None else 0
        
        if matches_df is not None and 'battle_time' in matches_df.columns:
            latest_match = matches_df['battle_time'].max()
            if pd.notna(latest_match):
                time_diff = pd.Timestamp.now(tz='UTC') - pd.to_datetime(latest_match, utc=True)
                hours = int(time_diff.total_seconds() / 3600)
                if hours < 1:
                    minutes = int(time_diff.total_seconds() / 60)
                    last_update = f"{minutes} min ago"
                elif hours < 24:
                    last_update = f"{hours}h ago"
                else:
                    days = int(time_diff.total_seconds() / 86400)
                    last_update = f"{days}d ago"
            else:
                last_update = "Unknown"
        else:
            last_update = "Unknown"
        
        embed = discord.Embed(
            title="Current Meta Analysis",
            description="Select a region below to view detailed meta statistics and tier rankings.",
            color=discord.Color.red()
        )
        
        total_players = len(players_data)
        valid_regions = [r for r in region_stats.keys() if not r.startswith('_')]
        total_matches = sum(region_stats[r]['total_matches'] for r in valid_regions)

        embed.add_field(name="Brawlers Tracked", value=f"{total_brawlers}", inline=True)
        embed.add_field(name="Matches Analyzed", value=f"{total_matches}", inline=True)
        embed.add_field(name="Latest Match", value=f"{last_update}", inline=True)
        
        view = MetaView()
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    
    @discord.ui.button(label="Tracked Players", style=discord.ButtonStyle.primary, row=2)
    async def tracked_players_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = TrackedPlayersListView()
        embed = view.create_players_list_embed()
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    
    @discord.ui.button(label="Possible Teams", style=discord.ButtonStyle.primary, row=2)
    async def possible_teams_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = PossibleTeamsView()
        embed = view.create_possible_teams_embed()
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    
    @discord.ui.button(label="🇺🇸 NA", style=discord.ButtonStyle.secondary, row=1)
    async def na_button(self, interaction: discord.Interaction, button: Button):
        await self.show_region(interaction, 'NA')
    
    @discord.ui.button(label="🇪🇺 EU", style=discord.ButtonStyle.secondary, row=1)
    async def eu_button(self, interaction: discord.Interaction, button: Button):
        await self.show_region(interaction, 'EU')
    
    @discord.ui.button(label="🇧🇷 LATAM", style=discord.ButtonStyle.secondary, row=1)
    async def latam_button(self, interaction: discord.Interaction, button: Button):
        await self.show_region(interaction, 'LATAM')
    
    @discord.ui.button(label="🌏 EA", style=discord.ButtonStyle.secondary, row=1)
    async def ea_button(self, interaction: discord.Interaction, button: Button):
        await self.show_region(interaction, 'EA')
    
    @discord.ui.button(label="🌏 SEA", style=discord.ButtonStyle.secondary, row=1)
    async def sea_button(self, interaction: discord.Interaction, button: Button):
        await self.show_region(interaction, 'SEA')
    
    async def show_region(self, interaction: discord.Interaction, region: str):
        await interaction.response.defer()
        view = OffseasonRegionView(region)
        embed = view.create_region_embed()
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)


class PossibleTeamsView(View):
    """Shows possible team combinations based on teammate frequency"""
    def __init__(self, region: str = 'ALL', sort_by: str = 'games'):
        super().__init__(timeout=300)
        self.region = region
        self.sort_by = sort_by
    
    def create_possible_teams_embed(self):
        """Generate embed showing possible team combinations"""
        region_display = "All Regions" if self.region == 'ALL' else f"{self.region} Region"
        sort_text = "Games Together" if self.sort_by == 'games' else "Win Rate Together"
        
        embed = discord.Embed(
            title="🏖️ Possible Teams For Next Season",
            description=f"**{region_display}**\n**Sorted by:** {sort_text}",
            color=discord.Color.red()
        )
        
        # Group players by region first
        if self.region == 'ALL':
            # Group all players by their region
            players_by_region = defaultdict(dict)
            for tag, data in players_data.items():
                players_by_region[data['region']][tag] = data
        else:
            # Only one region
            players_by_region = {
                self.region: {tag: data for tag, data in players_data.items() if data['region'] == self.region}
            }
        
        # Calculate trios per region (players from same region only)
        all_trios = []
        
        for region, region_players in players_by_region.items():
            if len(region_players) < 3:
                continue  # Need at least 3 players to make a trio
            
            player_tags = list(region_players.keys())
            
            for i in range(len(player_tags)):
                for j in range(i + 1, len(player_tags)):
                    for k in range(j + 1, len(player_tags)):
                        tag1, tag2, tag3 = player_tags[i], player_tags[j], player_tags[k]
                        
                        # Use cached trio stats
                        trio_key = tuple(sorted([tag1, tag2, tag3]))
                        
                        if trio_key not in cached_trios:
                            continue  # Skip if this trio hasn't played together
                        
                        trio_stats = cached_trios[trio_key]
                        trio_games = trio_stats['games']
                        trio_wins = trio_stats['wins']
                        
                        # Only include if all three have played together at least 3 games
                        if trio_games >= 3:
                            player1 = region_players[tag1]
                            player2 = region_players[tag2]
                            player3 = region_players[tag3]
                            
                            trio_wr = (trio_wins / trio_games * 100) if trio_games > 0 else 0
                            
                            all_trios.append({
                                'players': [player1['name'], player2['name'], player3['name']],
                                'tags': [tag1, tag2, tag3],
                                'region': region,
                                'games_together': trio_games,
                                'trio_winrate': trio_wr,
                                'trio_wins': trio_wins,
                            })
        
        # Sort trios
        if self.sort_by == 'games':
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
        
        # Display results
        if not unique_trios:
            embed.add_field(
                name="No Combinations Found",
                value="No players have played together enough to form trios (min 3 games)",
                inline=False
            )
        else:
            trio_text = []
            for i, trio in enumerate(unique_trios, 1):
                players_str = " • ".join(trio['players'])
                
                # Add region flag if showing all regions
                region_flag = {
                    'NA': '🇺🇸',
                    'EU': '🇪🇺',
                    'LATAM': '🇧🇷',
                    'EA': '🌏',
                    'SEA': '🌏'
                }.get(trio['region'], '🌐')
                
                if self.region == 'ALL':
                    trio_text.append(
                        f"`#{i:2d}` {region_flag} **{players_str}**\n"
                        f"      └ {trio['games_together']} games • {trio['trio_wins']}-{trio['games_together']-trio['trio_wins']} ({trio['trio_winrate']:.1f}% WR together)"
                    )
                else:
                    trio_text.append(
                        f"`#{i:2d}` **{players_str}**\n"
                        f"      └ {trio['games_together']} games • {trio['trio_wins']}-{trio['games_together']-trio['trio_wins']} ({trio['trio_winrate']:.1f}% WR together)"
                    )
            
            # Split into chunks of 5 trios each
            chunk_size = 5
            chunks = [trio_text[i:i+chunk_size] for i in range(0, len(trio_text), chunk_size)]
            
            for idx, chunk in enumerate(chunks):
                field_name = "🏆 Top Teams" if idx == 0 else "\u200b"
                embed.add_field(
                    name=field_name,
                    value="\n".join(chunk),
                    inline=False
                )
        
        return embed

    @discord.ui.button(label="Sort by Games", style=discord.ButtonStyle.primary, row=0)
    async def sort_games_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        self.sort_by = 'games'
        embed = self.create_possible_teams_embed()
        await interaction.edit_original_response(embed=embed, view=self)
    
    @discord.ui.button(label="Sort by Win Rate", style=discord.ButtonStyle.primary, row=0)
    async def sort_wr_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        self.sort_by = 'winrate'
        embed = self.create_possible_teams_embed()
        await interaction.edit_original_response(embed=embed, view=self)
    
    @discord.ui.button(label="🇺🇸 NA", style=discord.ButtonStyle.secondary, row=1)
    async def na_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = PossibleTeamsView(region='NA', sort_by=self.sort_by)
        embed = view.create_possible_teams_embed()
        await interaction.edit_original_response(embed=embed, view=view)
    
    @discord.ui.button(label="🇪🇺 EU", style=discord.ButtonStyle.secondary, row=1)
    async def eu_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = PossibleTeamsView(region='EU', sort_by=self.sort_by)
        embed = view.create_possible_teams_embed()
        await interaction.edit_original_response(embed=embed, view=view)
    
    @discord.ui.button(label="🇧🇷 LATAM", style=discord.ButtonStyle.secondary, row=1)
    async def latam_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = PossibleTeamsView(region='LATAM', sort_by=self.sort_by)
        embed = view.create_possible_teams_embed()
        await interaction.edit_original_response(embed=embed, view=view)
    
    @discord.ui.button(label="🌏 EA", style=discord.ButtonStyle.secondary, row=2)
    async def ea_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = PossibleTeamsView(region='EA', sort_by=self.sort_by)
        embed = view.create_possible_teams_embed()
        await interaction.edit_original_response(embed=embed, view=view)
    
    @discord.ui.button(label="🌏 SEA", style=discord.ButtonStyle.secondary, row=2)
    async def sea_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = PossibleTeamsView(region='SEA', sort_by=self.sort_by)
        embed = view.create_possible_teams_embed()
        await interaction.edit_original_response(embed=embed, view=view)
    
    @discord.ui.button(label="🌍 ALL", style=discord.ButtonStyle.secondary, row=2)
    async def all_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = PossibleTeamsView(region='ALL', sort_by=self.sort_by)
        embed = view.create_possible_teams_embed()
        await interaction.edit_original_response(embed=embed, view=view)
    
    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=3)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        await interaction.delete_original_response()


class OffseasonAllPlayersView(View):
    """View showing all tracked players across regions"""
    def __init__(self):
        super().__init__(timeout=300)
    
    @discord.ui.button(label="Select Player", style=discord.ButtonStyle.primary, row=0)
    async def select_player_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        
        # Get top 10 players from the leaderboard
        top_players = []
        for tag, data in players_data.items():
            if data['matches'] >= 5:  # Minimum 5 matches
                wr = (data['wins'] / data['matches'] * 100) if data['matches'] > 0 else 0
                top_players.append((tag, data, wr))
        
        top_players.sort(key=lambda x: x[2], reverse=True)
        top_players = top_players[:10]  # Only top 10
        
        # Format for the dropdown view
        top_10_list = [(tag, data) for tag, data, wr in top_players]
        
        view = PlayerSelectViewOffseasonFromList(top_10_list)
        await interaction.followup.send("Select a player from the top 10:", view=view, ephemeral=True)

    @discord.ui.button(label="View Modes & Maps", style=discord.ButtonStyle.primary, row=0)
    async def modes_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = OffseasonModeSelectView('ALL')  # Pass 'ALL' for global stats
        await interaction.followup.send("Select a game mode:", view=view, ephemeral=True)
    
    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = WelcomeViewOffseason()
        embed = create_welcome_embed()
        await interaction.edit_original_response(embed=embed, view=view, attachments=[])
    
    def create_all_players_embed(self):
        embed = discord.Embed(
            title="🌐 All Players Overview",
            description="Statistics across all tracked players",
            color=discord.Color.red(),
            timestamp=datetime.now()
        )
        
        # Calculate total matches by summing regional matches
        valid_regions = [r for r in region_stats.keys() if not r.startswith('_')]
        total_matches = sum(region_stats[r]['total_matches'] for r in valid_regions)
        total_players = len(players_data)
        
        embed.add_field(name="Total Matches", value=f"**{total_matches}**", inline=True)
        embed.add_field(name="Total Players", value=f"**{total_players}**", inline=True)
        embed.add_field(name="Regions", value=f"**{len(valid_regions)}**", inline=True)
        
        region_text = []
        for region in sorted(valid_regions):
            stats = region_stats[region]
            player_count = len(stats['players'])
            matches = stats['total_matches']
            region_text.append(f"**{region}**: {player_count} players • {matches} matches")
        
        embed.add_field(
            name="Regional Breakdown",
            value="\n".join(region_text) if region_text else "No data",
            inline=False
        )
        
        # Top 10 players by win rate (LIMITED TO 10)
        top_players = []
        for tag, data in players_data.items():
            if data['matches'] >= 5:  # Minimum 5 matches
                wr = (data['wins'] / data['matches'] * 100) if data['matches'] > 0 else 0
                top_players.append((data['name'], data['region'], data['wins'], data['losses'], wr, data['matches']))
        
        top_players.sort(key=lambda x: x[4], reverse=True)
        top_players = top_players[:10]  # LIMIT TO TOP 10
        
        leaderboard = []
        for i, (name, region, wins, losses, wr, matches) in enumerate(top_players, 1):
            leaderboard.append(
                f"**{i}.** {name} ({region})\n"
                f"     └ {wins}-{losses} • {wr:.1f}% WR"
            )
        
        embed.add_field(
            name="\u200b\n🏆 Top 10 Players (by Win Rate, min 5 matches)",
            value="\n".join(leaderboard) if leaderboard else "No data",
            inline=False
        )
        
        return embed


class OffseasonRegionView(View):
    """View for a specific region in off-season with pagination"""
    def __init__(self, region: str, page: int = 0):
        super().__init__(timeout=300)
        self.region = region
        self.page = page
        self.sort_by = 'matches'
        self.players_per_page = 15
        
        # Add pagination buttons only if needed
        total_players = len([tag for tag, data in players_data.items() if data['region'] == region])
        total_pages = (total_players + self.players_per_page - 1) // self.players_per_page
        
        if total_pages > 1:
            # Add Previous button
            prev_btn = Button(label="◀️ Previous", style=discord.ButtonStyle.secondary, row=0, custom_id="prev_page_region")
            prev_btn.callback = self.prev_button_callback
            self.add_item(prev_btn)
            
            # Add Next button
            next_btn = Button(label="Next ▶️", style=discord.ButtonStyle.secondary, row=0, custom_id="next_page_region")
            next_btn.callback = self.next_button_callback
            self.add_item(next_btn)
    
    def get_sorted_players(self):
        """Get sorted list of players based on current sort criteria - exclude 0 matches"""
        # Filter by region first, then filter out players with no matches
        active_players = [
            (tag, data) for tag, data in players_data.items() 
            if data['region'] == self.region and data['matches'] > 0
        ]
        
        if self.sort_by == 'matches':
            return sorted(active_players, key=lambda x: x[1]['matches'], reverse=True)
        elif self.sort_by == 'winrate':
            return sorted(
                active_players,
                key=lambda x: (x[1]['wins'] / x[1]['matches']) if x[1]['matches'] > 0 else 0,
                reverse=True
            )
        else:  # name
            return sorted(active_players, key=lambda x: x[1]['name'].lower())
    
    def create_region_embed(self):
        stats = region_stats.get(self.region, {})
        
        # Filter players by region
        region_players = {tag: data for tag, data in players_data.items() if data['region'] == self.region}
        
        embed = discord.Embed(
            title=f"🌐 {self.region} Region Statistics",
            description=f"Player statistics for {self.region} region",
            color=discord.Color.red(),
            timestamp=datetime.now()
        )
        
        total_matches = stats.get('total_matches', 0)
        player_count = len(region_players)
        
        embed.add_field(name="⚔️ Total Matches", value=f"**{total_matches}**", inline=True)
        embed.add_field(name="Players", value=f"**{player_count}**", inline=True)
        
        # Calculate average win rate
        total_wins = sum(p['wins'] for p in region_players.values())
        total_games = sum(p['matches'] for p in region_players.values())
        avg_wr = (total_wins / total_games * 100) if total_games > 0 else 0
        embed.add_field(name="Avg Win Rate", value=f"**{avg_wr:.1f}%\n\n**", inline=True)
        
        # Get sorted players and paginate
        sorted_players = self.get_sorted_players()
        total_players = len(sorted_players)
        total_pages = (total_players + self.players_per_page - 1) // self.players_per_page
        
        # Ensure page is within bounds
        self.page = max(0, min(self.page, total_pages - 1))
        
        start_idx = self.page * self.players_per_page
        end_idx = min(start_idx + self.players_per_page, total_players)
        page_players = sorted_players[start_idx:end_idx]
        
        leaderboard = []
        for i, (tag, data) in enumerate(page_players, start_idx + 1):
            wr = (data['wins'] / data['matches'] * 100) if data['matches'] > 0 else 0
            leaderboard.append(
                f"**{i}.** {data['name']}\n"
                f"     └ {data['wins']}-{data['losses']} • {wr:.1f}% WR"
            )
        
        page_info = f" (Page {self.page + 1}/{total_pages})" if total_pages > 1 else ""
        embed.add_field(
            name=f"🏆 {self.region} Player Rankings{page_info}",
            value="\n".join(leaderboard) if leaderboard else "No players",
            inline=False
        )
        
        if total_pages > 1:
            embed.set_footer(text=f"Showing players {start_idx + 1}-{end_idx} of {total_players}")
        
        return embed
    
    def update_button_states(self):
        """Enable/disable navigation buttons based on current page"""
        sorted_players = self.get_sorted_players()
        total_pages = (len(sorted_players) + self.players_per_page - 1) // self.players_per_page
        
        for item in self.children:
            if isinstance(item, Button):
                if item.custom_id == "prev_page_region":
                    item.disabled = (self.page == 0)
                elif item.custom_id == "next_page_region":
                    item.disabled = (self.page >= total_pages - 1)
    
    async def prev_button_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if self.page > 0:
            self.page -= 1
            embed = self.create_region_embed()
            self.update_button_states()
            await interaction.edit_original_response(embed=embed, view=self)
    
    async def next_button_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        sorted_players = self.get_sorted_players()
        total_pages = (len(sorted_players) + self.players_per_page - 1) // self.players_per_page
        if self.page < total_pages - 1:
            self.page += 1
            embed = self.create_region_embed()
            self.update_button_states()
            await interaction.edit_original_response(embed=embed, view=self)
    
    @discord.ui.button(label="Select Player", style=discord.ButtonStyle.primary, row=1)
    async def players_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        
        # Get current page players
        sorted_players = self.get_sorted_players()
        start_idx = self.page * self.players_per_page
        end_idx = min(start_idx + self.players_per_page, len(sorted_players))
        page_players = sorted_players[start_idx:end_idx]
        
        view = OffseasonRegionPlayerSelectView(self.region, self.page, page_players)
        await interaction.followup.send("Select a player from this page:", view=view, ephemeral=True)
    
    @discord.ui.button(label="View Modes & Maps", style=discord.ButtonStyle.primary, row=1)
    async def modes_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = OffseasonModeSelectView(region=self.region)
        await interaction.followup.send("Select a game mode:", view=view, ephemeral=True)
    
    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=2)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = WelcomeViewOffseason()
        embed = create_welcome_embed()
        await interaction.edit_original_response(embed=embed, view=view, attachments=[])



class OffseasonRegionPlayerSelectView(View):
    """Dropdown to select a player from a specific region - shows current page players"""
    def __init__(self, region: str, page: int = 0, page_players: list = None):
        super().__init__(timeout=300)
        self.region = region
        self.page = page
        
        # Use provided page_players or get all region players
        if page_players is None:
            region_players = [(tag, data) for tag, data in players_data.items() if data['region'] == region]
            region_players.sort(key=lambda x: x[1]['name'].lower())
            page_players = region_players[:15]  # Default to first 15
        
        options = [
            discord.SelectOption(
                label=data['name'],
                description=f"{data['matches']} games • {data['wins']}-{data['losses']}",
                value=tag
            )
            for tag, data in page_players
        ]
        
        if options:
            select = Select(placeholder="Choose a player from this page...", options=options)
            select.callback = self.select_callback
            self.add_item(select)
        
        # Add back button
        back_btn = Button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=1)
        back_btn.callback = self.back_callback
        self.add_item(back_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        player_tag = interaction.data['values'][0]
        
        view = PlayerDetailViewOffseason(player_tag)
        embed = view.create_player_embed()
        
        # Get top brawler image for thumbnail
        player = players_data[player_tag]
        sorted_brawlers = sorted(player['brawlers'].items(), key=lambda x: x[1]['picks'], reverse=True)
        
        if sorted_brawlers:
            top_brawler = sorted_brawlers[0][0]
            brawler_img_path = get_brawler_image(top_brawler)
            if brawler_img_path:
                file = discord.File(brawler_img_path, filename=f"{top_brawler}_icon.png")
                await interaction.response.send_message(embed=embed, view=view, file=file, ephemeral=True)
                return
        
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    
    async def back_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        view = OffseasonRegionView(self.region, self.page)
        embed = view.create_region_embed()
        view.update_button_states()
        await interaction.edit_original_response(embed=embed, view=view)


class OffseasonMapSelectView(View):
    """Map selection for off-season mode"""
    def __init__(self, mode: str, region: str = 'ALL'):
        super().__init__(timeout=300)
        self.mode = mode
        self.region = region
        
        mode_map_data = region_stats.get('_mode_map_stats', {})
        maps_data = mode_map_data.get(mode, {})
        
        sorted_maps = sorted(maps_data.items(), key=lambda x: x[1]['matches'], reverse=True)
        
        if sorted_maps:
            options = [
                discord.SelectOption(
                    label=map_name[:100],
                    description=f"{data['matches']} matches"[:100],
                    value=map_name[:100]
                )
                for map_name, data in sorted_maps[:25]
                if map_name not in ['Unknown', 'nan']
            ]
            
            if options:
                select = Select(placeholder="Choose a map...", options=options)
                select.callback = self.select_callback
                self.add_item(select)
        
        back_btn = Button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=1)
        back_btn.callback = self.back_callback
        self.add_item(back_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        map_name = interaction.data['values'][0]
        
        # Generate and send image immediately
        await interaction.response.defer()
        
        img_bytes = generate_map_stats_image_offseason(self.mode, map_name, sort_by='meta')
        
        if img_bytes:
            file = discord.File(img_bytes, filename=f"{self.mode}_{map_name}_stats.png")
            
            embed = discord.Embed(
                title=f"{map_name}",
                description=f"**{self.mode}**\n**Sorted by:** Meta Score (WR × Pick Rate)",
                color=discord.Color.red(),
                timestamp=datetime.now()
            )
            embed.set_image(url=f"attachment://{self.mode}_{map_name}_stats.png")
            
            # Add map thumbnail
            map_image_path = get_map_image(self.mode, map_name)
            files_to_send = [file]  # Start with stats image
            
            if map_image_path and os.path.exists(map_image_path):
                map_file = discord.File(map_image_path, filename=f"{map_name}_icon.png")
                embed.set_thumbnail(url=f"attachment://{map_name}_icon.png")
                files_to_send.append(map_file)
            
            # Create view with sort buttons
            view = OffseasonMapDetailView(self.mode, map_name, sort_by='meta')
            await interaction.followup.send(embed=embed, files=files_to_send, view=view, ephemeral=True)
        else:
            await interaction.followup.send("❌ Could not generate stats image - not enough data", ephemeral=True)
    
    async def back_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        view = OffseasonModeDetailView(self.mode, self.region, sort_by='meta')
        
        # Regenerate mode image
        img_bytes = generate_mode_stats_image_offseason_region(self.mode, self.region, sort_by='meta')
        if img_bytes:
            file = discord.File(img_bytes, filename=f"{self.mode}_stats.png")
            
            # Get mode data for top 10
            mode_map_data = region_stats.get('_mode_map_stats', {})
            mode_data = mode_map_data.get(self.mode, {})
            
            brawler_picks = defaultdict(int)
            brawler_wins = defaultdict(int)
            
            # Filter by region if not ALL
            if self.region != 'ALL':
                for map_name, map_stats in mode_data.items():
                    for brawler, data in map_stats['brawlers'].items():
                        region_picks = 0
                        region_wins = 0
                        
                        for player_tag, player_data in players_data.items():
                            if player_data['region'] == self.region:
                                if self.mode in player_data['modes'] and map_name in player_data['modes'][self.mode]['maps']:
                                    brawler_stats = player_data['modes'][self.mode]['maps'][map_name]['brawlers'].get(brawler, {})
                                    region_picks += brawler_stats.get('picks', 0)
                                    region_wins += brawler_stats.get('wins', 0)
                        
                        brawler_picks[brawler] += region_picks
                        brawler_wins[brawler] += region_wins
            else:
                for map_name, map_stats in mode_data.items():
                    for brawler, data in map_stats['brawlers'].items():
                        brawler_picks[brawler] += data['picks']
                        brawler_wins[brawler] += data['wins']
            
            total_picks = sum(brawler_picks.values())
            meta_scores = []
            
            for brawler in brawler_picks:
                if brawler_picks[brawler] >= 1:
                    pick_rate = (brawler_picks[brawler] / total_picks) * 100
                    win_rate = (brawler_wins[brawler] / brawler_picks[brawler]) * 100
                    meta_score = (win_rate * pick_rate) / 100
                    
                    meta_scores.append({
                        'brawler': brawler,
                        'score': meta_score,
                        'pick_rate': pick_rate,
                        'win_rate': win_rate,
                        'picks': brawler_picks[brawler]
                    })
            
            meta_scores.sort(key=lambda x: x['score'], reverse=True)
            
            top_10_text = []
            for i, data in enumerate(meta_scores[:10], 1):
                top_10_text.append(
                    f"**{i}.** {data['brawler']} - {data['win_rate']:.1f}% WR • {data['pick_rate']:.1f}% PR"
                )
            
            region_display = "All Regions" if self.region == 'ALL' else f"{self.region} Region"
            
            embed = discord.Embed(
                title=f"{self.mode}",
                description=f"**{region_display}**\n**Sorted by:** Meta Score (WR × Pick Rate)",
                color=discord.Color.red(),
                timestamp=datetime.now()
            )
            
            embed.add_field(
                name="🏆 Top 10 Brawlers",
                value="\n".join(top_10_text),
                inline=False
            )
            
            embed.set_image(url=f"attachment://{self.mode}_stats.png")
            
            # Add mode thumbnail
            mode_image_path = None
            modes_dir = './static/images/modes/'
            files_to_send = [file]
            
            if os.path.exists(modes_dir):
                mode_clean = self.mode.lower().replace(' ', '_').replace('-', '_')
                for ext in ['.png', '.jpg', '.jpeg', '.webp']:
                    filepath = os.path.join(modes_dir, f"{mode_clean}{ext}")
                    if os.path.exists(filepath):
                        mode_image_path = filepath
                        break
            
            if mode_image_path:
                mode_file = discord.File(mode_image_path, filename=f"{self.mode}_icon.png")
                embed.set_thumbnail(url=f"attachment://{self.mode}_icon.png")
                files_to_send.append(mode_file)
            
            await interaction.edit_original_response(embed=embed, view=view, attachments=files_to_send)
        else:
            await interaction.edit_original_response(content="Select a map:", embed=None, view=view, attachments=[])


class OffseasonMapDetailView(View):
    """Map details for off-season mode with sortable brawlers"""
    def __init__(self, mode: str, map_name: str, sort_by: str = 'meta'):
        super().__init__(timeout=300)
        self.mode = mode
        self.map_name = map_name
        self.sort_by = sort_by
    
    async def regenerate_image(self, interaction: discord.Interaction):
        """Helper to regenerate and send the image"""
        await interaction.response.defer()
        
        img_bytes = generate_map_stats_image_offseason(self.mode, self.map_name, self.sort_by)
        
        if img_bytes:
            file = discord.File(img_bytes, filename=f"{self.mode}_{self.map_name}_stats.png")
            
            sort_text = {
                'meta': 'Meta Score (WR × Pick Rate)',
                'picks': 'Pick Rate',
                'winrate': 'Win Rate'
            }.get(self.sort_by, 'Meta Score')
            
            embed = discord.Embed(
                title=f"{self.map_name}",
                description=f"**{self.mode}**\n**Sorted by:** {sort_text}",
                color=discord.Color.red(),
                timestamp=datetime.now()
            )
            embed.set_image(url=f"attachment://{self.mode}_{self.map_name}_stats.png")
            
            # Add map thumbnail
            map_image_path = get_map_image(self.mode, self.map_name)
            files_to_send = [file]  # Start with stats image
            
            if map_image_path and os.path.exists(map_image_path):
                map_file = discord.File(map_image_path, filename=f"{self.map_name}_icon.png")
                embed.set_thumbnail(url=f"attachment://{self.map_name}_icon.png")
                files_to_send.append(map_file)
            
            await interaction.edit_original_response(embed=embed, attachments=files_to_send, view=self)
        else:
            await interaction.followup.send("❌ Could not generate stats image - not enough data", ephemeral=True)
    
    @discord.ui.button(label="Sort by Meta Score", style=discord.ButtonStyle.primary, row=0)
    async def sort_meta_button(self, interaction: discord.Interaction, button: Button):
        self.sort_by = 'meta'
        await self.regenerate_image(interaction)
    
    @discord.ui.button(label="Sort by Pick Rate", style=discord.ButtonStyle.primary, row=0)
    async def sort_picks_button(self, interaction: discord.Interaction, button: Button):
        self.sort_by = 'picks'
        await self.regenerate_image(interaction)
    
    @discord.ui.button(label="Sort by Win Rate", style=discord.ButtonStyle.primary, row=1)
    async def sort_wr_button(self, interaction: discord.Interaction, button: Button):
        self.sort_by = 'winrate'
        await self.regenerate_image(interaction)
    
    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = OffseasonMapSelectView(self.mode)
        await interaction.edit_original_response(content="Select a map:", embed=None, view=view, attachments=[])

    """Map details for off-season mode with sortable brawlers"""
    def __init__(self, mode: str, map_name: str, sort_by: str = 'meta'):
        super().__init__(timeout=300)
        self.mode = mode
        self.map_name = map_name
        self.sort_by = sort_by
    
    async def regenerate_image(self, interaction: discord.Interaction):
        """Helper to regenerate and send the image"""
        await interaction.response.defer()
        
        img_bytes = generate_map_stats_image_offseason(self.mode, self.map_name, self.sort_by)
        
        if img_bytes:
            file = discord.File(img_bytes, filename=f"{self.mode}_{self.map_name}_stats.png")
            
            sort_text = {
                'meta': 'Meta Score (WR × Pick Rate)',
                'picks': 'Pick Rate',
                'winrate': 'Win Rate'
            }.get(self.sort_by, 'Meta Score')
            
            # Get map data for top 10
            mode_map_data = region_stats.get('_mode_map_stats', {})
            mode_data = mode_map_data.get(self.mode, {})
            map_data = mode_data.get(self.map_name, {})
            
            brawler_stats = []
            if 'brawlers' in map_data:
                for brawler, data in map_data['brawlers'].items():
                    if data['picks'] >= 1:
                        pick_rate = (data['picks'] / sum(b['picks'] for b in map_data['brawlers'].values())) * 100
                        win_rate = (data['wins'] / data['picks']) * 100
                        meta_score = (win_rate * pick_rate) / 100
                        
                        brawler_stats.append({
                            'brawler': brawler,
                            'score': meta_score,
                            'pick_rate': pick_rate,
                            'win_rate': win_rate,
                            'picks': data['picks']
                        })
            
            # Sort based on current sort_by
            if self.sort_by == 'meta':
                brawler_stats.sort(key=lambda x: x['score'], reverse=True)
            elif self.sort_by == 'picks':
                brawler_stats.sort(key=lambda x: x['pick_rate'], reverse=True)
            elif self.sort_by == 'winrate':
                brawler_stats.sort(key=lambda x: x['win_rate'], reverse=True)
            
            # Generate top 10 text
            top_10_text = []
            for i, data in enumerate(brawler_stats[:10], 1):
                top_10_text.append(
                    f"**{i}.** {data['brawler']} - {data['win_rate']:.1f}% WR • {data['pick_rate']:.1f}% PR"
                )
            
            embed = discord.Embed(
                title=f"{self.map_name}",
                description=f"**{self.mode}**\n**Sorted by:** {sort_text}",
                color=discord.Color.red(),
                timestamp=datetime.now()
            )
            
            if top_10_text:
                embed.add_field(
                    name="🏆 Top 10 Brawlers",
                    value="\n".join(top_10_text),
                    inline=False
                )
            
            embed.set_image(url=f"attachment://{self.mode}_{self.map_name}_stats.png")
            
            # Add map thumbnail
            map_image_path = get_map_image(self.mode, self.map_name)
            files_to_send = [file]  # Start with stats image
            
            if map_image_path and os.path.exists(map_image_path):
                map_file = discord.File(map_image_path, filename=f"{self.map_name}_icon.png")
                embed.set_thumbnail(url=f"attachment://{self.map_name}_icon.png")
                files_to_send.append(map_file)
            
            await interaction.edit_original_response(embed=embed, attachments=files_to_send, view=self)
        else:
            await interaction.followup.send("❌ Could not generate stats image - not enough data", ephemeral=True)
    
    @discord.ui.button(label="Sort by Meta Score", style=discord.ButtonStyle.primary, row=0)
    async def sort_meta_button(self, interaction: discord.Interaction, button: Button):
        self.sort_by = 'meta'
        await self.regenerate_image(interaction)
    
    @discord.ui.button(label="Sort by Pick Rate", style=discord.ButtonStyle.primary, row=0)
    async def sort_picks_button(self, interaction: discord.Interaction, button: Button):
        self.sort_by = 'picks'
        await self.regenerate_image(interaction)
    
    @discord.ui.button(label="Sort by Win Rate", style=discord.ButtonStyle.primary, row=1)
    async def sort_wr_button(self, interaction: discord.Interaction, button: Button):
        self.sort_by = 'winrate'
        await self.regenerate_image(interaction)
    
    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = OffseasonMapSelectView(self.mode)
        await interaction.edit_original_response(content="Select a map:", embed=None, view=view, attachments=[])


def generate_map_stats_image_offseason(mode: str, map_name: str, sort_by: str = 'meta'):
    """
    Generate a comprehensive stats image for a map in off-season
    Similar to mode stats but for a specific map
    
    sort_by options: 'meta' (WR × Pick), 'picks', 'winrate'
    """
    mode_map_data = region_stats.get('_mode_map_stats', {})
    map_data = mode_map_data.get(mode, {}).get(map_name, {})
    
    if not map_data:
        return None
    
    total_matches = map_data.get('matches', 0)
    brawler_data = map_data.get('brawlers', {})
    
    if not brawler_data:
        return None
    
    # Calculate meta scores
    total_picks = sum(data['picks'] for data in brawler_data.values())
    meta_scores = []
    
    for brawler, data in brawler_data.items():
        if data['picks'] >= 1:
            pick_rate = (data['picks'] / total_picks) * 100
            win_rate = (data['wins'] / data['picks']) * 100
            meta_score = (win_rate * pick_rate) / 100
            
            meta_scores.append({
                'brawler': brawler,
                'score': meta_score,
                'pick_rate': pick_rate,
                'win_rate': win_rate,
                'picks': data['picks'],
                'wins': data['wins']
            })
    
    # Sort based on selected criteria
    if sort_by == 'picks':
        meta_scores.sort(key=lambda x: x['picks'], reverse=True)
    elif sort_by == 'winrate':
        meta_scores.sort(key=lambda x: x['win_rate'], reverse=True)
    else:  # 'meta' - default
        meta_scores.sort(key=lambda x: x['score'], reverse=True)
    
    total_brawlers = len(meta_scores)
    
    # Image settings - optimized for 9:16 ratio
    BRAWLER_SIZE = 70
    STAT_WIDTH = 125
    ROW_HEIGHT = 80
    COLUMN_PADDING = 15
    PADDING = 12
    HEADER_HEIGHT = 120
    MAP_IMAGE_SIZE = 180
    SIDE_MARGIN = 20
    FOOTER_HEIGHT = 60
    MAX_BOTTOM_ROWS = 8
    
    # Layout: 2 rows with map image (4 brawlers), then 3-column rows below
    TOP_ROWS = 2
    BOTTOM_COLS = 3
    
    brawlers_top_section = min(4, total_brawlers)
    remaining_brawlers = max(0, total_brawlers - brawlers_top_section)
    
    bottom_rows = min((remaining_brawlers + BOTTOM_COLS - 1) // BOTTOM_COLS, MAX_BOTTOM_ROWS)
    max_brawlers_to_display = brawlers_top_section + (bottom_rows * BOTTOM_COLS)
    
    # Calculate total height
    top_section_height = TOP_ROWS * ROW_HEIGHT + 20
    bottom_section_height = bottom_rows * ROW_HEIGHT + (20 if bottom_rows > 0 else 0)
    
    img_height = HEADER_HEIGHT + top_section_height + bottom_section_height + FOOTER_HEIGHT
    
    # 9:16 aspect ratio
    img_width = int(img_height * 9 / 16)
    img_width = max(img_width, 720)
    
    # Create image with BLACK background
    img = Image.new('RGB', (img_width, img_height), color=(0, 0, 0))
    draw = ImageDraw.Draw(img)
    
    # Load fonts
    try:
        title_font = ImageFont.truetype("arial.ttf", 42)
        subtitle_font = ImageFont.truetype("arial.ttf", 24)
        stat_font = ImageFont.truetype("arial.ttf", 21)
        small_font = ImageFont.truetype("arial.ttf", 19)
        tiny_font = ImageFont.truetype("arial.ttf", 17)
        rank_font = ImageFont.truetype("arial.ttf", 16)
    except:
        title_font = ImageFont.load_default()
        subtitle_font = ImageFont.load_default()
        stat_font = ImageFont.load_default()
        small_font = ImageFont.load_default()
        tiny_font = ImageFont.load_default()
        rank_font = ImageFont.load_default()
    
    # === HEADER ===
    header_overlay = Image.new('RGBA', (img_width, HEADER_HEIGHT), (15, 15, 15, 240))
    img.paste(header_overlay, (0, 0), header_overlay)
    
    # Map name with shadow
    title_bbox = draw.textbbox((0, 0), map_name, font=title_font)
    title_width = title_bbox[2] - title_bbox[0]
    title_x = (img_width - title_width) // 2
    draw.text((title_x + 2, 18), map_name, fill=(0, 0, 0, 180), font=title_font)
    draw.text((title_x, 16), map_name, fill=(255, 255, 255), font=title_font)
    
    # Mode subtitle
    mode_y = 65
    mode_bbox = draw.textbbox((0, 0), mode, font=subtitle_font)
    mode_width = mode_bbox[2] - mode_bbox[0]
    draw.text(((img_width - mode_width) // 2, mode_y), mode, fill=(200, 200, 200), font=subtitle_font)
    
    # Stats bar
    stats_y = 95
    stats_text = f"{total_matches} Matches • {total_picks} Picks • {len(meta_scores)} Brawlers"
    stats_bbox = draw.textbbox((0, 0), stats_text, font=tiny_font)
    stats_width = stats_bbox[2] - stats_bbox[0]
    draw.text(((img_width - stats_width) // 2, stats_y), stats_text, fill=(150, 200, 255), font=tiny_font)
    
    # === RED LINE SEPARATOR AFTER HEADER ===
    header_line_y = HEADER_HEIGHT

    # Draw glow layers
    for i in range(5, 0, -1):
        glow_alpha = int(30 * (6 - i) / 5)
        glow_overlay = Image.new('RGBA', (img_width, img_height), (0, 0, 0, 0))
        glow_draw = ImageDraw.Draw(glow_overlay)
        glow_draw.rectangle(
            [(0, header_line_y - i), (img_width, header_line_y + 3 + i)],
            fill=(220, 50, 50, glow_alpha)
        )
        img.paste(Image.alpha_composite(img.convert('RGBA'), glow_overlay).convert('RGB'))

    # Draw main red line
   
    draw.rectangle([(0, header_line_y), (img_width, header_line_y + 3)], 
                fill=(220, 50, 50))

    # Helper function for WR color
    def get_wr_color(wr):
        if wr >= 70:
            return (50, 255, 100)
        elif wr >= 60:
            return (120, 255, 120)
        elif wr >= 50:
            return (255, 230, 100)
        elif wr >= 40:
            return (255, 170, 80)
        else:
            return (255, 80, 80)
    
    def draw_brawler_card(x, y, data, global_idx):
        """Helper function to draw a brawler card"""
        brawler = data['brawler']
        wr = data['win_rate']
        pr = data['pick_rate']
        picks = data['picks']
        wr_color = get_wr_color(wr)
        
        card_width = BRAWLER_SIZE + STAT_WIDTH + PADDING
        
        # Background card
        draw.rectangle(
            [(x - 4, y), (x + card_width + 4, y + BRAWLER_SIZE + 8)],
            fill=(25, 25, 30)
        )
        
        # Brawler image
        brawler_img_path = get_brawler_image(brawler)
        if brawler_img_path and os.path.exists(brawler_img_path):
            try:
                brawler_img = Image.open(brawler_img_path).convert('RGBA')
                brawler_img = brawler_img.resize((BRAWLER_SIZE, BRAWLER_SIZE), Image.Resampling.LANCZOS)
                img.paste(brawler_img, (x, y + 4), brawler_img)
            except:
                draw.rectangle([(x, y + 4), (x + BRAWLER_SIZE, y + BRAWLER_SIZE + 4)], 
                             fill=(40, 40, 45))
        else:
            draw.rectangle([(x, y + 4), (x + BRAWLER_SIZE, y + BRAWLER_SIZE + 4)], 
                         fill=(40, 40, 45))
        
        # Stats next to brawler
        stats_x = x + BRAWLER_SIZE + PADDING
        
        # Rank number
        rank_text = f"#{global_idx + 1}"
        
        if global_idx < 5:
            rank_colors = [
                (255, 215, 0), (192, 192, 192), (205, 127, 50),
                (100, 149, 237), (147, 112, 219)
            ]
            rank_color = rank_colors[global_idx]
        else:
            rank_color = (120, 120, 140)
        
        draw.text((stats_x, y + 8), rank_text, fill=rank_color, font=rank_font)
        
        rank_bbox = draw.textbbox((0, 0), rank_text, font=rank_font)
        rank_width = rank_bbox[2] - rank_bbox[0]
        
        # Brawler name
        name_display = brawler if len(brawler) <= 9 else brawler[:7] + ".."
        draw.text((stats_x + rank_width + 6, y + 8), name_display, fill=(255, 255, 255), font=small_font)
        
        # Picks
        draw.text((stats_x, y + 35), f"{picks} picks", fill=(180, 180, 200), font=tiny_font)
        
        # WR and PR
        draw.text((stats_x, y + 54), f"{wr:.1f}%", fill=wr_color, font=tiny_font)
        wr_text_bbox = draw.textbbox((stats_x, y + 54), f"{wr:.1f}%", font=tiny_font)
        wr_text_width = wr_text_bbox[2] - wr_text_bbox[0]
        draw.text((stats_x + wr_text_width, y + 54), f" • {pr:.1f}%", fill=(255, 255, 255), font=tiny_font)
    
    # === CALCULATE COLUMN POSITIONS ===
    card_width = BRAWLER_SIZE + STAT_WIDTH + PADDING
    total_content_width = card_width * BOTTOM_COLS + COLUMN_PADDING * (BOTTOM_COLS - 1)
    left_edge = (img_width - total_content_width) // 2
    
    column_x_positions = [
        left_edge + i * (card_width + COLUMN_PADDING)
        for i in range(BOTTOM_COLS)
    ]
    
    # === TOP SECTION: MAP IMAGE WITH BRAWLERS ON SIDES ===
    content_start_y = HEADER_HEIGHT + 20
    
    # Map image centered
    map_img_x = (img_width - MAP_IMAGE_SIZE) // 2
    map_img_y = content_start_y
    
    # Get map image
    map_image_path = get_map_image(mode, map_name)
    
    if map_image_path and os.path.exists(map_image_path):
        try:
            map_img = Image.open(map_image_path).convert('RGBA')
            
            # Preserve aspect ratio
            original_width, original_height = map_img.size
            aspect_ratio = original_width / original_height
            
            if aspect_ratio > 1:
                new_width = MAP_IMAGE_SIZE
                new_height = int(MAP_IMAGE_SIZE / aspect_ratio)
            else:
                new_height = MAP_IMAGE_SIZE
                new_width = int(MAP_IMAGE_SIZE * aspect_ratio)
            
            map_img = map_img.resize((new_width, new_height), Image.Resampling.LANCZOS)
            
            paste_x = map_img_x + (MAP_IMAGE_SIZE - new_width) // 2
            paste_y = map_img_y + (MAP_IMAGE_SIZE - new_height) // 2
            
            img.paste(map_img, (paste_x, paste_y), map_img)
        except Exception as e:
            print(f"Failed to load map image: {e}")
    
    brawler_idx = 0
    
    # Left side - 2 brawlers
    left_x = column_x_positions[0]
    for row in range(TOP_ROWS):
        if brawler_idx < total_brawlers:
            row_y = content_start_y + row * ROW_HEIGHT
            draw_brawler_card(left_x, row_y, meta_scores[brawler_idx], brawler_idx)
            brawler_idx += 1
    
    # Right side - 2 brawlers
    right_x = column_x_positions[2]
    for row in range(TOP_ROWS):
        if brawler_idx < total_brawlers:
            row_y = content_start_y + row * ROW_HEIGHT
            draw_brawler_card(right_x, row_y, meta_scores[brawler_idx], brawler_idx)
            brawler_idx += 1
    
    # === BOTTOM SECTION: 3-COLUMN GRID ===
    if remaining_brawlers > 0 and brawler_idx < max_brawlers_to_display:
        bottom_start_y = content_start_y + top_section_height
        
        for row in range(bottom_rows):
            for col in range(BOTTOM_COLS):
                if brawler_idx < total_brawlers and brawler_idx < max_brawlers_to_display:
                    row_y = bottom_start_y + row * ROW_HEIGHT
                    draw_brawler_card(column_x_positions[col], row_y, meta_scores[brawler_idx], brawler_idx)
                    brawler_idx += 1
    
    # === FOOTER ===
    footer_y = img_height - 50
    
    
    # Update legend text based on sort_by
    sort_text = {
        'meta': 'Meta Score (WR × Pick Rate)',
        'picks': 'Pick Rate',
        'winrate': 'Win Rate'
    }.get(sort_by, 'Meta Score')
    
    legend_text = f"Sorted by {sort_text} • Color indicates Win Rate"
    legend_bbox = draw.textbbox((0, 0), legend_text, font=tiny_font)
    legend_width = legend_bbox[2] - legend_bbox[0]
    draw.text(
        ((img_width - legend_width) // 2, footer_y + 5),
        legend_text,
        fill=(120, 140, 180),
        font=tiny_font
    )
    
    # Save to BytesIO
    img_bytes = io.BytesIO()
    img.save(img_bytes, format='PNG', quality=95)
    img_bytes.seek(0)
    
    return img_bytes


class OffseasonModeSelectView(View):
    """Mode selection for off-season - shows global or region-specific stats"""
    def __init__(self, region: str = 'ALL'):
        super().__init__(timeout=300)
        self.region = region
        
        mode_map_data = region_stats.get('_mode_map_stats', {})
        sorted_modes = sorted([mode for mode in mode_map_data.keys() if mode in VALID_MODES])
        
        if sorted_modes:
            options = [
                discord.SelectOption(label=mode, value=mode)
                for mode in sorted_modes
            ]
            
            if options:
                select = Select(placeholder="Choose a game mode...", options=options)
                select.callback = self.select_callback
                self.add_item(select)
        
        back_btn = Button(label="◀️ Back", style=discord.ButtonStyle.secondary)
        back_btn.callback = self.back_callback
        self.add_item(back_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        mode = interaction.data['values'][0]
        
        await interaction.response.defer()
        
        img_bytes = generate_mode_stats_image_offseason_region(mode, self.region, sort_by='meta')
        
        if img_bytes:
            file = discord.File(img_bytes, filename=f"{mode}_stats.png")
            
            # Get top 10 brawlers for display (filtered by region)
            mode_map_data = region_stats.get('_mode_map_stats', {})
            mode_data = mode_map_data.get(mode, {})
            
            brawler_picks = defaultdict(int)
            brawler_wins = defaultdict(int)
            
            # Filter by region if not ALL
            if self.region != 'ALL':
                # Count only matches from players in this region
                for map_name, map_stats in mode_data.items():
                    for brawler, data in map_stats['brawlers'].items():
                        # Filter by checking if any player in this region used this brawler
                        region_picks = 0
                        region_wins = 0
                        
                        for player_tag, player_data in players_data.items():
                            if player_data['region'] == self.region:
                                if mode in player_data['modes'] and map_name in player_data['modes'][mode]['maps']:
                                    brawler_stats = player_data['modes'][mode]['maps'][map_name]['brawlers'].get(brawler, {})
                                    region_picks += brawler_stats.get('picks', 0)
                                    region_wins += brawler_stats.get('wins', 0)
                        
                        brawler_picks[brawler] += region_picks
                        brawler_wins[brawler] += region_wins
            else:
                # Global stats
                for map_name, map_stats in mode_data.items():
                    for brawler, data in map_stats['brawlers'].items():
                        brawler_picks[brawler] += data['picks']
                        brawler_wins[brawler] += data['wins']
            
            total_picks = sum(brawler_picks.values())
            meta_scores = []
            
            for brawler in brawler_picks:
                if brawler_picks[brawler] >= 1:
                    pick_rate = (brawler_picks[brawler] / total_picks) * 100
                    win_rate = (brawler_wins[brawler] / brawler_picks[brawler]) * 100
                    meta_score = (win_rate * pick_rate) / 100
                    
                    meta_scores.append({
                        'brawler': brawler,
                        'score': meta_score,
                        'pick_rate': pick_rate,
                        'win_rate': win_rate,
                        'picks': brawler_picks[brawler]
                    })
            
            # Sort by meta score
            meta_scores.sort(key=lambda x: x['score'], reverse=True)
            
            # Create top 10 text
            top_10_text = []
            for i, data in enumerate(meta_scores[:10], 1):
                top_10_text.append(
                    f"**{i}.** {data['brawler']} - {data['win_rate']:.1f}% WR • {data['pick_rate']:.1f}% PR"
                )
            
            region_display = "All Regions" if self.region == 'ALL' else f"{self.region} Region"
            
            embed = discord.Embed(
                title=f"{mode}",
                description=f"**{region_display}**\n**Sorted by:** Meta Score (WR × Pick Rate)",
                color=discord.Color.red(),
                timestamp=datetime.now()
            )
            
            embed.add_field(
                name="🏆 Top 10 Brawlers",
                value="\n".join(top_10_text) if top_10_text else "No data",
                inline=False
            )
            
            embed.set_image(url=f"attachment://{mode}_stats.png")
            
            # Add mode thumbnail
            mode_image_path = None
            modes_dir = './static/images/modes/'
            files_to_send = [file]  # Start with stats image
            
            if os.path.exists(modes_dir):
                mode_clean = mode.lower().replace(' ', '_').replace('-', '_')
                for ext in ['.png', '.jpg', '.jpeg', '.webp']:
                    filepath = os.path.join(modes_dir, f"{mode_clean}{ext}")
                    if os.path.exists(filepath):
                        mode_image_path = filepath
                        break
            
            if mode_image_path:
                mode_file = discord.File(mode_image_path, filename=f"{mode}_icon.png")
                embed.set_thumbnail(url=f"attachment://{mode}_icon.png")
                files_to_send.append(mode_file)
            
            view = OffseasonModeDetailView(mode, self.region, sort_by='meta')
            await interaction.followup.send(embed=embed, files=files_to_send, view=view, ephemeral=True)
        else:
            await interaction.followup.send("❌ Could not generate stats image - not enough data", ephemeral=True)
    
    async def back_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        
        if self.region == 'ALL':
            view = OffseasonAllPlayersView()
            embed = view.create_all_players_embed()
        else:
            view = OffseasonRegionView(self.region)
            embed = view.create_region_embed()
        
        await interaction.edit_original_response(embed=embed, view=view, attachments=[])


class OffseasonModeDetailView(View):
    """View with sort buttons for mode stats image"""
    def __init__(self, mode: str, region: str = 'ALL', sort_by: str = 'meta'):
        super().__init__(timeout=300)
        self.mode = mode
        self.region = region
        self.sort_by = sort_by
    
    async def regenerate_image(self, interaction: discord.Interaction):
        """Helper to regenerate and send the image"""
        await interaction.response.defer()
        
        img_bytes = generate_mode_stats_image_offseason_region(self.mode, self.region, self.sort_by)
        
        if img_bytes:
            file = discord.File(img_bytes, filename=f"{self.mode}_stats.png")
            
            sort_text = {
                'meta': 'Meta Score (WR × Pick)',
                'picks': 'Pick Rate',
                'winrate': 'Win Rate'
            }.get(self.sort_by, 'Meta Score')
            
            # Get top 10 brawlers for current sort (filtered by region)
            mode_map_data = region_stats.get('_mode_map_stats', {})
            mode_data = mode_map_data.get(self.mode, {})
            
            brawler_picks = defaultdict(int)
            brawler_wins = defaultdict(int)
            
            # Filter by region if not ALL
            if self.region != 'ALL':
                for map_name, map_stats in mode_data.items():
                    for brawler, data in map_stats['brawlers'].items():
                        region_picks = 0
                        region_wins = 0
                        
                        for player_tag, player_data in players_data.items():
                            if player_data['region'] == self.region:
                                if self.mode in player_data['modes'] and map_name in player_data['modes'][self.mode]['maps']:
                                    brawler_stats = player_data['modes'][self.mode]['maps'][map_name]['brawlers'].get(brawler, {})
                                    region_picks += brawler_stats.get('picks', 0)
                                    region_wins += brawler_stats.get('wins', 0)
                        
                        brawler_picks[brawler] += region_picks
                        brawler_wins[brawler] += region_wins
            else:
                for map_name, map_stats in mode_data.items():
                    for brawler, data in map_stats['brawlers'].items():
                        brawler_picks[brawler] += data['picks']
                        brawler_wins[brawler] += data['wins']
            
            total_picks = sum(brawler_picks.values())
            meta_scores = []
            
            for brawler in brawler_picks:
                if brawler_picks[brawler] >= 1:
                    pick_rate = (brawler_picks[brawler] / total_picks) * 100
                    win_rate = (brawler_wins[brawler] / brawler_picks[brawler]) * 100
                    meta_score = (win_rate * pick_rate) / 100
                    
                    meta_scores.append({
                        'brawler': brawler,
                        'score': meta_score,
                        'pick_rate': pick_rate,
                        'win_rate': win_rate,
                        'picks': brawler_picks[brawler]
                    })
            
            # Sort based on current sort_by
            if self.sort_by == 'picks':
                meta_scores.sort(key=lambda x: x['picks'], reverse=True)
            elif self.sort_by == 'winrate':
                meta_scores.sort(key=lambda x: x['win_rate'], reverse=True)
            else:
                meta_scores.sort(key=lambda x: x['score'], reverse=True)
            
            # Create top 10 text
            top_10_text = []
            for i, data in enumerate(meta_scores[:10], 1):
                top_10_text.append(
                    f"**{i}.** {data['brawler']} - {data['win_rate']:.1f}% WR • {data['pick_rate']:.1f}% PR"
                )
            
            region_display = "All Regions" if self.region == 'ALL' else f"{self.region} Region"
            
            embed = discord.Embed(
                title=f"{self.mode}",
                description=f"**{region_display}**\n**Sorted by:** {sort_text}",
                color=discord.Color.red(),
                timestamp=datetime.now()
            )
            
            embed.add_field(
                name="🏆 Top 10 Brawlers",
                value="\n".join(top_10_text) if top_10_text else "No data",
                inline=False
            )
            
            embed.set_image(url=f"attachment://{self.mode}_stats.png")
            
            # Add mode thumbnail
            mode_image_path = None
            modes_dir = './static/images/modes/'
            files_to_send = [file]
            
            if os.path.exists(modes_dir):
                mode_clean = self.mode.lower().replace(' ', '_').replace('-', '_')
                for ext in ['.png', '.jpg', '.jpeg', '.webp']:
                    filepath = os.path.join(modes_dir, f"{mode_clean}{ext}")
                    if os.path.exists(filepath):
                        mode_image_path = filepath
                        break
            
            if mode_image_path:
                mode_file = discord.File(mode_image_path, filename=f"{self.mode}_icon.png")
                embed.set_thumbnail(url=f"attachment://{self.mode}_icon.png")
                files_to_send.append(mode_file)
            
            await interaction.edit_original_response(embed=embed, attachments=files_to_send, view=self)
        else:
            await interaction.followup.send("❌ Could not generate stats image - not enough data", ephemeral=True)
    
    @discord.ui.button(label="Sort by Meta Score", style=discord.ButtonStyle.primary, row=0)
    async def sort_meta_button(self, interaction: discord.Interaction, button: Button):
        self.sort_by = 'meta'
        await self.regenerate_image(interaction)
    
    @discord.ui.button(label="Sort by Pick Rate", style=discord.ButtonStyle.primary, row=0)
    async def sort_picks_button(self, interaction: discord.Interaction, button: Button):
        self.sort_by = 'picks'
        await self.regenerate_image(interaction)
    
    @discord.ui.button(label="Sort by Win Rate", style=discord.ButtonStyle.primary, row=1)
    async def sort_wr_button(self, interaction: discord.Interaction, button: Button):
        self.sort_by = 'winrate'
        await self.regenerate_image(interaction)
    
    @discord.ui.button(label="View Maps", style=discord.ButtonStyle.secondary, row=1)
    async def maps_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = OffseasonMapSelectView(self.mode, self.region)  # Pass region here!
        await interaction.followup.send("Select a map:", view=view, ephemeral=True)
    
    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=2)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = OffseasonModeSelectView(self.region)
        await interaction.edit_original_response(content="Select a game mode:", embed=None, view=view, attachments=[])


def generate_mode_stats_image_offseason_region(mode: str, region: str = 'ALL', sort_by: str = 'meta'):
    """
    Generate mode stats image filtered by region
    NOW CORRECTLY USES REGION-FILTERED DATA FOR THE IMAGE
    """
    mode_map_data = region_stats.get('_mode_map_stats', {})
    mode_data = mode_map_data.get(mode, {})
    
    if not mode_data:
        return None
    
    # Aggregate brawler stats (filtered by region if specified)
    brawler_picks = defaultdict(int)
    brawler_wins = defaultdict(int)
    total_matches = 0
    
    if region != 'ALL':
        # Count matches and stats only from players in this region
        for player_tag, player_data in players_data.items():
            if player_data['region'] == region and mode in player_data['modes']:
                total_matches += player_data['modes'][mode]['matches']
                
                for map_name, map_data in player_data['modes'][mode]['maps'].items():
                    for brawler, data in map_data['brawlers'].items():
                        brawler_picks[brawler] += data['picks']
                        brawler_wins[brawler] += data['wins']
    else:
        # Global stats
        total_matches = sum(map_stats['matches'] for map_stats in mode_data.values())
        
        for map_name, map_stats in mode_data.items():
            for brawler, data in map_stats['brawlers'].items():
                brawler_picks[brawler] += data['picks']
                brawler_wins[brawler] += data['wins']
    
    if not brawler_picks:
        return None
    
    # Calculate meta scores using the FILTERED data
    total_picks = sum(brawler_picks.values())
    meta_scores = []
    
    for brawler in brawler_picks:
        if brawler_picks[brawler] >= 1:
            pick_rate = (brawler_picks[brawler] / total_picks) * 100
            win_rate = (brawler_wins[brawler] / brawler_picks[brawler]) * 100
            meta_score = (win_rate * pick_rate) / 100
            
            meta_scores.append({
                'brawler': brawler,
                'score': meta_score,
                'pick_rate': pick_rate,
                'win_rate': win_rate,
                'picks': brawler_picks[brawler],
                'wins': brawler_wins[brawler]
            })
    
    # Sort based on selected criteria
    if sort_by == 'picks':
        meta_scores.sort(key=lambda x: x['picks'], reverse=True)
    elif sort_by == 'winrate':
        meta_scores.sort(key=lambda x: x['win_rate'], reverse=True)
    else:  # 'meta' - default
        meta_scores.sort(key=lambda x: x['score'], reverse=True)
    
    # NOW GENERATE IMAGE USING THE FILTERED meta_scores
    # Copy the image generation code from generate_mode_stats_image_offseason
    # but use our filtered data
    
    total_brawlers = len(meta_scores)
    
    # Image settings - optimized for 9:16 ratio
    BRAWLER_SIZE = 70
    STAT_WIDTH = 125
    ROW_HEIGHT = 80
    COLUMN_PADDING = 15
    PADDING = 12
    HEADER_HEIGHT = 100
    MODE_IMAGE_SIZE = 180
    SIDE_MARGIN = 20
    FOOTER_HEIGHT = 60
    MAX_BOTTOM_ROWS = 8
    
    # Layout: 2 rows with mode (4 brawlers), then 3-column rows below
    TOP_ROWS = 2
    BOTTOM_COLS = 3
    
    brawlers_top_section = min(4, total_brawlers)
    remaining_brawlers = max(0, total_brawlers - brawlers_top_section)
    
    bottom_rows = min((remaining_brawlers + BOTTOM_COLS - 1) // BOTTOM_COLS, MAX_BOTTOM_ROWS)
    max_brawlers_to_display = brawlers_top_section + (bottom_rows * BOTTOM_COLS)
    
    # Calculate total height
    top_section_height = TOP_ROWS * ROW_HEIGHT + 20
    bottom_section_height = bottom_rows * ROW_HEIGHT + (20 if bottom_rows > 0 else 0)
    
    img_height = HEADER_HEIGHT + top_section_height + bottom_section_height + FOOTER_HEIGHT
    
    # 9:16 aspect ratio
    img_width = int(img_height * 9 / 16)
    img_width = max(img_width, 720)
    
    # Create image with BLACK background
    img = Image.new('RGB', (img_width, img_height), color=(0, 0, 0))
    draw = ImageDraw.Draw(img)
    
    # Load fonts
    try:
        title_font = ImageFont.truetype("arial.ttf", 50)
        subtitle_font = ImageFont.truetype("arial.ttf", 20)
        stat_font = ImageFont.truetype("arial.ttf", 21)
        small_font = ImageFont.truetype("arial.ttf", 19)
        tiny_font = ImageFont.truetype("arial.ttf", 17)
        rank_font = ImageFont.truetype("arial.ttf", 16)
    except:
        title_font = ImageFont.load_default()
        subtitle_font = ImageFont.load_default()
        stat_font = ImageFont.load_default()
        small_font = ImageFont.load_default()
        tiny_font = ImageFont.load_default()
        rank_font = ImageFont.load_default()
    
    # === HEADER ===
    header_overlay = Image.new('RGBA', (img_width, HEADER_HEIGHT), (15, 15, 15, 240))
    img.paste(header_overlay, (0, 0), header_overlay)
    
    # Mode title with shadow
    title_bbox = draw.textbbox((0, 0), mode, font=title_font)
    title_width = title_bbox[2] - title_bbox[0]
    title_x = (img_width - title_width) // 2
    draw.text((title_x + 2, 18), mode, fill=(0, 0, 0, 180), font=title_font)
    draw.text((title_x, 16), mode, fill=(255, 255, 255), font=title_font)
    
    # Stats bar
    stats_y = 70
    stats_text = f"{total_matches} Matches • {total_picks} Picks • {len(meta_scores)} Brawlers"
    stats_bbox = draw.textbbox((0, 0), stats_text, font=tiny_font)
    stats_width = stats_bbox[2] - stats_bbox[0]
    draw.text(((img_width - stats_width) // 2, stats_y), stats_text, fill=(150, 200, 255), font=tiny_font)

    # === RED LINE SEPARATOR ===
    header_line_y = HEADER_HEIGHT - 6
    for i in range(5, 0, -1):
        glow_alpha = int(30 * (6 - i) / 5)
        glow_overlay = Image.new('RGBA', (img_width, img_height), (0, 0, 0, 0))
        glow_draw = ImageDraw.Draw(glow_overlay)
        glow_draw.rectangle(
            [(0, header_line_y - i), (img_width, header_line_y + 3 + i)],
            fill=(220, 50, 50, glow_alpha)
        )
        img.paste(Image.alpha_composite(img.convert('RGBA'), glow_overlay).convert('RGB'))
    
    draw.rectangle([(0, header_line_y), (img_width, header_line_y + 3)], fill=(220, 50, 50))
    
    # Helper functions
    def get_wr_color(wr):
        if wr >= 70:
            return (50, 255, 100)
        elif wr >= 60:
            return (120, 255, 120)
        elif wr >= 50:
            return (255, 230, 100)
        elif wr >= 40:
            return (255, 170, 80)
        else:
            return (255, 80, 80)
    
    def draw_brawler_card(x, y, data, global_idx):
        brawler = data['brawler']
        wr = data['win_rate']
        pr = data['pick_rate']
        picks = data['picks']
        wr_color = get_wr_color(wr)
        
        card_width = BRAWLER_SIZE + STAT_WIDTH + PADDING
        
        draw.rectangle(
            [(x - 4, y), (x + card_width + 4, y + BRAWLER_SIZE + 8)],
            fill=(25, 25, 30)
        )
        
        brawler_img_path = get_brawler_image(brawler)
        if brawler_img_path and os.path.exists(brawler_img_path):
            try:
                brawler_img = Image.open(brawler_img_path).convert('RGBA')
                brawler_img = brawler_img.resize((BRAWLER_SIZE, BRAWLER_SIZE), Image.Resampling.LANCZOS)
                img.paste(brawler_img, (x, y + 4), brawler_img)
            except:
                draw.rectangle([(x, y + 4), (x + BRAWLER_SIZE, y + BRAWLER_SIZE + 4)], fill=(40, 40, 45))
        else:
            draw.rectangle([(x, y + 4), (x + BRAWLER_SIZE, y + BRAWLER_SIZE + 4)], fill=(40, 40, 45))
        
        stats_x = x + BRAWLER_SIZE + PADDING
        
        rank_text = f"#{global_idx + 1}"
        if global_idx < 5:
            rank_colors = [
                (255, 215, 0), (192, 192, 192), (205, 127, 50),
                (100, 149, 237), (147, 112, 219)
            ]
            rank_color = rank_colors[global_idx]
        else:
            rank_color = (120, 120, 140)
        
        draw.text((stats_x, y + 8), rank_text, fill=rank_color, font=rank_font)
        
        rank_bbox = draw.textbbox((0, 0), rank_text, font=rank_font)
        rank_width = rank_bbox[2] - rank_bbox[0]
        
        name_display = brawler if len(brawler) <= 9 else brawler[:7] + ".."
        draw.text((stats_x + rank_width + 6, y + 8), name_display, fill=(255, 255, 255), font=small_font)
        
        draw.text((stats_x, y + 35), f"{picks} picks", fill=(180, 180, 200), font=tiny_font)
        
        draw.text((stats_x, y + 54), f"{wr:.1f}%", fill=wr_color, font=tiny_font)
        wr_text_bbox = draw.textbbox((stats_x, y + 54), f"{wr:.1f}%", font=tiny_font)
        wr_text_width = wr_text_bbox[2] - wr_text_bbox[0]
        draw.text((stats_x + wr_text_width, y + 54), f" • {pr:.1f}%", fill=(255, 255, 255), font=tiny_font)
    
    # === LAYOUT ===
    card_width = BRAWLER_SIZE + STAT_WIDTH + PADDING
    total_content_width = card_width * BOTTOM_COLS + COLUMN_PADDING * (BOTTOM_COLS - 1)
    left_edge = (img_width - total_content_width) // 2
    
    column_x_positions = [
        left_edge + i * (card_width + COLUMN_PADDING)
        for i in range(BOTTOM_COLS)
    ]
    
    content_start_y = HEADER_HEIGHT + 20
    
    # Mode image
    mode_img_x = (img_width - MODE_IMAGE_SIZE) // 2
    mode_img_y = content_start_y
    
    mode_image_path = None
    modes_dir = './static/images/modes/'
    
    if os.path.exists(modes_dir):
        mode_clean = mode.lower().replace(' ', '_').replace('-', '_')
        for ext in ['.png', '.jpg', '.jpeg', '.webp']:
            filepath = os.path.join(modes_dir, f"{mode_clean}{ext}")
            if os.path.exists(filepath):
                mode_image_path = filepath
                break
    
    if mode_image_path and os.path.exists(mode_image_path):
        try:
            mode_img = Image.open(mode_image_path).convert('RGBA')
            original_width, original_height = mode_img.size
            aspect_ratio = original_width / original_height
            
            if aspect_ratio > 1:
                new_width = MODE_IMAGE_SIZE
                new_height = int(MODE_IMAGE_SIZE / aspect_ratio)
            else:
                new_height = MODE_IMAGE_SIZE
                new_width = int(MODE_IMAGE_SIZE * aspect_ratio)
            
            mode_img = mode_img.resize((new_width, new_height), Image.Resampling.LANCZOS)
            paste_x = mode_img_x + (MODE_IMAGE_SIZE - new_width) // 2
            paste_y = mode_img_y + (MODE_IMAGE_SIZE - new_height) // 2
            img.paste(mode_img, (paste_x, paste_y), mode_img)
        except Exception as e:
            print(f"Failed to load mode image: {e}")
    
    brawler_idx = 0
    
    # Left side - 2 brawlers
    left_x = column_x_positions[0]
    for row in range(TOP_ROWS):
        if brawler_idx < total_brawlers:
            row_y = content_start_y + row * ROW_HEIGHT
            draw_brawler_card(left_x, row_y, meta_scores[brawler_idx], brawler_idx)
            brawler_idx += 1
    
    # Right side - 2 brawlers
    right_x = column_x_positions[2]
    for row in range(TOP_ROWS):
        if brawler_idx < total_brawlers:
            row_y = content_start_y + row * ROW_HEIGHT
            draw_brawler_card(right_x, row_y, meta_scores[brawler_idx], brawler_idx)
            brawler_idx += 1
    
    # Bottom section
    if remaining_brawlers > 0 and brawler_idx < max_brawlers_to_display:
        bottom_start_y = content_start_y + top_section_height
        
        for row in range(bottom_rows):
            for col in range(BOTTOM_COLS):
                if brawler_idx < total_brawlers and brawler_idx < max_brawlers_to_display:
                    row_y = bottom_start_y + row * ROW_HEIGHT
                    draw_brawler_card(column_x_positions[col], row_y, meta_scores[brawler_idx], brawler_idx)
                    brawler_idx += 1
    
    # Footer
    footer_y = img_height - 50
    
    sort_text = {
        'meta': 'Meta Score (WR × Pick Rate)',
        'picks': 'Pick Rate',
        'winrate': 'Win Rate'
    }.get(sort_by, 'Meta Score')

    legend_text = f"Sorted by {sort_text}"
    legend_bbox = draw.textbbox((0, 0), legend_text, font=tiny_font)
    legend_width = legend_bbox[2] - legend_bbox[0]
    draw.text(
        ((img_width - legend_width) // 2, footer_y + 5),
        legend_text,
        fill=(120, 140, 180),
        font=tiny_font
    )
    
    # Save to BytesIO
    img_bytes = io.BytesIO()
    img.save(img_bytes, format='PNG', quality=95)
    img_bytes.seek(0)
    
    return img_bytes


class TrackedPlayersListView(View):
    """Shows all tracked players with quick stats - paginated"""
    def __init__(self, sort_by: str = 'matches', page: int = 0):
        super().__init__(timeout=300)
        self.sort_by = sort_by
        self.page = page
        self.players_per_page = 10  # Show 10 players per page
    
    def get_sorted_players(self):
        """Get sorted list of players based on current sort criteria"""
        if self.sort_by == 'matches':
            return sorted(players_data.items(), key=lambda x: x[1]['matches'], reverse=True)
        elif self.sort_by == 'winrate':
            return sorted(
                players_data.items(),
                key=lambda x: (x[1]['wins'] / x[1]['matches']) if x[1]['matches'] > 0 else 0,
                reverse=True
            )
        else:  # name
            return sorted(players_data.items(), key=lambda x: x[1]['name'].lower())
    
    def create_players_list_embed(self):
        sort_text = {
            'matches': 'Total Matches',
            'winrate': 'Win Rate',
            'name': 'Name (A-Z)'
        }.get(self.sort_by, 'Total Matches')
        
        sorted_players = [(tag, data) for tag, data in self.get_sorted_players() if data['matches'] > 0]
    
        total_players = len(sorted_players)
        total_pages = (total_players + self.players_per_page - 1) // self.players_per_page
        
        # Ensure page is within bounds
        self.page = max(0, min(self.page, total_pages - 1))
        
        start_idx = self.page * self.players_per_page
        end_idx = min(start_idx + self.players_per_page, total_players)
        page_players = sorted_players[start_idx:end_idx]
        
        embed = discord.Embed(
            title="🏖️ Tracked Players",
            description=f"Currently tracking **{total_players}** players\n**Sorted by:** {sort_text}\n**Page {self.page + 1} of {total_pages}**",
            color=discord.Color.red()
        )
        
        for player_tag, data in page_players:
            wr = (data['wins'] / data['matches'] * 100) if data['matches'] > 0 else 0
            
            # Get top 3 brawlers
            top_brawlers = sorted(data['brawlers'].items(), key=lambda x: x[1]['picks'], reverse=True)[:3]
            top_brawlers_str = ", ".join([b[0] for b in top_brawlers]) if top_brawlers else "None"
            
            embed.add_field(
                name=f"{data['name']} ({data['region']})",
                value=(
                    f"**Record:** {data['wins']}-{data['losses']} ({wr:.1f}% WR)\n"
                    f"**Top Brawlers:** {top_brawlers_str}\n"
                    f"**Matches:** {data['matches']}"
                ),
                inline=False
            )
        
        embed.set_footer(text=f"Showing players {start_idx + 1}-{end_idx} of {total_players}")
        
        return embed
    
    def get_current_page_players(self):
        """Get the players currently displayed on this page"""
        sorted_players = self.get_sorted_players()
        start_idx = self.page * self.players_per_page
        end_idx = min(start_idx + self.players_per_page, len(sorted_players))
        return sorted_players[start_idx:end_idx]
    
    @discord.ui.button(label="◀️ Previous", style=discord.ButtonStyle.secondary, row=0, custom_id="prev_page")
    async def prev_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        if self.page > 0:
            self.page -= 1
            embed = self.create_players_list_embed()
            self.update_button_states()
            await interaction.edit_original_response(embed=embed, view=self)
    
    @discord.ui.button(label="Next ▶️", style=discord.ButtonStyle.secondary, row=0, custom_id="next_page")
    async def next_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        sorted_players = self.get_sorted_players()
        total_pages = (len(sorted_players) + self.players_per_page - 1) // self.players_per_page
        if self.page < total_pages - 1:
            self.page += 1
            embed = self.create_players_list_embed()
            self.update_button_states()
            await interaction.edit_original_response(embed=embed, view=self)
    
    @discord.ui.button(label="Sort by Matches", style=discord.ButtonStyle.primary, row=1)
    async def sort_matches_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        self.sort_by = 'matches'
        self.page = 0
        embed = self.create_players_list_embed()
        self.update_button_states()
        await interaction.edit_original_response(embed=embed, view=self)
    
    @discord.ui.button(label="Sort by Win Rate", style=discord.ButtonStyle.primary, row=1)
    async def sort_wr_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        self.sort_by = 'winrate'
        self.page = 0
        embed = self.create_players_list_embed()
        self.update_button_states()
        await interaction.edit_original_response(embed=embed, view=self)
    
    @discord.ui.button(label="Sort by Name", style=discord.ButtonStyle.primary, row=2)
    async def sort_name_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        self.sort_by = 'name'
        self.page = 0
        embed = self.create_players_list_embed()
        self.update_button_states()
        await interaction.edit_original_response(embed=embed, view=self)
    
    @discord.ui.button(label="Select Player", style=discord.ButtonStyle.success, row=2)
    async def select_player_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        
        # Get only the players shown on current page
        page_players = self.get_current_page_players()
        
        # Create dropdown with only current page players
        view = PlayerSelectViewOffseasonFromList(page_players)
        await interaction.followup.send("Select a player from the list above:", view=view, ephemeral=True)
    
    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=3)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = WelcomeViewOffseason()
        embed = create_welcome_embed()
        await interaction.edit_original_response(embed=embed, view=view, attachments=[])
    
    def update_button_states(self):
        """Enable/disable navigation buttons based on current page"""
        sorted_players = self.get_sorted_players()
        total_pages = (len(sorted_players) + self.players_per_page - 1) // self.players_per_page
        
        for item in self.children:
            if isinstance(item, Button):
                if item.custom_id == "prev_page":
                    item.disabled = (self.page == 0)
                elif item.custom_id == "next_page":
                    item.disabled = (self.page >= total_pages - 1)


class PlayerSelectViewOffseasonFromList(View):
    """Simple dropdown to select from a specific list of players"""
    def __init__(self, players_list):
        super().__init__(timeout=300)
        self.players_list = players_list
        
        # Create options from provided players
        options = [
            discord.SelectOption(
                label=data['name'][:100],
                description=f"{data['region']} • {data['matches']} games • {data['wins']}-{data['losses']}"[:100],
                value=tag
            )
            for tag, data in players_list
        ]
        
        if options:
            select = Select(
                placeholder="Choose a player from this page...",
                options=options
            )
            select.callback = self.select_callback
            self.add_item(select)
        
        # Add back button
        back_btn = Button(label="◀️ Cancel", style=discord.ButtonStyle.secondary, row=1)
        back_btn.callback = self.back_callback
        self.add_item(back_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        player_tag = interaction.data['values'][0]
        
        view = PlayerDetailViewOffseason(player_tag)
        embed = view.create_player_embed()
        
        # Get top brawler image for thumbnail
        player = players_data[player_tag]
        sorted_brawlers = sorted(player['brawlers'].items(), key=lambda x: x[1]['picks'], reverse=True)
        
        if sorted_brawlers:
            top_brawler = sorted_brawlers[0][0]
            brawler_img_path = get_brawler_image(top_brawler)
            if brawler_img_path:
                file = discord.File(brawler_img_path, filename=f"{top_brawler}_icon.png")
                await interaction.response.send_message(embed=embed, view=view, file=file, ephemeral=True)
                return
        
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    
    async def back_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        await interaction.delete_original_response()




class PlayerSelectViewOffseason(View):
    """Dropdown to select a tracked player - LIMITED TO 25"""
    def __init__(self):
        super().__init__(timeout=300)
        
        # Sort players by name and limit to 25
        all_players = sorted(players_data.items(), key=lambda x: x[1]['name'].lower())
        limited_players = all_players[:25]  # LIMIT TO 25 FOR DISCORD
        
        options = [
            discord.SelectOption(
                label=data['name'],
                description=f"{data['matches']} games • {data['wins']}-{data['losses']}",
                value=tag
            )
            for tag, data in limited_players
        ]
        
        if options:
            select = Select(placeholder="Choose a player (showing first 25)...", options=options)
            select.callback = self.select_callback
            self.add_item(select)
        
        # Add back button
        back_btn = Button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=1)
        back_btn.callback = self.back_callback
        self.add_item(back_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        player_tag = interaction.data['values'][0]
        
        view = PlayerDetailViewOffseason(player_tag)
        embed = view.create_player_embed()
        
        # Get top brawler image for thumbnail
        player = players_data[player_tag]
        sorted_brawlers = sorted(player['brawlers'].items(), key=lambda x: x[1]['picks'], reverse=True)
        
        if sorted_brawlers:
            top_brawler = sorted_brawlers[0][0]
            brawler_img_path = get_brawler_image(top_brawler)
            if brawler_img_path:
                file = discord.File(brawler_img_path, filename=f"{top_brawler}_icon.png")
                await interaction.response.send_message(embed=embed, view=view, file=file, ephemeral=True)
                return
        
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    
    async def back_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        view = TrackedPlayersListView()
        embed = view.create_players_list_embed()
        await interaction.edit_original_response(embed=embed, view=view)



class PlayerDetailViewOffseason(View):
    """Detailed stats for one tracked player"""
    def __init__(self, player_tag: str):
        super().__init__(timeout=300)
        self.player_tag = player_tag
    
    def create_player_embed(self):
        player = players_data[self.player_tag]
        
        embed = discord.Embed(
            title=f"{player['name']}",
            description=f"**Region:** {player['region']}",
            color=discord.Color.red(),
            timestamp=datetime.now()
        )
        
        if player.get('notes'):
            embed.add_field(name="📝 Notes", value=player['notes'], inline=False)
        
        wr = (player['wins'] / player['matches'] * 100) if player['matches'] > 0 else 0
        embed.add_field(name="⚔️ Matches", value=f"**{player['matches']}**", inline=True)
        embed.add_field(name="✅ Wins", value=f"**{player['wins']}**", inline=True)
        embed.add_field(name="❌ Losses", value=f"**{player['losses']}**", inline=True)
        embed.add_field(name="📈 Win Rate", value=f"**{wr:.1f}%**", inline=True)
        embed.add_field(name="⭐ Star Player", value=f"**{player['star_player']}**", inline=True)
        
        # Sort brawlers by picks
        total_picks = sum(b['picks'] for b in player['brawlers'].values())
        sorted_brawlers = sorted(player['brawlers'].items(), key=lambda x: x[1]['picks'], reverse=True)[:10]
        
        brawler_text = []
        for brawler, data in sorted_brawlers:
            b_wr = (data['wins'] / data['picks'] * 100) if data['picks'] > 0 else 0
            pick_rate = (data['picks'] / total_picks * 100) if total_picks > 0 else 0
            brawler_text.append(f"**{brawler}**: {data['picks']} ({pick_rate:.1f}%) • {b_wr:.1f}% WR")
        
        embed.add_field(
            name="\u200b\nBrawlers (Top 10):",
            value="\n".join(brawler_text) if brawler_text else "No data",
            inline=False
        )
        
        # Set thumbnail to top brawler if available
        if sorted_brawlers:
            top_brawler = sorted_brawlers[0][0]
            brawler_img_path = get_brawler_image(top_brawler)
            if brawler_img_path:
                embed.set_thumbnail(url=f"attachment://{top_brawler}_icon.png")
                embed.set_footer(text=f"Most played: {top_brawler}")
        
        return embed
    
    @discord.ui.button(label="Common Teammates", style=discord.ButtonStyle.secondary, row=0)
    async def teammates_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        
        player = players_data[self.player_tag]
        
        embed = discord.Embed(
            title=f"{player['name']}'s Common Teammates",
            description="",
            color=discord.Color.red()
        )
        
        sorted_teammates = sorted(
            player['teammates_seen'].items(),
            key=lambda x: x[1]['matches'],
            reverse=True
        )[:15]
        
        if sorted_teammates:
            # Split into chunks for better readability
            teammates_text = []
            for i, (tag, data) in enumerate(sorted_teammates, 1):
                # Add visual separator every 5 entries
                if i > 1 and (i - 1) % 5 == 0:
                    teammates_text.append("")
                
                matches_plural = "game" if data['matches'] == 1 else "games"
                teammates_text.append(f"`#{i:2d}` **{data['name']}** • {data['matches']} {matches_plural}")
            
            embed.add_field(
                name="Top Teammates",
                value="\n".join(teammates_text),
                inline=False
            )
            
            # Add footer with total count
            total_teammates = len(player['teammates_seen'])
            embed.set_footer(text=f"Showing top 15 of {total_teammates} total teammates")
        else:
            embed.description = "No teammate data available yet"
            embed.color = discord.Color.red()
        
        await interaction.followup.send(embed=embed, ephemeral=True)
    
    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=0)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = PlayerSelectViewOffseason()
        await interaction.edit_original_response(content="Select a player:", embed=None, view=view, attachments=[])


def generate_player_stats_image_offseason(player_tag: str, player_data: dict):
    """Generate player stats image for off-season mode"""
    # Same as generate_player_stats_image but adapted for the new structure
    # ... (use your existing image generation code)
    return generate_player_stats_image("TRACKED_PLAYER", player_data, {'region': player_data['region'], 'players': {player_tag: player_data}})




class RegionMapDetailView(View):
    """View showing map statistics for a specific region with sortable brawlers"""
    def __init__(self, region: str, mode: str, map_name: str, sort_by: str = 'picks'):
        super().__init__(timeout=300)
        self.region = region
        self.mode = mode
        self.map_name = map_name
        self.sort_by = sort_by
    
    def create_map_embed(self):
        sort_text = 'Pick Rate' if self.sort_by == 'picks' else ('Win Rate' if self.sort_by == 'winrate' else 'Best Pick (WR × Pick)')
        embed = discord.Embed(
            title=f"{self.map_name}",
            description=f"**{self.mode}** - {self.region} Region\n**Sorted by:** {sort_text}",
            color=discord.Color.red()
        )
        
        map_image = get_map_image(self.mode, self.map_name)
        if map_image:
            embed.set_image(url="attachment://map.png")
        
        brawler_picks = defaultdict(int)
        brawler_wins = defaultdict(int)
        total_matches = 0
        
        for team_name, team_data in teams_data.items():
            if team_data['region'] == self.region and self.mode in team_data['modes']:
                if self.map_name in team_data['modes'][self.mode]['maps']:
                    map_data = team_data['modes'][self.mode]['maps'][self.map_name]
                    total_matches += map_data['matches']
                    
                    for brawler, brawler_data in map_data['brawlers'].items():
                        brawler_picks[brawler] += brawler_data['picks']
                        brawler_wins[brawler] += brawler_data['wins']
        
        embed.add_field(name="⚔️ Matches", value=f"**{total_matches}**", inline=True)
        
        total_picks = sum(brawler_picks.values())
        
        if self.sort_by == 'picks':
            sorted_brawlers = sorted(brawler_picks.items(), key=lambda x: x[1], reverse=True)
        elif self.sort_by == 'winrate':
            sorted_brawlers = sorted(
                [(b, brawler_wins[b] / brawler_picks[b]) for b in brawler_picks if brawler_picks[b] >= 1],
                key=lambda x: x[1],
                reverse=True
            )
        else:  # value = pick_rate * win_rate
            brawler_values = []
            for brawler in brawler_picks:
                if brawler_picks[brawler] >= 1:
                    pick_rate_pct = (brawler_picks[brawler] / total_picks) * 100
                    win_rate_pct = (brawler_wins[brawler] / brawler_picks[brawler]) * 100
                    value_score = win_rate_pct * pick_rate_pct
                    brawler_values.append((brawler, value_score))
            sorted_brawlers = sorted(brawler_values, key=lambda x: x[1], reverse=True)
        
        brawler_text = []
        
        for item in sorted_brawlers:
            if self.sort_by == 'picks':
                brawler, picks = item
            elif self.sort_by == 'winrate':
                brawler, _ = item
            else:  # value sort
                brawler, _ = item
            
            # Always get actual picks and wins for display
            picks = brawler_picks[brawler]
            wr = (brawler_wins[brawler] / picks * 100) if picks > 0 else 0
            pick_rate = (picks / total_picks * 100) if total_picks > 0 else 0
            
            brawler_text.append(f"**{brawler}**: {picks} picks ({pick_rate:.1f}%) • {wr:.1f}% WR")
        
        all_brawlers = "\n".join(brawler_text) if brawler_text else "No data"
        
        if len(all_brawlers) > 1024:
            current_chunk = []
            current_length = 0
            field_num = 0
            
            for line in brawler_text:
                line_length = len(line) + 1
                if current_length + line_length > 1024:
                    field_name = "\u200b\nBrawler Picks & Win Rates" if field_num == 0 else "\u200b"
                    embed.add_field(name=field_name, value="\n".join(current_chunk), inline=False)
                    current_chunk = [line]
                    current_length = line_length
                    field_num += 1
                else:
                    current_chunk.append(line)
                    current_length += line_length
            
            if current_chunk:
                field_name = "\u200b\nBrawler Picks & Win Rates" if field_num == 0 else "\u200b"
                embed.add_field(name=field_name, value="\n".join(current_chunk), inline=False)
        else:
            embed.add_field(
                name="\u200b\nBrawler Picks & Win Rates",
                value="\n" + all_brawlers,
                inline=False
            )
        
        return embed
    
    @discord.ui.button(label="Sort by Pick Rate", style=discord.ButtonStyle.primary, row=0)
    async def sort_picks_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        self.sort_by = 'picks'
        embed = self.create_map_embed()
        
        map_image = get_map_image(self.mode, self.map_name)
        if map_image:
            file = discord.File(map_image, filename="map.png")
            await interaction.edit_original_response(embed=embed, view=self, attachments=[file])
        else:
            await interaction.edit_original_response(embed=embed, view=self)
    
    @discord.ui.button(label="Sort by Win Rate", style=discord.ButtonStyle.primary, row=0)
    async def sort_wr_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        self.sort_by = 'winrate'
        embed = self.create_map_embed()
        
        map_image = get_map_image(self.mode, self.map_name)
        if map_image:
            file = discord.File(map_image, filename="map.png")
            await interaction.edit_original_response(embed=embed, view=self, attachments=[file])
        else:
            await interaction.edit_original_response(embed=embed, view=self)
    
    @discord.ui.button(label="Sort by Best Pick", style=discord.ButtonStyle.success, row=1)
    async def sort_value_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        self.sort_by = 'value'
        embed = self.create_map_embed()
        
        map_image = get_map_image(self.mode, self.map_name)
        if map_image:
            file = discord.File(map_image, filename="map.png")
            await interaction.edit_original_response(embed=embed, view=self, attachments=[file])
        else:
            await interaction.edit_original_response(embed=embed, view=self)
    
    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = RegionMapSelectView(self.region, self.mode)
        await interaction.edit_original_response(content="Select a map:", embed=None, view=view, attachments=[])



class RegionMapSelectView(View):
    """Dropdown to select a map for region-specific statistics"""
    def __init__(self, region: str, mode: str):
        super().__init__(timeout=300)
        self.region = region
        self.mode = mode
        
        all_maps = defaultdict(int)
        for team_name, team_data in teams_data.items():
            if team_data['region'] == region and mode in team_data['modes']:
                for map_name, map_data in team_data['modes'][mode]['maps'].items():
                    all_maps[map_name] += map_data['matches']
        
        sorted_maps = sorted(all_maps.items(), key=lambda x: x[1], reverse=True)
        
        if sorted_maps:
            options = [
                discord.SelectOption(
                    label=map_name[:100],
                    description=f"{matches} matches"[:100],
                    value=map_name[:100]
                )
                for map_name, matches in sorted_maps[:25]
            ]
            
            select = Select(placeholder="Choose a map...", options=options)
            select.callback = self.select_callback
            self.add_item(select)
        
        # Add back button
        back_btn = Button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=1)
        back_btn.callback = self.back_callback
        self.add_item(back_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        map_name = interaction.data['values'][0]
        view = RegionMapDetailView(self.region, self.mode, map_name)
        embed = view.create_map_embed()
        
        map_image = get_map_image(self.mode, map_name)
        if map_image:
            file = discord.File(map_image, filename="map.png")
            await interaction.response.send_message(embed=embed, view=view, file=file, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    
    async def back_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        view = RegionModeDetailView(self.region, self.mode)
        embed = view.create_mode_embed()
        await interaction.edit_original_response(embed=embed, view=view)


class TeamSelectView(View):
    """Dropdown to select a team"""
    def __init__(self, region: str = None):
        super().__init__(timeout=300)
        self.region = region
        
        if region:
            region_teams = [(name, data) for name, data in teams_data.items() if data['region'] == region]
        else:
            region_teams = list(teams_data.items())
        
        region_teams.sort(key=lambda x: x[1]['wins'], reverse=True)
        
        options = [
            discord.SelectOption(
                label=name,
                description=f"{data['wins']}-{data['losses']} ({data['wins']/(data['matches'])*100:.1f}% WR)",
                value=name
            )
            for name, data in region_teams[:25]
        ]
        
        if options:
            select = Select(placeholder="Choose a team...", options=options)
            select.callback = self.select_callback
            self.add_item(select)
        
        # Add back button
        back_btn = Button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=1)
        back_btn.callback = self.back_callback
        self.add_item(back_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        team_name = interaction.data['values'][0]
        
        view = TeamDetailView(team_name)
        embed, team_img = view.create_team_embed()
        
        if team_img:
            file = discord.File(team_img, filename="team_logo.png")
            await interaction.response.send_message(embed=embed, view=view, file=file, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    
    async def back_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        if self.region:
            view = RegionView(self.region)
            embed = view.create_region_embed()
            await interaction.edit_original_response(embed=embed, view=view)
        else:
            view = WelcomeView()
            embed = create_welcome_embed()
            await interaction.edit_original_response(embed=embed, view=view)


class TeamDetailView(View):
    """Detailed view of a team"""
    def __init__(self, team_name: str):
        super().__init__(timeout=300)
        self.team_name = team_name
    
    def create_team_embed(self):
        team = teams_data[self.team_name]
        
        embed = discord.Embed(
            title=f"{self.team_name}",
            description=f"**Region:** {team['region']}",
            color=discord.Color.red(),
            timestamp=datetime.now()
        )
        
        team_img = get_team_image(self.team_name)
        if team_img:
            embed.set_thumbnail(url="attachment://team_logo.png")
        
        wr = (team['wins'] / team['matches'] * 100) if team['matches'] > 0 else 0
        embed.add_field(name="⚔️ Matches", value=f"**{team['matches']}**", inline=True)
        embed.add_field(name="✅ Wins", value=f"**{team['wins']}**", inline=True)
        embed.add_field(name="❌ Losses", value=f"**{team['losses']}**", inline=True)
        embed.add_field(name="📈 Win Rate", value=f"**{wr:.1f}%**", inline=True)
        
        player_text = []
        total_stars = sum(p['star_player'] for p in team['players'].values())
        for player_tag, player_data in team['players'].items():
            p_wr = (player_data['wins'] / player_data['matches'] * 100) if player_data['matches'] > 0 else 0
            star_rate = (player_data['star_player'] / total_stars * 100) if total_stars > 0 else 0
            player_text.append(
                f"**{player_data['name']}**\n"
                f"  └ {player_data['matches']} m • {p_wr:.1f}% WR • ⭐ {star_rate:.1f}%"
            )
        
        embed.add_field(
            name="\u200b\nPlayers",
            value="\n".join(player_text) if player_text else "No players",
            inline=False
        )
        
        return embed, team_img
    
    @discord.ui.button(label=" Brawlers (Pick Rate)", style=discord.ButtonStyle.primary, row=0)
    async def brawlers_picks_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        embed = self.create_brawler_embed(sort_by='picks')
        team = teams_data[self.team_name]
        most_picked = max(team['brawlers'].items(), key=lambda x: x[1]['picks'])[0] if team['brawlers'] else None
        if most_picked:
            brawler_img = get_brawler_image(most_picked)
            if brawler_img:
                file = discord.File(brawler_img, filename="brawler.png")
                embed.set_author(name=f"Most Picked: {most_picked}", icon_url="attachment://brawler.png")
                await interaction.followup.send(embed=embed, file=file, ephemeral=True)
                return
        await interaction.followup.send(embed=embed, ephemeral=True)
    
    @discord.ui.button(label="Brawlers (Win Rate)", style=discord.ButtonStyle.primary, row=0)
    async def brawlers_wr_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        embed = self.create_brawler_embed(sort_by='winrate')
        team = teams_data[self.team_name]
        filtered = [(b, d) for b, d in team['brawlers'].items() if d['picks'] >= 1]
        if filtered:
            highest_wr = max(filtered, key=lambda x: x[1]['wins']/x[1]['picks'])[0]
            brawler_img = get_brawler_image(highest_wr)
            if brawler_img:
                file = discord.File(brawler_img, filename="brawler.png")
                embed.set_author(name=f"Highest Win Rate: {highest_wr}", icon_url="attachment://brawler.png")
                await interaction.followup.send(embed=embed, file=file, ephemeral=True)
                return
        await interaction.followup.send(embed=embed, ephemeral=True)
    
    @discord.ui.button(label="Modes & Maps", style=discord.ButtonStyle.secondary, row=1)
    async def modes_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        
        team = teams_data[self.team_name]
        
        valid_modes = []
        for mode in team['modes'].keys():
            if mode != 'Unknown' and mode != 'nan' and team['modes'][mode]['matches'] > 0:
                valid_modes.append(mode)
        
        if not valid_modes:
            await interaction.followup.send("❌ No mode data available for this team.", ephemeral=True)
            return
        
        view = ModeSelectView(self.team_name)
        await interaction.followup.send("Select a game mode:", view=view, ephemeral=True)
    
    @discord.ui.button(label="Player Stats", style=discord.ButtonStyle.secondary, row=1)
    async def players_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = PlayerSelectView(self.team_name)
        await interaction.followup.send("Select a player:", view=view, ephemeral=True)
    
    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=2)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        team = teams_data[self.team_name]
        view = TeamSelectView(team['region'])
        await interaction.edit_original_response(content="Select a team to view detailed stats:", embed=None, view=view, attachments=[])
    
    def create_brawler_embed(self, sort_by='picks'):
        team = teams_data[self.team_name]
        
        embed = discord.Embed(
            title=f"{self.team_name} - Brawler Statistics",
            description=f"Sorted by: **{'Pick Rate' if sort_by == 'picks' else 'Win Rate'}**",
            color=discord.Color.red()
        )
        
        if sort_by == 'picks':
            sorted_brawlers = sorted(
                team['brawlers'].items(),
                key=lambda x: x[1]['picks'],
                reverse=True
            )
        else:
            sorted_brawlers = sorted(
                [(b, d) for b, d in team['brawlers'].items() if d['picks'] >= 1],
                key=lambda x: (x[1]['wins'] / x[1]['picks']) if x[1]['picks'] > 0 else 0,
                reverse=True
            )
        
        brawler_text = []
        total_picks = sum(b['picks'] for b in team['brawlers'].values())
        
        for brawler, data in sorted_brawlers:
            b_wr = (data['wins'] / data['picks'] * 100) if data['picks'] > 0 else 0
            pick_rate = (data['picks'] / total_picks * 100) if total_picks > 0 else 0
            brawler_text.append(
                f"**{brawler}**: {data['picks']} picks ({pick_rate:.1f}%) • {b_wr:.1f}% WR"
            )
        
        all_brawlers = "\n".join(brawler_text) if brawler_text else "No data"
        
        if len(all_brawlers) > 1024:
            current_chunk = []
            current_length = 0
            field_num = 0
            
            for line in brawler_text:
                line_length = len(line) + 1
                if current_length + line_length > 1024:
                    field_name = "Brawler Pool" if field_num == 0 else "\u200b"
                    embed.add_field(
                        name=field_name,
                        value="\n".join(current_chunk),
                        inline=False
                    )
                    current_chunk = [line]
                    current_length = line_length
                    field_num += 1
                else:
                    current_chunk.append(line)
                    current_length += line_length
            
            if current_chunk:
                field_name = "Brawler Pool" if field_num == 0 else "\u200b"
                embed.add_field(
                    name=field_name,
                    value="\n".join(current_chunk),
                    inline=False
                )
        else:
            embed.add_field(
                name="Brawler Pool",
                value=all_brawlers,
                inline=False
            )
        
        return embed


class ModeSelectView(View):
    """Dropdown to select a game mode"""
    def __init__(self, team_name: str):
        super().__init__(timeout=300)
        self.team_name = team_name
        
        team = teams_data[team_name]
        
        available_modes = []
        for mode, data in team['modes'].items():
            if mode in ['Unknown', 'nan', '', 'None'] or data['matches'] == 0:
                continue
            available_modes.append((mode, data))
        
        available_modes.sort(key=lambda x: x[1]['matches'], reverse=True)
        
        if not available_modes:
            return
        
        options = [
            discord.SelectOption(
                label=mode,
                description=f"{data['wins']}-{data['matches']-data['wins']} ({data['wins']/data['matches']*100:.1f}% WR)",
                value=mode
            )
            for mode, data in available_modes[:25]
        ]
        
        if options:
            select = Select(placeholder="Choose a game mode...", options=options)
            select.callback = self.select_callback
            self.add_item(select)
        
        # Add back button
        back_btn = Button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=1)
        back_btn.callback = self.back_callback
        self.add_item(back_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        mode = interaction.data['values'][0]
        
        view = ModeDetailView(self.team_name, mode)
        embed = view.create_mode_embed()
        
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    
    async def back_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        view = TeamDetailView(self.team_name)
        embed, team_img = view.create_team_embed()
        
        if team_img:
            file = discord.File(team_img, filename="team_logo.png")
            await interaction.edit_original_response(embed=embed, view=view, attachments=[file])
        else:
            await interaction.edit_original_response(embed=embed, view=view, attachments=[])


class ModeDetailView(View):
    """Detailed view of a team's performance in a specific mode"""
    def __init__(self, team_name: str, mode: str):
        super().__init__(timeout=300)
        self.team_name = team_name
        self.mode = mode
    
    def create_mode_embed(self):
        team = teams_data[self.team_name]
        mode_data = team['modes'][self.mode]
        
        embed = discord.Embed(
            title=f" {self.team_name} - {self.mode}",
            description=f"Performance statistics in {self.mode}",
            color=discord.Color.red()
        )
        
        wr = (mode_data['wins'] / mode_data['matches'] * 100) if mode_data['matches'] > 0 else 0
        embed.add_field(name="⚔️ Matches", value=f"**{mode_data['matches']}**", inline=True)
        embed.add_field(name="📈 Win Rate", value=f"**{wr:.1f}%\n\n**", inline=True)
        
        map_text = []
        sorted_maps = sorted(
            mode_data['maps'].items(),
            key=lambda x: x[1]['matches'],
            reverse=True
        )
        
        for map_name, map_data in sorted_maps:
            map_wr = (map_data['wins'] / map_data['matches'] * 100) if map_data['matches'] > 0 else 0
            map_text.append(
                f"**{map_name}**: {map_data['wins']}-{map_data['matches']-map_data['wins']} • {map_wr:.1f}% WR"
            )
        
        if len("\n".join(map_text)) > 1024:
            chunk_size = 10
            for i in range(0, len(map_text), chunk_size):
                chunk = map_text[i:i+chunk_size]
                field_name = f"Map Performance ({i+1}-{min(i+chunk_size, len(map_text))})" if i > 0 else "Map Performance"
                embed.add_field(
                    name=field_name,
                    value="\n".join(chunk),
                    inline=False
                )
        else:
            embed.add_field(
                name="Map Performance",
                value="\n".join(map_text) if map_text else "No maps",
                inline=False
            )
        
        return embed
    
    @discord.ui.button(label="View Map Details", style=discord.ButtonStyle.primary, row=0)
    async def maps_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        
        team = teams_data[self.team_name]
        
        if self.mode not in team['modes'] or not team['modes'][self.mode]['maps']:
            await interaction.followup.send("❌ No map data available for this mode.", ephemeral=True)
            return
        
        view = MapSelectView(self.team_name, self.mode)
        
        if not view.children:
            await interaction.followup.send("❌ No maps available for this mode.", ephemeral=True)
            return
            
        await interaction.followup.send("Select a map:", view=view, ephemeral=True)
    
    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=0)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = ModeSelectView(self.team_name)
        await interaction.edit_original_response(content="Select a game mode:", embed=None, view=view)


class MapSelectView(View):
    """Dropdown to select a specific map"""
    def __init__(self, team_name: str, mode: str):
        super().__init__(timeout=300)
        self.team_name = team_name
        self.mode = mode
        
        team = teams_data[team_name]
        
        if mode not in team['modes']:
            return
        
        mode_data = team['modes'][mode]
        
        sorted_maps = sorted(
            mode_data['maps'].items(),
            key=lambda x: x[1]['matches'],
            reverse=True
        )
        
        if not sorted_maps:
            return
        
        options = [
            discord.SelectOption(
                label=map_name[:100],
                description=f"{data['wins']}-{data['matches']-data['wins']} ({data['wins']/data['matches']*100:.1f}% WR)"[:100],
                value=map_name[:100]
            )
            for map_name, data in sorted_maps[:25]
        ]
        
        if options:
            select = Select(placeholder="Choose a map...", options=options)
            select.callback = self.select_callback
            self.add_item(select)
        
        # Add back button
        back_btn = Button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=1)
        back_btn.callback = self.back_callback
        self.add_item(back_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        map_name = interaction.data['values'][0]
        
        view = MapDetailView(self.team_name, self.mode, map_name)
        embed = view.create_map_embed()
        
        map_image = get_map_image(self.mode, map_name)
        
        if map_image:
            file = discord.File(map_image, filename="map.png")
            await interaction.response.send_message(embed=embed, view=view, file=file, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    
    async def back_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        view = ModeDetailView(self.team_name, self.mode)
        embed = view.create_mode_embed()
        await interaction.edit_original_response(embed=embed, view=view)


class MapDetailView(View):
    """Detailed view of a specific map with sortable brawlers"""
    def __init__(self, team_name: str, mode: str, map_name: str, sort_by: str = 'picks'):
        super().__init__(timeout=300)
        self.team_name = team_name
        self.mode = mode
        self.map_name = map_name
        self.sort_by = sort_by
    
    def create_map_embed(self):
        team = teams_data[self.team_name]
        map_data = team['modes'][self.mode]['maps'][self.map_name]
        
        sort_text = 'Pick Rate' if self.sort_by == 'picks' else ('Win Rate' if self.sort_by == 'winrate' else 'Best Pick (WR × Pick)')
        embed = discord.Embed(
            title=f"{self.team_name}",
            description=f"**{self.mode}** - {self.map_name}\n**Sorted by:** {sort_text}",
            color=discord.Color.red()
        )
        
        map_image = get_map_image(self.mode, self.map_name)
        if map_image:
            embed.set_image(url="attachment://map.png")
        
        wr = (map_data['wins'] / map_data['matches'] * 100) if map_data['matches'] > 0 else 0
        embed.add_field(name="⚔️ Matches", value=f"**{map_data['matches']}**", inline=True)
        embed.add_field(name="📈 Win Rate", value=f"**{wr:.1f}%**", inline=True)
        
        total_picks = sum(b['picks'] for b in map_data['brawlers'].values())
        
        if self.sort_by == 'picks':
            sorted_brawlers = sorted(
                map_data['brawlers'].items(),
                key=lambda x: x[1]['picks'],
                reverse=True
            )
        elif self.sort_by == 'winrate':
            sorted_brawlers = sorted(
                [(b, d) for b, d in map_data['brawlers'].items() if d['picks'] >= 1],
                key=lambda x: (x[1]['wins'] / x[1]['picks']) if x[1]['picks'] > 0 else 0,
                reverse=True
            )
        else:  # value = pick_rate * win_rate
            brawler_values = []
            for brawler, data in map_data['brawlers'].items():
                if data['picks'] >= 1:
                    pick_rate = data['picks'] / total_picks
                    win_rate = data['wins'] / data['picks']
                    value_score = win_rate * pick_rate
                    brawler_values.append((brawler, data, value_score))
            sorted_brawlers = sorted(brawler_values, key=lambda x: x[2], reverse=True)
            # Convert to same format as other sorts
            sorted_brawlers = [(b, d) for b, d, _ in sorted_brawlers]
        
        brawler_text = []
        
        for brawler, data in sorted_brawlers:
            b_wr = (data['wins'] / data['picks'] * 100) if data['picks'] > 0 else 0
            pick_rate = (data['picks'] / total_picks * 100) if total_picks > 0 else 0
            brawler_text.append(
                f"**{brawler}**: {data['picks']} picks ({pick_rate:.1f}%) • {b_wr:.1f}% WR"
            )
        
        if len("\n".join(brawler_text)) > 1024:
            chunk_size = 12
            for i in range(0, len(brawler_text), chunk_size):
                chunk = brawler_text[i:i+chunk_size]
                field_name = f"Brawler Picks & Win Rates ({i+1}-{min(i+chunk_size, len(brawler_text))})" if i > 0 else "Brawler Picks & Win Rates"
                embed.add_field(
                    name=field_name,
                    value="\n" + "\n".join(chunk),
                    inline=False
                )
        else:
            embed.add_field(
                name="\u200b\nBrawler Picks & Win Rates",
                value="\n" + ("\n".join(brawler_text) if brawler_text else "No data"),
                inline=False
            )
        
        return embed
    
    @discord.ui.button(label="Sort by Pick Rate", style=discord.ButtonStyle.primary, row=0)
    async def sort_picks_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        self.sort_by = 'picks'
        embed = self.create_map_embed()
        
        map_image = get_map_image(self.mode, self.map_name)
        if map_image:
            file = discord.File(map_image, filename="map.png")
            await interaction.edit_original_response(embed=embed, view=self, attachments=[file])
        else:
            await interaction.edit_original_response(embed=embed, view=self)
    
    @discord.ui.button(label="Sort by Win Rate", style=discord.ButtonStyle.primary, row=0)
    async def sort_wr_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        self.sort_by = 'winrate'
        embed = self.create_map_embed()
        
        map_image = get_map_image(self.mode, self.map_name)
        if map_image:
            file = discord.File(map_image, filename="map.png")
            await interaction.edit_original_response(embed=embed, view=self, attachments=[file])
        else:
            await interaction.edit_original_response(embed=embed, view=self)
    
    @discord.ui.button(label="Sort by Best Pick", style=discord.ButtonStyle.success, row=1)
    async def sort_value_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        self.sort_by = 'value'
        embed = self.create_map_embed()
        
        map_image = get_map_image(self.mode, self.map_name)
        if map_image:
            file = discord.File(map_image, filename="map.png")
            await interaction.edit_original_response(embed=embed, view=self, attachments=[file])
        else:
            await interaction.edit_original_response(embed=embed, view=self)
    
    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = MapSelectView(self.team_name, self.mode)
        await interaction.edit_original_response(content="Select a map:", embed=None, view=view, attachments=[])

        
class PlayerSelectView(View):
    """Dropdown to select a player"""
    def __init__(self, team_name: str):
        super().__init__(timeout=300)
        self.team_name = team_name
        
        team = teams_data[team_name]
        
        options = [
            discord.SelectOption(
                label=player_data['name'],
                description=f"{player_data['matches']} games • {player_data['wins']/(player_data['matches'])*100:.1f}% WR",
                value=player_tag
            )
            for player_tag, player_data in team['players'].items()
        ]
        
        if options:
            select = Select(placeholder="Choose a player...", options=options)
            select.callback = self.select_callback
            self.add_item(select)
        
        # Add back button
        back_btn = Button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=1)
        back_btn.callback = self.back_callback
        self.add_item(back_btn)
    
    async def select_callback(self, interaction: discord.Interaction):
        player_tag = interaction.data['values'][0]
        team = teams_data[self.team_name]
        player_data = team['players'][player_tag]
        
        embed = discord.Embed(
            title=f"{player_data['name']}",
            description=f"**Team:** {self.team_name} ({team['region']})",
            color=discord.Color.red()
        )
        
        p_wr = (player_data['wins'] / player_data['matches'] * 100) if player_data['matches'] > 0 else 0
        total_stars = sum(p['star_player'] for p in team['players'].values())
        star_rate = (player_data['star_player'] / total_stars * 100) if total_stars > 0 else 0

        embed.add_field(name="📊 Matches", value=f"**{player_data['matches']}**", inline=True)
        embed.add_field(name="📈 Win Rate", value=f"**{p_wr:.1f}%**", inline=True)
        embed.add_field(name="⭐ Star Player", value=f"**{player_data['star_player']}** ({star_rate:.1f}%)", inline=True)
        
        brawler_stats = sorted(
            player_data['brawlers'].items(),
            key=lambda x: x[1]['picks'],
            reverse=True
        )
        
        brawler_text = []
        total_picks = sum(d['picks'] for d in player_data['brawlers'].values())
        for brawler, data in brawler_stats:
            b_wr = (data['wins'] / data['picks'] * 100) if data['picks'] > 0 else 0
            pick_rate = (data['picks'] / total_picks * 100) if total_picks > 0 else 0
            
            brawler_text.append(
                f"**{brawler}**: {data['picks']} ({pick_rate:.1f}%) • {b_wr:.1f}%"
            )
        
        if len("\n".join(brawler_text)) > 1024:
            chunk_size = 12
            for i in range(0, len(brawler_text), chunk_size):
                chunk = brawler_text[i:i+chunk_size]
                field_name = f"\u200b\nBrawler Pool ({i+1}-{min(i+chunk_size, len(brawler_text))})" if i > 0 else "\u200b\nBrawler Pool"
                embed.add_field(
                    name=field_name,
                    value="\n".join(chunk),
                    inline=False
                )
        else:
            embed.add_field(
                name="\u200b\nBrawler Pool\n(Picks, Pick Rate, WR)",
                value="\n".join(brawler_text) if brawler_text else "No data",
                inline=False
            )
        
        if brawler_stats:
            most_played = brawler_stats[0][0]
            brawler_img = get_brawler_image(most_played)
            if brawler_img:
                file = discord.File(brawler_img, filename="brawler.png")
                embed.set_thumbnail(url="attachment://brawler.png")
                embed.set_footer(text=f"Most played: {most_played}")
                await interaction.response.send_message(embed=embed, file=file, ephemeral=True)
                return
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
    
    async def back_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        view = TeamDetailView(self.team_name)
        embed, team_img = view.create_team_embed()
        
        if team_img:
            file = discord.File(team_img, filename="team_logo.png")
            await interaction.edit_original_response(embed=embed, view=view, attachments=[file])
        else:
            await interaction.edit_original_response(embed=embed, view=view, attachments=[])

def create_welcome_embed():
    """Create the welcome/intro embed - adapts to current mode"""
    mode_config = get_config_for_mode()
    
    embed = discord.Embed(
        description=(
            f"**{mode_config['MODE_EMOJI']} {mode_config['MODE_NAME']} Mode**\n\n"
        ),
        color=discord.Color.red(),
        timestamp=datetime.now()
    )
    
    mode = load_bot_mode()
    
    if mode == 'offseason':
        # OFF-SEASON STATS
        total_players = len(players_data)
        valid_regions = [r for r in region_stats.keys() if not r.startswith('_')]
        total_matches = sum(region_stats[r]['total_matches'] for r in valid_regions)
        
        embed.description += (
            "**Track individual players during the off season.**\n\n"
            "The bot automatically refreshes data every 5 minutes.\n\n"
            "Use !help to see all possible commands.\n\n"
        )
        
        # Count only actual regions (exclude internal keys)
        actual_regions = [r for r in region_stats.keys() if not r.startswith('_')]
         
        embed.add_field(name="Matches Analyzed", value=f"**{total_matches}**", inline=True)
        embed.add_field(name="Tracked Players", value=f"**{total_players}**", inline=True)
        embed.add_field(name="Regions", value=f"**{len(actual_regions)}**", inline=True)
        
        embed.add_field(
            name="ℹ️ Features",
            value=(
                "• Individual player statistics\n"
                "• Brawler pools and win rates\n"
                "• Common teammates tracking\n"
                "• Mode/map stats\n"
                "• Meta analysis\n\n"
                "If you see any inaccurate data, bugs, or have suggestions please contact @xiaku\n\n"
                "***Select an option below:***"
            ),
            inline=False
        )
    else:
        # SEASON STATS (existing code)
        total_teams = len(teams_data)
        total_matches = len(matches_df) if matches_df is not None else 0
        
        embed.description += (
            "**Get all data needed for any team from any region.**\n\n"
            "The bot automatically refreshes data every 5 minutes.\n\n"
            "Use !help to see all possible commands.\n\n"
        )
        
        # Count only actual regions (exclude internal keys)
        actual_regions = [r for r in region_stats.keys() if not r.startswith('_')]
        
        embed.add_field(name="Matches Analyzed", value=f"**{total_matches * 2}**", inline=True)
        embed.add_field(name="Teams", value=f"**{total_teams}**", inline=True)
        embed.add_field(name="Regions", value=f"**{len(actual_regions)}**", inline=True)
        
        embed.add_field(name="Note that:", value="Brawler WR and picks are per sets, overall team WR is per matches.\n\n", inline=False)
        
        embed.add_field(
            name="ℹ️ Features",
            value=(
                "• Region based map stats\n"
                "• Modes stats\n"
                "• Team overall stats\n"
                "• Team map picks \n"
                "• Players stats\n"
                "• Sorting by PR, WR or best pick\n"
                "• Filtering by date\n\n"
                "If you see any inaccurate data, bugs, or have suggestions please contact @xiaku\n\n"
                "***Select a region below:***"
            ),
            inline=False
        )
    
    return embed


@bot.command(name='mode')
@is_authorized()
async def mode_command(ctx, new_mode: str = None):
    """
    View or change bot mode (All users)
    Usage: !mode [season/offseason]
    """
    current_mode = load_bot_mode()
    mode_config = get_config_for_mode()
    
    if not new_mode:
        # Show current mode
        embed = discord.Embed(
            title=f"{mode_config['MODE_EMOJI']} Current Bot Mode",
            description=f"**{mode_config['MODE_NAME']} Mode**",
            color=discord.Color.red()
        )
        
        # Check if files exist
        teams_exists = os.path.exists(mode_config['TEAMS_FILE'])
        matches_exists = os.path.exists(mode_config['MATCHES_FILE'])
        
        status_icon = "✅" if (teams_exists and matches_exists) else "⚠️"
        
        
        
        if not teams_exists or not matches_exists:
            if mode_config.get('IS_PLAYER_MODE'):
                embed.add_field(
                    name="⚠️ Missing Files",
                    value="Create `players_off.xlsx` to track individual players. See !help for structure.",
                    inline=False
                )
            else:
                embed.add_field(
                    name="⚠️ Missing Files",
                    value="Create the required files to use this mode. The bot will create empty match files automatically.",
                    inline=False
                )
        
        embed.add_field(
            name="Change Mode",
            value="Use `!mode season` or `!mode offseason` to switch",
            inline=False
        )
        await ctx.send(embed=embed)
        return
    
    # Change mode
    new_mode = new_mode.lower()
    if new_mode not in ['season', 'offseason']:
        await ctx.send("❌ Invalid mode. Use `season` or `offseason`")
        return
    
    if new_mode == current_mode:
        await ctx.send(f"ℹ️ Already in {new_mode} mode")
        return
    
    # Save new mode
    save_bot_mode(new_mode)
    
    # Check if required files exist
    new_config = get_config_for_mode()
    teams_file = new_config['TEAMS_FILE']
    matches_file = new_config['MATCHES_FILE']
    
    if not os.path.exists(teams_file):
        save_bot_mode(current_mode)  # Revert
        await ctx.send(
            f"❌ Cannot switch to {new_mode} mode: `{teams_file}` not found!\n"
            f"Please create the teams file first."
        )
        return
    
    # Create empty matches file if it doesn't exist
    if not os.path.exists(matches_file):
        try:
            empty_df = pd.DataFrame(columns=[
                'battle_time', 'team1_name', 'team1_region', 'team2_name', 'team2_region',
                'winner', 'mode', 'map', 'star_player_tag',
                'team1_player1', 'team1_player1_tag', 'team1_player1_brawler',
                'team1_player2', 'team1_player2_tag', 'team1_player2_brawler',
                'team1_player3', 'team1_player3_tag', 'team1_player3_brawler',
                'team2_player1', 'team2_player1_tag', 'team2_player1_brawler',
                'team2_player2', 'team2_player2_tag', 'team2_player2_brawler',
                'team2_player3', 'team2_player3_tag', 'team2_player3_brawler'
            ])
            empty_df.to_excel(matches_file, index=False)
            print(f"✅ Created empty matches file: {matches_file}")
        except Exception as e:
            save_bot_mode(current_mode)  # Revert
            await ctx.send(f"❌ Error creating matches file: {e}")
            return
    
    # Reload data with new mode
    global matches_df, teams_data, region_stats, original_matches_df, filter_start_date, filter_end_date
    
    # Clear filters when switching modes
    filter_start_date = None
    filter_end_date = None
    original_matches_df = None
    
    if load_matches_data():
        embed = discord.Embed(
            title=f"✅ Mode Changed",
            description=f"Switched to **{new_config['MODE_NAME']} Mode** {new_config['MODE_EMOJI']}",
            color=discord.Color.green()
        )
        
        
        
        
        await ctx.send(embed=embed)
    else:
        # Revert if loading failed
        save_bot_mode(current_mode)
        await ctx.send(f"❌ Failed to load data for {new_mode} mode. Reverted to {current_mode} mode.")


@bot.command(name='status')
@is_authorized()
async def status_command(ctx):
    """Show bot status and current mode"""
    mode_config = get_config_for_mode()
    
    embed = discord.Embed(
        title=f"🤖 Bot Status",
        color=discord.Color.red(),
        timestamp=datetime.now()
    )
    
    embed.add_field(
        name=f"{mode_config['MODE_EMOJI']} Mode",
        value=f"**{mode_config['MODE_NAME']}**",
        inline=True
    )
    
    total_teams = len(teams_data)
    total_matches = len(matches_df) if matches_df is not None else 0
    
    embed.add_field(name="Matches", value=f"**{total_matches}**", inline=True)
    embed.add_field(name="Teams", value=f"**{total_teams}**", inline=True)
    
    embed.add_field(
        name="Data Files",
        value=f"Teams: `{mode_config['TEAMS_FILE']}`\nMatches: `{mode_config['MATCHES_FILE']}`",
        inline=False
    )
    
    # Show filter status
    if filter_start_date or filter_end_date:
        start_str = filter_start_date.strftime('%Y-%m-%d') if filter_start_date else "Beginning"
        end_str = filter_end_date.strftime('%Y-%m-%d') if filter_end_date else "Now"
        embed.add_field(
            name="📅 Date Filter",
            value=f"{start_str} → {end_str}",
            inline=False
        )
    
    # Last update time
    if matches_df is not None and 'battle_time' in matches_df.columns:
        latest_match = matches_df['battle_time'].max()
        if pd.notna(latest_match):
            time_diff = pd.Timestamp.now(tz='UTC') - pd.to_datetime(latest_match, utc=True)
            hours = int(time_diff.total_seconds() / 3600)
            if hours < 1:
                minutes = int(time_diff.total_seconds() / 60)
                last_update = f"{minutes} min ago"
            elif hours < 24:
                last_update = f"{hours}h ago"
            else:
                days = int(time_diff.total_seconds() / 86400)
                last_update = f"{days}d ago"
            
            embed.add_field(
                name="⏰ Latest Match",
                value=last_update,
                inline=True
            )
    
    await ctx.send(embed=embed)


@bot.command(name='menu')
async def menu_command(ctx):
    """Display main menu"""
    banner_path = './static/banner.jpg'
    
    # Send banner image first
    if os.path.exists(banner_path):
        await ctx.send(file=discord.File(banner_path))
    
    # Determine which view to use based on mode
    mode = load_bot_mode()
    
    if mode == 'offseason':
        view = WelcomeViewOffseason()
    else:
        view = WelcomeView()
    
    content_embed = create_welcome_embed()
    await ctx.send(embed=content_embed, view=view)


def get_team_image(team_name):
    """Get the image file for a team logo if it exists"""
    if not os.path.exists('./static/images/teams/'):
        return None
    
    # Strip spaces from team name before converting to filename
    filename = team_name.strip().lower().replace(' ', '_').replace('-', '_')
    
    for ext in ['.png', '.jpg', '.jpeg', '.webp']:
        filepath = os.path.join('./static/images/teams/', f"{filename}{ext}")
        if os.path.exists(filepath):
            return filepath
    
    return None

def load_team_rosters():
    """Load valid player tags from teams.xlsx"""
    valid_players = {}
    
    teams_file = 'teams.xlsx'
    if not os.path.exists(teams_file):
        print(f"Warning: {teams_file} not found - all players will be included")
        return None
    
    try:
        teams_df = pd.read_excel(teams_file)
        
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
        
        print(f"Loaded rosters for {len(valid_players)} teams")
        return valid_players
    except Exception as e:
        print(f"Error loading team rosters: {e}")
        return None
# ==================== BOT EVENTS ====================

@bot.event
async def on_ready():
    global schedule_initialized, load_process
    
    print(f'Bot logged in as {bot.user}')
    
    # Start load.py if not already running
    if load_process is None:
        try:
            load_process = subprocess.Popen([sys.executable, 'load.py'])
            print("✓ Started load.py process")
        except Exception as e:
            print(f"✗ Failed to start load.py: {e}")
    
    if load_matches_data():
        print("Bot ready!")
    else:
        print("Bot started but no data loaded. Make sure matches.xlsx exists!")
    
    if not data_refresh.is_running():
        data_refresh.start()
    
    # Initialize schedule commands only once
    if not schedule_initialized:
        setup_schedule(bot)
        schedule_initialized = True



@tasks.loop(minutes=CONFIG['CHECK_INTERVAL_MINUTES'])
async def data_refresh():
    """Periodically refresh data from Excel"""
    print(f'Refreshing data... ({datetime.now().strftime("%H:%M:%S")})')
    if load_matches_data():
        print("Data refreshed successfully")
    else:
        print("Failed to refresh data")




@bot.command(name='na')
@is_authorized()
async def na_command(ctx):
    """Quick access to NA region"""
    view = RegionView('NA')
    embed = view.create_region_embed()
    await ctx.send(embed=embed, view=view)


@bot.command(name='eu')
@is_authorized()
async def eu_command(ctx):
    """Quick access to EU region"""
    view = RegionView('EU')
    embed = view.create_region_embed()
    await ctx.send(embed=embed, view=view)


@bot.command(name='latam')
@is_authorized()
async def latam_command(ctx):
    """Quick access to LATAM region"""
    view = RegionView('LATAM')
    embed = view.create_region_embed()
    await ctx.send(embed=embed, view=view)


@bot.command(name='ea')
@is_authorized()
async def ea_command(ctx):
    """Quick access to EA region"""
    view = RegionView('EA')
    embed = view.create_region_embed()
    await ctx.send(embed=embed, view=view)


@bot.command(name='sea')
@is_authorized()
async def sea_command(ctx):
    """Quick access to SEA region"""
    view = RegionView('SEA')
    embed = view.create_region_embed()
    await ctx.send(embed=embed, view=view)


@bot.command(name='all')
@is_authorized()
async def all_command(ctx):
    """Quick access to all regions overview"""
    view = AllRegionsView()
    embed = view.create_all_regions_embed()
    await ctx.send(embed=embed, view=view)


@bot.command(name='team')
@is_authorized()
async def team_command(ctx, *, team_name: str = None):
    """Quick access to any team. Usage: !team <team_name>"""
    if not team_name:
        await ctx.send("Please specify a team name. Usage: `!team <team_name>`")
        return
    
    found_team = None
    for name in teams_data.keys():
        if name.lower() == team_name.lower():
            found_team = name
            break
    
    if not found_team:
        matches = [name for name in teams_data.keys() if team_name.lower() in name.lower()]
        if len(matches) == 1:
            found_team = matches[0]
        elif len(matches) > 1:
            await ctx.send(f"Multiple teams found: {', '.join(matches)}")
            return
        else:
            await ctx.send(f"Team '{team_name}' not found")
            return
    
    view = TeamDetailView(found_team)
    embed, team_img = view.create_team_embed()
    
    if team_img:
        file = discord.File(team_img, filename="team_logo.png")
        await ctx.send(embed=embed, view=view, file=file)
    else:
        await ctx.send(embed=embed, view=view)

@bot.command(name='teams')
@is_authorized()
async def teams_command(ctx):
    """List all teams by region with their players"""
    if not teams_data:
        await ctx.send("No team data available.")
        return
    
    # Region flag mapping
    region_flags = {
        'NA': '🇺🇸',
        'EU': '🇪🇺',
        'LATAM': '🇧🇷',
        'EA': '🌏',
        'SEA': '🌏'
    }
    
    # Organize teams by region
    teams_by_region = {}
    for team_name, team_data in teams_data.items():
        region = team_data['region']
        if region not in teams_by_region:
            teams_by_region[region] = []
        
        # Get player names
        player_names = [p['name'] for p in team_data['players'].values()]
        
        teams_by_region[region].append({
            'name': team_name,
            'players': player_names
        })
    
    # Sort regions and teams
    sorted_regions = sorted(teams_by_region.keys())
    
    embeds = []
    for region in sorted_regions:
        teams = sorted(teams_by_region[region], key=lambda x: x['name'])
        
        flag = region_flags.get(region, '🌐')
        
        embed = discord.Embed(
            title=f"{flag} {region} Teams",
            description=f"Total teams: **{len(teams)}**",
            color=discord.Color.red()
        )
        
        for team in teams:
            players_str = ", ".join(team['players']) if team['players'] else "No players"
            embed.add_field(
                name=team['name'],
                value=players_str,
                inline=False
            )
        
        embeds.append(embed)
    
    # Send all embeds
    for embed in embeds:
        await ctx.send(embed=embed)


@bot.command(name='suggest')
@is_admin()
async def suggest_players_command(ctx, min_games: int = 5):
    """
    Suggest players to track based on frequent teammates (Admin only)
    Usage: !suggest [min_games]
    Example: !suggest 10 (shows players with 10+ games together)
    """
    mode = load_bot_mode()
    
    if mode != 'offseason':
        await ctx.send("âŒ This command only works in off-season mode!")
        return
    
    if not players_data:
        await ctx.send("âŒ No player data available!")
        return
    
    # Collect all teammates and their game counts
    teammate_stats = defaultdict(lambda: {
        'name': '',
        'games': 0,
        'seen_with': []  # List of tracked players they've played with
    })
    
    tracked_tags = set(players_data.keys())
    
    # Analyze teammates from all tracked players
    for player_tag, player_data in players_data.items():
        for teammate_tag, teammate_data in player_data['teammates_seen'].items():
            # Skip if already tracked
            if teammate_tag in tracked_tags:
                continue
            
            teammate_stats[teammate_tag]['name'] = teammate_data['name']
            teammate_stats[teammate_tag]['games'] += teammate_data['matches']
            teammate_stats[teammate_tag]['seen_with'].append({
                'name': player_data['name'],
                'games': teammate_data['matches']
            })
    
    # Filter by minimum games and sort
    suggested = [
        (tag, data) for tag, data in teammate_stats.items() 
        if data['games'] >= min_games
    ]
    suggested.sort(key=lambda x: x[1]['games'], reverse=True)
    
    if not suggested:
        await ctx.send(f"No players found with {min_games}+ games with tracked players.")
        return
    
    # Create view with suggestions
    view = SuggestPlayersView(suggested, min_games)
    embed = view.create_suggestions_embed()
    
    await ctx.send(embed=embed, view=view)


class SuggestPlayersView(View):
    """View for managing player suggestions"""
    def __init__(self, suggested_players, min_games):
        super().__init__(timeout=300)
        self.suggested_players = suggested_players
        self.min_games = min_games
        self.current_page = 0
        self.players_per_page = 10
        
        # Add navigation buttons
        self.update_buttons()
    
    def update_buttons(self):
        """Update button states based on current page"""
        self.clear_items()
        
        total_pages = (len(self.suggested_players) + self.players_per_page - 1) // self.players_per_page
        
        # Add Previous button
        if total_pages > 1:
            prev_btn = Button(label="◀️ Previous", style=discord.ButtonStyle.secondary, row=0)
            prev_btn.callback = self.prev_page
            prev_btn.disabled = (self.current_page == 0)
            self.add_item(prev_btn)
            
            # Add Next button
            next_btn = Button(label="Next ▶️", style=discord.ButtonStyle.secondary, row=0)
            next_btn.callback = self.next_page
            next_btn.disabled = (self.current_page >= total_pages - 1)
            self.add_item(next_btn)
        
        # Add "Add Player" button
        add_btn = Button(label="➕ Add Player", style=discord.ButtonStyle.success, row=1)
        add_btn.callback = self.show_add_menu
        self.add_item(add_btn)
        
        # Add "Refresh" button
        refresh_btn = Button(label="🔄 Refresh", style=discord.ButtonStyle.primary, row=1)
        refresh_btn.callback = self.refresh_suggestions
        self.add_item(refresh_btn)
    
    def create_suggestions_embed(self):
        """Create embed showing suggested players"""
        total_pages = (len(self.suggested_players) + self.players_per_page - 1) // self.players_per_page
        
        embed = discord.Embed(
            title="🔍 Suggested Players to Track",
            description=f"Players with {self.min_games}+ games with tracked players\n**Page {self.current_page + 1}/{total_pages}**",
            color=discord.Color.blue()
        )
        
        # Get current page players
        start_idx = self.current_page * self.players_per_page
        end_idx = min(start_idx + self.players_per_page, len(self.suggested_players))
        page_players = self.suggested_players[start_idx:end_idx]
        
        for i, (tag, data) in enumerate(page_players, start_idx + 1):
            # Format the "seen with" information
            seen_with_text = []
            for teammate in sorted(data['seen_with'], key=lambda x: x['games'], reverse=True)[:3]:
                seen_with_text.append(f"• {teammate['name']} ({teammate['games']} games)")
            
            seen_with_str = "\n".join(seen_with_text)
            if len(data['seen_with']) > 3:
                seen_with_str += f"\n• ...and {len(data['seen_with']) - 3} more"
            
            embed.add_field(
                name=f"#{i} {data['name']}",
                value=(
                    f"**Tag:** `{tag}`\n"
                    f"**Total Games:** {data['games']}\n"
                    f"**Seen with:**\n{seen_with_str}"
                ),
                inline=False
            )
        
        embed.set_footer(text=f"Showing {len(self.suggested_players)} suggested players • Use !suggest [number] to change threshold")
        
        return embed
    
    async def prev_page(self, interaction: discord.Interaction):
        """Go to previous page"""
        await interaction.response.defer()
        if self.current_page > 0:
            self.current_page -= 1
            self.update_buttons()
            embed = self.create_suggestions_embed()
            await interaction.edit_original_response(embed=embed, view=self)
    
    async def next_page(self, interaction: discord.Interaction):
        """Go to next page"""
        await interaction.response.defer()
        total_pages = (len(self.suggested_players) + self.players_per_page - 1) // self.players_per_page
        if self.current_page < total_pages - 1:
            self.current_page += 1
            self.update_buttons()
            embed = self.create_suggestions_embed()
            await interaction.edit_original_response(embed=embed, view=self)
    
    async def show_add_menu(self, interaction: discord.Interaction):
        """Show dropdown to select player to add"""
        await interaction.response.defer()
        
        # Get current page players
        start_idx = self.current_page * self.players_per_page
        end_idx = min(start_idx + self.players_per_page, len(self.suggested_players))
        page_players = self.suggested_players[start_idx:end_idx]
        
        view = AddPlayerSelectView(page_players)
        await interaction.followup.send("Select a player to add to tracking:", view=view, ephemeral=True)
    
    async def refresh_suggestions(self, interaction: discord.Interaction):
        """Refresh the suggestions list"""
        await interaction.response.defer()
        
        # Recalculate suggestions
        teammate_stats = defaultdict(lambda: {
            'name': '',
            'games': 0,
            'seen_with': []
        })
        
        tracked_tags = set(players_data.keys())
        
        for player_tag, player_data in players_data.items():
            for teammate_tag, teammate_data in player_data['teammates_seen'].items():
                if teammate_tag in tracked_tags:
                    continue
                
                teammate_stats[teammate_tag]['name'] = teammate_data['name']
                teammate_stats[teammate_tag]['games'] += teammate_data['matches']
                teammate_stats[teammate_tag]['seen_with'].append({
                    'name': player_data['name'],
                    'games': teammate_data['matches']
                })
        
        suggested = [
            (tag, data) for tag, data in teammate_stats.items() 
            if data['games'] >= self.min_games
        ]
        suggested.sort(key=lambda x: x[1]['games'], reverse=True)
        
        self.suggested_players = suggested
        self.current_page = 0
        self.update_buttons()
        
        embed = self.create_suggestions_embed()
        await interaction.edit_original_response(embed=embed, view=self)


class AddPlayerSelectView(View):
    """Dropdown to select which player to add"""
    def __init__(self, page_players):
        super().__init__(timeout=300)
        
        options = [
            discord.SelectOption(
                label=data['name'][:100],
                description=f"{data['games']} games • {tag}"[:100],
                value=tag
            )
            for tag, data in page_players
        ]
        
        if options:
            select = Select(placeholder="Choose a player to add...", options=options)
            select.callback = self.select_callback
            self.add_item(select)
    
    async def select_callback(self, interaction: discord.Interaction):
        """Handle player selection"""
        selected_tag = interaction.data['values'][0]
        
        # Show region selection
        view = RegionSelectView(selected_tag)
        await interaction.response.send_message("Select a region for this player:", view=view, ephemeral=True)


class RegionSelectView(View):
    """Dropdown to select region for new player"""
    def __init__(self, player_tag):
        super().__init__(timeout=300)
        self.player_tag = player_tag
        
        regions = ['NA', 'EU', 'LATAM', 'EA', 'SEA']
        
        options = [
            discord.SelectOption(label=region, value=region)
            for region in regions
        ]
        
        select = Select(placeholder="Choose a region...", options=options)
        select.callback = self.select_callback
        self.add_item(select)
    
    async def select_callback(self, interaction: discord.Interaction):
        """Add player to tracking file"""
        selected_region = interaction.data['values'][0]
        
        await interaction.response.defer()
        
        try:
            # Load existing players file
            players_file = 'players_off.xlsx'
            
            if os.path.exists(players_file):
                df = pd.read_excel(players_file)
            else:
                # Create new file with headers
                df = pd.DataFrame(columns=['Player Name', 'Player ID', 'Region'])
            
            # Check if player already exists
            if self.player_tag in df['Player ID'].values:
                await interaction.followup.send(f"⚠️ Player `{self.player_tag}` is already being tracked!", ephemeral=True)
                return
            
            # Find player name from teammate data
            player_name = None
            for tracked_player in players_data.values():
                if self.player_tag in tracked_player['teammates_seen']:
                    player_name = tracked_player['teammates_seen'][self.player_tag]['name']
                    break
            
            if not player_name:
                player_name = "Unknown"
            
            # Add new row
            new_row = pd.DataFrame({
                'Player Name': [player_name],
                'Player ID': [self.player_tag],
                'Region': [selected_region]
            })
            
            df = pd.concat([df, new_row], ignore_index=True)
            
            # Save file
            df.to_excel(players_file, index=False)
            
            embed = discord.Embed(
                title="✅ Player Added",
                description=f"**{player_name}** has been added to tracking!",
                color=discord.Color.green()
            )
            embed.add_field(name="Tag", value=f"`{self.player_tag}`", inline=True)
            embed.add_field(name="Region", value=selected_region, inline=True)
            embed.set_footer(text="Data will be collected starting from the next match refresh")
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
            # Reload data to include new player
            load_matches_data()
            
        except Exception as e:
            await interaction.followup.send(f"❌ Error adding player: {str(e)}", ephemeral=True)



@bot.command(name='filter')
@is_authorized()
async def filter_command(ctx, start_date: str = None, end_date: str = None):
    """
    Filter data by date range
    Usage: 
    !filter - Show current filter
    !filter YYYY-MM-DD - Filter from date to now
    !filter YYYY-MM-DD YYYY-MM-DD - Filter between dates
    !filter clear - Remove filter
    
    Examples:
    !filter 2024-11-01
    !filter 2024-11-01 2024-11-15
    !filter clear
    """
    global filter_start_date, filter_end_date, matches_df
    
    # Show current filter
    if not start_date:
        if filter_start_date or filter_end_date:
            start_str = filter_start_date.strftime('%Y-%m-%d') if filter_start_date else "Beginning"
            end_str = filter_end_date.strftime('%Y-%m-%d') if filter_end_date else "Now"
            
            embed = discord.Embed(
                title="📅 Current Date Filter",
                description=f"**From:** {start_str}\n**To:** {end_str}",
                color=discord.Color.red()
            )
            embed.add_field(name="Matches", value=f"`{len(matches_df) if matches_df is not None else 0}`", inline=True)
            embed.set_footer(text="Use !filter clear to remove filter")
            await ctx.send(embed=embed)
        else:
            match_count = len(matches_df) if matches_df is not None else 0
            await ctx.send(f"No date filter applied.")
        return
    
    # Clear filter
    if start_date.lower() == 'clear':
        filter_start_date = None
        filter_end_date = None
        if load_matches_data():
            embed = discord.Embed(
                title="✅ Filter Cleared",
                description="Showing all data from last 30 days",
                color=discord.Color.red()
            )
            embed.add_field(name="Matches", value=f"{len(matches_df) if matches_df is not None else 0}", inline=True)
            await ctx.send(embed=embed)
        else:
            await ctx.send("❌ Error reloading data")
        return
    
    # Parse dates
    try:
        # Parse start date
        start = pd.to_datetime(start_date, format='%Y-%m-%d', utc=True)
        
        # Parse end date if provided, otherwise use now
        if end_date:
            end = pd.to_datetime(end_date, format='%Y-%m-%d', utc=True)
        else:
            end = pd.Timestamp.now(tz='UTC')
        
        if start > end:
            await ctx.send("❌ Start date must be before end date!")
            return
        
        success, message = apply_date_filter(start, end)
        
        if success:
            embed = discord.Embed(
                title="✅ Filter Applied",
                description=f"**From:** {start.strftime('%Y-%m-%d')}\n**To:** {end.strftime('%Y-%m-%d')}",
                color=discord.Color.red()
            )
            embed.add_field(name="Matches Found", value=f"{len(matches_df) if matches_df is not None else 0}", inline=True)
            embed.set_footer(text="Use !filter clear to remove filter")
            await ctx.send(embed=embed)
        else:
            await ctx.send(f"❌ {message}")
            
    except ValueError as e:
        await ctx.send(f"❌ Invalid date format. Use YYYY-MM-DD (e.g., 2024-11-01)")
    except Exception as e:
        await ctx.send(f"❌ Error applying filter: {str(e)}")        
# At the top with other bot setup
bot.remove_command('help')  # Remove default help

@bot.command(name='help')
@is_authorized()
async def help_command(ctx):
    """Custom help command with sorted categories"""
    embed = discord.Embed(
        title="Bot Commands",
        description="Available commands and shortcuts for the Brawlytics Bot",
        color=discord.Color.red()
    )
    
    # Stats Commands
    embed.add_field(
        name="\u200B\n📊 Statistics",
        value=(
            "`!menu` - Main statistics menu\n"
            "`!team <name>` - View specific team stats\n"
            "`!player <tag>` - View specific player stats\n"
            "`!teams` - Lists all monitored teams\n"
        ),
        inline=False
    )
    
    # Region Commands
    embed.add_field(
        name="\u200B\n🌍 Regions (only works for season mode)",
        value=(
            "`!all` - All regions overview\n"
            "`!na` - North America stats\n"
            "`!eu` - Europe stats\n"
            "`!latam` - LATAM stats\n"
            "`!ea` - EA stats\n"
            "`!sea` - SEA stats\n\n"
        ),
        inline=False
    )

    embed.add_field(
        name="\u200B\n🌐 Web",
        value=(
            "`!web` - Access the website\n"
        ),
        inline=False
    )



    embed.add_field(
        name="\u200B\n🏆 Modes",
        value=(
            "`!mode` - Show current filter\n"
            "`!mode season` - Change to the season mode\n"
            "`!mode offseason` - Change to the offseason mode\n"
        ),
        inline=False
    )


    embed.add_field(
        name="\u200B\n🔍 Filters",
        value=(
            "`!filter` - Show current filter\n"
            "`!filter YYYY-MM-DD` - Filter from date\n"
            "`!filter YYYY-MM-DD YYYY-MM-DD` - Filter range\n"
            "`!filter clear` - Remove filter\n"
        ),
        inline=False
    )
    
    # Schedule Commands
    embed.add_field(
        name="\u200B\n📅 Schedule",
        value=(
            "`!schedule` - Set up weekly schedule\n"
            "`!next` - Show next upcoming event\n"
            "`!clear` - Clear schedule & message"
        ),
        inline=False
    )

    await ctx.send(embed=embed)


@bot.command(name='player')
@is_authorized()
async def player_command(ctx, player_tag: str = None):
    """
    Quick access to any player by tag
    Usage: !player <tag> or !player #tag
    Example: !player #2PP0V2R8Q
    """
    if not player_tag:
        await ctx.send("Please specify a player tag. Usage: `!player <tag>` or `!player #tag`")
        return
    
    # Normalize the tag
    normalized_tag = normalize_tag(player_tag)
    
    if not normalized_tag:
        await ctx.send("❌ Invalid player tag format")
        return
    
    mode = load_bot_mode()
    
    if mode == 'offseason':
        # OFF-SEASON MODE: Search in players_data
        if normalized_tag not in players_data:
            await ctx.send(f"❌ Player with tag `{normalized_tag}` is not being tracked.\n\nUse `!suggest` to see players you might want to track.")
            return
        
        # Show player details
        view = PlayerDetailViewOffseason(normalized_tag)
        embed = view.create_player_embed()
        
        # Get top brawler image for thumbnail
        player = players_data[normalized_tag]
        sorted_brawlers = sorted(player['brawlers'].items(), key=lambda x: x[1]['picks'], reverse=True)
        
        if sorted_brawlers:
            top_brawler = sorted_brawlers[0][0]
            brawler_img_path = get_brawler_image(top_brawler)
            if brawler_img_path:
                file = discord.File(brawler_img_path, filename=f"{top_brawler}_icon.png")
                await ctx.send(embed=embed, view=view, file=file)
                return
        
        await ctx.send(embed=embed, view=view)
    
    else:
        # SEASON MODE: Search through all teams
        found_player = None
        found_team = None
        
        for team_name, team_data in teams_data.items():
            if normalized_tag in team_data['players']:
                found_player = team_data['players'][normalized_tag]
                found_team = team_name
                break
        
        if not found_player:
            await ctx.send(f"❌ Player with tag `{normalized_tag}` not found in any team.")
            return
        
        # Create player embed
        team = teams_data[found_team]
        
        embed = discord.Embed(
            title=f"{found_player['name']}",
            description=f"**Team:** {found_team} ({team['region']})",
            color=discord.Color.red()
        )
        
        p_wr = (found_player['wins'] / found_player['matches'] * 100) if found_player['matches'] > 0 else 0
        total_stars = sum(p['star_player'] for p in team['players'].values())
        star_rate = (found_player['star_player'] / total_stars * 100) if total_stars > 0 else 0

        embed.add_field(name="📊 Matches", value=f"**{found_player['matches']}**", inline=True)
        embed.add_field(name="📈 Win Rate", value=f"**{p_wr:.1f}%**", inline=True)
        embed.add_field(name="⭐ Star Player", value=f"**{found_player['star_player']}** ({star_rate:.1f}%)", inline=True)
        
        brawler_stats = sorted(
            found_player['brawlers'].items(),
            key=lambda x: x[1]['picks'],
            reverse=True
        )
        
        brawler_text = []
        total_picks = sum(d['picks'] for d in found_player['brawlers'].values())
        for brawler, data in brawler_stats:
            b_wr = (data['wins'] / data['picks'] * 100) if data['picks'] > 0 else 0
            pick_rate = (data['picks'] / total_picks * 100) if total_picks > 0 else 0
            
            brawler_text.append(
                f"**{brawler}**: {data['picks']} ({pick_rate:.1f}%) • {b_wr:.1f}%"
            )
        
        if len("\n".join(brawler_text)) > 1024:
            chunk_size = 12
            for i in range(0, len(brawler_text), chunk_size):
                chunk = brawler_text[i:i+chunk_size]
                field_name = f"\u200b\nBrawler Pool ({i+1}-{min(i+chunk_size, len(brawler_text))})" if i > 0 else "\u200b\nBrawler Pool\n(Picks, Pick Rate, WR)"
                embed.add_field(
                    name=field_name,
                    value="\n".join(chunk),
                    inline=False
                )
        else:
            embed.add_field(
                name="\u200b\nBrawler Pool\n(Picks, Pick Rate, WR)",
                value="\n".join(brawler_text) if brawler_text else "No data",
                inline=False
            )
        
        if brawler_stats:
            most_played = brawler_stats[0][0]
            brawler_img = get_brawler_image(most_played)
            if brawler_img:
                file = discord.File(brawler_img, filename="brawler.png")
                embed.set_thumbnail(url="attachment://brawler.png")
                embed.set_footer(text=f"Most played: {most_played}")
                await ctx.send(embed=embed, file=file)
                return
        
        await ctx.send(embed=embed)


@bot.command(name='web')
async def access_command(ctx):
    """Generate access link for authorized users"""
    user_id = str(ctx.author.id)
    user_tag = str(ctx.author)
    
    if not is_user_authorized(user_id):
        embed = discord.Embed(
            title="❌ Access Denied",
            description="You are not authorized to access the web dashboard.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
        return
    
    token = generate_access_token(user_id, user_tag)
    
    # Use the configured server URL instead of localhost
    access_link = f"{WEB_SERVER_URL}/auth?token={token}"
    
    try:
        embed = discord.Embed(
            title="🔑 Your Access Link",
            description=f"Click the link below to access the web dashboard:",
            color=discord.Color.red()
        )
        embed.add_field(
            name="Access Link",
            value=f"[Click here to access dashboard]({access_link})",
            inline=False
        )
        embed.add_field(
            name="⚠️ Important",
            value="• This token is single-use only\n• Do not share this link with others\n• Generate a new token with !web if needed",
            inline=False
        )
        embed.set_footer(text=f"Generated for {user_tag}")
        
        await ctx.author.send(embed=embed)
        await ctx.send(f"Web link sent to your DMs {ctx.author.mention}")
        
    except discord.Forbidden:
        await ctx.send(f"Could not send DM. Please enable DMs from server members.\n\nYour token: `{token}`\n\nAccess at: `{access_link}`")


@bot.command(name='add')
@is_admin()
async def adduser_command(ctx, user: discord.User, duration: str = "30d"):
    """
    Add a user to authorized list with expiration (Admin only)
    Duration format: 7d, 30d, 90d, 1y, or 'permanent'
    Examples: !adduser @user 30d, !adduser @user 1y, !adduser @user permanent
    """
    authorized = load_json(AUTHORIZED_USERS_FILE)
    
    user_id = str(user.id)
    
    # Parse duration
    expiration_date = None
    if duration.lower() != 'permanent':
        try:
            if duration.endswith('d'):
                days = int(duration[:-1])
                expiration_date = (datetime.now() + pd.Timedelta(days=days)).isoformat()
            elif duration.endswith('y'):
                years = int(duration[:-1])
                expiration_date = (datetime.now() + pd.Timedelta(days=years*365)).isoformat()
            else:
                await ctx.send("❌ Invalid duration format. Use: 7d, 30d, 90d, 1y, or 'permanent'")
                return
        except ValueError:
            await ctx.send("❌ Invalid duration format. Use: 7d, 30d, 90d, 1y, or 'permanent'")
            return
    
    # Check if already authorized
    if user_id in authorized:
        await ctx.send(f"{user.mention} is already authorized. Use `!removeuser` first to change their access.")
        return
    
    authorized[user_id] = {
        'discord_tag': str(user),
        'added_at': datetime.now().isoformat(),
        'added_by': str(ctx.author),
        'expires_at': expiration_date  # None if permanent
    }
    
    save_json(AUTHORIZED_USERS_FILE, authorized)
    
    embed = discord.Embed(
        title="✅ User Authorized",
        description=f"{user.mention} has been added to the authorized users list.",
        color=discord.Color.green()
    )
    embed.add_field(name="User", value=str(user), inline=True)
    embed.add_field(name="ID", value=user_id, inline=True)
    
    if expiration_date:
        expiration_display = pd.to_datetime(expiration_date).strftime('%Y-%m-%d %H:%M')
        embed.add_field(name="Expires", value=expiration_display, inline=True)
    else:
        embed.add_field(name="Duration", value="Permanent", inline=True)
    
    await ctx.send(embed=embed)


@bot.command(name='rmv')
@is_admin()
async def removeuser_command(ctx, user: discord.User):
    """Remove a user from authorized list (Admin only)"""
    authorized = load_json(AUTHORIZED_USERS_FILE)
    
    user_id = str(user.id)
    
    if user_id not in authorized:
        await ctx.send(f"{user.mention} is not in the authorized list.")
        return
    
    del authorized[user_id]
    save_json(AUTHORIZED_USERS_FILE, authorized)
    
    embed = discord.Embed(
        title="✅ User Removed",
        description=f"{user.mention} has been removed from the authorized users list.",
        color=discord.Color.red()
    )
    
    await ctx.send(embed=embed)



@bot.command(name='users')
@is_admin()
async def listusers_command(ctx):
    """List all authorized users with expiration dates (Admin only)"""
    authorized = load_json(AUTHORIZED_USERS_FILE)
    
    if not authorized:
        await ctx.send("No authorized users.")
        return
    
    # Create paginated view
    view = AuthorizedUsersView(authorized)
    embed = view.create_users_embed()
    
    await ctx.send(embed=embed, view=view)


class AuthorizedUsersView(View):
    """Paginated view for authorized users list"""
    def __init__(self, authorized_users):
        super().__init__(timeout=300)
        self.authorized_users = authorized_users
        self.current_page = 0
        self.users_per_page = 15
        
        # Sort users by discord tag
        self.sorted_users = sorted(
            authorized_users.items(),
            key=lambda x: x[1]['discord_tag'].lower()
        )
        
        # Add navigation buttons
        self.update_buttons()
    
    def update_buttons(self):
        """Update button states based on current page"""
        self.clear_items()
        
        total_pages = (len(self.sorted_users) + self.users_per_page - 1) // self.users_per_page
        
        # Only add pagination buttons if there's more than one page
        if total_pages > 1:
            # Previous button
            prev_btn = Button(label="◀️ Previous", style=discord.ButtonStyle.secondary, row=0, custom_id="prev_users")
            prev_btn.callback = self.prev_page
            prev_btn.disabled = (self.current_page == 0)
            self.add_item(prev_btn)
            
            # Next button
            next_btn = Button(label="Next ▶️", style=discord.ButtonStyle.secondary, row=0, custom_id="next_users")
            next_btn.callback = self.next_page
            next_btn.disabled = (self.current_page >= total_pages - 1)
            self.add_item(next_btn)
        
        # Refresh button
        refresh_btn = Button(label="🔄 Refresh", style=discord.ButtonStyle.primary, row=1)
        refresh_btn.callback = self.refresh_list
        self.add_item(refresh_btn)
    
    def create_users_embed(self):
        """Create embed showing current page of users"""
        total_users = len(self.sorted_users)
        total_pages = (total_users + self.users_per_page - 1) // self.users_per_page
        
        # Calculate active/expired counts
        now = pd.Timestamp.now()
        active_count = 0
        expired_count = 0
        permanent_count = 0
        
        for user_id, data in self.sorted_users:
            expires_at = data.get('expires_at')
            if expires_at:
                if pd.to_datetime(expires_at) > now:
                    active_count += 1
                else:
                    expired_count += 1
            else:
                permanent_count += 1
        
        embed = discord.Embed(
            title="👥 Authorized Users",
            description=(
                f"**Total:** {total_users} users\n"
                f"✅ Active: {active_count} | ⚠️ Expired: {expired_count} | ♾️ Permanent: {permanent_count}\n\n"
                f"**Page {self.current_page + 1}/{total_pages}**"
            ),
            color=discord.Color.red(),
            timestamp=datetime.now()
        )
        
        # Get current page users
        start_idx = self.current_page * self.users_per_page
        end_idx = min(start_idx + self.users_per_page, total_users)
        page_users = self.sorted_users[start_idx:end_idx]
        
        user_list = []
        for user_id, data in page_users:
            expires_at = data.get('expires_at')
            
            if expires_at:
                expiration_date = pd.to_datetime(expires_at)
                expires_str = expiration_date.strftime('%Y-%m-%d')
                
                # Check if expired
                if now > expiration_date:
                    status = f"⚠️ EXPIRED ({expires_str})"
                    status_emoji = "⚠️"
                else:
                    days_left = (expiration_date - now).days
                    if days_left <= 7:
                        status_emoji = "⏰"
                        status = f"{status_emoji} {days_left}d left"
                    else:
                        status_emoji = "✅"
                        status = f"{status_emoji} {days_left}d left"
            else:
                status = "♾️ Permanent"
            
            # Truncate discord tag if too long
            discord_tag = data['discord_tag']
            if len(discord_tag) > 25:
                discord_tag = discord_tag[:22] + "..."
            
            user_list.append(f"**{discord_tag}**\n└ `{user_id}` • {status}")
        
        # Add users to embed
        embed.add_field(
            name=f"Users {start_idx + 1}-{end_idx}",
            value="\n".join(user_list) if user_list else "No users",
            inline=False
        )
        
        embed.set_footer(text=f"Use !add or !rmv to manage users • Showing {start_idx + 1}-{end_idx} of {total_users}")
        
        return embed
    
    async def prev_page(self, interaction: discord.Interaction):
        """Go to previous page"""
        await interaction.response.defer()
        if self.current_page > 0:
            self.current_page -= 1
            self.update_buttons()
            embed = self.create_users_embed()
            await interaction.edit_original_response(embed=embed, view=self)
    
    async def next_page(self, interaction: discord.Interaction):
        """Go to next page"""
        await interaction.response.defer()
        total_pages = (len(self.sorted_users) + self.users_per_page - 1) // self.users_per_page
        if self.current_page < total_pages - 1:
            self.current_page += 1
            self.update_buttons()
            embed = self.create_users_embed()
            await interaction.edit_original_response(embed=embed, view=self)
    
    async def refresh_list(self, interaction: discord.Interaction):
        """Refresh the users list"""
        await interaction.response.defer()
        
        # Reload authorized users from file
        authorized = load_json(AUTHORIZED_USERS_FILE)
        self.authorized_users = authorized
        self.sorted_users = sorted(
            authorized.items(),
            key=lambda x: x[1]['discord_tag'].lower()
        )
        
        # Reset to first page if current page is now out of bounds
        total_pages = (len(self.sorted_users) + self.users_per_page - 1) // self.users_per_page
        if self.current_page >= total_pages:
            self.current_page = max(0, total_pages - 1)
        
        self.update_buttons()
        embed = self.create_users_embed()
        
        await interaction.edit_original_response(embed=embed, view=self)


# ==================== RUN BOT ====================

if __name__ == "__main__":
    if CONFIG['DISCORD_TOKEN'] == 'YOUR_DISCORD_BOT_TOKEN':
        print("Error: Please set DISCORD_TOKEN in .env file!")
        print("\nCreate a .env file with:")
        print("DISCORD_TOKEN=your_discord_bot_token_here")
    else:
        bot.run(CONFIG['DISCORD_TOKEN'])