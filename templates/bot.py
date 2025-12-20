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

TOKENS_FILE = 'data/tokens.json'
AUTHORIZED_USERS_FILE = 'data/authorized_users.json'

# Global data storage
matches_df = None
teams_data = {}
region_stats = {}
schedule_initialized = False
load_process = None

filter_start_date = None
filter_end_date = None
original_matches_df = None

# Bot setup
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)

BOT_MODE_FILE = 'data/bot_mode.json'

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
            'MODE_NAME': 'Off-Season',
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

def assign_brawlers_to_tiers_bot(meta_scores):
    """
    Improved tier assignment for Discord bot
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
        if wr >= 70:
            return (100, 255, 100)  # Bright green
        elif wr >= 60:
            return (150, 255, 100)  # Light green
        elif wr >= 50:
            return (255, 255, 100)  # Yellow
        elif wr >= 40:
            return (255, 200, 100)  # Orange
        else:
            return (255, 100, 100)  # Red
    
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
    Returns: BytesIO object containing the PNG image
    """
    from PIL import Image, ImageDraw, ImageFont
    from io import BytesIO
    
    # Collect brawler stats
    brawler_picks = defaultdict(int)
    brawler_wins = defaultdict(int)
    
    relevant_teams = teams_data.items()
    if region != 'ALL':
        relevant_teams = [(name, data) for name, data in teams_data.items() if data['region'] == region]
    
    # Aggregate brawler data
    total_picks = 0
    for team_name, team_data in relevant_teams:
        for mode_name, mode_data in team_data['modes'].items():
            if mode_name in ['Unknown', 'nan', '', 'None']:
                continue
            
            if mode and mode != 'ALL' and mode_name != mode:
                continue
            
            for map_name, map_data in mode_data['maps'].items():
                for brawler, brawler_data in map_data['brawlers'].items():
                    brawler_picks[brawler] += brawler_data['picks']
                    brawler_wins[brawler] += brawler_data['wins']
                    total_picks += brawler_data['picks']
    
    if total_picks == 0:
        return None
    
    # Calculate meta scores
    meta_scores = []
    for brawler in brawler_picks:
        if brawler_picks[brawler] < 1:
            continue
        
        pick_rate = (brawler_picks[brawler] / total_picks * 100)
        win_rate = (brawler_wins[brawler] / brawler_picks[brawler] * 100)
        meta_score = (win_rate * pick_rate) / 100
        
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
    
    # Use improved tier assignment
    tiers = assign_brawlers_to_tiers_bot(meta_scores)
    
    if not tiers:
        return None

    print("\n" + "="*80)
    print("DISCORD BOT - TIER ASSIGNMENTS")
    print("="*80)
    print(f"Total brawlers: {len(meta_scores)}")
    
    print("\nTier contents:")
    for tier_name in ['S', 'A', 'B', 'C', 'D', 'F']:
        brawlers = tiers[tier_name]['brawlers']
        print(f"\n{tier_name} Tier ({len(brawlers)} brawlers):")
        for b in brawlers:
            print(f"  {b['brawler']}: score={b['score']:.4f}, wr={b['win_rate']:.2f}%, pr={b['pick_rate']:.2f}%")
    print("="*80 + "\n")
    
    # Image generation
    BRAWLER_IMG_SIZE = 80
    PADDING = 20
    TIER_LABEL_WIDTH = 80
    HEADER_HEIGHT = 180
    ROW_HEIGHT = BRAWLER_IMG_SIZE + PADDING * 2
    MAX_BRAWLERS_PER_ROW = 14
    
    total_rows = 0
    for tier in tiers.values():
        if tier['brawlers']:
            rows_for_tier = (len(tier['brawlers']) + MAX_BRAWLERS_PER_ROW - 1) // MAX_BRAWLERS_PER_ROW
            total_rows += rows_for_tier
    
    img_width = TIER_LABEL_WIDTH + (BRAWLER_IMG_SIZE + PADDING) * MAX_BRAWLERS_PER_ROW + PADDING
    img_height = HEADER_HEIGHT + (ROW_HEIGHT * total_rows) + PADDING + 50
    
    img = Image.new('RGB', (img_width, img_height), color=(30, 30, 40))
    draw = ImageDraw.Draw(img)
    
    try:
        title_font = ImageFont.truetype("arial.ttf", 55)
        subtitle_font = ImageFont.truetype("arial.ttf", 30)
        tier_font = ImageFont.truetype("arial.ttf", 27)
        brawler_font = ImageFont.truetype("arial.ttf", 14)
        stat_font = ImageFont.truetype("arial.ttf", 11)
    except:
        title_font = ImageFont.load_default()
        subtitle_font = ImageFont.load_default()
        tier_font = ImageFont.load_default()
        brawler_font = ImageFont.load_default()
        stat_font = ImageFont.load_default()
    
    region_text = "All Regions" if region == 'ALL' else f"{region} Region"
    mode_text = f" - {mode}" if mode else ""
    title = f"Meta Tier List"
    subtitle = f"{region_text}{mode_text}"
    
    title_bbox = draw.textbbox((0, 0), title, font=title_font)
    title_width = title_bbox[2] - title_bbox[0]
    draw.text(((img_width - title_width) // 2, 25), title, fill=(255, 255, 255), font=title_font)
    
    subtitle_bbox = draw.textbbox((0, 0), subtitle, font=subtitle_font)
    subtitle_width = subtitle_bbox[2] - subtitle_bbox[0]
    draw.text(((img_width - subtitle_width) // 2, 115), subtitle, fill=(200, 200, 200), font=subtitle_font)
    
    y_offset = HEADER_HEIGHT
    
    for tier_name, tier_data in tiers.items():
        if not tier_data['brawlers']:
            continue
        
        brawler_rows = []
        for i in range(0, len(tier_data['brawlers']), MAX_BRAWLERS_PER_ROW):
            brawler_rows.append(tier_data['brawlers'][i:i + MAX_BRAWLERS_PER_ROW])
        
        tier_total_height = ROW_HEIGHT * len(brawler_rows)
        
        draw.rectangle(
            [(0, y_offset), (img_width, y_offset + tier_total_height)],
            fill=(40, 40, 50),
            outline=(60, 60, 70),
            width=2
        )
        
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
        
        current_row_y = y_offset
        for brawler_row in brawler_rows:
            x_offset = TIER_LABEL_WIDTH + PADDING
            
            for brawler_data in brawler_row:
                brawler_name = brawler_data['brawler']
                
                brawler_img_path = get_brawler_image(brawler_name)
                
                if brawler_img_path and os.path.exists(brawler_img_path):
                    try:
                        brawler_img = Image.open(brawler_img_path)
                        brawler_img = brawler_img.resize((BRAWLER_IMG_SIZE, BRAWLER_IMG_SIZE), Image.Resampling.LANCZOS)
                        img.paste(brawler_img, (x_offset, current_row_y + PADDING))
                    except:
                        draw.rectangle(
                            [(x_offset, current_row_y + PADDING), 
                             (x_offset + BRAWLER_IMG_SIZE, current_row_y + PADDING + BRAWLER_IMG_SIZE)],
                            fill=(80, 80, 80),
                            outline=(120, 120, 120),
                            width=2
                        )
                else:
                    draw.rectangle(
                        [(x_offset, current_row_y + PADDING), 
                         (x_offset + BRAWLER_IMG_SIZE, current_row_y + PADDING + BRAWLER_IMG_SIZE)],
                        fill=(80, 80, 80),
                        outline=(120, 120, 120),
                        width=2
                    )
                
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
    
    img_bytes = io.BytesIO()
    img.save(img_bytes, format='PNG')
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
    global matches_df, teams_data, region_stats
    
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
        calculate_all_stats()
        return True
    except Exception as e:
        print(f"Error loading Excel: {e}")
        return False

def calculate_all_stats_offseason():
    """
    Calculate comprehensive statistics from matches for OFF-SEASON mode
    Focus on individual player tracking instead of team tracking
    """
    global teams_data, region_stats
    
    valid_rosters = load_team_rosters_offseason()

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
    series_tracking_brawlers = {}

    for _, match in matches_df.iterrows():
        match_id = match.get('battle_time', str(_))
        
        # Create series ID (same logic as season mode)
        team1 = match['team1_name']
        team2 = match['team2_name']
        teams_sorted = tuple(sorted([team1, team2]))
        mode = str(match['mode'])
        map_name = str(match['map'])
        
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
        
        comps_sorted = tuple(sorted([tuple(team1_comp), tuple(team2_comp)]))
        
        battle_time = match.get('battle_time')
        if pd.notna(battle_time):
            time_rounded = pd.Timestamp(battle_time).floor('30min')
        else:
            time_rounded = match_id
        
        series_id = f"{teams_sorted}_{mode}_{map_name}_{comps_sorted}_{time_rounded}"
        
        if series_id not in series_tracking_brawlers:
            series_tracking_brawlers[series_id] = {}
        
        # Get tracked players from match data
        tracked_team1 = str(match.get('tracked_team1', '')).split(',') if pd.notna(match.get('tracked_team1')) else []
        tracked_team2 = str(match.get('tracked_team2', '')).split(',') if pd.notna(match.get('tracked_team2')) else []
        tracked_team1 = [t.strip() for t in tracked_team1 if t.strip()]
        tracked_team2 = [t.strip() for t in tracked_team2 if t.strip()]
        
        for team_prefix in ['team1', 'team2']:
            team_name = match[f'{team_prefix}_name']
            team_region = str(match[f'{team_prefix}_region']).strip().upper()
            
            if team_region in ['NAN', 'NONE', '', 'UNKNOWN'] or pd.isna(match[f'{team_prefix}_region']):
                team_region = 'NA'
            
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

            winner_name = str(match['winner']).strip()
            is_winner = (winner_name == team_prefix)
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
            
            star_player_tag = str(match.get('star_player_tag', '')).strip().upper().replace('0', 'O')
            
            # Process players - but only count stats for TRACKED players
            tracked_list = tracked_team1 if team_prefix == 'team1' else tracked_team2
            
            for i in range(1, 4):
                player_name = str(match[f'{team_prefix}_player{i}'])
                player_tag = str(match[f'{team_prefix}_player{i}_tag']).strip().upper().replace('0', 'O')
                brawler = str(match[f'{team_prefix}_player{i}_brawler'])
                is_tracked = match.get(f'{team_prefix}_player{i}_tracked', False)
                
                if pd.isna(match[f'{team_prefix}_player{i}']) or player_name == 'nan':
                    continue
                
                # Only process stats for TRACKED players
                if not is_tracked or player_tag not in tracked_list:
                    continue
                
                # Skip players not in the roster (if roster validation is enabled)
                if valid_rosters and team_name in valid_rosters:
                    if player_tag not in valid_rosters[team_name]:
                        continue
                
                player = team['players'][player_tag]
                player['name'] = player_name
                player['matches'] += 1
                
                if is_winner:
                    player['wins'] += 1
                
                # Track brawler picks per SERIES
                if team_name not in series_tracking_brawlers[series_id]:
                    series_tracking_brawlers[series_id][team_name] = set()

                brawler_key = f"{player_tag}_{brawler}"
                if brawler_key not in series_tracking_brawlers[series_id][team_name]:
                    series_tracking_brawlers[series_id][team_name].add(brawler_key)
                    
                    player['brawlers'][brawler]['picks'] += 1
                    team['brawlers'][brawler]['picks'] += 1
                    team['modes'][mode]['maps'][map_name]['brawlers'][brawler]['picks'] += 1
                    
                    if is_winner:
                        player['brawlers'][brawler]['wins'] += 1
                        team['brawlers'][brawler]['wins'] += 1
                        team['modes'][mode]['maps'][map_name]['brawlers'][brawler]['wins'] += 1
                
                # Star player tracking
                if star_player_tag and star_player_tag != 'NAN' and star_player_tag == player_tag:
                    player['star_player'] += 1
            
            region_stats[team_region]['total_matches'] += 1
            region_stats[team_region]['teams'].add(team_name)
    
    for region in region_stats:
        region_stats[region]['teams'] = list(region_stats[region]['teams'])


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
        
        print(f"Loaded {sum(len(tags) for tags in valid_players.values())} tracked players from {players_file} (off-season mode)")
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
            title="📊 Current Meta Analysis",
            description="Select a region below to view detailed meta statistics and tier rankings.",
            color=discord.Color.red()
        )
        
        embed.add_field(name="Brawlers Tracked", value=f"{total_brawlers}", inline=True)
        embed.add_field(name="Matches Analyzed", value=f"{games_analyzed * 2:,}", inline=True)
        embed.add_field(name="Latest Match", value=f"{last_update}", inline=True)
        
        
        
        view = MetaView()
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    
    async def show_region(self, interaction: discord.Interaction, region: str):
        await interaction.response.defer()
        view = RegionView(region)
        embed = view.create_region_embed()
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)

# Add these new view classes to your bot.py file (after the WelcomeView class)

# UPDATED MetaView class
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
        view = MetaModeSelectView(self.region)
        await interaction.followup.send("Select a mode to view meta:", view=view, ephemeral=True)
    
    @discord.ui.button(label="◀️ Back", style=discord.ButtonStyle.secondary, row=0)
    async def back_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        view = MetaView()
        await interaction.edit_original_response(content="**Current Meta Analysis**\n\nSelect a region:", embed=None, view=view, attachments=[])


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
        
        total_matches = len(matches_df)
        total_teams = len(teams_data)
        
        embed.add_field(name="Total Matches", value=f"**{total_matches * 2}**", inline=True)
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
        
        embed.add_field(name="⚔️ Total Matches", value=f"**{total_matches * 2}**", inline=True)
        
        sorted_by_picks = sorted(brawler_picks.items(), key=lambda x: x[1], reverse=True)[:15]
        picks_text = []
        total_picks = sum(brawler_picks.values())
        
        for brawler, picks in sorted_by_picks:
            pick_rate = (picks / total_picks * 100) if total_picks > 0 else 0
            wr = (brawler_wins[brawler] / picks * 100) if picks > 0 else 0
            picks_text.append(f"**{brawler}**: {picks} ({pick_rate:.1f}%) • {wr:.1f}% WR")
        
        embed.add_field(
            name="\u200b\n📊 Most Picked Brawlers",
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
        
        embed.add_field(name="⚔️ Matches", value=f"**{total_matches * 2}**", inline=True)
        
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
        
        total_matches = stats.get('total_matches', 0)
        team_count = len(region_teams)
        
        embed.add_field(name="⚔️ Total Matches", value=f"**{total_matches * 2}**", inline=True)
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
            name="\u200b\n📊 Most Picked Brawlers",
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
        
        embed.add_field(name="⚔️ Matches", value=f"**{total_matches * 2}**", inline=True)
        
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
                    field_name = "📊 Brawler Pool" if field_num == 0 else "\u200b"
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
                field_name = "📊 Brawler Pool" if field_num == 0 else "\u200b"
                embed.add_field(
                    name=field_name,
                    value="\n".join(current_chunk),
                    inline=False
                )
        else:
            embed.add_field(
                name="📊 Brawler Pool",
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
    """Create the welcome/intro embed"""
    mode_config = get_config_for_mode()
    
    embed = discord.Embed(
        description=(
            f"**{mode_config['MODE_EMOJI']} {mode_config['MODE_NAME']} Mode**\n\n"
            "**Get all data needed for any team from any region.**\n\n"
            "The bot automatically refreshes data every 5 minutes.\n\n"
            "Use !help to see all possible commands.\n\n"
        ),
        color=discord.Color.red(),
        timestamp=datetime.now()
    )
    
    total_teams = len(teams_data)
    total_matches = len(matches_df) if matches_df is not None else 0
    
    embed.add_field(name="Tracked Matches", value=f"**{total_matches * 2}**", inline=True)
    embed.add_field(name="Teams", value=f"**{total_teams}**", inline=True)
    embed.add_field(name="Regions", value=f"**{len(region_stats)}**", inline=True)

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
        
        embed.add_field(
            name="Data Files",
            value=(
                f"{'Players' if mode_config.get('IS_PLAYER_MODE') else 'Teams'}: `{mode_config['TEAMS_FILE']}` {('✅' if teams_exists else '❌ Not found')}\n"
                f"Matches: `{mode_config['MATCHES_FILE']}` {('✅' if matches_exists else '❌ Not found')}"
            ),
            inline=False
        )
        
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
        embed.add_field(
            name="Now Using",
            value=f"Teams: `{new_config['TEAMS_FILE']}`\nMatches: `{new_config['MATCHES_FILE']}`",
            inline=False
        )
        
        match_count = len(matches_df) * 2 if matches_df is not None else 0
        team_count = len(teams_data)
        
        embed.add_field(
            name="Loaded Data",
            value=f"**{match_count}** matches from **{team_count}** teams",
            inline=False
        )
        
        if match_count == 0:
            embed.add_field(
                name="ℹ️ No Matches Yet",
                value="The matches file is empty. Data will appear as matches are played and fetched by load.py",
                inline=False
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
    
    embed.add_field(name="Matches", value=f"**{total_matches * 2}**", inline=True)
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
    
    # Send banner image first (full width, no embed)
    if os.path.exists(banner_path):
        await ctx.send(file=discord.File(banner_path))
    
    # Then send the content embed
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
            embed.add_field(name="Matches", value=f"{len(matches_df) * 2 if matches_df is not None else 0}", inline=True)
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
            embed.add_field(name="Matches Found", value=f"{len(matches_df) * 2 if matches_df is not None else 0}", inline=True)
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
        description="Available commands and shortcuts for the Brawlnalytics Bot",
        color=discord.Color.red()
    )
    
    # Stats Commands
    embed.add_field(
        name="\u200B\n📊 Statistics",
        value=(
            "`!menu` - Main statistics menu\n"
            "`!team <name>` - View specific team stats\n"
            "`!teams` - Lists all monitored teams\n"
        ),
        inline=False
    )
    
    # Region Commands
    embed.add_field(
        name="\u200B\n🌍 Regions",
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
    
    embed = discord.Embed(
        title="Authorized Users",
        description=f"Total: **{len(authorized)}** users",
        color=discord.Color.red()
    )
    
    user_list = []
    for user_id, data in authorized.items():
        expires_at = data.get('expires_at')
        
        if expires_at:
            expiration_date = pd.to_datetime(expires_at)
            expires_str = expiration_date.strftime('%Y-%m-%d')
            
            # Check if expired
            if pd.Timestamp.now() > expiration_date:
                status = "⚠️ EXPIRED"
            else:
                days_left = (expiration_date - pd.Timestamp.now()).days
                status = f"{days_left}d left"
        else:
            status = "Permanent"
        
        user_list.append(f"• {data['discord_tag']} (`{user_id}`) - {status}")
    
    # Split into chunks if too long
    chunks = [user_list[i:i+15] for i in range(0, len(user_list), 15)]
    
    for i, chunk in enumerate(chunks):
        field_name = "Users" if i == 0 else f"Users (cont. {i+1})"
        embed.add_field(
            name=field_name,
            value="\n".join(chunk),
            inline=False
        )
    
    await ctx.send(embed=embed)

# ==================== RUN BOT ====================

if __name__ == "__main__":
    if CONFIG['DISCORD_TOKEN'] == 'YOUR_DISCORD_BOT_TOKEN':
        print("Error: Please set DISCORD_TOKEN in .env file!")
        print("\nCreate a .env file with:")
        print("DISCORD_TOKEN=your_discord_bot_token_here")
    else:
        bot.run(CONFIG['DISCORD_TOKEN'])