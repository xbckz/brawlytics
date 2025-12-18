"""
Off-Season Player Tracker - Scans individual players instead of teams
This runs separately from load.py and tracks players from players_off.xlsx

FIXED: Now properly tracks ONLY the players you specify, ignoring all others
UPDATED: Supports multiple API keys with automatic rotation on rate limits
"""

import aiohttp
import asyncio
from datetime import datetime
import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import time

load_dotenv()

# Configuration
CONFIG = {
    'BRAWL_STARS_API_TOKENS': [],  # Will be loaded from env
    'PLAYERS_FILE': 'players_off.xlsx',
    'MATCHES_FILE': 'matches_off.xlsx',
    'API_CHECK_INTERVAL_MINUTES': 5,
    'RATE_LIMIT_COOLDOWN_SECONDS': 60,  # How long to wait before retrying a rate-limited key
}

# Global storage for tracking processed battles
processed_battle_times = set()

# API key rotation state
class APIKeyManager:
    def __init__(self, api_keys):
        self.api_keys = api_keys
        self.current_index = 0
        self.rate_limited_until = {}  # key_index -> timestamp when it can be used again
        self.total_keys = len(api_keys)
        
        if not self.api_keys:
            raise ValueError("No API keys provided!")
        
        print(f"🔑 Loaded {self.total_keys} API key(s)")
    
    def get_current_key(self):
        """Get the current active API key"""
        return self.api_keys[self.current_index]
    
    def get_current_index(self):
        """Get current key index for tracking"""
        return self.current_index
    
    def mark_rate_limited(self, key_index=None):
        """Mark a key as rate-limited"""
        if key_index is None:
            key_index = self.current_index
        
        cooldown_until = time.time() + CONFIG['RATE_LIMIT_COOLDOWN_SECONDS']
        self.rate_limited_until[key_index] = cooldown_until
        
        print(f"⚠️  API Key #{key_index + 1} rate-limited. Cooling down for {CONFIG['RATE_LIMIT_COOLDOWN_SECONDS']}s")
    
    def switch_to_next_key(self):
        """Switch to the next available API key"""
        original_index = self.current_index
        attempts = 0
        current_time = time.time()
        
        while attempts < self.total_keys:
            # Try next key
            self.current_index = (self.current_index + 1) % self.total_keys
            attempts += 1
            
            # Check if this key is rate-limited
            if self.current_index in self.rate_limited_until:
                if current_time < self.rate_limited_until[self.current_index]:
                    # Still rate-limited, try next
                    continue
                else:
                    # Cooldown expired, remove from rate-limited list
                    del self.rate_limited_until[self.current_index]
            
            # Found an available key
            if self.current_index != original_index:
                print(f"🔄 Switched to API Key #{self.current_index + 1}")
            return True
        
        # All keys are rate-limited
        return False
    
    def get_next_available_time(self):
        """Get the timestamp when the next key will be available"""
        if not self.rate_limited_until:
            return 0
        
        return min(self.rate_limited_until.values())
    
    def are_all_keys_rate_limited(self):
        """Check if all keys are currently rate-limited"""
        current_time = time.time()
        
        for i in range(self.total_keys):
            if i not in self.rate_limited_until:
                return False
            if current_time >= self.rate_limited_until[i]:
                return False
        
        return True


def load_api_keys():
    """Load API keys from environment variables"""
    # Try loading multiple keys: BRAWL_STARS_API_TOKEN, BRAWL_STARS_API_TOKEN_2, etc.
    api_keys = []
    
    # Try the main key first
    main_key = os.getenv('BRAWL_STARS_API_TOKEN', '')
    if main_key:
        api_keys.append(main_key)
    
    # Try numbered keys (1-10)
    for i in range(1, 11):
        key = os.getenv(f'BRAWL_STARS_API_TOKEN_{i}', '')
        if key:
            api_keys.append(key)
    
    if not api_keys:
        print("❌ No API keys found!")
        print("💡 Set BRAWL_STARS_API_TOKEN and/or BRAWL_STARS_API_TOKEN_1, BRAWL_STARS_API_TOKEN_2, etc. in .env")
        return []
    
    # Remove duplicates
    api_keys = list(dict.fromkeys(api_keys))
    
    return api_keys


def load_players_config():
    """Load individual players from players_off.xlsx"""
    if not os.path.exists(CONFIG['PLAYERS_FILE']):
        print(f"❌ {CONFIG['PLAYERS_FILE']} not found!")
        print("💡 Create players_off.xlsx with columns: Player Name, Player ID, Region, Notes, Potential Team")
        return []
    
    try:
        players_df = pd.read_excel(CONFIG['PLAYERS_FILE'])
        players = []
        
        for idx, row in players_df.iterrows():
            try:
                player_name = str(row['Player Name']).strip()
                player_id = str(row['Player ID']).strip()
                region = str(row['Region']).strip().upper()
                
                # Add # if missing
                if player_id and player_id != 'nan' and not pd.isna(row['Player ID']):
                    if not player_id.startswith('#'):
                        player_id = '#' + player_id
                    
                    # Get optional fields
                    notes = str(row.get('Notes', '')).strip() if 'Notes' in row else ''
                    potential_team = str(row.get('Potential Team', '')).strip() if 'Potential Team' in row else ''
                    
                    players.append({
                        'name': player_name,
                        'tag': player_id,
                        'region': region,
                        'notes': notes,
                        'potential_team': potential_team
                    })
                    
            except Exception as e:
                print(f"⚠️  Error processing row {idx}: {e}")
                continue
        
        print(f"✅ Successfully loaded {len(players)} tracked players from {CONFIG['PLAYERS_FILE']}")
        return players
        
    except Exception as e:
        print(f"❌ Error loading players config: {e}")
        import traceback
        traceback.print_exc()
        return []


def load_existing_matches():
    """Load existing matches to avoid duplicates"""
    if not os.path.exists(CONFIG['MATCHES_FILE']):
        return set()
    
    try:
        df = pd.read_excel(CONFIG['MATCHES_FILE'])
        if 'battle_time' in df.columns:
            return set(df['battle_time'].values)
        return set()
    except Exception as e:
        print(f"⚠️  Error loading existing matches: {e}")
        return set()


def parse_battle_to_match(battle, players_config):
    """
    Convert API battle data to match format for Excel
    FIXED: Each match stored independently per player
    """
    try:
        if 'battle' not in battle:
            return None
            
        # Only process friendly matches
        if battle['battle'].get('type') != 'friendly':
            return None
        
        battle_time = battle['battleTime']
        
        # Skip if already processed
        if battle_time in processed_battle_times:
            return None
        
        # Extract teams
        teams = battle['battle'].get('teams', [])
        if len(teams) != 2:
            return None
        
        # Find which tracked players are in this match
        tracked_players = {p['tag']: p for p in players_config}
        
        team1_tracked = []
        team2_tracked = []
        
        for player in teams[0]:
            player_tag = player.get('tag')
            if player_tag in tracked_players:
                team1_tracked.append(player_tag)
        
        for player in teams[1]:
            player_tag = player.get('tag')
            if player_tag in tracked_players:
                team2_tracked.append(player_tag)
        
        # Skip if NO tracked players in EITHER team
        if not team1_tracked and not team2_tracked:
            return None
        
        # Determine winner based on source player
        result = battle['battle'].get('result')
        source_player_tag = battle.get('_source_player_tag')
        
        if not result or not source_player_tag:
            return None
        
        source_on_team1 = any(p['tag'] == source_player_tag for p in teams[0])
        source_on_team2 = any(p['tag'] == source_player_tag for p in teams[1])
        
        # Determine winner
        if result == 'victory':
            if source_on_team1:
                winner = 'team1'
            elif source_on_team2:
                winner = 'team2'
            else:
                return None
        elif result == 'defeat':
            if source_on_team1:
                winner = 'team2'
            elif source_on_team2:
                winner = 'team1'
            else:
                return None
        else:
            winner = 'draw'
        
        # Get star player
        star_player_tag = None
        star_player_data = battle['battle'].get('starPlayer')
        if star_player_data:
            star_player_tag = star_player_data.get('tag')
        
        # Convert mode name
        event = battle.get('event', {})
        mode = convert_mode_name(event.get('mode', 'Unknown'))
        map_name = event.get('map', 'Unknown')
        
        # Use generic team names
        team1_name = "Team_A"
        team2_name = "Team_B"
        team1_region = "Unknown"
        team2_region = "Unknown"
        
        # Get region from first tracked player on each team
        if team1_tracked:
            team1_region = tracked_players[team1_tracked[0]]['region']
        if team2_tracked:
            team2_region = tracked_players[team2_tracked[0]]['region']
        
        # Build match data
        match_data = {
            'battle_time': battle_time,
            'team1_name': team1_name,
            'team1_region': team1_region,
            'team2_name': team2_name,
            'team2_region': team2_region,
            'winner': winner,
            'mode': mode,
            'map': map_name,
            'star_player_tag': star_player_tag,
        }
        
        # Add ALL player data from both teams
        for i, player in enumerate(teams[0], 1):
            match_data[f'team1_player{i}'] = player.get('name', 'Unknown')
            match_data[f'team1_player{i}_tag'] = player.get('tag', '')
            brawler = player.get('brawler', {})
            match_data[f'team1_player{i}_brawler'] = brawler.get('name', 'Unknown')
        
        for i, player in enumerate(teams[1], 1):
            match_data[f'team2_player{i}'] = player.get('name', 'Unknown')
            match_data[f'team2_player{i}_tag'] = player.get('tag', '')
            brawler = player.get('brawler', {})
            match_data[f'team2_player{i}_brawler'] = brawler.get('name', 'Unknown')
        
        # Mark as processed
        processed_battle_times.add(battle_time)
        
        return match_data
        
    except Exception as e:
        print(f"❌ Error parsing battle: {e}")
        import traceback
        traceback.print_exc()
        return None


def convert_mode_name(api_mode):
    """Convert API mode names to readable format"""
    mode_map = {
        'gemGrab': 'Gem Grab',
        'brawlBall': 'Brawl Ball',
        'heist': 'Heist',
        'bounty': 'Bounty',
        'knockout': 'Knockout',
        'hotZone': 'Hot Zone'
    }
    return mode_map.get(api_mode, api_mode)


async def fetch_player_battles(session, player_tag, headers, api_key_manager, retry_count=0):
    """Fetch battle log for a single player with rate limit handling"""
    max_retries = api_key_manager.total_keys
    
    try:
        url = f"https://api.brawlstars.com/v1/players/{player_tag.replace('#', '%23')}/battlelog"
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                battles = data.get('items', [])
                # Tag each battle with the source player tag
                for battle in battles:
                    battle['_source_player_tag'] = player_tag
                return battles
            
            elif response.status == 429:
                # Rate limited - mark current key and switch
                current_key_index = api_key_manager.get_current_index()
                api_key_manager.mark_rate_limited(current_key_index)
                
                # Try to switch to another key
                if retry_count < max_retries:
                    if api_key_manager.switch_to_next_key():
                        # Successfully switched, retry with new key
                        new_headers = {
                            'Authorization': f"Bearer {api_key_manager.get_current_key()}"
                        }
                        await asyncio.sleep(0.5)  # Small delay before retry
                        return await fetch_player_battles(session, player_tag, new_headers, api_key_manager, retry_count + 1)
                    else:
                        # All keys are rate-limited
                        next_available = api_key_manager.get_next_available_time()
                        wait_time = max(0, next_available - time.time())
                        print(f"⚠️  All API keys rate-limited! Waiting {int(wait_time)}s...")
                        await asyncio.sleep(wait_time + 1)
                        
                        # Reset and try again
                        api_key_manager.current_index = 0
                        new_headers = {
                            'Authorization': f"Bearer {api_key_manager.get_current_key()}"
                        }
                        return await fetch_player_battles(session, player_tag, new_headers, api_key_manager, 0)
                else:
                    print(f"⚠️  Max retries reached for {player_tag}")
                    return []
            
            else:
                print(f"⚠️  API error for {player_tag}: {response.status}")
                return []
                
    except Exception as e:
        print(f"❌ Error fetching {player_tag}: {e}")
        return []


async def fetch_new_matches():
    """Fetch new matches from Brawl Stars API with rate limit handling"""
    # Load API keys
    api_keys = load_api_keys()
    if not api_keys:
        print("❌ No API keys configured!")
        print("💡 Please set BRAWL_STARS_API_TOKEN (and optionally BRAWL_STARS_API_TOKEN_1, _2, etc.) in .env file")
        return
    
    # Initialize API key manager
    api_key_manager = APIKeyManager(api_keys)
    
    # Load players configuration
    players_config = load_players_config()
    if not players_config:
        print("⚠️  No players configured in players_off.xlsx")
        return
    
    print(f"🎯 Tracking {len(players_config)} player(s)")
    
    # Load existing battle times
    global processed_battle_times
    processed_battle_times = load_existing_matches()
    print(f"📋 Loaded {len(processed_battle_times)} existing matches")
    
    new_matches = []
    all_battles = []
    
    # Process players in batches to manage rate limits better
    batch_size = 20  # Adjust based on your needs
    player_batches = [players_config[i:i + batch_size] for i in range(0, len(players_config), batch_size)]
    
    print(f"📦 Processing {len(player_batches)} batch(es) of players...")
    
    for batch_num, player_batch in enumerate(player_batches, 1):
        print(f"🔄 Processing batch {batch_num}/{len(player_batches)} ({len(player_batch)} players)...")
        
        headers = {
            'Authorization': f"Bearer {api_key_manager.get_current_key()}"
        }
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            for player in player_batch:
                tasks.append(fetch_player_battles(session, player['tag'], headers, api_key_manager))
            
            results = await asyncio.gather(*tasks)
            
            # Flatten battles from this batch
            for battles in results:
                all_battles.extend(battles)
        
        # Small delay between batches to avoid hammering the API
        if batch_num < len(player_batches):
            await asyncio.sleep(1)
    
    print(f"📥 Fetched {len(all_battles)} total battles")
    
    # Process battles and convert to matches
    friendly_count = 0
    for battle in all_battles:
        if 'battle' in battle and battle['battle'].get('type') == 'friendly':
            friendly_count += 1
            match_data = parse_battle_to_match(battle, players_config)
            if match_data:
                new_matches.append(match_data)
    
    print(f"🎮 Found {friendly_count} friendly matches")
    
    # Remove duplicates based on battle_time
    seen_times = set()
    unique_matches = []
    for match in new_matches:
        if match['battle_time'] not in seen_times:
            seen_times.add(match['battle_time'])
            unique_matches.append(match)
    
    print(f"✅ Found {len(unique_matches)} NEW matches with tracked players")
    
    # Write to Excel
    if unique_matches:
        write_matches_to_excel(unique_matches)
        print(f"💾 Added {len(unique_matches)} new matches to database")
    else:
        print("ℹ️  No new matches to add")


def write_matches_to_excel(new_matches):
    """Append new matches to Excel file"""
    new_df = pd.DataFrame(new_matches)
    
    if os.path.exists(CONFIG['MATCHES_FILE']):
        existing_df = pd.read_excel(CONFIG['MATCHES_FILE'])
        matches_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        matches_df = new_df
    
    # Remove duplicates based on battle_time
    matches_df.drop_duplicates(subset=['battle_time'], keep='last', inplace=True)
    
    # Save to Excel
    matches_df.to_excel(CONFIG['MATCHES_FILE'], index=False)
    print(f"💾 Saved to {CONFIG['MATCHES_FILE']}")


async def main():
    """Main loop"""
    print("=" * 80)
    print("🏖️  BRAWL STARS OFF-SEASON PLAYER TRACKER")
    print("=" * 80)
    print(f"📊 Matches file: {CONFIG['MATCHES_FILE']}")
    print(f"👤 Players file: {CONFIG['PLAYERS_FILE']}")
    print(f"⏰ Check interval: {CONFIG['API_CHECK_INTERVAL_MINUTES']} minutes")
    print("=" * 80)
    print()
    
    while True:
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n🔍 Checking for new matches... ({timestamp})")
            print("-" * 80)
            await fetch_new_matches()
            print("-" * 80)
            print(f"⏳ Next check in {CONFIG['API_CHECK_INTERVAL_MINUTES']} minutes...")
            await asyncio.sleep(CONFIG['API_CHECK_INTERVAL_MINUTES'] * 60)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"❌ Error in main loop: {e}")
            import traceback
            traceback.print_exc()
            await asyncio.sleep(60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n")
        print("=" * 80)
        print("👋 Shutting down off-season tracker...")
        print("=" * 80)