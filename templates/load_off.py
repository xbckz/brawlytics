"""
Fixed API key rotation with proper concurrent request handling
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
    'BRAWL_STARS_API_TOKENS': [],
    'PLAYERS_FILE': 'players_off.xlsx',
    'MATCHES_FILE': 'matches_off.xlsx',
    'API_CHECK_INTERVAL_MINUTES': 5,
    'RATE_LIMIT_COOLDOWN_SECONDS': 60,
    'BATCH_SIZE': 10,  # Smaller batches to detect rate limits faster
}

# Global storage
processed_battle_times = set()

class APIKeyManager:
    def __init__(self, api_keys):
        self.api_keys = api_keys
        self.current_index = 0
        self.rate_limited_until = {}
        self.total_keys = len(api_keys)
        self.lock = asyncio.Lock()  # Thread-safe key switching
        
        if not self.api_keys:
            raise ValueError("No API keys provided!")
        
        print(f"🔑 Loaded {self.total_keys} API key(s)")
    
    async def get_current_key(self):
        """Get the current active API key (thread-safe)"""
        async with self.lock:
            return self.api_keys[self.current_index]
    
    async def get_headers(self):
        """Get headers with current API key"""
        key = await self.get_current_key()
        return {'Authorization': f"Bearer {key}"}
    
    def get_current_index(self):
        """Get current key index"""
        return self.current_index
    
    async def mark_rate_limited(self, key_index=None):
        """Mark a key as rate-limited"""
        async with self.lock:
            if key_index is None:
                key_index = self.current_index
            
            cooldown_until = time.time() + CONFIG['RATE_LIMIT_COOLDOWN_SECONDS']
            self.rate_limited_until[key_index] = cooldown_until
            
            print(f"⚠️  API Key #{key_index + 1} rate-limited. Cooldown: {CONFIG['RATE_LIMIT_COOLDOWN_SECONDS']}s")
    
    async def switch_to_next_key(self):
        """Switch to next available key (thread-safe)"""
        async with self.lock:
            original_index = self.current_index
            attempts = 0
            current_time = time.time()
            
            while attempts < self.total_keys:
                self.current_index = (self.current_index + 1) % self.total_keys
                attempts += 1
                
                # Check if key is available
                if self.current_index in self.rate_limited_until:
                    if current_time < self.rate_limited_until[self.current_index]:
                        continue  # Still rate-limited
                    else:
                        del self.rate_limited_until[self.current_index]
                
                # Found available key
                if self.current_index != original_index:
                    print(f"🔄 Switched to API Key #{self.current_index + 1}")
                return True
            
            return False  # All keys rate-limited
    
    async def wait_for_available_key(self):
        """Wait until a key becomes available"""
        async with self.lock:
            if not self.rate_limited_until:
                return
            
            next_available = min(self.rate_limited_until.values())
            wait_time = max(0, next_available - time.time())
            
            if wait_time > 0:
                print(f"⏳ All keys rate-limited. Waiting {int(wait_time)}s...")
                await asyncio.sleep(wait_time + 1)
                
                # Clear expired rate limits
                current_time = time.time()
                expired_keys = [k for k, t in self.rate_limited_until.items() if current_time >= t]
                for k in expired_keys:
                    del self.rate_limited_until[k]
                
                # Reset to first available key
                self.current_index = 0
                print(f"✅ Resuming with API Key #{self.current_index + 1}")


def load_api_keys():
    """Load API keys from environment"""
    api_keys = []
    
    main_key = os.getenv('BRAWL_STARS_API_TOKEN', '')
    if main_key:
        api_keys.append(main_key)
    
    for i in range(1, 11):
        key = os.getenv(f'BRAWL_STARS_API_TOKEN_{i}', '')
        if key:
            api_keys.append(key)
    
    if not api_keys:
        print("❌ No API keys found!")
        return []
    
    return list(dict.fromkeys(api_keys))  # Remove duplicates


def load_players_config():
    """Load players from Excel"""
    if not os.path.exists(CONFIG['PLAYERS_FILE']):
        print(f"❌ {CONFIG['PLAYERS_FILE']} not found!")
        return []
    
    try:
        players_df = pd.read_excel(CONFIG['PLAYERS_FILE'])
        players = []
        
        for idx, row in players_df.iterrows():
            try:
                player_name = str(row['Player Name']).strip()
                player_id = str(row['Player ID']).strip()
                region = str(row['Region']).strip().upper()
                
                if player_id and player_id != 'nan' and not pd.isna(row['Player ID']):
                    if not player_id.startswith('#'):
                        player_id = '#' + player_id
                    
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
        
        print(f"✅ Loaded {len(players)} players from {CONFIG['PLAYERS_FILE']}")
        return players
        
    except Exception as e:
        print(f"❌ Error loading players: {e}")
        return []


def load_existing_matches():
    """Load existing matches"""
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


async def fetch_player_battles(session, player_tag, api_key_manager, max_retries=3):
    """Fetch battles with proper retry logic"""
    retries = 0
    
    while retries < max_retries:
        try:
            # Get fresh headers with current key
            headers = await api_key_manager.get_headers()
            url = f"https://api.brawlstars.com/v1/players/{player_tag.replace('#', '%23')}/battlelog"
            
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    battles = data.get('items', [])
                    # Tag battles with source
                    for battle in battles:
                        battle['_source_player_tag'] = player_tag
                    return battles
                
                elif response.status == 429:
                    # Rate limited!
                    current_index = api_key_manager.get_current_index()
                    await api_key_manager.mark_rate_limited(current_index)
                    
                    # Try switching to another key
                    switched = await api_key_manager.switch_to_next_key()
                    
                    if not switched:
                        # All keys rate-limited, wait
                        await api_key_manager.wait_for_available_key()
                    
                    retries += 1
                    await asyncio.sleep(0.5)  # Small delay before retry
                    continue
                
                else:
                    print(f"⚠️  API error {response.status} for {player_tag}")
                    return []
                    
        except Exception as e:
            print(f"❌ Error fetching {player_tag}: {e}")
            retries += 1
            await asyncio.sleep(1)
    
    print(f"⚠️  Max retries reached for {player_tag}")
    return []


def parse_battle_to_match(battle, players_config):
    """Convert battle to match format"""
    try:
        if 'battle' not in battle:
            return None
        
        if battle['battle'].get('type') != 'friendly':
            return None
        
        battle_time = battle['battleTime']

        
        
        if battle_time in processed_battle_times:
            return None
        
        teams = battle['battle'].get('teams', [])
        if len(teams) != 2:
            return None
        
        # Check for tracked players
        tracked_players = {p['tag']: p for p in players_config}
        
        team1_tracked = [p.get('tag') for p in teams[0] if p.get('tag') in tracked_players]
        team2_tracked = [p.get('tag') for p in teams[1] if p.get('tag') in tracked_players]
        
        if not team1_tracked and not team2_tracked:
            return None
        
        # Determine winner
        result = battle['battle'].get('result')
        source_player_tag = battle.get('_source_player_tag')
        
        if not result or not source_player_tag:
            return None
        
        source_on_team1 = any(p['tag'] == source_player_tag for p in teams[0])
        source_on_team2 = any(p['tag'] == source_player_tag for p in teams[1])
        
        if result == 'victory':
            winner = 'team1' if source_on_team1 else 'team2' if source_on_team2 else None
        elif result == 'defeat':
            winner = 'team2' if source_on_team1 else 'team1' if source_on_team2 else None
        else:
            winner = 'draw'
        
        if winner is None:
            return None
        
        # Get star player
        star_player_tag = None
        star_player_data = battle['battle'].get('starPlayer')
        if star_player_data:
            star_player_tag = star_player_data.get('tag')
        
        # Mode and map
        event = battle.get('event', {})
        mode = convert_mode_name(event.get('mode', 'Unknown'))
        map_name = event.get('map', 'Unknown')
        
        # Build match data
        match_data = {
            'battle_time': battle_time,
            'team1_name': "Team_A",
            'team1_region': tracked_players[team1_tracked[0]]['region'] if team1_tracked else "Unknown",
            'team2_name': "Team_B",
            'team2_region': tracked_players[team2_tracked[0]]['region'] if team2_tracked else "Unknown",
            'winner': winner,
            'mode': mode,
            'map': map_name,
            'star_player_tag': star_player_tag,
        }
        
        # Add player data
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
        
        processed_battle_times.add(battle_time)
        return match_data
        
    except Exception as e:
        print(f"❌ Error parsing battle: {e}")
        return None


def convert_mode_name(api_mode):
    """Convert mode names"""
    mode_map = {
        'gemGrab': 'Gem Grab',
        'brawlBall': 'Brawl Ball',
        'heist': 'Heist',
        'bounty': 'Bounty',
        'knockout': 'Knockout',
        'hotZone': 'Hot Zone'
    }
    return mode_map.get(api_mode, api_mode)


async def fetch_new_matches():
    """Fetch new matches with proper rate limit handling"""
    api_keys = load_api_keys()
    if not api_keys:
        print("❌ No API keys configured!")
        return
    
    api_key_manager = APIKeyManager(api_keys)
    
    players_config = load_players_config()
    if not players_config:
        print("⚠️  No players configured")
        return
    
    print(f"🎯 Tracking {len(players_config)} player(s)")
    
    global processed_battle_times
    processed_battle_times = load_existing_matches()
    print(f"📋 Loaded {len(processed_battle_times)} existing matches")
    
    new_matches = []
    all_battles = []
    
    # Process in smaller batches
    batch_size = CONFIG['BATCH_SIZE']
    player_batches = [players_config[i:i + batch_size] for i in range(0, len(players_config), batch_size)]
    
    print(f"📦 Processing {len(player_batches)} batch(es)...")
    
    async with aiohttp.ClientSession() as session:
        for batch_num, player_batch in enumerate(player_batches, 1):
            print(f"🔄 Batch {batch_num}/{len(player_batches)} ({len(player_batch)} players)...")
            
            tasks = [fetch_player_battles(session, player['tag'], api_key_manager) for player in player_batch]
            results = await asyncio.gather(*tasks)
            
            for battles in results:
                all_battles.extend(battles)
            
            # Delay between batches
            if batch_num < len(player_batches):
                await asyncio.sleep(1)
    
    print(f"📥 Fetched {len(all_battles)} total battles")
    
    # Process battles
    friendly_count = 0
    for battle in all_battles:
        if 'battle' in battle and battle['battle'].get('type') == 'friendly':
            friendly_count += 1
            match_data = parse_battle_to_match(battle, players_config)
            if match_data:
                new_matches.append(match_data)
    
    print(f"🎮 Found {friendly_count} friendly matches")
    
    # Remove duplicates
    seen_times = set()
    unique_matches = []
    for match in new_matches:
        if match['battle_time'] not in seen_times:
            seen_times.add(match['battle_time'])
            unique_matches.append(match)
    
    print(f"✅ Found {len(unique_matches)} NEW matches")
    
    if unique_matches:
        write_matches_to_excel(unique_matches)
        print(f"💾 Added {len(unique_matches)} matches to database")
    else:
        print("ℹ️  No new matches to add")


def write_matches_to_excel(new_matches):
    """Save matches to Excel"""
    new_df = pd.DataFrame(new_matches)
    
    if os.path.exists(CONFIG['MATCHES_FILE']):
        existing_df = pd.read_excel(CONFIG['MATCHES_FILE'])
        matches_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        matches_df = new_df
    
    matches_df.drop_duplicates(subset=['battle_time'], keep='last', inplace=True)
    matches_df.to_excel(CONFIG['MATCHES_FILE'], index=False)
    print(f"💾 Saved to {CONFIG['MATCHES_FILE']}")


async def main():
    """Main loop"""
    print("=" * 80)
    print("🏖️  BRAWL STARS OFF-SEASON PLAYER TRACKER")
    print("=" * 80)
    print(f"📊 Matches: {CONFIG['MATCHES_FILE']}")
    print(f"👤 Players: {CONFIG['PLAYERS_FILE']}")
    print(f"⏰ Interval: {CONFIG['API_CHECK_INTERVAL_MINUTES']} min")
    print(f"📦 Batch size: {CONFIG['BATCH_SIZE']} players")
    print("=" * 80)
    
    while True:
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n🔍 Checking... ({timestamp})")
            print("-" * 80)
            await fetch_new_matches()
            print("-" * 80)
            print(f"⏳ Next check in {CONFIG['API_CHECK_INTERVAL_MINUTES']} minutes...")
            await asyncio.sleep(CONFIG['API_CHECK_INTERVAL_MINUTES'] * 60)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()
            await asyncio.sleep(60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n" + "=" * 80)
        print("👋 Shutting down...")
        print("=" * 80)