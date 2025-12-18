import aiohttp
import asyncio
from datetime import datetime
import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration
CONFIG = {
    'BRAWL_STARS_API_TOKEN': os.getenv('BRAWL_STARS_API_TOKEN', ''),
    'TEAMS_FILE': 'teams.xlsx',
    'MATCHES_FILE': 'matches.xlsx',
    'API_CHECK_INTERVAL_MINUTES': 5,
}




# Global storage for tracking processed battles
processed_battle_times = set()


def load_teams_config():
    """Load team and player information from teams.xlsx"""
    if not os.path.exists(CONFIG['TEAMS_FILE']):
        print(f"{CONFIG['TEAMS_FILE']} not found!")
        return {}
    
    try:
        teams_df = pd.read_excel(CONFIG['TEAMS_FILE'])
        teams_config = {}
        
        for idx, row in teams_df.iterrows():
            try:
                team_name = row['Team Name']
                region = row['Region']
                
                # Store player tags for this team
                players = []
                for i in range(1, 4):
                    try:
                        player_id_col = f'Player {i} ID'
                        player_name_col = f'Player {i} Name'
                        
                        if player_id_col not in row or player_name_col not in row:
                            continue
                        
                        player_id = str(row[player_id_col]).strip()
                        player_name = str(row[player_name_col]).strip()
                        
                        # Add # if missing
                        if player_id and player_id != 'nan' and not pd.isna(row[player_id_col]):
                            if not player_id.startswith('#'):
                                player_id = '#' + player_id
                            
                            players.append({
                                'tag': player_id,
                                'name': player_name
                            })
                    except Exception as e:
                        print(f"⚠️  Error processing player {i} for team {team_name}: {e}")
                        continue
                
                if players:
                    teams_config[team_name] = {
                        'region': region,
                        'players': players
                    }
                    print(f"✅ Loaded team '{team_name}' with {len(players)} players")
                    
            except Exception as e:
                print(f"⚠️  Error processing row {idx}: {e}")
                continue
        
        print(f"✅ Successfully loaded {len(teams_config)} teams from {CONFIG['TEAMS_FILE']}")
        return teams_config
        
    except Exception as e:
        print(f"❌ Error loading teams config: {e}")
        import traceback
        traceback.print_exc()
        return {}


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


def parse_battle_to_match(battle, teams_config):
    """Convert API battle data to match format for Excel"""
    try:
        # Check if 'battle' key exists
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
        
        # Match players to teams
        team1_info = match_team(teams[0], teams_config)
        team2_info = match_team(teams[1], teams_config)
        
        if not team1_info or not team2_info:
            return None
        
        # Determine winner based on which team the source player is on
        result = battle['battle'].get('result')
        source_player_tag = battle.get('_source_player_tag')
        
        if not result or not source_player_tag:
            return None
        
        # Check which team the source player is on
        source_on_team1 = any(p['tag'] == source_player_tag for p in teams[0])
        source_on_team2 = any(p['tag'] == source_player_tag for p in teams[1])
        
        # Determine winner based on result and source player's team
        if result == 'victory':
            if source_on_team1:
                winner = team1_info['name']
            elif source_on_team2:
                winner = team2_info['name']
            else:
                print(f"⚠️  Source player {source_player_tag} not found in either team")
                return None
        elif result == 'defeat':
            if source_on_team1:
                winner = team2_info['name']
            elif source_on_team2:
                winner = team1_info['name']
            else:
                print(f"⚠️  Source player {source_player_tag} not found in either team")
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
        
        # Build match data
        match_data = {
            'battle_time': battle_time,
            'team1_name': team1_info['name'],
            'team1_region': team1_info['region'],
            'team2_name': team2_info['name'],
            'team2_region': team2_info['region'],
            'winner': winner,
            'mode': mode,
            'map': map_name,
            'star_player_tag': star_player_tag
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
        
        # Mark as processed
        processed_battle_times.add(battle_time)
        
        return match_data
        
    except Exception as e:
        print(f"❌ Error parsing battle: {e}")
        import traceback
        traceback.print_exc()
        return None


def match_team(player_list, teams_config):
    """Identify which team these players belong to"""
    player_tags = {p['tag'] for p in player_list}
    
    for team_name, team_info in teams_config.items():
        team_tags = {p['tag'] for p in team_info['players']}
        
        # If at least 2 players match, consider it the same team
        matching_players = player_tags & team_tags
        if len(matching_players) >= 2:
            return {
                'name': team_name,
                'region': team_info['region']
            }
    
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


async def fetch_player_battles(session, player_tag, headers):
    """Fetch battle log for a single player"""
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
            else:
                print(f"⚠️  API error for {player_tag}: {response.status}")
                return []
    except Exception as e:
        print(f"❌ Error fetching {player_tag}: {e}")
        return []


async def fetch_new_matches():
    """Fetch new matches from Brawl Stars API"""
    if not CONFIG['BRAWL_STARS_API_TOKEN']:
        print("❌ No Brawl Stars API token configured!")
        print("💡 Please set BRAWL_STARS_API_TOKEN in .env file or as environment variable")
        return
    
    # Load teams configuration
    teams_config = load_teams_config()
    if not teams_config:
        print("⚠️  No teams configured")
        return
    
    # Load existing battle times
    global processed_battle_times
    processed_battle_times = load_existing_matches()
    print(f"📋 Loaded {len(processed_battle_times)} existing matches")
    
    headers = {
        'Authorization': f"Bearer {CONFIG['BRAWL_STARS_API_TOKEN']}"
    }
    
    new_matches = []
    all_battles = []
    
    # Fetch battles for all players
    async with aiohttp.ClientSession() as session:
        tasks = []
        for team_name, team_info in teams_config.items():
            for player in team_info['players']:
                tasks.append(fetch_player_battles(session, player['tag'], headers))
        
        results = await asyncio.gather(*tasks)
        
        # Flatten all battles
        for battles in results:
            all_battles.extend(battles)
    
    # Process battles and convert to matches
    friendly_count = 0
    for battle in all_battles:
        if 'battle' in battle and battle['battle'].get('type') == 'friendly':
            friendly_count += 1
        match_data = parse_battle_to_match(battle, teams_config)
        if match_data:
            new_matches.append(match_data)
    
    # Remove duplicates based on battle_time
    seen_times = set()
    unique_matches = []
    for match in new_matches:
        if match['battle_time'] not in seen_times:
            seen_times.add(match['battle_time'])
            unique_matches.append(match)
    
    print(f"✅ Found {len(unique_matches)} new friendly matches")
    
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
    print("🚀 Brawl Stars API Fetcher Started")
    print(f"📊 Matches file: {CONFIG['MATCHES_FILE']}")
    print(f"👥 Teams file: {CONFIG['TEAMS_FILE']}")
    print(f"⏰ Check interval: {CONFIG['API_CHECK_INTERVAL_MINUTES']} minutes")
    print("=" * 60)
    
    while True:
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n🔍 Checking for new matches... ({timestamp})")
            await fetch_new_matches()
            print("=" * 60)
            await asyncio.sleep(CONFIG['API_CHECK_INTERVAL_MINUTES'] * 60)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"❌ Error in main loop: {e}")
            await asyncio.sleep(60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Shutting down...")