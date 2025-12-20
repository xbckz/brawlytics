"""
Star Player Counter
Counts how many times each player was star player from matches.xlsx
"""

import pandas as pd
from collections import defaultdict

def count_star_players():
    # Load matches
    df = pd.read_excel('matches.xlsx')
    
    # Load team rosters to filter only roster players
    teams_df = pd.read_excel('teams.xlsx')
    
    # Build valid roster
    valid_rosters = {}
    for _, row in teams_df.iterrows():
        team_name = row['Team Name']
        if team_name not in valid_rosters:
            valid_rosters[team_name] = set()
        
        for i in range(1, 4):
            tag_col = f'Player {i} ID'
            if tag_col in teams_df.columns and pd.notna(row.get(tag_col)):
                tag = str(row[tag_col]).strip().upper().replace('0', 'O')
                valid_rosters[team_name].add(tag)
    
    # Count star players
    star_counts = defaultdict(lambda: {'name': '', 'team': '', 'count': 0})
    
    for _, match in df.iterrows():
        star_tag = str(match.get('star_player_tag', '')).strip().upper().replace('0', 'O')
        
        if not star_tag or star_tag == 'NAN':
            continue
        
        # Find which player this is
        for team_prefix in ['team1', 'team2']:
            team_name = match[f'{team_prefix}_name']
            
            for i in range(1, 4):
                player_tag = str(match[f'{team_prefix}_player{i}_tag']).strip().upper().replace('0', 'O')
                player_name = str(match[f'{team_prefix}_player{i}'])
                
                if player_tag == star_tag:
                    # Check if player is in roster
                    if team_name in valid_rosters and player_tag in valid_rosters[team_name]:
                        star_counts[player_tag]['name'] = player_name
                        star_counts[player_tag]['team'] = team_name
                        star_counts[player_tag]['count'] += 1
    
    # Sort by count and print
    sorted_players = sorted(star_counts.items(), key=lambda x: x[1]['count'], reverse=True)
    
    print("\n STAR PLAYER LEADERBOARD \n")
    print(f"{'Rank':<6}{'Player':<25}{'Team':<15}{'Stars':<10}")
    print("-" * 56)
    
    for rank, (tag, data) in enumerate(sorted_players, 1):
        print(f"{rank:<6}{data['name']:<25}{data['team']:<15}{data['count']:<10}")
    
    print(f"\nTotal star player awards: {sum(p['count'] for p in star_counts.values())}")

if __name__ == "__main__":
    count_star_players()

