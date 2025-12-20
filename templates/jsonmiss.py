import pandas as pd
import json

def create_team_rosters():
    """Generate team_rosters.json from teams.xlsx"""
    print("Creating team_rosters.json from teams.xlsx...")
    
    try:
        teams_df = pd.read_excel('teams.xlsx')
        rosters = {}
        
        for _, row in teams_df.iterrows():
            team_name = row['Team Name']
            roster = []
            
            for i in range(1, 4):
                tag_col = f'Player {i} ID'
                if tag_col in teams_df.columns and pd.notna(row.get(tag_col)):
                    # Normalize: uppercase, strip, replace 0 with O
                    tag = str(row[tag_col]).strip().upper().replace('0', 'O')
                    if not tag.startswith('#'):
                        tag = '#' + tag
                    roster.append(tag)
            
            if roster:
                rosters[team_name] = roster
        
        # Save to JSON
        with open('team_rosters.json', 'w') as f:
            json.dump(rosters, f, indent=2)
        
        print(f"✓ Created team_rosters.json with {len(rosters)} teams")
        return True
        
    except Exception as e:
        print(f"❌ Error creating team_rosters.json: {e}")
        # Create empty file as fallback
        with open('team_rosters.json', 'w') as f:
            json.dump({}, f)
        print("✓ Created empty team_rosters.json")
        return False

def create_tracked_players():
    """Generate tracked_players.json from players_off.xlsx"""
    print("\nCreating tracked_players.json from players_off.xlsx...")
    
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
        
        # Save to JSON
        with open('tracked_players.json', 'w') as f:
            json.dump(tracked, f, indent=2)
        
        print(f"✓ Created tracked_players.json with {len(tracked)} players")
        return True
        
    except Exception as e:
        print(f"❌ Error creating tracked_players.json: {e}")
        # Create empty file as fallback
        with open('tracked_players.json', 'w') as f:
            json.dump({}, f)
        print("✓ Created empty tracked_players.json")
        return False

def main():
    print("="*80)
    print("CREATING MISSING JSON FILES")
    print("="*80 + "\n")
    
    # Create both files
    rosters_ok = create_team_rosters()
    tracked_ok = create_tracked_players()
    
    print("\n" + "="*80)
    if rosters_ok and tracked_ok:
        print("✓ All files created successfully!")
    else:
        print("⚠️  Some files created with empty data")
    print("="*80 + "\n")
    
    print("Now restart your Flask server and the data should load correctly.")

if __name__ == "__main__":
    main()