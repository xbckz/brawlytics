import os
import json

def debug_file_paths():
    """Check all required files for the Rust processor"""
    print("\n" + "="*80)
    print("FILE EXISTENCE CHECK")
    print("="*80)
    
    # Get current working directory
    cwd = os.getcwd()
    print(f"Current working directory: {cwd}\n")
    
    # Files to check
    files_to_check = [
        "matches.xlsx",
        "matches_off.xlsx",
        "data/user_settings.json",
        "team_rosters.json",
        "tracked_players.json",
        "players_off.xlsx",
        "teams.xlsx"
    ]
    
    print("File Status:")
    print("-" * 80)
    for filepath in files_to_check:
        exists = os.path.exists(filepath)
        size = os.path.getsize(filepath) if exists else 0
        status = " EXISTS" if exists else "✗ MISSING"
        print(f"{status:12} | {filepath:30} | {size:>10} bytes")
    
    print("\n" + "="*80)
    print("USER SETTINGS CONTENT")
    print("="*80)
    
    # Check user_settings.json content
    settings_file = "data/user_settings.json"
    if os.path.exists(settings_file):
        try:
            with open(settings_file, 'r') as f:
                settings = json.load(f)
            print(f"Settings loaded successfully:")
            print(json.dumps(settings, indent=2))
        except Exception as e:
            print(f"Error loading settings: {e}")
    else:
        print(f"{settings_file} does not exist")
    
    print("\n" + "="*80)
    print("DIRECTORY CONTENTS")
    print("="*80)
    
    # List all files in current directory
    print("\nFiles in current directory:")
    for item in sorted(os.listdir('.')):
        if os.path.isfile(item):
            size = os.path.getsize(item)
            print(f"  {item:40} {size:>10} bytes")
    
    # List files in data directory if it exists
    if os.path.exists('data'):
        print("\nFiles in 'data' directory:")
        for item in sorted(os.listdir('data')):
            filepath = os.path.join('data', item)
            if os.path.isfile(filepath):
                size = os.path.getsize(filepath)
                print(f"  {item:40} {size:>10} bytes")
    
    print("\n" + "="*80)

if __name__ == "__main__":
    debug_file_paths()