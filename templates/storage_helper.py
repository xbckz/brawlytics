import json
import os

def save_tokens(data):
    os.makedirs('data', exist_ok=True)
    with open('data/tokens.json', 'w') as f:
        json.dump(data, f, indent=2)

def load_tokens():
    if os.path.exists('data/tokens.json'):
        with open('data/tokens.json', 'r') as f:
            return json.load(f)
    return {}

def save_authorized_users(data):
    os.makedirs('data', exist_ok=True)
    with open('data/authorized_users.json', 'w') as f:
        json.dump(data, f, indent=2)

def load_authorized_users():
    if os.path.exists('data/authorized_users.json'):
        with open('data/authorized_users.json', 'r') as f:
            return json.load(f)
    return {}

def save_matches(data):
    pass