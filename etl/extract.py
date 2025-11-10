import requests
import json
from pathlib import Path

def extract_pokemon(limit=50, offset=0):
    url = f"https://pokeapi.co/api/v2/pokemon?={limit}&offset={offset}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()["results"]
    Path("data").mkdir(exist_ok=True)
    with open("data/pokemon_raw.json", "w") as f:
        json.dump(data, f, indent=2)
    return data