import json
import pandas as pd
from pathlib import Path

def load_curated_json():
    """Read the curated Pokémon JSON and turn it into a DataFrame."""
    path = Path("data/pokemon_curated.json")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    print("✅ Loaded", len(df), "rows")
    print(df.head())  # show the first few rows
    return df

if __name__ == "__main__":
    load_curated_json()
