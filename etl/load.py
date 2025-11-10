from pathlib import Path
import json
import sqlite3
import pandas as pd

CURATED_PATH = Path("data/pokemon_curated.json")
DB_PATH = Path("data/pokemon.db")

# ---------- tiny helper: read curated JSON into a DataFrame ----------
def read_curated_df() -> pd.DataFrame:
    with open(CURATED_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)                 # list[dict]
    return pd.DataFrame(data)               # table-like view

# ---------- connect to SQLite with foreign keys ON ----------
def get_conn():
    DB_PATH.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)         # creates file if not exists
    conn.execute("PRAGMA foreign_keys = ON")# enforce FK constraints
    return conn

# ---------- create normalized tables if they don't exist ----------
def init_schema(conn: sqlite3.Connection):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS pokemon (
            id INTEGER PRIMARY KEY,                 -- stable PK from API
            name TEXT NOT NULL,
            height INTEGER,
            weight INTEGER,
            base_experience INTEGER
        );

        CREATE TABLE IF NOT EXISTS pokemon_type (
            pokemon_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            -- 1 row per (pokemon_id, type)
            UNIQUE (pokemon_id, type),
            FOREIGN KEY (pokemon_id) REFERENCES pokemon(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS pokemon_ability (
            pokemon_id INTEGER NOT NULL,
            ability TEXT NOT NULL,
            -- 1 row per (pokemon_id, ability)
            UNIQUE (pokemon_id, ability),
            FOREIGN KEY (pokemon_id) REFERENCES pokemon(id) ON DELETE CASCADE
        );
        """
    )

# ---------- UPSERT rows into pokemon (idempotent) ----------
def upsert_pokemon(conn: sqlite3.Connection, df: pd.DataFrame):
    rows = [
        (int(r["id"]), r["name"], r.get("height"), r.get("weight"), r.get("base_experience"))
        for _, r in df.iterrows()
    ]
    # UPSERT: if id exists -> update columns; else insert
    conn.executemany(
        """
        INSERT INTO pokemon (id, name, height, weight, base_experience)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name = excluded.name,
            height = excluded.height,
            weight = excluded.weight,
            base_experience = excluded.base_experience
        """,
        rows,
    )

# ---------- Insert child rows; ignore duplicates (idempotent) ----------
def upsert_types(conn: sqlite3.Connection, df: pd.DataFrame):
    pairs = []
    for _, r in df.iterrows():
        pid = int(r["id"])
        for t in (r.get("types") or []):
            pairs.append((pid, t))
    conn.executemany(
        "INSERT OR IGNORE INTO pokemon_type (pokemon_id, type) VALUES (?, ?)",
        pairs,
    )

def upsert_abilities(conn: sqlite3.Connection, df: pd.DataFrame):
    pairs = []
    for _, r in df.iterrows():
        pid = int(r["id"])
        for a in (r.get("abilities") or []):
            pairs.append((pid, a))
    conn.executemany(
        "INSERT OR IGNORE INTO pokemon_ability (pokemon_id, ability) VALUES (?, ?)",
        pairs,
    )

# ---------- Orchestrate the load in a single transaction ----------
def load_to_sqlite():
    df = read_curated_df()                 # 1) read the curated list
    conn = get_conn()
    try:
        init_schema(conn)                  # 2) ensure tables exist
        conn.execute("BEGIN")              # 3) start transaction (all-or-nothing)
        upsert_pokemon(conn, df)           # 4) upsert parent
        upsert_types(conn, df)             # 5) insert child rows (types)
        upsert_abilities(conn, df)         # 6) insert child rows (abilities)
        conn.commit()                      # 7) make it permanent
    except Exception:
        conn.rollback()                    # any error -> revert
        raise
    finally:
        # quick sanity counts so you can see results each run
        cur = conn.cursor()
        p = cur.execute("SELECT COUNT(*) FROM pokemon").fetchone()[0]
        t = cur.execute("SELECT COUNT(*) FROM pokemon_type").fetchone()[0]
        a = cur.execute("SELECT COUNT(*) FROM pokemon_ability").fetchone()[0]
        print(f"✅ load complete: pokemon={p}, types={t}, abilities={a}")
        conn.close()

if __name__ == "__main__":
    load_to_sqlite()
