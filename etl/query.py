import sqlite3
from pathlib import Path

DB_PATH = Path("data/pokemon.db")

def list_pokemon_with_types(limit=10):
    # 1) open the database file
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        # 2) SQL:
        #   - join parent (pokemon) to child (pokemon_type)
        #   - group rows per pokemon and glue types into one string
        sql = """
        SELECT
            p.id,
            p.name,
            GROUP_CONCAT(pt.type, ', ') AS types
        FROM pokemon AS p
        JOIN pokemon_type AS pt
            ON pt.pokemon_id = p.id
        GROUP BY p.id, p.name
        ORDER BY p.id
        LIMIT ?
        """
        cur.execute(sql, (limit,))   # parameterized LIMIT to avoid SQL injection habits
        rows = cur.fetchall()
        # 3) pretty print
        for r in rows:
            print(f"{r[0]:>3}  {r[1]:<15}  {r[2]}")
    finally:
        conn.close()

if __name__ == "__main__":
    list_pokemon_with_types(limit=10)
