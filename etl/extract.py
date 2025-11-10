# ========== Imports ==========
import json
import time
from pathlib import Path
from typing import Tuple, Dict, Any, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ========== Constants ==========
BASE = "https://pokeapi.co/api/v2/pokemon"
CONNECT_TIMEOUT = 5     # seconds to establish TCP (fail fast on dead hosts)
READ_TIMEOUT = 20       # seconds waiting for server response body
SLEEP_BETWEEN = 0.15    # polite delay to avoid hammering the API
MAX_PER_PAGE = 50       # keep first runs small; scale after it's stable


# ========== Session with retries ==========
def make_session() -> requests.Session:
    """
    Create a requests.Session with retry + backoff for transient errors.
    """
    s = requests.Session()  # reuse TCP connections (faster, fewer handshakes)

    # Retry on 429/5xx, exponential backoff (0.5, 1, 2, ...)
    retry = Retry(
        total=5,                     # up to 5 attempts
        backoff_factor=0.5,          # sleep growth between retries
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],     # only retry idempotent GETs
        raise_on_status=False,
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    # A friendly User-Agent avoids some API blocks and helps logs on their side
    s.headers.update({"User-Agent": "pokemon-pipeline/1.0 (+educational)"})
    return s


# ========== HTTP helpers ==========
def fetch_index_page(session: requests.Session, limit=MAX_PER_PAGE, offset=0) -> List[Dict[str, Any]]:
    """
    Get one index page (names + detail URLs).
    """
    url = f"{BASE}?limit={limit}&offset={offset}"
    # Separate timeouts: (connect, read)
    resp = session.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    resp.raise_for_status()
    return resp.json()["results"]


def fetch_pokemon_detail(session: requests.Session, detail_url: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Download one Pokémon's full detail JSON and keep curated fields.
    """
    r = session.get(detail_url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    r.raise_for_status()
    raw = r.json()
    record = {
        "id": raw["id"],
        "name": raw["name"],
        "height": raw.get("height"),
        "weight": raw.get("weight"),
        "base_experience": raw.get("base_experience"),
        "types": [t["type"]["name"] for t in raw.get("types", [])],
        "abilities": [a["ability"]["name"] for a in raw.get("abilities", [])],
        "sprites_front_default": raw.get("sprites", {}).get("front_default"),
    }
    return record, raw


# ========== Batch extraction ==========
def extract_pokemon_batch(limit=MAX_PER_PAGE, offset=0, sleep_sec=SLEEP_BETWEEN) -> List[Dict[str, Any]]:
    """
    Pull one page of index, then fetch details for each Pokémon with resilience.
    Persists curated and raw JSON to /data.
    """
    Path("data").mkdir(exist_ok=True)
    session = make_session()

    print(f"[INFO] Fetching index page: limit={limit} offset={offset}")
    index = fetch_index_page(session, limit=limit, offset=offset)

    curated, raw_dump = [], []

    for i, item in enumerate(index, start=1):
        detail_url = item["url"]
        name = item.get("name", f"idx_{i}")
        try:
            # progress line — you’ll see where it is if it slows
            print(f"[{i}/{len(index)}] GET {name} -> {detail_url}")
            rec, raw = fetch_pokemon_detail(session, detail_url)
            curated.append(rec)
            raw_dump.append(raw)
        except requests.exceptions.Timeout:
            # connect or read timed out — skip but continue the batch
            print(f"[WARN] Timeout fetching {name}. Skipping.")
        except requests.exceptions.RequestException as e:
            # any other HTTP/connection error — log + skip
            print(f"[WARN] Error fetching {name}: {e}. Skipping.")
        finally:
            time.sleep(sleep_sec)

    # Persist artifacts
    with open("data/pokemon_curated.json", "w", encoding="utf-8") as f:
        json.dump(curated, f, indent=2, ensure_ascii=False)

    with open("data/pokemon_raw.json", "w", encoding="utf-8") as f:
        json.dump(raw_dump, f, indent=2, ensure_ascii=False)

    print(f"[INFO] Wrote curated={len(curated)} raw={len(raw_dump)}")
    return curated


# ========== CLI entry ==========
if __name__ == "__main__":
    records = extract_pokemon_batch(limit=MAX_PER_PAGE, offset=0)
    print(f"[DONE] Fetched {len(records)} Pokémon.")
