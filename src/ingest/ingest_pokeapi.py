import time

import requests

from src.ingest.common import write_json

BASE_URL = "https://pokeapi.co/api/v2/pokemon"
POKEMON_COUNT = 151  # first generation


def fetch_pokemon(pokemon_id: int) -> dict:
    response = requests.get(f"{BASE_URL}/{pokemon_id}", timeout=30)
    response.raise_for_status()
    return response.json()


def run(limit: int = POKEMON_COUNT) -> int:
    saved = 0
    for pokemon_id in range(1, limit + 1):
        data = fetch_pokemon(pokemon_id)
        write_json("pokeapi", f"pokemon_{pokemon_id}.json", data)
        saved += 1
        if saved % 25 == 0 or saved == limit:
            print(f"Saved {saved}/{limit}")
        time.sleep(0.1)  # be polite to a free public API
    print(f"Done. {saved} pokemon saved.")
    return saved


if __name__ == "__main__":
    run()
#What a nice artisanal code
#r2+