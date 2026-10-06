import os
import time
from datetime import datetime, timezone

import requests

from src.ingest.common import write_json

# name -> (latitude, longitude). Add or change cities as you like.
LOCATIONS = {
    "new_york": (40.7128, -74.0060),
    "newark": (40.7357, -74.1724),
    "miami": (25.7617, -80.1918),
    "chicago": (41.8781, -87.6298),
}


def _headers() -> dict:
    user_agent = os.getenv("WEATHER_USER_AGENT")
    if not user_agent:
        raise RuntimeError("Set WEATHER_USER_AGENT in your .env file (weather.gov requires it).")
    return {"User-Agent": user_agent, "Accept": "application/geo+json"}


def get_json(url: str, retries: int = 3) -> dict:
    """GET with simple retries (weather.gov occasionally returns 5xx errors)."""
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=_headers(), timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as error:
            print(f"Attempt {attempt}/{retries} failed for {url}: {error}")
            if attempt == retries:
                raise
            time.sleep(2 * attempt)


def run() -> int:
    saved = 0
    for name, (lat, lon) in LOCATIONS.items():
        # Step A: which forecast grid does this point belong to?
        points = get_json(f"https://api.weather.gov/points/{lat:.4f},{lon:.4f}")
        forecast_url = points["properties"]["forecast"]

        # Step B: fetch the forecast from the URL we just received
        forecast = get_json(forecast_url)

        payload = {
            "location": name,
            "latitude": lat,
            "longitude": lon,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "forecast": forecast,
        }
        write_json("weather", f"{name}_forecast.json", payload)
        saved += 1
        print(f"Saved {name}")
        time.sleep(1)

    print(f"Done. {saved} locations saved.")
    return saved


if __name__ == "__main__":
    run()