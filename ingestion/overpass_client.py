# ingestion/overpass_client.py — fixed and improved
import requests
import time
import logging
from config import CAIRO_BBOX

logger = logging.getLogger(__name__)

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

ROAD_QUERY = """
[out:json][timeout:90][bbox:{south},{west},{north},{east}];
(
    way["highway"~"motorway|trunk|primary|secondary|residential|service"];
);
out body geom;
""".strip()


def fetch_roads(
    bbox: tuple = CAIRO_BBOX,
    max_retries: int = 3,
    backoff_seconds: float = 10,
) -> dict:
    """Fetch road network from Overpass API with exponential backoff."""
    south, west, north, east = bbox
    query = ROAD_QUERY.format(south=south, west=west, north=north, east=east)

    for attempt in range(1, max_retries + 1):
        try:
            logger.info("Fetching roads, attempt %d/%d", attempt, max_retries)
            response = requests.post(
                OVERPASS_URL,
                data={"data": query},
                headers={
                    "User-Agent": "Smart-Delivery-Route-Optimization-Platform/1.0"
                },
                timeout=120,
            )
            response.raise_for_status()
            data = response.json()
            n_elements = len(data.get("elements", []))
            logger.info("Fetched %d road elements", n_elements)
            return data

        except requests.exceptions.HTTPError as e:
            if response.status_code != 429:
                raise
            wait = backoff_seconds * (2 ** attempt)
            logger.warning("Rate limited (429). Waiting %.0fs before retry.", wait)
            time.sleep(wait)

        except requests.exceptions.Timeout:
            logger.warning("Timeout on attempt %d/%d", attempt, max_retries)
            if attempt == max_retries:
                raise
            time.sleep(backoff_seconds)

        except requests.exceptions.ConnectionError as e:
            logger.warning("Connection error on attempt %d: %s", attempt, e)
            if attempt == max_retries:
                raise
            time.sleep(backoff_seconds * (2 ** attempt))

    raise RuntimeError(f"Max retries ({max_retries}) exceeded fetching OSM data")