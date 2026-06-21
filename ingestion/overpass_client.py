# ingestion/overpass_client.py
import requests
import time
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Cairo bounding box: south, west, north, east
CAIRO_BBOX = (29.9, 31.1, 30.2, 31.5)

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

ROAD_QUERY = """
[out:json][timeout:90][bbox:{south},{west},{north},{east}];
(
  way["highway"~"motorway|trunk|primary|secondary|residential|service"];
);
out body geom;
""".strip()

def fetch_roads(bbox=CAIRO_BBOX, max_retries=3, backoff_seconds=10):
    south, west, north, east = bbox
    query = ROAD_QUERY.format(south=south, west=west, north=north, east=east)

    for attempt in range(1, max_retries + 1):
        try:
                        logger.info(f"Fetching roads, attempt {attempt}")
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
                        logger.info(f"Fetched {len(data.get('elements', []))} road elements")
                        return data
        except requests.exceptions.HTTPError as e:
            if response.status_code != 429:
                raise
            wait = backoff_seconds * (2 ** attempt)
            logger.warning(f"Rate limited. Waiting {wait}s before retry.")
            time.sleep(wait)
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout on attempt {attempt}. Retrying...")
            time.sleep(backoff_seconds)

    raise RuntimeError("Max retries exceeded fetching OSM data")

# terminal test
if __name__ == "__main__":
    import json
    import logging

    logging.basicConfig(level=logging.INFO)

    roads = fetch_roads()

    print(f"Total roads: {len(roads['elements'])}")

    with open("roads_test.json", "w") as f:
        json.dump(roads, f, indent=2)

    print("Done")