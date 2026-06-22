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
            logger.info(f"Fetching roads, attempt {attempt}/{max_retries}")
            response = requests.post(
                OVERPASS_URL,
                data={"data": query},
                headers={
                    "User-Agent": "Smart-Delivery-Route-Optimization-Platform/1.0"
                },
                timeout=120, # Client-side timeout
            )
            response.raise_for_status()
            data = response.json()
            logger.info(f"Fetched {len(data.get('elements', []))} road elements")
            return data

        except requests.exceptions.HTTPError as e:
            # Retry on Rate Limits (429) OR Server Errors (5xx like 504, 502, 500)
            if response.status_code == 429 or response.status_code >= 500:
                wait = backoff_seconds * (2 ** attempt)
                logger.warning(f"HTTP {response.status_code} encountered. Waiting {wait}s before retry.")
                time.sleep(wait)
            else:
                # If it's a 400 Bad Request, retrying won't fix your query. Fail immediately.
                logger.error(f"Fatal HTTP Error {response.status_code}: {response.text}")
                raise

        except requests.exceptions.Timeout:
            # Handles client-side timeouts
            wait = backoff_seconds * (2 ** attempt)
            logger.warning(f"Client timeout on attempt {attempt}. Waiting {wait}s before retry.")
            time.sleep(wait)

        except requests.exceptions.RequestException as e:
            # Handles dropped connections, DNS failures, etc.
            logger.warning(f"Connection error on attempt {attempt}: {e}")
            time.sleep(backoff_seconds)

    # If the loop finishes without returning, we are out of retries
    raise Exception(f"Failed to fetch roads after {max_retries} attempts.")

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