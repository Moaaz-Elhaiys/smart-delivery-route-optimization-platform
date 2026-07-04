# ingestion/data_simulator.py
import random
import json
import uuid
from datetime import datetime, timedelta
from config import DEFAULT_ORDER_COUNT , DEFAULT_DRIVER_COUNT
import random
from typing import Optional

# Cairo districts bounding boxes (simplified)
DISTRICTS = {
    "Maadi":        {"lat": (29.95, 30.00), "lon": (31.22, 31.28)},
    "Zamalek":      {"lat": (30.05, 30.07), "lon": (31.21, 31.24)},
    "Heliopolis":   {"lat": (30.08, 30.12), "lon": (31.31, 31.36)},
    "Dokki":        {"lat": (30.03, 30.06), "lon": (31.20, 31.22)},
    "Nasr City":    {"lat": (30.05, 30.10), "lon": (31.30, 31.35)},
    "New Cairo":    {"lat": (30.00, 30.05), "lon": (31.40, 31.50)},
}

def random_point_in_district(district_name):
    d = DISTRICTS[district_name]
    lat = random.uniform(*d["lat"])
    lon = random.uniform(*d["lon"])
    return lat, lon

def simulate_orders(n: int = 500,date=None,seed: Optional[int] = None,) -> list[dict]:
    """Generate simulated delivery orders.
    Args:
        seed: If provided, results are reproducible (for testing).
    """
    if seed is not None:
        random.seed(seed)
    date = date or datetime.utcnow().date()
    orders = []
    for _ in range(n):
        district = random.choice(list(DISTRICTS.keys()))
        lat, lon = random_point_in_district(district)
        created_offset = random.randint(0, 8 * 3600)
        orders.append({
            "order_id": str(uuid.uuid4()),
            "lat": lat,
            "lon": lon,
            "district": district,
            "priority": random.choice(["high", "medium", "low"]),
            "weight_kg": round(random.uniform(0.5, 15.0), 2),
            "created_at": (
                datetime.combine(date, datetime.min.time()) +
                timedelta(seconds=created_offset)
            ).isoformat(),
            "delivery_window_start": "09:00",
            "delivery_window_end": "21:00",
        })
    return orders

def simulate_drivers(n=DEFAULT_DRIVER_COUNT):
    drivers = []
    for i in range(n):
        district = random.choice(list(DISTRICTS.keys()))
        lat, lon = random_point_in_district(district)
        drivers.append({
            "driver_id": f"DRV-{i+1:03d}",
            "lat": lat,
            "lon": lon,
            "capacity_kg": random.choice([20.0, 30.0, 50.0]),
            "status": "available",
            "district": district,
        })
    return drivers

# terminal test
if __name__ == "__main__":
    import json
    import logging

    logging.basicConfig(level=logging.INFO)

    orders = simulate_orders()
    drivers = simulate_drivers()

    print(f"Total orders: {len(orders)}")
    print(f"Total drivers: {len(drivers)}")

    with open("orders_test.json", "w") as f:
        json.dump(orders, f, indent=2)
    with open("drivers_test.json", "w") as f:
        json.dump(drivers, f, indent=2)

    print("Done")