from datetime import datetimes
# -------------------------
# Required fields
# -------------------------
ORDER_REQUIRED_FIELDS = {
    "order_id",
    "lat",
    "lon",
    "district",
    "priority",
    "weight_kg",
    "created_at",
    "delivery_window_start",
    "delivery_window_end",
}

DRIVER_REQUIRED_FIELDS = {
    "driver_id",
    "lat",
    "lon",
    "capacity_kg",
    "status",
    "district",
}

ROAD_REQUIRED_FIELDS = {
    "type",
    "id",
    "geometry",
}

# -------------------------
# Generic validation
# -------------------------

def validate_required_fields(record, required_fields):
    """
    Checks if one dictionary has all required fields
    """

    if missing := required_fields - record.keys():
        raise ValueError(
            f"Missing fields: {missing}"
        )

    return True

# -------------------------
# Orders validation
# -------------------------

def validate_orders(orders):
    """
    Validate simulated orders before Bronze upload
    """

    if not isinstance(orders, list):
        raise TypeError(
            "Orders must be a list"
        )


    for order in orders:

        validate_required_fields(
            order,
            ORDER_REQUIRED_FIELDS
        )

        if not isinstance(order["lat"], float):
            raise TypeError(
                "lat must be float"
            )

        if not isinstance(order["lon"], float):
            raise TypeError(
                "lon must be float"
            )

        if order["priority"] not in [
            "high",
            "medium",
            "low"
        ]:
            raise ValueError(
                "Invalid priority"
            )

    return True

# -------------------------
# Drivers validation
# -------------------------

def validate_drivers(drivers):

    if not isinstance(drivers, list):
        raise TypeError(
            "Drivers must be list"
        )

    for driver in drivers:

        validate_required_fields(
            driver,
            DRIVER_REQUIRED_FIELDS
        )

        if driver["status"] not in [
            "available",
            "busy",
            "offline"
        ]:
            raise ValueError(
                "Invalid driver status"
            )

    return True
# -------------------------
# OSM roads validation
# -------------------------

def validate_roads(osm_data):
    """
    Validate Overpass response
    """

    if not isinstance(osm_data, dict):
        raise TypeError(
            "OSM response must be dict"
        )


    if "elements" not in osm_data:
        raise ValueError(
            "Missing elements from OSM response"
        )

    roads = osm_data["elements"]

    if len(roads) == 0:
        raise ValueError(
            "No roads received"
        )

    for road in roads[:10]:

        validate_required_fields(
            road,
            ROAD_REQUIRED_FIELDS
        )
    return True

# -------------------------
# Test locally
# -------------------------

if __name__ == "__main__":

    from data_simulator import (
        simulate_orders,
        simulate_drivers
    )

    orders = simulate_orders(10)
    drivers = simulate_drivers(5)

    validate_orders(orders)
    validate_drivers(drivers)

    print(
        "Schema validation passed"
    )