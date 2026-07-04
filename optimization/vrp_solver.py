# optimization/vrp_solver.py
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
import math
import json

def haversine_meters(lat1, lon1, lat2, lon2):
    """Compute great-circle distance in meters between two coordinates."""
    R = 6_371_000  # Earth radius in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * R * math.asin(math.sqrt(a))

def build_distance_matrix(locations):
    """
    locations: list of (lat, lon) tuples
    Returns NxN matrix of integer distances in meters
    """
    n = len(locations)
    matrix = [[0] * n for _ in range(n)]
    for i in range(n):
        for j in range(n):
            if i != j:
                matrix[i][j] = int(haversine_meters(
                    locations[i][0], locations[i][1],
                    locations[j][0], locations[j][1]
                ))
    return matrix

def solve_vrp(orders, drivers, max_route_distance_m=50_000):
    """
    orders:  list of dicts with lat, lon, order_id
    drivers: list of dicts with lat, lon, driver_id, capacity_kg
    Returns: list of routes (one per driver)
    """
    # ── Input validation ──
    if not orders:
        raise ValueError("No orders provided to VRP solver")
    if not drivers:
        raise ValueError("No drivers provided to VRP solver")
    if len(drivers) > len(orders):
        logger.warning(
            "More drivers (%d) than orders (%d) — some drivers will be idle",
            len(drivers), len(orders)
        )

    for o in orders:
        if not all(k in o for k in ("lat", "lon", "order_id")):
            raise ValueError(f"Order missing required fields: {o}")
    # Node 0 is the depot (city center / warehouse)
    depot_lat, depot_lon = 30.0444, 31.2357  # Cairo center
    depot = {"lat": depot_lat, "lon": depot_lon, "order_id": "DEPOT"}

    # All locations: depot first, then orders
    all_nodes = [depot] + orders
    locations  = [(n["lat"], n["lon"]) for n in all_nodes]
    n_nodes    = len(locations)
    n_vehicles = len(drivers)

    distance_matrix = build_distance_matrix(locations)

    # OR-Tools setup
    manager = pywrapcp.RoutingIndexManager(
        n_nodes,       # number of locations
        n_vehicles,    # number of drivers
        0              # depot index (always 0)
    )
    routing = pywrapcp.RoutingModel(manager)

    # Distance callback
    def distance_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node   = manager.IndexToNode(to_index)
        return distance_matrix[from_node][to_node]

    transit_callback_index = routing.RegisterTransitCallback(distance_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

    # Add distance constraint per vehicle
    dimension_name = "Distance"
    routing.AddDimension(
        transit_callback_index,
        0,                       # no slack
        max_route_distance_m,    # max distance per vehicle
        True,                    # start cumul at zero
        dimension_name,
    )
    distance_dimension = routing.GetDimensionOrDie(dimension_name)
    distance_dimension.SetGlobalSpanCostCoefficient(100)

    # Search parameters
    search_params = pywrapcp.DefaultRoutingSearchParameters()
    search_params.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    )
    search_params.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    )
    search_params.time_limit.seconds = 30  # max optimization time

    solution = routing.SolveWithParameters(search_params)

    if not solution:
        raise RuntimeError("No VRP solution found — check distance constraints")

    # Extract routes
    routes = []
    total_distance = 0

    for vehicle_id in range(n_vehicles):
        driver  = drivers[vehicle_id]
        index   = routing.Start(vehicle_id)
        route   = {"driver_id": driver["driver_id"], "stops": [], "total_distance_m": 0}

        while not routing.IsEnd(index):
            node_index = manager.IndexToNode(index)
            if node_index != 0:  # skip depot
                route["stops"].append({
                    "order_id": all_nodes[node_index]["order_id"],
                    "lat":      all_nodes[node_index]["lat"],
                    "lon":      all_nodes[node_index]["lon"],
                    "sequence": len(route["stops"]) + 1,
                })
            previous_index = index
            index = solution.Value(routing.NextVar(index))
            route["total_distance_m"] += routing.GetArcCostForVehicle(
                previous_index, index, vehicle_id
            )

        if route["stops"]:  # only include drivers with deliveries
            route["total_distance_km"] = round(route["total_distance_m"] / 1000, 2)
            routes.append(route)
            total_distance += route["total_distance_m"]

    print(f"Solved: {len(routes)} routes, {total_distance/1000:.1f} km total")
    return routes