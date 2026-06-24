# optimization/vrp_constrained.py
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

def solve_vrp_with_constraints(orders, drivers, depot_lat=30.0444, depot_lon=31.2357):
    depot = {"lat": depot_lat, "lon": depot_lon, "weight_kg": 0,
            "window_start_min": 0, "window_end_min": 24*60}

    all_nodes  = [depot] + orders
    locations  = [(n["lat"], n["lon"]) for n in all_nodes]
    demands    = [0] + [int(o.get("weight_kg", 5) * 10) for o in orders]  # decagrams
    capacities = [int(d["capacity_kg"] * 10) for d in drivers]

    # Time windows (minutes from midnight)
    def parse_time(t):
        h, m = map(int, t.split(":"))
        return h * 60 + m

    time_windows = [(0, 24*60)]  # depot: all day
    for o in orders:
        start = parse_time(o.get("delivery_window_start", "09:00"))
        end   = parse_time(o.get("delivery_window_end",   "21:00"))
        time_windows.append((start, end))

    manager = pywrapcp.RoutingIndexManager(len(locations), len(drivers), 0)
    routing = pywrapcp.RoutingModel(manager)

    # Distance/time callback (assume 40 km/h avg in Cairo)
    distance_matrix = build_distance_matrix(locations)
    avg_speed_m_per_min = (40 * 1000) / 60

    def time_callback(from_idx, to_idx):
        from_node = manager.IndexToNode(from_idx)
        to_node   = manager.IndexToNode(to_idx)
        travel_m  = distance_matrix[from_node][to_node]
        service_min = 5  # 5 minutes per delivery stop
        return int(travel_m / avg_speed_m_per_min) + service_min

    transit_idx = routing.RegisterTransitCallback(time_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_idx)

    # Capacity constraint
    def demand_callback(from_idx):
        return demands[manager.IndexToNode(from_idx)]

    demand_idx = routing.RegisterUnaryTransitCallback(demand_callback)
    routing.AddDimensionWithVehicleCapacity(
        demand_idx, 0, capacities, True, "Capacity"
    )

    # Time window constraint
    routing.AddDimension(transit_idx, 30, 24*60, False, "Time")
    time_dim = routing.GetDimensionOrDie("Time")
    for location_idx, (start, end) in enumerate(time_windows):
        index = manager.NodeToIndex(location_idx)
        time_dim.CumulVar(index).SetRange(start, end)

    # Penalize dropped orders (soft constraint — solver can skip if impossible)
    penalty = 100_000
    for node in range(1, len(all_nodes)):
        routing.AddDisjunction([manager.NodeToIndex(node)], penalty)

    search_params = pywrapcp.DefaultRoutingSearchParameters()
    search_params.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    )
    search_params.time_limit.seconds = 60

    solution = routing.SolveWithParameters(search_params)

    if not solution:
        # Check if the penalty for dropping orders was too high, or time windows too strict
        raise RuntimeError("No VRP solution found — constraints are too tight.")

    routes = []
    total_distance_m = 0

    for vehicle_id in range(len(drivers)):
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
            
            # Get real distance from matrix since ArcCost is now calculating time
            prev_node = manager.IndexToNode(previous_index)
            curr_node = manager.IndexToNode(index)
            route["total_distance_m"] += distance_matrix[prev_node][curr_node]

        if route["stops"]:  # only include drivers with deliveries
            route["total_distance_km"] = round(route["total_distance_m"] / 1000, 2)
            routes.append(route)
            total_distance_m += route["total_distance_m"]

    print(f"Solved constrained VRP: {len(routes)} routes, {total_distance_m/1000:.1f} km total")
    return routes