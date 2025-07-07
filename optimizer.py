from flask import Flask, request, jsonify
import copy

app = Flask(__name__)

# Endpoint simple para mantener el servidor despierto
@app.route("/ping")
def ping():
    return "pong", 200

@app.route("/optimize", methods=["POST"])
def optimize():
    # Importar ortools sólo cuando se necesite
    from ortools.constraint_solver import pywrapcp, routing_enums_pb2

    raw_data = request.get_json()
    print("========== RAW DATA ==========")
    print(raw_data)
    print("================================")
    if raw_data is None:
        return jsonify(error="No se recibió JSON válido"), 400

    # Si es array (viene de Sidekiq), tomar el primer elemento
    if isinstance(raw_data, list):
        data = raw_data[0]
    else:
        data = raw_data

    print("========== DATA ==========")
    print(data)
    print("================================")

    try:
        locations = data["locations"]
        base_num_vehicles = data["max_vehicles"]
        vehicle_capacities_base = data["vehicle_capacities"]
        vehicle_consume_base = data.get("vehicle_consume", [1]*base_num_vehicles)
        distance_matrix = data["distance_matrix"]
        time_matrix = data.get("time_matrix")

        max_trips_per_vehicle = 2  # Puedes parametrizarlo si quieres

        if len(vehicle_capacities_base) != base_num_vehicles:
            return jsonify(error="La cantidad de capacidades no coincide con max_vehicles"), 400
        if len(vehicle_consume_base) != base_num_vehicles:
            return jsonify(error="La cantidad de consumos no coincide con max_vehicles"), 400

        # Duplicar vehículos ficticios por viaje
        vehicle_capacities = []
        vehicle_consume = []
        vehicle_mapping = {}

        for idx in range(base_num_vehicles):
            for trip in range(max_trips_per_vehicle):
                vehicle_capacities.append(vehicle_capacities_base[idx])
                vehicle_consume.append(vehicle_consume_base[idx])
                vehicle_mapping[len(vehicle_capacities)-1] = idx

        num_vehicles = len(vehicle_capacities)
        max_vehicle_capacity = max(vehicle_capacities)
        depot = 0

        all_products = set()
        for loc in locations:
            all_products.update(loc.get("demanda", {}).keys())
        all_products = sorted(all_products)

        extended_locations = []
        extended_demands = []
        split_mapping = {}

        extended_locations.append(locations[0])
        extended_demands.append(0)

        for idx, loc in enumerate(locations[1:], start=1):
            prod_quantities = {p: int(float(loc.get("demanda", {}).get(p, 0))) for p in all_products}
            total_demand = sum(prod_quantities.values())

            if total_demand <= max_vehicle_capacity:
                extended_locations.append(loc)
                extended_demands.append(total_demand)
                split_mapping[len(extended_locations)-1] = idx
            else:
                proportions = {p: (q / total_demand) if total_demand > 0 else 0 for p, q in prod_quantities.items()}
                remaining = total_demand
                remaining_per_product = prod_quantities.copy()
                while remaining > 0:
                    amount = min(remaining, max_vehicle_capacity)
                    split_loc = copy.deepcopy(loc)
                    split_demand = {}

                    if remaining - amount > 0:
                        for p in all_products:
                            q = int(round(amount * proportions[p]))
                            split_demand[p] = q
                            remaining_per_product[p] -= q
                    else:
                        split_demand = remaining_per_product.copy()

                    split_loc["demanda"] = {p: str(split_demand[p]) for p in all_products if split_demand[p] > 0}
                    extended_locations.append(split_loc)
                    extended_demands.append(sum(split_demand.values()))
                    split_mapping[len(extended_locations)-1] = idx
                    remaining -= amount

        num_nodes = len(extended_locations)

        def extend_matrix(base_matrix):
            new_matrix = [[0]*num_nodes for _ in range(num_nodes)]
            for i in range(num_nodes):
                for j in range(num_nodes):
                    orig_i = 0 if i == 0 else split_mapping.get(i, i)
                    orig_j = 0 if j == 0 else split_mapping.get(j, j)
                    new_matrix[i][j] = base_matrix[orig_i][orig_j]
            return new_matrix

        extended_distance_matrix = extend_matrix(distance_matrix)
        extended_time_matrix = extend_matrix(time_matrix) if time_matrix else None

        manager = pywrapcp.RoutingIndexManager(num_nodes, num_vehicles, depot)
        routing = pywrapcp.RoutingModel(manager)

        for v in range(num_vehicles):
            def make_vehicle_callback(v_idx):
                return lambda from_index, to_index: int(
                    extended_distance_matrix[manager.IndexToNode(from_index)][manager.IndexToNode(to_index)]
                    / vehicle_consume[v_idx] * 1000
                )
            callback_idx = routing.RegisterTransitCallback(make_vehicle_callback(v))
            routing.SetArcCostEvaluatorOfVehicle(callback_idx, v)

        def demand_callback(from_index):
            node = manager.IndexToNode(from_index)
            return extended_demands[node]

        demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)

        routing.AddDimensionWithVehicleCapacity(
            demand_callback_index,
            0,
            vehicle_capacities,
            True,
            "Capacity"
        )

        if extended_time_matrix:
            def time_callback(from_index, to_index):
                from_node = manager.IndexToNode(from_index)
                to_node = manager.IndexToNode(to_index)
                return int(extended_time_matrix[from_node][to_node])

            time_callback_index = routing.RegisterTransitCallback(time_callback)
            routing.AddDimension(
                time_callback_index,
                0,
                480,
                True,
                "Time"
            )
            time_dimension = routing.GetDimensionOrDie("Time")
        else:
            time_dimension = None

        search_parameters = pywrapcp.DefaultRoutingSearchParameters()
        search_parameters.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
        search_parameters.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
        search_parameters.time_limit.seconds = 30

        solution = routing.SolveWithParameters(search_parameters)

        if not solution:
            return jsonify(error="No se pudo encontrar solución."), 400

        assignments = []
        total_distance = 0
        total_fuel_liters = 0.0
        vehicle_trips = {}

        for vehicle_id in range(num_vehicles * max_trips_per_vehicle):
            index = routing.Start(vehicle_id)
            if routing.IsEnd(index):
                continue

            main_vehicle = vehicle_id // max_trips_per_vehicle
            trip_number = (vehicle_id % max_trips_per_vehicle) + 1

            route = []
            deliveries = []
            route_time = 0
            distance_vehicle = 0.0

            while not routing.IsEnd(index):
                node = manager.IndexToNode(index)
                route.append(node)
                if node != depot:
                    delivered = extended_locations[node].get("demanda", {})
                    deliveries.append({
                        "location_id": extended_locations[node].get("id"),
                        "products": delivered
                    })

                previous_index = index
                index = solution.Value(routing.NextVar(index))
                dist = extended_distance_matrix[manager.IndexToNode(previous_index)][manager.IndexToNode(index)]
                distance_vehicle += dist
                total_distance += dist

            fuel_liters = distance_vehicle / vehicle_consume[main_vehicle]
            total_fuel_liters += fuel_liters

            if time_dimension:
                cumul = time_dimension.CumulVar(routing.End(vehicle_id))
                route_time = solution.Value(cumul)

            if main_vehicle not in vehicle_trips:
                vehicle_trips[main_vehicle] = {
                    "vehicle": main_vehicle,
                    "route": [],
                    "deliveries": [],
                    "total_time_minutes": 0,
                    "total_distance": 0.0,
                    "fuel_liters": 0.0
                }

            vehicle_trips[main_vehicle]["route"].extend(route + [depot])
            vehicle_trips[main_vehicle]["deliveries"].extend(deliveries)
            vehicle_trips[main_vehicle]["total_time_minutes"] += route_time
            vehicle_trips[main_vehicle]["total_distance"] += distance_vehicle
            vehicle_trips[main_vehicle]["fuel_liters"] += fuel_liters

        assignments = list(vehicle_trips.values())

        # Limpiar rutas
        for trip in assignments:
            raw_route = trip.get("route", [])
            cleaned_route = []
            prev = None
            for node in raw_route:
                if node == 0 and prev == 0:
                    continue
                cleaned_route.append(node)
                prev = node

            if len(cleaned_route) == 0:
                cleaned_route = [0, 0]
            elif cleaned_route[-1] != 0:
                cleaned_route.append(0)

            trip["route"] = [extended_locations[node].get("id") if node != 0 else 0 for node in cleaned_route]

        return jsonify({
            "status": "success",
            "total_distance": total_distance,
            "total_fuel_liters": round(total_fuel_liters, 2),
            "vehicles_used": len(assignments),
            "assignments": assignments
        })

    except Exception as e:
        return jsonify(error=f"Error interno: {str(e)}"), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000, debug=True)
