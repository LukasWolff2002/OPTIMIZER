from flask import Flask, request, jsonify
from ortools.constraint_solver import pywrapcp, routing_enums_pb2
import copy

app = Flask(__name__)

@app.route("/optimize", methods=["POST"])
def optimize():

    ubicaciones_refrigeradas = [
        "WALMART CD",
"CENCOSUD CD",
"SANTA ISABEL LOCAL",
"JUMBO LOCAL",
"TOTTUS CD",
"TOTTUS LOCAL",
"UNIMARC CD",
"UNIMARC LOCAL",
"ARAMARK",
"SODEXO"
    ]

    raw_data = request.get_json()
    if raw_data is None:
        return jsonify(error="No se recibió JSON válido"), 400

    # Si es array (viene de Sidekiq), tomar los 3 elementos
    if isinstance(raw_data, list):
        if len(raw_data) != 3:
            return jsonify(error="Se esperaba un array de 3 elementos: [data, truck_ids, user_id]"), 400
        data = raw_data[0]
        truck_ids = raw_data[1]
        user_id = raw_data[2]
    else:
        data = raw_data
        truck_ids = []
        user_id = None

    try:
        locations = data["locations"]
        base_num_vehicles = data["max_vehicles"]
        vehicle_capacities_base = data["vehicle_capacities"]
        vehicle_consume_base = data.get("vehicle_consume", [1]*base_num_vehicles)
        distance_matrix = data["distance_matrix"]
        time_matrix = data.get("time_matrix")
        max_trips_per_vehicle = 2  # ✅ Permitir múltiples viajes

        if len(vehicle_capacities_base) != base_num_vehicles:
            return jsonify(error="La cantidad de capacidades no coincide con max_vehicles"), 400
        if len(vehicle_consume_base) != base_num_vehicles:
            return jsonify(error="La cantidad de consumos no coincide con max_vehicles"), 400
        
        vehicle_free_base = data.get("vehicle_free", [0]*base_num_vehicles)

        if len(vehicle_free_base) != base_num_vehicles:
            return jsonify(error="La cantidad de valores en vehicle_free no coincide con max_vehicles"), 400

        # ✅ Duplicar vehículos ficticios por viaje
        vehicle_capacities = []
        vehicle_consume = []
        vehicle_free = []
        vehicle_mapping = {}

        for idx in range(base_num_vehicles):
            for trip in range(max_trips_per_vehicle):
                vehicle_capacities.append(vehicle_capacities_base[idx])
                vehicle_consume.append(vehicle_consume_base[idx])
                vehicle_free.append(vehicle_free_base[idx])
                vehicle_mapping[len(vehicle_capacities)-1] = idx

        num_vehicles = len(vehicle_capacities)
        max_vehicle_capacity = max(vehicle_capacities)
        depot = 0

        # Lista de productos
        all_products = set()
        for loc in locations:
            all_products.update(loc.get("demanda", {}).keys())
        all_products = sorted(all_products)

        # Generar nodos extendidos
        extended_locations = [locations[0]]
        extended_demands = [0]
        split_mapping = {}

        extended_requires_refrigeration = [False]  # el depósito no requiere refrigeración

        for idx, loc in enumerate(locations[1:], start=1):
            prod_quantities = {p: int(float(loc.get("demanda", {}).get(p, 0))) for p in all_products}
            total_demand = sum(prod_quantities.values())

            identificador = loc.get("identificador", "").upper()
            requires_refrigeration = any(nombre.upper() in identificador for nombre in ubicaciones_refrigeradas)

            if total_demand <= max_vehicle_capacity:
                extended_locations.append(loc)
                extended_demands.append(int(total_demand))
                split_mapping[len(extended_locations)-1] = idx
                extended_requires_refrigeration.append(requires_refrigeration)

            else:
                proportions = {p: (q / total_demand) for p, q in prod_quantities.items()}
                remaining = total_demand
                remaining_per_product = prod_quantities.copy()
                while remaining > 0:
                    amount = min(remaining, max_vehicle_capacity)
                    split_loc = copy.deepcopy(loc)
                    split_demand = {}
                    extended_requires_refrigeration.append(requires_refrigeration)


                    if remaining - amount > 0:
                        for p in all_products:
                            q = int(round(amount * proportions[p]))
                            split_demand[p] = q
                            remaining_per_product[p] -= q
                    else:
                        split_demand = remaining_per_product.copy()

                    split_loc["demanda"] = {p: str(split_demand[p]) for p in all_products if split_demand[p] > 0}
                    extended_locations.append(split_loc)
                    extended_demands.append(int(sum(split_demand.values())))
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

        for node_index in range(1, num_nodes):  # Omitir depósito
            if extended_requires_refrigeration[node_index]:
                for vehicle_id in range(num_vehicles):
                    if not vehicle_free[vehicle_id]:  # No refrigerado
                        routing.VehicleVar(manager.NodeToIndex(node_index)).RemoveValue(vehicle_id)


        # Callback de coste
        for v in range(num_vehicles):
            def make_vehicle_callback(v_idx):
                return lambda from_index, to_index: int(
                    extended_distance_matrix[manager.IndexToNode(from_index)][manager.IndexToNode(to_index)]
                    / vehicle_consume[v_idx] * 1000
                )
            callback_idx = routing.RegisterTransitCallback(make_vehicle_callback(v))
            routing.SetArcCostEvaluatorOfVehicle(callback_idx, v)

        # Callback de demanda
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

        # Tiempo
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

        # Parámetros
        search_parameters = pywrapcp.DefaultRoutingSearchParameters()
        search_parameters.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
        search_parameters.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
        search_parameters.time_limit.seconds = 30

        solution = routing.SolveWithParameters(search_parameters)

        if not solution:
            return jsonify(error="No se pudo encontrar solución."), 400

        # Extraer rutas
        assignments = []
        total_distance = 0
        total_fuel_liters = 0.0
        vehicle_trips = {}

        for vehicle_id in range(num_vehicles):
            index = routing.Start(vehicle_id)
            if routing.IsEnd(index):
                continue  # este viaje ficticio no se usó

            main_vehicle = vehicle_mapping[vehicle_id]
            route = []
            deliveries = []
            distance_vehicle = 0.0
            route_time = 0

            while not routing.IsEnd(index):
                node = manager.IndexToNode(index)
                route.append(node)
                if node != depot:
                    delivered = extended_locations[node].get("demanda", {})
                    deliveries.append({
    "location_id": extended_locations[node].get("id"),
    "node_index": node,   # importante!
    "products": delivered
})


                prev_index = index
                index = solution.Value(routing.NextVar(index))
                dist = extended_distance_matrix[manager.IndexToNode(prev_index)][manager.IndexToNode(index)]
                distance_vehicle += dist
                total_distance += dist

            fuel_liters = distance_vehicle / vehicle_consume[vehicle_id]
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

            # Convertir índices a IDs
            # Convertir índices a IDs reales
            trip_route_raw = [0] + [extended_locations[n].get("id") if n != 0 else 0 for n in route] + [0]

            # Eliminar ceros duplicados consecutivos
            cleaned_trip_route = []
            prev = None
            for node in trip_route_raw:
                if node == 0 and prev == 0:
                    continue
                cleaned_trip_route.append(node)
                prev = node

            # Si ya hay rutas previas, quitar el primer 0 del nuevo tramo para no repetir
            if vehicle_trips[main_vehicle]["route"]:
                if cleaned_trip_route[0] == 0:
                    cleaned_trip_route = cleaned_trip_route[1:]

            vehicle_trips[main_vehicle]["route"].extend(cleaned_trip_route)


            vehicle_trips[main_vehicle]["deliveries"].extend(deliveries)
            vehicle_trips[main_vehicle]["total_time_minutes"] += route_time
            vehicle_trips[main_vehicle]["total_distance"] += distance_vehicle
            vehicle_trips[main_vehicle]["fuel_liters"] += fuel_liters

        return jsonify({
            "status": "success",
            "total_distance": total_distance,
            "total_fuel_liters": round(total_fuel_liters, 2),
            "vehicles_used": len(vehicle_trips),
            "assignments": list(vehicle_trips.values())
        })

    except Exception as e:
        import traceback
        return jsonify(error=f"Error interno: {str(e)}", traceback=traceback.format_exc()), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000, debug=False)
