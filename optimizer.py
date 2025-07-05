from flask import Flask, request, jsonify
from ortools.constraint_solver import pywrapcp, routing_enums_pb2
import copy

app = Flask(__name__)

@app.route("/optimize", methods=["POST"])
def optimize():
    data = request.get_json()
    if data is None:
        return jsonify(error="No se recibió JSON válido"), 400

    try:
        locations = data["locations"]
        num_vehicles = data["max_vehicles"]
        vehicle_capacities = data["vehicle_capacities"]
        distance_matrix = data["distance_matrix"]
        time_matrix = data.get("time_matrix")

        if len(vehicle_capacities) != num_vehicles:
            return jsonify(error="La cantidad de capacidades no coincide con max_vehicles"), 400

        max_vehicle_capacity = max(vehicle_capacities)
        depot = 0

        # Lista de productos
        all_products = set()
        for loc in locations:
            all_products.update(loc.get("demanda", {}).keys())
        all_products = sorted(all_products)

        # Generar nodos extendidos
        extended_locations = []
        extended_demands = []
        split_mapping = {}  # mapa sub_nodo -> nodo original

        extended_locations.append(locations[0])  # Depósito
        extended_demands.append(0)

        for idx, loc in enumerate(locations[1:], start=1):
            total_demand = sum(float(loc.get("demanda", {}).get(p, 0)) for p in all_products)
            if total_demand <= max_vehicle_capacity:
                extended_locations.append(loc)
                extended_demands.append(int(total_demand))
                split_mapping[len(extended_locations)-1] = idx
            else:
                remaining = total_demand
                while remaining > 0:
                    amount = min(remaining, max_vehicle_capacity)
                    split_loc = copy.deepcopy(loc)
                    # Marcar que es un sub-nodo
                    split_loc["demanda"] = {p: str(amount) for p in all_products if p in loc["demanda"]}
                    extended_locations.append(split_loc)
                    extended_demands.append(int(amount))
                    split_mapping[len(extended_locations)-1] = idx
                    remaining -= amount

        num_nodes = len(extended_locations)

        # Construir matrices extendidas
        def extend_matrix(base_matrix):
            n = len(base_matrix)
            new_matrix = [[0]*num_nodes for _ in range(num_nodes)]
            for i in range(num_nodes):
                for j in range(num_nodes):
                    orig_i = 0 if i==0 else split_mapping.get(i, i)
                    orig_j = 0 if j==0 else split_mapping.get(j, j)
                    new_matrix[i][j] = base_matrix[orig_i][orig_j]
            return new_matrix

        extended_distance_matrix = extend_matrix(distance_matrix)
        extended_time_matrix = extend_matrix(time_matrix) if time_matrix else None

        # OR-Tools setup
        manager = pywrapcp.RoutingIndexManager(num_nodes, num_vehicles, depot)
        routing = pywrapcp.RoutingModel(manager)

        # Distancia
        def distance_callback(from_index, to_index):
            from_node = manager.IndexToNode(from_index)
            to_node = manager.IndexToNode(to_index)
            return int(extended_distance_matrix[from_node][to_node])

        transit_callback_index = routing.RegisterTransitCallback(distance_callback)
        routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

        # Demanda
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

        # Parámetros de búsqueda
        search_parameters = pywrapcp.DefaultRoutingSearchParameters()
        search_parameters.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
        search_parameters.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
        search_parameters.time_limit.seconds = 30

        solution = routing.SolveWithParameters(search_parameters)

        if not solution:
            return jsonify(error="No se pudo encontrar solución."), 400

        # Procesar resultado
        assignments = []
        total_distance = 0

        for vehicle_id in range(num_vehicles):
            index = routing.Start(vehicle_id)
            route = []
            deliveries = []
            route_time = 0

            while not routing.IsEnd(index):
                node = manager.IndexToNode(index)
                route.append(node)
                if node != depot:
                    original_idx = split_mapping.get(node, node)
                    delivered = locations[original_idx].get("demanda", {})
                    deliveries.append({
                        "client_index": original_idx,
                        "products": delivered
                    })
                previous_index = index
                index = solution.Value(routing.NextVar(index))
                total_distance += routing.GetArcCostForVehicle(previous_index, index, vehicle_id)

            if time_dimension:
                cumul = time_dimension.CumulVar(routing.End(vehicle_id))
                route_time = solution.Value(cumul)

            if len(route) > 1:
                assignments.append({
                    "vehicle": vehicle_id,
                    "route": route + [depot],
                    "deliveries": deliveries,
                    "total_time_minutes": route_time
                })

        return jsonify({
            "status": "success",
            "total_cost": total_distance,
            "vehicles_used": len(assignments),
            "assignments": assignments
        })

    except Exception as e:
        return jsonify(error=f"Error interno: {str(e)}"), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000, debug=True)
