from flask import Flask, request, jsonify
from ortools.constraint_solver import pywrapcp, routing_enums_pb2

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

        if len(vehicle_capacities) != num_vehicles:
            return jsonify(error="La cantidad de capacidades no coincide con max_vehicles"), 400

        num_nodes = len(locations)
        depot = 0

        # Lista de productos
        all_products = set()
        for loc in locations:
            all_products.update(loc.get("demanda", {}).keys())
        all_products = sorted(all_products)

        # Calculamos demanda total por nodo
        demands = []
        for loc in locations:
            total = sum(float(loc.get("demanda", {}).get(p, 0)) for p in all_products)
            demands.append(int(total))

        # Create index manager
        manager = pywrapcp.RoutingIndexManager(num_nodes, num_vehicles, depot)

        # Create Routing Model
        routing = pywrapcp.RoutingModel(manager)

        # Callback de distancias
        def distance_callback(from_index, to_index):
            from_node = manager.IndexToNode(from_index)
            to_node = manager.IndexToNode(to_index)
            return int(distance_matrix[from_node][to_node])

        transit_callback_index = routing.RegisterTransitCallback(distance_callback)
        routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

        # Callback de demanda
        def demand_callback(from_index):
            node = manager.IndexToNode(from_index)
            return demands[node]

        demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)

        routing.AddDimensionWithVehicleCapacity(
            demand_callback_index,
            0,  # Sin capacidad slack
            vehicle_capacities,
            True,
            "Capacity"
        )

        # Opcional: Restricción de máximo paradas
        if "max_stops" in data:
            max_stops = int(data["max_stops"])
            def count_callback(from_index):
                return 1 if manager.IndexToNode(from_index) != depot else 0

            count_callback_index = routing.RegisterUnaryTransitCallback(count_callback)

            routing.AddDimension(
                count_callback_index,
                0,
                max_stops,
                True,
                "Stops"
            )

        # Parámetros de búsqueda
        search_parameters = pywrapcp.DefaultRoutingSearchParameters()
        search_parameters.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
        )
        search_parameters.local_search_metaheuristic = (
            routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
        )
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
            while not routing.IsEnd(index):
                node = manager.IndexToNode(index)
                route.append(node)
                if node != depot:
                    delivered = locations[node].get("demanda", {})
                    deliveries.append({
                        "client_index": node,
                        "products": delivered
                    })
                previous_index = index
                index = solution.Value(routing.NextVar(index))
                total_distance += routing.GetArcCostForVehicle(previous_index, index, vehicle_id)

            if len(route) > 1:
                assignments.append({
                    "vehicle": vehicle_id,
                    "route": route + [depot],
                    "deliveries": deliveries
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
    app.run(host="0.0.0.0", port=5000, debug=True)