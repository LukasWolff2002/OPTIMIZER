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
        vehicle_capacity = data["vehicle_capacity"]
        max_stops_per_vehicle = data["max_stops"]
        distance_matrix_orig = data["distance_matrix"]

        # Extraer productos únicos
        all_products = set()
        for loc in locations:
            if isinstance(loc.get("demanda"), dict):
                all_products.update(loc["demanda"].keys())
        all_products = sorted(all_products)

        # 🚀 Subdividir nodos
        subdivided_locations = []
        for i, loc in enumerate(locations):
            demanda = loc.get("demanda", {})
            demanda = {k: float(v) for k, v in demanda.items() if float(v) > 0}

            if not demanda:
                subdivided_locations.append({
                    "lat": loc["lat"],
                    "lng": loc["lng"],
                    "demanda": {},
                    "original_location_index": i
                })
                continue

            # Inicializar pendientes
            remaining = demanda.copy()

            # Mientras haya algo que repartir
            while any(qty > 0 for qty in remaining.values()):
                split = {}
                total_this_split = 0
                for product, qty in remaining.items():
                    if qty > 0:
                        to_take = min(qty, vehicle_capacity - total_this_split)
                        split[product] = to_take
                        remaining[product] -= to_take
                        total_this_split += to_take
                subdivided_locations.append({
                    "lat": loc["lat"],
                    "lng": loc["lng"],
                    "demanda": split,
                    "original_location_index": i
                })

        # Construir matriz de distancias entre subdivididos
        n = len(subdivided_locations)
        distance_matrix = [[0] * n for _ in range(n)]
        for i, from_loc in enumerate(subdivided_locations):
            for j, to_loc in enumerate(subdivided_locations):
                if i == j:
                    distance_matrix[i][j] = 0
                else:
                    from_idx = from_loc["original_location_index"]
                    to_idx = to_loc["original_location_index"]
                    distance_matrix[i][j] = distance_matrix_orig[from_idx][to_idx]

        depot = 0

        manager = pywrapcp.RoutingIndexManager(n, num_vehicles, depot)
        routing = pywrapcp.RoutingModel(manager)

        # Distancia
        def distance_callback(from_index, to_index):
            return distance_matrix[manager.IndexToNode(from_index)][manager.IndexToNode(to_index)]
        transit_callback_index = routing.RegisterTransitCallback(distance_callback)
        routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

        # Capacidad única: suma todas las demandas por nodo
        total_demands = []
        for loc in subdivided_locations:
            total = sum(loc["demanda"].values())
            total_demands.append(total)

        def demand_callback(from_index):
            return total_demands[manager.IndexToNode(from_index)]
        demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
        routing.AddDimensionWithVehicleCapacity(
            demand_callback_index,
            0,
            [vehicle_capacity] * num_vehicles,
            True,
            "Capacity"
        )

        # Dimensión de paradas
        def count_callback(from_index):
            return 1
        count_callback_index = routing.RegisterUnaryTransitCallback(count_callback)
        routing.AddDimension(
            count_callback_index,
            0,
            max_stops_per_vehicle,
            True,
            "Stops"
        )

        # Parámetros
        search_params = pywrapcp.DefaultRoutingSearchParameters()
        search_params.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
        search_params.time_limit.seconds = 20

        solution = routing.SolveWithParameters(search_params)

        if not solution:
            return jsonify(error="No se pudo encontrar solución."), 400

        total_cost = 0
        rutas = []
        deliveries = []

        for v in range(num_vehicles):
            index = routing.Start(v)
            ruta = []
            vehicle_deliveries = []

            while not routing.IsEnd(index):
                node_index = manager.IndexToNode(index)
                ruta.append(node_index)

                delivered_here = {}
                # Simplemente devolvemos la demanda del nodo
                for p in all_products:
                    delivered_here[p] = subdivided_locations[node_index]["demanda"].get(p, 0)

                vehicle_deliveries.append({
                    "location_index": node_index,
                    "original_location_index": subdivided_locations[node_index]["original_location_index"],
                    "delivered": delivered_here
                })

                next_index = solution.Value(routing.NextVar(index))
                total_cost += routing.GetArcCostForVehicle(index, next_index, v)
                index = next_index

            ruta.append(manager.IndexToNode(index))
            deliveries.append(vehicle_deliveries)
            if len(ruta) > 2:
                rutas.append(ruta)

        return jsonify({
            "used_vehicles": len(rutas),
            "total_cost": total_cost,
            "routes": rutas,
            "deliveries": deliveries
        })

    except Exception as e:
        return jsonify(error=f"Error interno: {str(e)}"), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
