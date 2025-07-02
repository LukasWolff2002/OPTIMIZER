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
        distance_matrix = data["distance_matrix"]

        all_products = set()
        for loc in locations:
            if isinstance(loc.get("demanda"), dict):
                all_products.update(loc["demanda"].keys())
        all_products = sorted(all_products)

        product_demands = {product: [] for product in all_products}
        for loc in locations:
            demanda = loc.get("demanda", {})
            for product in all_products:
                product_demands[product].append(demanda.get(product, 0))

        depot = 0
        n = len(locations)

        manager = pywrapcp.RoutingIndexManager(n, num_vehicles, depot)
        routing = pywrapcp.RoutingModel(manager)

        def distance_callback(from_index, to_index):
            return distance_matrix[manager.IndexToNode(from_index)][manager.IndexToNode(to_index)]
        transit_callback_index = routing.RegisterTransitCallback(distance_callback)
        routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

        for product, demands in product_demands.items():
            def demand_callback(from_index, demands=demands):
                return demands[manager.IndexToNode(from_index)]
            demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
            routing.AddDimensionWithVehicleCapacity(
                demand_callback_index,
                0,
                [vehicle_capacity] * num_vehicles,
                True,
                f"Capacity_{product}"
            )

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

        search_params = pywrapcp.DefaultRoutingSearchParameters()
        search_params.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
        search_params.time_limit.seconds = 20

        solution = routing.SolveWithParameters(search_params)

        if not solution:
            return jsonify(error="No se pudo encontrar solución."), 400

        total_cost = 0
        rutas = []

        for v in range(num_vehicles):
            index = routing.Start(v)
            ruta = []
            while not routing.IsEnd(index):
                ruta.append(manager.IndexToNode(index))
                next_index = solution.Value(routing.NextVar(index))
                total_cost += routing.GetArcCostForVehicle(index, next_index, v)
                index = next_index
            ruta.append(manager.IndexToNode(index))
            if len(ruta) > 1:
                rutas.append(ruta)

        return jsonify({
            "used_vehicles": len(rutas),
            "total_cost": total_cost,
            "routes": rutas
        })

    except Exception as e:
        # Devuelve el error como JSON
        return jsonify(error=f"Error interno: {str(e)}"), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000, debug=True)
