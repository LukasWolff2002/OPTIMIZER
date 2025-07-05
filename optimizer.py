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
        vehicle_consume = data.get("vehicle_consume", [1]*num_vehicles)
        distance_matrix = data["distance_matrix"]
        time_matrix = data.get("time_matrix")

        if len(vehicle_capacities) != num_vehicles:
            return jsonify(error="La cantidad de capacidades no coincide con max_vehicles"), 400
        if len(vehicle_consume) != num_vehicles:
            return jsonify(error="La cantidad de consumos no coincide con max_vehicles"), 400

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
            prod_quantities = {p: int(float(loc.get("demanda", {}).get(p, 0))) for p in all_products}
            total_demand = sum(prod_quantities.values())

            if total_demand <= max_vehicle_capacity:
                extended_locations.append(loc)
                extended_demands.append(int(total_demand))
                split_mapping[len(extended_locations)-1] = idx
            else:
                # Calcular proporciones
                proportions = {p: (q / total_demand) if total_demand > 0 else 0 for p, q in prod_quantities.items()}
                remaining = total_demand
                remaining_per_product = prod_quantities.copy()
                while remaining > 0:
                    amount = min(remaining, max_vehicle_capacity)
                    split_loc = copy.deepcopy(loc)

                    # Inicializar cantidades para este split
                    split_demand = {}
                    if remaining - amount > 0:
                        # No es el último split: redondear
                        for p in all_products:
                            q = int(round(amount * proportions[p]))
                            split_demand[p] = q
                            remaining_per_product[p] -= q
                    else:
                        # Último split: todo lo que queda
                        split_demand = remaining_per_product.copy()

                    split_loc["demanda"] = {p: str(split_demand[p]) for p in all_products if split_demand[p] > 0}
                    extended_locations.append(split_loc)
                    extended_demands.append(int(sum(split_demand.values())))
                    split_mapping[len(extended_locations)-1] = idx
                    remaining -= amount

        num_nodes = len(extended_locations)

        # Construir matrices extendidas
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

        # OR-Tools setup
        manager = pywrapcp.RoutingIndexManager(num_nodes, num_vehicles, depot)
        routing = pywrapcp.RoutingModel(manager)

        # Costo por vehículo en función de distancia y consumo
        for v in range(num_vehicles):
            def make_vehicle_callback(v_idx):
                return lambda from_index, to_index: int(
                    extended_distance_matrix[manager.IndexToNode(from_index)][manager.IndexToNode(to_index)]
                    / vehicle_consume[v_idx] * 1000
                )
            callback_idx = routing.RegisterTransitCallback(make_vehicle_callback(v))
            routing.SetArcCostEvaluatorOfVehicle(callback_idx, v)

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
        search_parameters.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.SAVINGS
        search_parameters.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
        search_parameters.time_limit.seconds = 30

        solution = routing.SolveWithParameters(search_parameters)

        if not solution:
            return jsonify(error="No se pudo encontrar solución."), 400

        # Procesar resultado
        assignments = []
        total_distance = 0
        total_fuel_liters = 0.0

        for vehicle_id in range(num_vehicles):
            index = routing.Start(vehicle_id)
            route = []
            deliveries = []
            route_time = 0
            distance_vehicle = 0.0

            while not routing.IsEnd(index):
                node = manager.IndexToNode(index)
                route.append(node)
                if node != depot:
                    delivered = extended_locations[node].get("demanda", {})
                    original_idx = split_mapping.get(node, node)
                    deliveries.append({
                        "client_index": original_idx,
                        "products": delivered
                    })
                previous_index = index
                index = solution.Value(routing.NextVar(index))
                dist = extended_distance_matrix[manager.IndexToNode(previous_index)][manager.IndexToNode(index)]
                distance_vehicle += dist
                total_distance += dist

            fuel_liters = distance_vehicle / vehicle_consume[vehicle_id]
            total_fuel_liters += fuel_liters

            if time_dimension:
                cumul = time_dimension.CumulVar(routing.End(vehicle_id))
                route_time = solution.Value(cumul)

            if len(route) > 1:
                assignments.append({
                    "vehicle": vehicle_id,
                    "route": route + [depot],
                    "deliveries": deliveries,
                    "total_time_minutes": route_time,
                    "total_distance": distance_vehicle,
                    "fuel_liters": round(fuel_liters, 2)
                })

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
    app.run(host="0.0.0.0", port=5000, debug=True)
