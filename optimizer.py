from flask import Flask, Response, request, stream_with_context, json
from ortools.constraint_solver import pywrapcp, routing_enums_pb2
import time

app = Flask(__name__)

@app.route("/optimize", methods=["POST"])
def optimize():
    data = request.get_json()
    if data is None:
        return Response("data: {\"error\": \"No se recibió JSON válido\"}\n\n", mimetype="text/event-stream")

    def generate():
        try:
            yield "data: [10%] Recibiendo datos...\n\n"
            time.sleep(0.2)

            locations = data["locations"]
            num_vehicles = data["max_vehicles"]
            vehicle_capacities = data["vehicle_capacities"]
            distance_matrix = data["distance_matrix"]

            if len(vehicle_capacities) != num_vehicles:
                yield "data: {\"error\": \"Cantidad de capacidades no coincide con max_vehicles\"}\n\n"
                return

            yield "data: [20%] Preparando demandas...\n\n"
            time.sleep(0.2)

            num_nodes = len(locations)
            depot = 0

            all_products = set()
            for loc in locations:
                all_products.update(loc.get("demanda", {}).keys())
            all_products = sorted(all_products)

            demands = []
            for loc in locations:
                total = sum(float(loc.get("demanda", {}).get(p, 0)) for p in all_products)
                demands.append(int(total))

            yield "data: [30%] Creando modelo...\n\n"
            time.sleep(0.2)
            manager = pywrapcp.RoutingIndexManager(num_nodes, num_vehicles, depot)
            routing = pywrapcp.RoutingModel(manager)

            yield "data: [40%] Registrando callbacks...\n\n"
            time.sleep(0.2)

            def distance_callback(from_index, to_index):
                from_node = manager.IndexToNode(from_index)
                to_node = manager.IndexToNode(to_index)
                return int(distance_matrix[from_node][to_node])

            transit_callback_index = routing.RegisterTransitCallback(distance_callback)
            routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

            def demand_callback(from_index):
                node = manager.IndexToNode(from_index)
                return demands[node]

            demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)

            routing.AddDimensionWithVehicleCapacity(
                demand_callback_index,
                0,
                vehicle_capacities,
                True,
                "Capacity"
            )

            if "max_stops" in data:
                yield "data: [50%] Añadiendo restricción de máximo paradas...\n\n"
                time.sleep(0.2)
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

            yield "data: [60%] Configurando parámetros de búsqueda...\n\n"
            time.sleep(0.2)
            search_parameters = pywrapcp.DefaultRoutingSearchParameters()
            search_parameters.first_solution_strategy = (
                routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
            )
            search_parameters.local_search_metaheuristic = (
                routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
            )
            search_parameters.time_limit.seconds = 15

            yield "data: [70%] Ejecutando solver...\n\n"
            solution = routing.SolveWithParameters(search_parameters)

            if not solution:
                yield "data: {\"error\": \"No se pudo encontrar solución.\"}\n\n"
                return

            yield "data: [90%] Procesando resultado...\n\n"
            time.sleep(0.2)

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

            result = {
                "status": "success",
                "total_cost": total_distance,
                "vehicles_used": len(assignments),
                "assignments": assignments
            }
            yield f"data: {json.dumps(result)}\n\n"

        except Exception as e:
            yield f"data: {{\"error\": \"Error interno: {str(e)}\"}}\n\n"

    return Response(stream_with_context(generate()), mimetype="text/event-stream")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000, debug=True)
