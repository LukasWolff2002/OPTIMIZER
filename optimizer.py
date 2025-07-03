from flask import Flask, request, jsonify, Response
from ortools.constraint_solver import pywrapcp, routing_enums_pb2
import json
import time
import threading

app = Flask(__name__)

# Aquí se guardan los progresos por job_id
tasks = {}

@app.route("/optimize", methods=["POST"])
def start_optimization():
    data = request.get_json()
    job_id = str(int(time.time() * 1000))

    # Creamos la lista de mensajes que se irán llenando
    queue = []
    tasks[job_id] = queue

    def worker():
        try:
            queue.append("[10%] Recibiendo datos...")

            if data is None:
                queue.append(json.dumps({"error": "No se recibió JSON válido"}))
                queue.append("__done__")
                return

            locations = data["locations"]
            num_vehicles = data["max_vehicles"]
            vehicle_capacities = data["vehicle_capacities"]
            distance_matrix = data["distance_matrix"]

            if len(vehicle_capacities) != num_vehicles:
                queue.append(json.dumps({"error": "Cantidad de capacidades no coincide con max_vehicles"}))
                queue.append("__done__")
                return

            queue.append("[20%] Preparando demandas...")
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

            queue.append("[30%] Creando modelo...")
            manager = pywrapcp.RoutingIndexManager(num_nodes, num_vehicles, depot)
            routing = pywrapcp.RoutingModel(manager)

            queue.append("[40%] Registrando callbacks...")
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
                queue.append("[50%] Añadiendo restricción de máximo paradas...")
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

            queue.append("[60%] Configurando parámetros de búsqueda...")
            search_parameters = pywrapcp.DefaultRoutingSearchParameters()
            search_parameters.first_solution_strategy = (
                routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
            )
            search_parameters.local_search_metaheuristic = (
                routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
            )
            search_parameters.time_limit.seconds = 30

            queue.append("[70%] Ejecutando solver...")
            solution = routing.SolveWithParameters(search_parameters)

            if not solution:
                queue.append(json.dumps({"error": "No se pudo encontrar solución."}))
                queue.append("__done__")
                return

            queue.append("[90%] Procesando resultado...")
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

            queue.append(json.dumps(result))
            queue.append("__done__")

        except Exception as e:
            queue.append(json.dumps({"error": f"Error interno: {str(e)}"}))
            queue.append("__done__")

    threading.Thread(target=worker).start()
    return jsonify(job_id=job_id)

@app.route("/progress/<job_id>")
def progress(job_id):
    def generate():
        while True:
            queue = tasks.get(job_id, [])
            while queue:
                msg = queue.pop(0)
                yield f"data: {msg}\n\n"
                if msg == "__done__":
                    return
            time.sleep(0.5)

    return Response(generate(), mimetype="text/event-stream")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
