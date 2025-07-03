from flask import Flask, request, jsonify
from ortools.sat.python import cp_model

app = Flask(__name__)

@app.route("/optimize", methods=["POST"])
def optimize():
    data = request.get_json()
    if data is None:
        return jsonify(error="No se recibió JSON válido"), 400

    try:
        locations = data["locations"]
        num_vehicles = data["max_vehicles"]
        vehicle_capacities = data["vehicle_capacities"]  # 🚀 Lista de capacidades
        vehicle_consumptions = data["vehicle_consumptions"]  # 🚀 Lista de consumos
        distance_matrix = data["distance_matrix"]

        if len(vehicle_capacities) != num_vehicles or len(vehicle_consumptions) != num_vehicles:
            return jsonify(error="La cantidad de capacidades o consumos no coincide con max_vehicles"), 400

        num_customers = len(locations)
        depot_index = 0

        # Lista de productos
        all_products = set()
        for loc in locations:
            all_products.update(loc.get("demanda", {}).keys())
        all_products = sorted(all_products)

        demands = {}
        for c, loc in enumerate(locations):
            for p in all_products:
                demands[c, p] = int(float(loc.get("demanda", {}).get(p, 0)))

        model = cp_model.CpModel()

        q = {}
        for v in range(num_vehicles):
            for c in range(1, num_customers):
                for p in all_products:
                    q[v, c, p] = model.NewIntVar(0, demands[c, p], f'q_{v}_{c}_{p}')

        x = {}
        for v in range(num_vehicles):
            for c in range(1, num_customers):
                x[v, c] = model.NewBoolVar(f'x_{v}_{c}')

        big_M = sum(demands[c, p] for c in range(1, num_customers) for p in all_products)

        # Restricción: cada cliente recibe toda su demanda
        for c in range(1, num_customers):
            for p in all_products:
                model.Add(sum(q[v, c, p] for v in range(num_vehicles)) == demands[c, p])

        # Restricción: cada vehículo respeta su capacidad
        for v in range(num_vehicles):
            model.Add(
                sum(q[v, c, p] for c in range(1, num_customers) for p in all_products)
                <= vehicle_capacities[v]
            )

        # Restricción: si entrega algo, entonces visita
        for v in range(num_vehicles):
            for c in range(1, num_customers):
                model.Add(
                    sum(q[v, c, p] for p in all_products) <= big_M * x[v, c]
                )

        # Objetivo: minimizar distancia ponderada por consumo
        total_cost = []
        for v in range(num_vehicles):
            for c in range(1, num_customers):
                distance = distance_matrix[depot_index][c] + distance_matrix[c][depot_index]
                cost = distance * vehicle_consumptions[v]
                total_cost.append(cost * x[v, c])
        model.Minimize(sum(total_cost))

        solver = cp_model.CpSolver()
        solver.parameters.max_time_in_seconds = 30
        status = solver.Solve(model)

        if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
            return jsonify(error="No se pudo encontrar solución."), 400

        assignments = []
        for v in range(num_vehicles):
            deliveries = []
            for c in range(1, num_customers):
                if solver.Value(x[v, c]):
                    delivered = {}
                    total = 0
                    for p in all_products:
                        qty = solver.Value(q[v, c, p])
                        if qty > 0:
                            delivered[p] = qty
                            total += qty
                    if delivered:
                        deliveries.append({
                            "client_index": c,
                            "products": delivered,
                            "total_quantity": total
                        })
            if deliveries:
                assignments.append({
                    "vehicle": v,
                    "capacity": vehicle_capacities[v],
                    "consumption": vehicle_consumptions[v],
                    "deliveries": deliveries
                })

        return jsonify({
            "status": "success",
            "total_cost": solver.ObjectiveValue(),
            "vehicles_used": len(assignments),
            "assignments": assignments
        })

    except Exception as e:
        return jsonify(error=f"Error interno: {str(e)}"), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
