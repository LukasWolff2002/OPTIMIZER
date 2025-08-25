from flask import Flask, request, jsonify
from ortools.constraint_solver import pywrapcp, routing_enums_pb2
import copy
import math

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
        vehicle_free_base = data.get("vehicle_free", [0]*base_num_vehicles)
        distance_matrix = data["distance_matrix"]
        time_matrix = data.get("time_matrix")

        # tiempos
        tiempo_calculo_min = data.get("tiempo_calculo", 2)  # minutos (default 2)
        tiempo_calculo = int(tiempo_calculo_min * 60)       # segundos
        HORIZON = int(data.get("max_time_per_trip", 480))   # minutos por viaje y tope global
        reload_service_time = int(data.get("reload_service_time", 0))  # min

        # multiviaje
        max_trips_per_vehicle = 2
        costo_varias_rutas = True
        costo_reingreso_valor = 100_000  # costo fijo para viajes adicionales

        # Validaciones básicas
        if len(vehicle_capacities_base) != base_num_vehicles:
            return jsonify(error="La cantidad de capacidades no coincide con max_vehicles"), 400
        if len(vehicle_consume_base) != base_num_vehicles:
            return jsonify(error="La cantidad de consumos no coincide con max_vehicles"), 400
        if len(vehicle_free_base) != base_num_vehicles:
            return jsonify(error="La cantidad de valores en vehicle_free no coincide con max_vehicles"), 400

        # ------------------ MODOS (exclusividad dura) ------------------
        MODE_FREE = 0   # solo OTHER
        MODE_W    = 1   # solo WALMART
        MODE_C    = 2   # solo CENCOSUD
        MODES = (MODE_FREE, MODE_W, MODE_C)

        # Duplicar vehículos por viaje y por modo
        vehicle_capacities, vehicle_consume, vehicle_free = [], [], []
        vehicle_mapping = {}   # dup idx -> camión base
        vehicle_trip_no = {}   # dup idx -> 0 ó 1
        vehicle_mode = {}      # dup idx -> modo

        for idx in range(base_num_vehicles):
            for trip in range(max_trips_per_vehicle):
                for mode in MODES:
                    vehicle_capacities.append(vehicle_capacities_base[idx])
                    vehicle_consume.append(vehicle_consume_base[idx])
                    vehicle_free.append(vehicle_free_base[idx])
                    v_idx = len(vehicle_capacities) - 1
                    vehicle_mapping[v_idx] = idx
                    vehicle_trip_no[v_idx] = trip
                    vehicle_mode[v_idx] = mode

        num_vehicles = len(vehicle_capacities)
        max_vehicle_capacity = max(vehicle_capacities)
        depot = 0

        # Productos
        all_products = set()
        for loc in locations:
            all_products.update(loc.get("demanda", {}).keys())
        all_products = sorted(all_products)

        # Generar nodos extendidos
        extended_locations = [locations[0]]
        extended_demands = [0]  # demanda total
        split_mapping = {}
        extended_requires_refrigeration = [False]  # depósito no requiere

        for idx, loc in enumerate(locations[1:], start=1):
            prod_quantities = {p: int(float(loc.get("demanda", {}).get(p, 0))) for p in all_products}
            total_demand = sum(prod_quantities.values())

            identificador = (loc.get("identificador", "") or "").upper()
            requires_refrigeration = any(nombre.upper() in identificador for nombre in ubicaciones_refrigeradas)

            if total_demand <= max_vehicle_capacity:
                extended_locations.append(loc)
                extended_demands.append(int(total_demand))
                split_mapping[len(extended_locations)-1] = idx
                extended_requires_refrigeration.append(requires_refrigeration)
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
                            q = min(q, remaining_per_product[p])
                            split_demand[p] = q
                            remaining_per_product[p] -= q
                    else:
                        split_demand = remaining_per_product.copy()

                    split_loc["demanda"] = {p: str(split_demand[p]) for p in all_products if split_demand[p] > 0}
                    extended_locations.append(split_loc)
                    extended_demands.append(int(sum(split_demand.values())))
                    split_mapping[len(extended_locations)-1] = idx
                    extended_requires_refrigeration.append(requires_refrigeration)
                    remaining -= amount

        num_nodes = len(extended_locations)

        # ----------- Agrupador EXACTO (como pediste) -----------
        def _group(identificador: str) -> str:
            ident = (identificador or "").upper()
            if any(k in ident for k in ["WALLMART CD"]):
                return "WALMART"
            if any(k in ident for k in ["CENCOSUD CD"]):
                return "CENCOSUD"
            return "OTHER"

        node_group = [_group(loc.get("identificador", "")) for loc in extended_locations]
        HIGH_PENALTY = 100_000  # sigue ahí; modos ya hacen la exclusividad dura

        # Prioridad de clientes: PUNTO AZUL / LA VEGA
        prioridad_clientes = ["PUNTO AZUL", "LA VEGA"]
        is_priority_node = []
        for loc in extended_locations:
            ident = (loc.get("identificador", "") or "").upper()
            es_prioritario = any(tag in ident for tag in prioridad_clientes)
            is_priority_node.append(es_prioritario)
        any_priority_exists = any(is_priority_node[1:])  # excluir depósito
        PRIORITY_START_PENALTY = 50_000
        PRIORITY_LATE_ENTRY_PENALTY = 70_000  # nuevo: castiga meter un prioritario después de un no-prioritario

        # Extensión de matrices
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

        # Si no hay matriz de tiempo, no se puede imponer el tope global de 480 min
        if extended_time_matrix is None:
            return jsonify(error="Se requiere 'time_matrix' para limitar el tiempo total por vehículo a 480 minutos."), 400

        # OR-Tools
        manager = pywrapcp.RoutingIndexManager(num_nodes, num_vehicles, depot)
        routing = pywrapcp.RoutingModel(manager)

        # --- RESTRICCIONES DURAS ---

        # (a) Refrigeración
        for node_index in range(1, num_nodes):
            if extended_requires_refrigeration[node_index]:
                node_idx = manager.NodeToIndex(node_index)
                for vehicle_id in range(num_vehicles):
                    if not vehicle_free[vehicle_id]:
                        routing.VehicleVar(node_idx).RemoveValue(vehicle_id)

        # (b) Exclusividad WALMART/CENCOSUD/OTHER por modo (rutas puras)
        for node_index in range(1, num_nodes):
            g = node_group[node_index]
            node_idx = manager.NodeToIndex(node_index)
            if g == "WALMART":
                allowed_modes = {MODE_W}
            elif g == "CENCOSUD":
                allowed_modes = {MODE_C}
            else:  # OTHER
                allowed_modes = {MODE_FREE}
            for v in range(num_vehicles):
                if vehicle_mode[v] not in allowed_modes:
                    routing.VehicleVar(node_idx).RemoveValue(v)

        # --- COSTOS ---
        for v in range(num_vehicles):
            def make_vehicle_callback(v_idx):
                def distance(from_index, to_index, rate=vehicle_consume[v_idx]):
                    from_node = manager.IndexToNode(from_index)
                    to_node   = manager.IndexToNode(to_index)
                    base_dist = extended_distance_matrix[from_node][to_node]
                    base = max(0, int(round((base_dist / max(rate, 1e-9)) * 1000)))

                    if any_priority_exists:
                        # 1) Salir del depósito a no-prioritario (si existen prioritarios) => penaliza
                        if from_node == depot and to_node != depot and not is_priority_node[to_node]:
                            base += PRIORITY_START_PENALTY
                        # 2) Entrar a un prioritario desde un no-prioritario (tarde en la ruta) => penaliza
                        if (to_node != depot and is_priority_node[to_node]
                                and from_node != depot and not is_priority_node[from_node]):
                            base += PRIORITY_LATE_ENTRY_PENALTY

                    # Penalización mezcla (redundante con modos, pero inocua)
                    if from_node != depot and to_node != depot:
                        g_from, g_to = node_group[from_node], node_group[to_node]
                        if g_from != g_to and ("WALMART" in (g_from, g_to) or "CENCOSUD" in (g_from, g_to)):
                            base += HIGH_PENALTY
                    return base
                return distance
            callback_idx = routing.RegisterTransitCallback(make_vehicle_callback(v))
            routing.SetArcCostEvaluatorOfVehicle(callback_idx, v)

        # Demanda (capacidad)
        def demand_callback(from_index):
            node = manager.IndexToNode(from_index)
            return int(extended_demands[node])

        demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
        routing.AddDimensionWithVehicleCapacity(
            demand_callback_index,
            0,
            vehicle_capacities,
            True,
            "Capacity"
        )

        # Tiempo (con servicio de recarga al salir del depósito en medio de ruta)
        start_indices = set(routing.Start(v) for v in range(num_vehicles))

        def time_callback(from_index, to_index):
            from_node = manager.IndexToNode(from_index)
            to_node   = manager.IndexToNode(to_index)
            travel = int(round(extended_time_matrix[from_node][to_node]))
            service = 0
            if from_node == depot and from_index not in start_indices:
                service = reload_service_time
            return travel + service

        time_callback_index = routing.RegisterTransitCallback(time_callback)
        routing.AddDimension(
            time_callback_index,
            0,
            HORIZON,
            True,
            "Time"
        )
        time_dimension = routing.GetDimensionOrDie("Time")

        # Límite duro por nodo y por fin de cada duplicado
        for node in range(1, num_nodes):
            idx = manager.NodeToIndex(node)
            time_dimension.CumulVar(idx).SetRange(0, HORIZON)
        for v in range(num_vehicles):
            time_dimension.CumulVar(routing.End(v)).SetMax(HORIZON)

        # Tope GLOBAL por vehículo real = suma de tiempos de sus duplicados <= HORIZON
        solver = routing.solver()
        for base in range(base_num_vehicles):
            end_cumuls = [time_dimension.CumulVar(routing.End(v))
                          for v in range(num_vehicles) if vehicle_mapping[v] == base]
            solver.Add(solver.Sum(end_cumuls) <= HORIZON)

        # (Opcional) compactar makespan
        time_dimension.SetGlobalSpanCostCoefficient(50)

        # Costo fijo para viajes adicionales
        if costo_varias_rutas:
            for v in range(num_vehicles):
                if vehicle_trip_no[v] > 0:
                    routing.SetFixedCostOfVehicle(costo_reingreso_valor, v)

        # Parámetros de búsqueda
        search_parameters = pywrapcp.DefaultRoutingSearchParameters()
        search_parameters.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
        search_parameters.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
        search_parameters.time_limit.FromSeconds(tiempo_calculo)

        solution = routing.SolveWithParameters(search_parameters)
        if not solution:
            return jsonify(error="No se pudo encontrar solución."), 400

        # Extraer resultados
        vehicle_trips = {}
        total_distance, total_fuel_liters = 0.0, 0.0
        total_kg, total_units = 0.0, 0.0

        for v in range(num_vehicles):
            index = routing.Start(v)
            if routing.IsEnd(solution.Value(routing.NextVar(index))):
                continue  # vehículo no usado

            main_vehicle = vehicle_mapping[v]
            route_nodes, deliveries, dist_v, time_v = [], [], 0.0, 0

            while not routing.IsEnd(index):
                node = manager.IndexToNode(index)
                route_nodes.append(node)

                if node != depot:
                    ext = extended_locations[node]
                    loc_id   = ext.get("id")
                    demanda  = ext.get("demanda", {}) or {}
                    precios  = ext.get("precios", {}) or {}
                    pesos    = ext.get("pesos", {}) or {}
                    packs    = ext.get("unidades", {}) or {}

                    products_detail = []
                    stop_kg = 0.0
                    stop_units = 0.0

                    for pid_key, kg_val in demanda.items():
                        pid_str = str(pid_key)
                        try:
                            kg = float(kg_val)
                        except Exception:
                            kg = float(str(kg_val).replace(",", ".")) if kg_val is not None else 0.0

                        price = float(precios.get(pid_str, 0) or 0)
                        unit_weight = float(pesos.get(pid_str, 0) or 0)
                        pack_units  = int(packs.get(pid_str, 0) or 0)

                        units = (kg / unit_weight) if unit_weight > 0 else None
                        subtotal = kg * price

                        stop_kg += kg
                        if units is not None:
                            stop_units += units

                        products_detail.append({
                            "product_id": int(pid_str) if pid_str.isdigit() else pid_str,
                            "kg": round(kg, 2),
                            "price_unit": price,
                            "unit_weight_kg": (unit_weight if unit_weight > 0 else None),
                            "units": (round(units, 2) if units is not None else None),
                            "pack_units": (pack_units if pack_units > 0 else None),
                            "subtotal": round(subtotal, 2)
                        })

                    deliveries.append({
                        "location_id": loc_id,
                        "node_index": node,
                        "products": demanda,
                        "products_detail": products_detail,
                        "totals": {"kg": round(stop_kg, 2),
                                   "units": (round(stop_units, 2) if stop_units > 0 else None)}
                    })

                prev = index
                index = solution.Value(routing.NextVar(index))
                d = extended_distance_matrix[manager.IndexToNode(prev)][manager.IndexToNode(index)]
                dist_v += d
                total_distance += d

            time_v = solution.Value(time_dimension.CumulVar(routing.End(v)))
            fuel = dist_v / max(vehicle_consume[v], 1e-9)
            total_fuel_liters += fuel

            if not deliveries:
                continue

            trip_kg = sum((d["totals"]["kg"] for d in deliveries if d.get("totals")), 0.0)
            trip_units_vals = [d["totals"].get("units") for d in deliveries if d.get("totals")]
            trip_units = sum((u for u in trip_units_vals if u is not None), 0.0)

            raw_route = [0] + [extended_locations[n].get("id") if n else 0 for n in route_nodes] + [0]
            cleaned = [raw_route[0]]
            for n in raw_route[1:]:
                if not (n == 0 and cleaned[-1] == 0):
                    cleaned.append(n)

            agg = vehicle_trips.setdefault(main_vehicle, {
                "vehicle": main_vehicle,
                "trips": [],
                "total_distance": 0.0,
                "total_fuel_liters": 0.0,
                "total_kg": 0.0,
                "total_units": 0.0
            })

            agg["trips"].append({
                "route": cleaned,
                "deliveries": deliveries,
                "time_minutes": time_v,
                "distance": dist_v,
                "fuel_liters": fuel,
                "total_kg": round(trip_kg, 2),
                "total_units": (round(trip_units, 2) if trip_units > 0 else None)
            })

            agg["total_distance"] += dist_v
            agg["total_fuel_liters"] += fuel
            agg["total_kg"] += trip_kg
            agg["total_units"] += trip_units

            total_kg += trip_kg
            total_units += trip_units

        return jsonify({
            "status": "success",
            "total_distance": round(total_distance, 2),
            "total_fuel_liters": round(total_fuel_liters, 2),
            "vehicles_used": len(vehicle_trips),
            "assignments": list(vehicle_trips.values())
        })

    except Exception as e:
        import traceback
        return jsonify(error=f"Error interno: {str(e)}", traceback=traceback.format_exc()), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000, debug=False)
