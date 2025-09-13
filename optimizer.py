from flask import Flask, request, jsonify
from ortools.constraint_solver import pywrapcp, routing_enums_pb2
import copy
import math
import os
from datetime import datetime

app = Flask(__name__)

def _safe_set(obj, attr, value):
    """Setea un atributo protobuf si existe; si no, lo ignora sin romper."""
    try:
        setattr(obj, attr, value)
    except Exception:
        pass

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
        vehicle_palets_base = data.get("vehicle_palets", [0]*base_num_vehicles)
        vehicle_consume_base = data.get("vehicle_consume", [1]*base_num_vehicles)
        vehicle_free_base = data.get("vehicle_free", [0]*base_num_vehicles)
        distance_matrix = data["distance_matrix"]
        time_matrix = data.get("time_matrix")
        multiplicador_tiempo = float(data.get("multiplicador_tiempo", 1.0) or 1.0)
        if multiplicador_tiempo <= 0:
            return jsonify(error="multiplicador_tiempo debe ser > 0"), 400

        maximo_de_paradas = int(data.get("maximas_paradas_camion", 100))
        if maximo_de_paradas <= 0:
            return jsonify(error="maximas_paradas_camion debe ser > 0"), 400

        # Ventanas de tiempo: hora de salida del packing (referencia t=0)
        departure_time_str = data.get("departure_time")  # "HH:MM" o None

        def _parse_departure_minutes(hhmm: str) -> int:
            hh, mm = hhmm.split(":")
            return int(hh) * 60 + int(mm)

        departure_minutes0 = None
        if departure_time_str:
            try:
                departure_minutes0 = _parse_departure_minutes(departure_time_str)
            except Exception:
                return jsonify(error="Formato inválido en departure_time; esperado 'HH:MM'"), 400

        if len(vehicle_palets_base) != base_num_vehicles:
            return jsonify(error="La cantidad de palets por vehículo no coincide con max_vehicles"), 400

        PALLET_INF = 10**9
        try:
            vehicle_palets_base = [int(p) for p in vehicle_palets_base]
        except Exception:
            return jsonify(error="vehicle_palets debe ser una lista de enteros"), 400

        vehicle_palets_base = [(p if p > 0 else PALLET_INF) for p in vehicle_palets_base]

        # tiempos
        tiempo_calculo_min = data.get("tiempo_calculo", 2)  # minutos (default 2)
        tiempo_calculo = int(tiempo_calculo_min * 60)       # segundos
        HORIZON = int(data.get("max_time_per_trip", 480))   # tope de conducción por viaje y tope global
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
        vehicle_palets = []

        for idx in range(base_num_vehicles):
            for trip in range(max_trips_per_vehicle):
                for mode in MODES:
                    vehicle_capacities.append(vehicle_capacities_base[idx])
                    vehicle_consume.append(vehicle_consume_base[idx])
                    vehicle_free.append(vehicle_free_base[idx])
                    vehicle_palets.append(vehicle_palets_base[idx])
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
        extended_demands = [0]      # demanda total (kg)
        extended_palets = [0]       # depósito usa 0 palets
        extended_wait = [0.0]       # tiempo de espera por nodo (min)
        extended_deadline = [None]  # deadline relativo (min desde departure)

        split_mapping = {}
        extended_requires_refrigeration = [False]  # depósito no requiere

        for idx_loc, loc in enumerate(locations[1:], start=1):
            prod_quantities = {p: int(float(loc.get("demanda", {}).get(p, 0))) for p in all_products}
            total_demand = sum(prod_quantities.values())
            palets_total = int(float(loc.get("palets_en_suelo", 0) or 0))
            palets_total = max(0, palets_total)

            # espera y deadline
            wait_minutes = float(loc.get("wait_minutes", 0) or 0.0)
            wait_minutes = max(0.0, wait_minutes)

            arrival_time_iso = loc.get("arrival_time")  # "2000-01-01T03:03:00.000-03:00" o None
            deadline_minutes_rel = None
            if departure_minutes0 is not None and arrival_time_iso:
                try:
                    arr_dt = datetime.fromisoformat(arrival_time_iso)  # timezone-aware
                    arr_minutes = arr_dt.hour * 60 + arr_dt.minute
                    delta = arr_minutes - departure_minutes0
                    if delta < 0:
                        delta += 24 * 60  # permitir cruce de medianoche
                    deadline_minutes_rel = int(delta)
                except Exception:
                    deadline_minutes_rel = None

            identificador = (loc.get("identificador", "") or "").upper()
            requires_refrigeration = any(nombre.upper() in identificador for nombre in ubicaciones_refrigeradas)

            if total_demand <= max_vehicle_capacity:
                extended_locations.append(loc)
                extended_demands.append(int(total_demand))
                split_mapping[len(extended_locations)-1] = idx_loc
                extended_requires_refrigeration.append(requires_refrigeration)
                extended_palets.append(palets_total)
                extended_wait.append(wait_minutes)
                extended_deadline.append(deadline_minutes_rel)
            else:
                proportions = {p: (q / total_demand) if total_demand > 0 else 0 for p, q in prod_quantities.items()}
                remaining = total_demand
                remaining_per_product = prod_quantities.copy()
                remaining_palets = palets_total

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
                        frac = amount / max(remaining, 1)
                        palets_assigned = int(round(frac * remaining_palets))
                        palets_assigned = min(palets_assigned, remaining_palets)
                    else:
                        split_demand = remaining_per_product.copy()
                        palets_assigned = remaining_palets

                    split_loc["demanda"] = {p: str(split_demand[p]) for p in all_products if split_demand[p] > 0}
                    extended_locations.append(split_loc)
                    extended_demands.append(int(sum(split_demand.values())))
                    split_mapping[len(extended_locations)-1] = idx_loc
                    extended_requires_refrigeration.append(requires_refrigeration)
                    extended_palets.append(int(palets_assigned))
                    extended_wait.append(wait_minutes)               # hereda
                    extended_deadline.append(deadline_minutes_rel)   # hereda

                    remaining -= amount
                    remaining_palets -= palets_assigned

        num_nodes = len(extended_locations)

        # ----------- Agrupador EXACTO -----------
        def _group(identificador: str) -> str:
            ident = (identificador or "").upper()
            if any(k in ident for k in ["WALMART CD"]):
                return "WALMART"
            if any(k in ident for k in ["CENCOSUD CD"]):
                return "CENCOSUD"
            return "OTHER"

        node_group = [_group(loc.get("identificador", "")) for loc in extended_locations]
        HIGH_PENALTY = 100_000  # (nota: con exclusividad dura, esto es redundante)

        # Prioridad escalonada: PA > LV > (TOTTUS CD | UNIMARC CD) > otros (solo en OTHER)
        prioridad_pa_tag = "PUNTO AZUL"
        prioridad_lv_tag = "LA VEGA"
        prioridad_tottus_tag = "TOTTUS CD"
        prioridad_unimarc_tag = "UNIMARC CD"

        is_pa_node, is_lv_node, is_tottus_node, is_unimarc_node, is_priority_node = [], [], [], [], []
        for loc in extended_locations:
            ident = (loc.get("identificador", "") or "").upper()
            is_pa = prioridad_pa_tag in ident
            is_lv = prioridad_lv_tag in ident
            is_tot = prioridad_tottus_tag in ident
            is_uni = prioridad_unimarc_tag in ident
            is_pa_node.append(is_pa)
            is_lv_node.append(is_lv)
            is_tottus_node.append(is_tot)
            is_unimarc_node.append(is_uni)
            is_priority_node.append(is_pa or is_lv or is_tot or is_uni)

        any_pa_exists = any(is_pa_node[1:])
        any_lv_exists = any(is_lv_node[1:])
        any_tier3_exists = any((is_tottus_node[i] or is_unimarc_node[i]) for i in range(1, len(is_tottus_node)))

        def is_tier3(i):
            return is_tottus_node[i] or is_unimarc_node[i]

        # Pesos de penalización
        START_PENALTY_OTHER_WITH_PA = 90_000
        START_PENALTY_TIER3_WITH_PA = 60_000
        START_PENALTY_LV_WITH_PA    = 35_000

        START_PENALTY_OTHER_WITH_LV = 60_000
        START_PENALTY_TIER3_WITH_LV = 25_000

        START_PENALTY_OTHER_WITH_TIER3 = 45_000

        PA_LATE_ENTRY_PENALTY       = 100_000
        LV_LATE_ENTRY_PENALTY       = 70_000
        TIER3_LATE_ENTRY_PENALTY    = 45_000

        PA_AFTER_TIER3_EXTRA_PENALTY = 70_000
        LV_AFTER_TIER3_EXTRA_PENALTY = 40_000

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

        if extended_time_matrix is None:
            return jsonify(error="Se requiere 'time_matrix' para limitar el tiempo total por vehículo a 480 minutos."), 400

        # Escalar la matriz de tiempos
        if multiplicador_tiempo != 1.0:
            extended_time_matrix = [
                [cell * multiplicador_tiempo for cell in row]
                for row in extended_time_matrix
            ]

        # OR-Tools
        manager = pywrapcp.RoutingIndexManager(num_nodes, num_vehicles, depot)
        routing = pywrapcp.RoutingModel(manager)

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
            else:
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

                    if node_group[to_node] == "OTHER":
                        # Al salir del depósito
                        if to_node != depot and from_node == depot:
                            if any_pa_exists:
                                if is_pa_node[to_node]:
                                    pass
                                elif is_lv_node[to_node]:
                                    base += START_PENALTY_LV_WITH_PA
                                elif is_tier3(to_node):
                                    base += START_PENALTY_TIER3_WITH_PA
                                else:
                                    base += START_PENALTY_OTHER_WITH_PA
                            elif any_lv_exists:
                                if is_lv_node[to_node]:
                                    pass
                                elif is_tier3(to_node):
                                    base += START_PENALTY_TIER3_WITH_LV
                                else:
                                    base += START_PENALTY_OTHER_WITH_LV
                            elif any_tier3_exists:
                                if not is_tier3(to_node):
                                    base += START_PENALTY_OTHER_WITH_TIER3

                        # Dentro de la ruta
                        if from_node != depot and to_node != depot:
                            if is_pa_node[to_node] and not is_pa_node[from_node]:
                                base += PA_LATE_ENTRY_PENALTY
                            if is_lv_node[to_node] and (not is_lv_node[from_node] and not is_pa_node[from_node] and not is_tier3(from_node)):
                                base += LV_LATE_ENTRY_PENALTY
                            if is_tier3(to_node) and (not is_pa_node[from_node] and not is_lv_node[from_node] and not is_tier3(from_node)):
                                base += TIER3_LATE_ENTRY_PENALTY
                            if is_tier3(from_node) and is_pa_node[to_node]:
                                base += PA_AFTER_TIER3_EXTRA_PENALTY
                            if is_tier3(from_node) and is_lv_node[to_node]:
                                base += LV_AFTER_TIER3_EXTRA_PENALTY

                    # Mezcla (redundante con modos)
                    if from_node != depot and to_node != depot:
                        g_from, g_to = node_group[from_node], node_group[to_node]
                        if g_from != g_to and ("WALMART" in (g_from, g_to) or "CENCOSUD" in (g_from, g_to)):
                            base += HIGH_PENALTY

                    return base
                return distance
            callback_idx = routing.RegisterTransitCallback(make_vehicle_callback(v))
            routing.SetArcCostEvaluatorOfVehicle(callback_idx, v)

        # Demanda (capacidad en kg)
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

        # ======= Dimensión de Palets (capacidad independiente) =======
        def palet_demand_callback(from_index):
            node = manager.IndexToNode(from_index)
            return int(extended_palets[node])

        palet_cb_idx = routing.RegisterUnaryTransitCallback(palet_demand_callback)
        routing.AddDimensionWithVehicleCapacity(
            palet_cb_idx,
            0,
            vehicle_palets,
            True,
            "Palets"
        )
        palet_dimension = routing.GetDimensionOrDie("Palets")

        # ===== Time (ventanas/secuencia): viaje + wait + reload =====
        start_indices = set(routing.Start(v) for v in range(num_vehicles))

        def time_callback(from_index, to_index):
            from_node = manager.IndexToNode(from_index)
            to_node   = manager.IndexToNode(to_index)
            travel = int(round(extended_time_matrix[from_node][to_node]))
            service = 0
            # Recarga al salir del depósito en viajes subsiguientes
            if from_node == depot and from_index not in start_indices:
                service += reload_service_time
            # Espera propia del nodo de salida (no aplicar en depósito)
            if from_node != depot:
                service += int(round(extended_wait[from_node]))
            return travel + service

        time_callback_index = routing.RegisterTransitCallback(time_callback)
        routing.AddDimension(
            time_callback_index,
            0,
            10**7,  # grande; el tope de 8h va en Drive
            True,
            "Time"
        )
        time_dimension = routing.GetDimensionOrDie("Time")

        # Ventanas: llegada + espera <= deadline relativo (si hay departure_time y arrival_time)
        if departure_minutes0 is not None:
            for node in range(1, num_nodes):
                if extended_deadline[node] is not None:
                    idx = manager.NodeToIndex(node)
                    ub = max(0, extended_deadline[node] - int(round(extended_wait[node])))
                    time_dimension.CumulVar(idx).SetRange(0, ub)

        # ======= Dimensión de Paradas =======
        def stop_callback(from_index, to_index):
            """Cuenta 1 cada vez que se visita un nodo distinto del depósito."""
            to_node = manager.IndexToNode(to_index)
            return 1 if to_node != depot else 0

        stop_cb_idx = routing.RegisterTransitCallback(stop_callback)
        routing.AddDimension(
            stop_cb_idx,
            0,
            maximo_de_paradas,
            True,
            "Stops"
        )
        stops_dimension = routing.GetDimensionOrDie("Stops")
        for v in range(num_vehicles):
            stops_dimension.CumulVar(routing.End(v)).SetMax(maximo_de_paradas)

        # ===== Drive (tope 8h): SOLO viaje, sin esperas ni reload =====
        def drive_callback(from_index, to_index):
            from_node = manager.IndexToNode(from_index)
            to_node   = manager.IndexToNode(to_index)
            return int(round(extended_time_matrix[from_node][to_node]))

        drive_cb_idx = routing.RegisterTransitCallback(drive_callback)
        routing.AddDimension(
            drive_cb_idx,
            0,
            HORIZON,  # p.ej., 480
            True,
            "Drive"
        )
        drive_dimension = routing.GetDimensionOrDie("Drive")

        # Tope por duplicado
        for v in range(num_vehicles):
            drive_dimension.CumulVar(routing.End(v)).SetMax(HORIZON)

        # Tope GLOBAL por vehículo real = suma de conducción de sus duplicados <= HORIZON
        solver = routing.solver()
        for base in range(base_num_vehicles):
            end_cumuls = [drive_dimension.CumulVar(routing.End(v))
                          for v in range(num_vehicles) if vehicle_mapping[v] == base]
            solver.Add(solver.Sum(end_cumuls) <= HORIZON)

        # (Opcional) compactar makespan usando la dimensión Time (que sí incluye esperas)
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

        # ----- MULTIHILO DEL SOLVER (asignación segura) -----
        req_workers = data.get("search_workers")
        if req_workers is None:
            req_workers = os.environ.get("ORTOOLS_WORKERS")
        try:
            req_workers = int(req_workers) if req_workers is not None else 0
        except Exception:
            req_workers = 0
        if req_workers <= 0:
            req_workers = min(32, os.cpu_count() or 1)
        _safe_set(search_parameters, "number_of_workers", req_workers)

        # Log de búsqueda opcional
        _safe_set(search_parameters, "log_search", bool(data.get("log_search", False)))

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
            route_nodes, deliveries, dist_v = [], [], 0.0

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

                    stop_palets = extended_palets[node]  # palets para esta sub-parada

                    deliveries.append({
                        "location_id": loc_id,
                        "node_index": node,
                        "products": demanda,
                        "products_detail": products_detail,
                        "totals": {
                            "kg": round(stop_kg, 2),
                            "units": (round(stop_units, 2) if stop_units > 0 else None),
                            "palets": int(stop_palets)
                        }
                    })

                prev = index
                index = solution.Value(routing.NextVar(index))
                d = extended_distance_matrix[manager.IndexToNode(prev)][manager.IndexToNode(index)]
                dist_v += d
                total_distance += d

            # tiempos por ruta
            time_total = solution.Value(time_dimension.CumulVar(routing.End(v)))   # viaje + esperas + reload
            time_drive = solution.Value(drive_dimension.CumulVar(routing.End(v)))  # SOLO viaje

            fuel = dist_v / max(vehicle_consume[v], 1e-9)
            total_fuel_liters += fuel

            if not deliveries:
                continue

            trip_kg = sum((d["totals"]["kg"] for d in deliveries if d.get("totals")), 0.0)
            trip_units_vals = [d["totals"].get("units") for d in deliveries if d.get("totals")]
            trip_units = sum((u for u in trip_units_vals if u is not None), 0.0)
            trip_palets = sum(int(d["totals"].get("palets", 0)) for d in deliveries if d.get("totals"))

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
                "total_units": 0.0,
                "total_palets": 0
            })

            agg["trips"].append({
                "route": cleaned,
                "deliveries": deliveries,
                "time_minutes_total": int(time_total),  # viaje + esperas + reload
                "time_minutes_drive": int(time_drive),  # SOLO viaje
                "distance": dist_v,
                "fuel_liters": fuel,
                "total_kg": round(trip_kg, 2),
                "total_units": (round(trip_units, 2) if trip_units > 0 else None),
                "total_palets": int(trip_palets)
            })

            agg["total_distance"] += dist_v
            agg["total_fuel_liters"] += fuel
            agg["total_kg"] += trip_kg
            agg["total_units"] += trip_units
            agg["total_palets"] += int(trip_palets)

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
        # Además del JSON, imprime al log del contenedor para ver en Railway
        traceback.print_exc()
        return jsonify(error=f"Error interno: {str(e)}", traceback=traceback.format_exc()), 500

if __name__ == "__main__":
    # En Railway, usa Gunicorn. Esto es solo para local.
    app.run(host="0.0.0.0", port=3000, debug=False)
