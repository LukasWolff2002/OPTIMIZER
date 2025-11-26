from flask import Flask, request, jsonify
from ortools.constraint_solver import pywrapcp, routing_enums_pb2
import copy
import os
from datetime import datetime

app = Flask(__name__)

# ---------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------

def _safe_set(obj, attr, value):
    """Setea un atributo protobuf si existe; si no, lo ignora sin romper."""
    try:
        setattr(obj, attr, value)
    except Exception:
        pass

def _parse_departure_minutes(hhmm: str) -> int:
    """Convierte 'HH:MM' a minutos desde 00:00."""
    hh, mm = hhmm.split(":")
    return int(hh) * 60 + int(mm)

def _fmt_hhmm(total_minutes: int) -> str:
    """Formatea minutos (mod 24h) a 'HH:MM'."""
    total_minutes %= (24 * 60)
    hh = total_minutes // 60
    mm = total_minutes % 60
    return f"{hh:02d}:{mm:02d}"

# ---------------------------------------------------------------------
# API
# ---------------------------------------------------------------------

@app.route("/optimize", methods=["POST"])
def optimize():
    """
    Resuelve un VRP con:
      - Capacidad por kg (Capacity) + palets (Palets)  [AND]
      - Duplicación de vehículos por (viajes x modos) y exclusividad por grupo
      - Dos dimensiones de tiempo:
         * Time  = viaje + espera + reload  [para ventanas/secuenciación]
         * Drive = SOLO viaje               [para tope de 8h]
      - Ventanas: 'departure_time' (HH:MM) + 'open_time'/'arrival_time' (ISO/HH:MM) por nodo
        ⇒ open_time - gap_time <= llegada <= (close_time - gap_time - wait) relativo a departure_time
      - Límite de paradas por vehículo (Stops)
    """
    # ---------------------- Datos fijos de negocio ---------------------
    ubicaciones_refrigeradas = [
        "WALMART CD", "CENCOSUD CD", "SANTA ISABEL LOCAL", "JUMBO LOCAL",
        "TOTTUS CD", "TOTTUS LOCAL", "UNIMARC CD", "UNIMARC LOCAL",
        "ARAMARK", "SODEXO"
    ]

    # ------------------------- Parse de entrada ------------------------
    raw_data = request.get_json()
    if raw_data is None:
        return jsonify(error="No se recibió JSON válido"), 400

    # Compat: payload puede venir como [data, truck_ids, user_id]
    if isinstance(raw_data, list):
        if len(raw_data) != 3:
            return jsonify(error="Se esperaba [data, truck_ids, user_id]"), 400
        data, truck_ids, user_id = raw_data
    else:
        data, truck_ids, user_id = raw_data, [], None

    try:
        # --- Datos base requeridos ---
        locations = data["locations"]
        base_num_vehicles = data["max_vehicles"]
        vehicle_capacities_base = data["vehicle_capacities"]
        distance_matrix = data["distance_matrix"]
        time_matrix = data.get("time_matrix")

        # --- Opcionales / default ---
        vehicle_palets_base = data.get("vehicle_palets", [0] * base_num_vehicles)
        vehicle_consume_base = data.get("vehicle_consume", [1] * base_num_vehicles)
        vehicle_free_base = data.get("vehicle_free", [0] * base_num_vehicles)
        multiplicador_tiempo = float(data.get("multiplicador_tiempo", 1.0) or 1.0)
        if multiplicador_tiempo <= 0:
            return jsonify(error="multiplicador_tiempo debe ser > 0"), 400

        maximo_de_paradas = int(data.get("maximas_paradas_camion", 100))
        if maximo_de_paradas <= 0:
            return jsonify(error="maximas_paradas_camion debe ser > 0"), 400

        # tiempos (min)
        tiempo_calculo_min = float(data.get("tiempo_calculo", 2))  # minutos
        tiempo_calculo = int(tiempo_calculo_min * 60)              # segundos
        HORIZON = int(data.get("max_time_per_trip", 480))          # tope de conducción (Drive)
        reload_service_time = int(data.get("reload_service_time", 0))

        # Ventanas: hora de salida común (t=0)
        departure_time_str = data.get("departure_time")  # "HH:MM" o None
        departure_minutes0 = None
        if departure_time_str:
            try:
                departure_minutes0 = _parse_departure_minutes(departure_time_str)
            except Exception:
                return jsonify(error="Formato inválido en departure_time; esperado 'HH:MM'"), 400

        # Validaciones tamaño
        if len(vehicle_capacities_base) != base_num_vehicles:
            return jsonify(error="capacidades no coinciden con max_vehicles"), 400
        if len(vehicle_consume_base) != base_num_vehicles:
            return jsonify(error="consumos no coinciden con max_vehicles"), 400
        if len(vehicle_free_base) != base_num_vehicles:
            return jsonify(error="vehicle_free no coincide con max_vehicles"), 400
        if len(vehicle_palets_base) != base_num_vehicles:
            return jsonify(error="palets por vehículo no coincide con max_vehicles"), 400

        # Palets: mapear 0 → "sin límite" grande
        PALLET_INF = 10**9
        try:
            vehicle_palets_base = [int(p) for p in vehicle_palets_base]
        except Exception:
            return jsonify(error="vehicle_palets debe ser lista de enteros"), 400
        vehicle_palets_base = [(p if p > 0 else PALLET_INF) for p in vehicle_palets_base]

        # --------------------------- Modos (rutas puras) ---------------------------
        MODE_FREE = 0   # OTHER
        MODE_W    = 1   # WALMART
        MODE_C    = 2   # CENCOSUD
        MODES = (MODE_FREE, MODE_W, MODE_C)

        # -------------------- Duplicación de vehículos ----------------------------
        vehicle_capacities, vehicle_consume, vehicle_free, vehicle_palets = [], [], [], []
        vehicle_mapping, vehicle_trip_no, vehicle_mode = {}, {}, {}
        max_trips_per_vehicle = 1  # si quieres 2 viajes, cambiar a 2

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

        # --------------------- Construcción de nodos extendidos -------------------
        # Recolecta catálogo de productos
        all_products = set()
        for loc in locations:
            all_products.update(loc.get("demanda", {}).keys())
        all_products = sorted(all_products)

        # Buffers extendidos (índice 0 = depósito)
        extended_locations   = [locations[0]]
        extended_demands     = [0]        # kg
        extended_palets      = [0]        # palets
        extended_wait        = [0.0]      # min
        extended_deadline    = [None]     # min relativo a t=0
        extended_opening     = [None]     # min relativo a t=0 (apertura)
        extended_gap         = [0]        # gap_time por nodo (min)
        extended_refrigerate = [False]    # flag
        split_mapping        = {}         # idx extendido -> idx original

        # Helper: agrupar WALMART/CENCOSUD/OTHER
        def _group(identificador: str) -> str:
            ident = (identificador or "").upper()
            if "WALMART CD" in ident:
                return "WALMART"
            if "CENCOSUD CD" in ident:
                return "CENCOSUD"
            return "OTHER"

        for idx_loc, loc in enumerate(locations[1:], start=1):
            # Demanda por producto (kg) y totales
            prod_quantities = {p: int(float(loc.get("demanda", {}).get(p, 0))) for p in all_products}
            total_demand = sum(prod_quantities.values())

            # Palets por ubicación
            palets_total = int(float(loc.get("palets_en_suelo", 0) or 0))
            palets_total = max(0, palets_total)

            # Espera por ubicación
            wait_minutes = float(loc.get("wait_minutes", 0) or 0.0)
            wait_minutes = max(0.0, wait_minutes)

            # Gap por ubicación (min) -> mismo para apertura y cierre
            gap_time = int(loc.get("gap_time", 0) or 0)
            if gap_time < 0:
                gap_time = 0

            # Deadline relativo si hay arrival_time + departure_time
            arrival_time_iso = loc.get("arrival_time")
            deadline_minutes_rel = None
            if departure_minutes0 is not None and arrival_time_iso:
                try:
                    arr_dt = datetime.fromisoformat(arrival_time_iso)  # soporta offset
                    arr_minutes = arr_dt.hour * 60 + arr_dt.minute
                    delta = arr_minutes - departure_minutes0
                    if delta < 0:
                        delta += 24 * 60
                    deadline_minutes_rel = int(delta)
                except Exception:
                    deadline_minutes_rel = None

            # Apertura relativa si hay open_time + departure_time
            open_time_raw = loc.get("open_time")
            opening_minutes_rel = None
            if departure_minutes0 is not None and open_time_raw:
                try:
                    # Si viene como ISO, úsalo como arrival_time
                    if "T" in str(open_time_raw):
                        open_dt = datetime.fromisoformat(open_time_raw)
                        open_minutes = open_dt.hour * 60 + open_dt.minute
                    else:
                        # Si viene como "HH:MM"
                        open_minutes = _parse_departure_minutes(str(open_time_raw))
                    delta_open = open_minutes - departure_minutes0
                    if delta_open < 0:
                        delta_open += 24 * 60
                    opening_minutes_rel = int(delta_open)
                except Exception:
                    opening_minutes_rel = None

            # Refrigeración por identificador
            identificador = (loc.get("identificador", "") or "").upper()
            requires_refrigeration = any(
                name in identificador for name in [s.upper() for s in ubicaciones_refrigeradas]
            )

            # Split si excede capacidad de kg
            if total_demand <= max_vehicle_capacity:
                extended_locations.append(loc)
                extended_demands.append(int(total_demand))
                extended_palets.append(palets_total)
                extended_wait.append(wait_minutes)
                extended_deadline.append(deadline_minutes_rel)
                extended_opening.append(opening_minutes_rel)
                extended_gap.append(gap_time)
                extended_refrigerate.append(requires_refrigeration)
                split_mapping[len(extended_locations) - 1] = idx_loc
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

                    split_loc["demanda"] = {
                        p: str(split_demand[p]) for p in all_products if split_demand[p] > 0
                    }

                    extended_locations.append(split_loc)
                    extended_demands.append(int(sum(split_demand.values())))
                    extended_palets.append(int(palets_assigned))
                    extended_wait.append(wait_minutes)              # hereda
                    extended_deadline.append(deadline_minutes_rel)  # hereda
                    extended_opening.append(opening_minutes_rel)    # hereda
                    extended_gap.append(gap_time)                   # hereda
                    extended_refrigerate.append(requires_refrigeration)
                    split_mapping[len(extended_locations) - 1] = idx_loc

                    remaining -= amount
                    remaining_palets -= palets_assigned

        num_nodes = len(extended_locations)

        # Map de grupos por nodo extendido
        node_group = [_group(loc.get("identificador", "")) for loc in extended_locations]

        # ---------------------------- Matrices extendidas --------------------------
        def extend_matrix(base_matrix):
            new_matrix = [[0] * num_nodes for _ in range(num_nodes)]
            for i in range(num_nodes):
                for j in range(num_nodes):
                    orig_i = 0 if i == 0 else split_mapping.get(i, i)
                    orig_j = 0 if j == 0 else split_mapping.get(j, j)
                    new_matrix[i][j] = base_matrix[orig_i][orig_j]
            return new_matrix

        extended_distance_matrix = extend_matrix(distance_matrix)
        extended_time_matrix = extend_matrix(time_matrix) if time_matrix else None
        if extended_time_matrix is None:
            return jsonify(error="Se requiere 'time_matrix' para limitar tiempos."), 400

        # Escala tiempos por multiplicador
        if multiplicador_tiempo != 1.0:
            extended_time_matrix = [
                [cell * multiplicador_tiempo for cell in row]
                for row in extended_time_matrix
            ]

        # ------------------------------- OR-Tools ---------------------------------
        manager = pywrapcp.RoutingIndexManager(num_nodes, num_vehicles, depot)
        routing = pywrapcp.RoutingModel(manager)

        # (a) Refrigeración: sólo vehículos 'free' pueden atender esos nodos
        for node_index in range(1, num_nodes):
            if extended_refrigerate[node_index]:
                node_idx = manager.NodeToIndex(node_index)
                for vehicle_id in range(num_vehicles):
                    if not vehicle_free[vehicle_id]:
                        routing.VehicleVar(node_idx).RemoveValue(vehicle_id)

        # (b) Exclusividad por modo (rutas puras)
        MODE_FREE, MODE_W, MODE_C = 0, 1, 2
        for node_index in range(1, num_nodes):
            g = node_group[node_index]
            node_idx = manager.NodeToIndex(node_index)
            if g == "WALMART":
                allowed = {MODE_W}
            elif g == "CENCOSUD":
                allowed = {MODE_C}
            else:
                allowed = {MODE_FREE}
            for v in range(num_vehicles):
                if vehicle_mode[v] not in allowed:
                    routing.VehicleVar(node_idx).RemoveValue(v)

        # (c) Costos (combustible + penalizaciones de prioridad)
        HIGH_PENALTY = 100_000
        prioridad_pa_tag = "PUNTO AZUL"
        prioridad_lv_tag = "LA VEGA"
        prioridad_tottus_tag = "TOTTUS CD"
        prioridad_unimarc_tag = "UNIMARC CD"

        is_pa_node, is_lv_node, is_tottus_node, is_unimarc_node = [], [], [], []
        for loc in extended_locations:
            ident = (loc.get("identificador", "") or "").upper()
            is_pa_node.append(prioridad_pa_tag in ident)
            is_lv_node.append(prioridad_lv_tag in ident)
            is_tottus_node.append(prioridad_tottus_tag in ident)
            is_unimarc_node.append(prioridad_unimarc_tag in ident)
        any_pa_exists = any(is_pa_node[1:])
        any_lv_exists = any(is_lv_node[1:])
        any_tier3_exists = any(
            (is_tottus_node[i] or is_unimarc_node[i]) for i in range(1, len(is_tottus_node))
        )

        def is_tier3(i):
            return is_tottus_node[i] or is_unimarc_node[i]

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

        for v in range(num_vehicles):
            def make_vehicle_callback(v_idx):
                def distance(from_index, to_index, rate=vehicle_consume[v_idx]):
                    from_node = manager.IndexToNode(from_index)
                    to_node   = manager.IndexToNode(to_index)
                    base_dist = extended_distance_matrix[from_node][to_node]
                    base = max(0, int(round((base_dist / max(rate, 1e-9)) * 1000)))

                    if node_group[to_node] == "OTHER":
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
                        if from_node != depot and to_node != depot:
                            if is_pa_node[to_node] and not is_pa_node[from_node]:
                                base += PA_LATE_ENTRY_PENALTY
                            if is_lv_node[to_node] and (
                                not is_lv_node[from_node]
                                and not is_pa_node[from_node]
                                and not is_tier3(from_node)
                            ):
                                base += LV_LATE_ENTRY_PENALTY
                            if is_tier3(to_node) and (
                                not is_pa_node[from_node]
                                and not is_lv_node[from_node]
                                and not is_tier3(from_node)
                            ):
                                base += TIER3_LATE_ENTRY_PENALTY
                            if is_tier3(from_node) and is_pa_node[to_node]:
                                base += PA_AFTER_TIER3_EXTRA_PENALTY
                            if is_tier3(from_node) and is_lv_node[to_node]:
                                base += LV_AFTER_TIER3_EXTRA_PENALTY

                    # Penalización por mezclar (redundante con modos, se mantiene por compat)
                    if from_node != depot and to_node != depot:
                        g_from, g_to = node_group[from_node], node_group[to_node]
                        if g_from != g_to and ("WALMART" in (g_from, g_to) or "CENCOSUD" in (g_from, g_to)):
                            base += HIGH_PENALTY
                    return base
                return distance
            callback_idx = routing.RegisterTransitCallback(make_vehicle_callback(v))
            routing.SetArcCostEvaluatorOfVehicle(callback_idx, v)

        # ------------------------ Dimensiones de capacidad -------------------------
        # Kg
        def demand_callback(from_index):
            node = manager.IndexToNode(from_index)
            return int(extended_demands[node])
        demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
        routing.AddDimensionWithVehicleCapacity(
            demand_callback_index, 0, vehicle_capacities, True, "Capacity"
        )

        # Palets
        def palet_demand_callback(from_index):
            node = manager.IndexToNode(from_index)
            return int(extended_palets[node])
        palet_cb_idx = routing.RegisterUnaryTransitCallback(palet_demand_callback)
        routing.AddDimensionWithVehicleCapacity(
            palet_cb_idx, 0, vehicle_palets, True, "Palets"
        )

        # ------------------------ Dimensiones de tiempo ----------------------------
        # Time (viaje + espera + reload) ⇒ ventanas y secuenciación
        start_indices = set(routing.Start(v) for v in range(num_vehicles))

        def time_callback(from_index, to_index):
            from_node = manager.IndexToNode(from_index)
            to_node   = manager.IndexToNode(to_index)
            travel = int(round(extended_time_matrix[from_node][to_node]))
            service = 0
            if from_node == depot and from_index not in start_indices:
                service += reload_service_time  # reload solo afecta Time
            if from_node != depot:
                service += int(round(extended_wait[from_node]))
            return travel + service

        time_cb = routing.RegisterTransitCallback(time_callback)
        routing.AddDimension(time_cb, 0, 10**7, True, "Time")
        time_dimension = routing.GetDimensionOrDie("Time")

        # Ventanas: open_time - gap <= arrival <= (deadline - gap - wait)
        if departure_minutes0 is not None:
            for node in range(1, num_nodes):
                idx = manager.NodeToIndex(node)
                lb = 0          # lower bound por defecto (sin apertura)
                ub = 10**7      # upper bound muy grande por defecto

                gap_here = int(extended_gap[node]) if extended_gap[node] is not None else 0
                if gap_here < 0:
                    gap_here = 0

                # Límite inferior: apertura desplazada hacia atrás por gap
                if extended_opening[node] is not None:
                    eff_open = int(extended_opening[node]) - gap_here
                    lb = max(lb, max(0, eff_open))

                # Límite superior: cierre desplazado hacia atrás por gap
                if extended_deadline[node] is not None:
                    wait_here = int(round(extended_wait[node]))
                    eff_deadline = int(extended_deadline[node]) - gap_here
                    ub_deadline = eff_deadline - wait_here
                    ub = min(ub, max(0, ub_deadline))

                if lb <= ub:
                    time_dimension.CumulVar(idx).SetRange(lb, ub)
                else:
                    # ventana inconsistente → se fuerza a un valor imposible (hará el problema infactible)
                    time_dimension.CumulVar(idx).SetRange(lb, lb)

        # Drive (solo viaje) ⇒ tope de 8h
        def drive_callback(from_index, to_index):
            from_node = manager.IndexToNode(from_index)
            to_node   = manager.IndexToNode(to_index)
            return int(round(extended_time_matrix[from_node][to_node]))

        drive_cb = routing.RegisterTransitCallback(drive_callback)
        routing.AddDimension(drive_cb, 0, HORIZON, True, "Drive")
        drive_dimension = routing.GetDimensionOrDie("Drive")

        # Límite por duplicado y global por camión real
        for v in range(num_vehicles):
            drive_dimension.CumulVar(routing.End(v)).SetMax(HORIZON)
        solver = routing.solver()
        for base in range(base_num_vehicles):
            end_cumuls = [
                drive_dimension.CumulVar(routing.End(v))
                for v in range(num_vehicles) if vehicle_mapping[v] == base
            ]
            solver.Add(solver.Sum(end_cumuls) <= HORIZON)

        # Stops (conteo de visitas)
        def stop_callback(from_index, to_index):
            to_node = manager.IndexToNode(to_index)
            return 1 if to_node != depot else 0

        stop_cb = routing.RegisterTransitCallback(stop_callback)
        routing.AddDimension(stop_cb, 0, maximo_de_paradas, True, "Stops")
        stops_dimension = routing.GetDimensionOrDie("Stops")
        for v in range(num_vehicles):
            stops_dimension.CumulVar(routing.End(v)).SetMax(maximo_de_paradas)

        # Compactar uso de Time
        time_dimension.SetGlobalSpanCostCoefficient(50)

        # Costo fijo por viajes adicionales (duplicados trip>0)
        costo_varias_rutas = True
        costo_reingreso_valor = int(data.get("costo_reingreso_valor", 100_000))
        if costo_varias_rutas:
            for v in range(num_vehicles):
                if vehicle_trip_no[v] > 0:
                    routing.SetFixedCostOfVehicle(costo_reingreso_valor, v)

        # ------------------------ Resolución del modelo ----------------------------
        search_parameters = pywrapcp.DefaultRoutingSearchParameters()
        search_parameters.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
        )
        search_parameters.local_search_metaheuristic = (
            routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
        )
        search_parameters.time_limit.FromSeconds(tiempo_calculo)

        # Multihilo
        req_workers = data.get("search_workers")
        if req_workers is None:
            req_workers = 4
        try:
            req_workers = int(req_workers) if req_workers is not None else 0
        except Exception:
            req_workers = 0
        if req_workers <= 0:
            req_workers = min(32, os.cpu_count() or 1)
        _safe_set(search_parameters, "number_of_workers", req_workers)
        _safe_set(search_parameters, "log_search", bool(data.get("log_search", False)))

        solution = routing.SolveWithParameters(search_parameters)
        if not solution:
            return jsonify(error="No se pudo encontrar solución."), 400

        # ----------------------------- Extracción -------------------------------
        vehicle_trips = {}
        total_distance, total_fuel_liters = 0.0, 0.0
        total_kg, total_units, total_palets_sum = 0.0, 0.0, 0

        total_time_minutes_total = 0      # viaje + esperas + reload (suma de todos los vehículos)
        total_time_minutes_drive = 0      # sólo conducción (Drive)
        total_stops_global = 0

        # Helper para leer cumul por nodo índice de enrutador
        def cumul(dim, idx):
            return solution.Value(dim.CumulVar(idx))

        for v in range(num_vehicles):
            start = routing.Start(v)
            if routing.IsEnd(solution.Value(routing.NextVar(start))):
                continue  # vehículo no usado

            main_vehicle = vehicle_mapping[v]
            trip_no = vehicle_trip_no[v]
            mode = vehicle_mode[v]

            route_nodes = []
            deliveries = []
            dist_v = 0.0

            index = start
            while not routing.IsEnd(index):
                node = manager.IndexToNode(index)
                route_nodes.append(node)

                # Datos de visita (si no es depósito)
                if node != depot:
                    ext = extended_locations[node]
                    loc_id   = ext.get("id")
                    demanda  = ext.get("demanda", {}) or {}
                    precios  = ext.get("precios", {}) or {}
                    pesos    = ext.get("pesos", {}) or {}
                    packs    = ext.get("unidades", {}) or {}

                    # Totales de productos
                    products_detail = []
                    stop_kg, stop_units = 0.0, 0.0
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

                    # Cúmulos en este nodo (ARRIVAL)
                    idx = index
                    time_cumul = cumul(time_dimension, idx)     # incluye waits previos + viajes
                    drive_cumul = cumul(drive_dimension, idx)   # sólo viajes
                    stops_cumul = cumul(stops_dimension, idx)
                    wait_here = int(round(extended_wait[node]))
                    gap_here = int(extended_gap[node]) if extended_gap[node] is not None else 0
                    if gap_here < 0:
                        gap_here = 0

                    # Ventana / slack (respecto a cierre efectivo)
                    deadline_rel = extended_deadline[node]
                    deadline_ub_eff = None
                    deadline_slack = None
                    if deadline_rel is not None:
                        eff_deadline = int(deadline_rel) - gap_here
                        deadline_ub_eff = max(0, eff_deadline - wait_here)
                        deadline_slack = deadline_ub_eff - time_cumul

                    # Horas absolutas (si hay departure_time)
                    eta_clock = _fmt_hhmm(departure_minutes0 + time_cumul) if departure_minutes0 is not None else None
                    etd_clock = _fmt_hhmm(departure_minutes0 + time_cumul + wait_here) if departure_minutes0 is not None else None

                    deliveries.append({
                        "location_id": loc_id,
                        "identificador": ext.get("identificador"),
                        "node_index": node,
                        "group": node_group[node],
                        "requires_refrigeration": bool(extended_refrigerate[node]),
                        "products": demanda,
                        "products_detail": products_detail,
                        "totals": {
                            "kg": round(stop_kg, 2),
                            "units": (round(stop_units, 2) if stop_units > 0 else None),
                            "palets": int(extended_palets[node])
                        },
                        "timing": {
                            "arrival_minutes_from_departure": int(time_cumul),
                            "departure_minutes_from_departure": int(time_cumul + wait_here),
                            # cierre real y cierre efectivo
                            "deadline_minutes_from_departure": (
                                int(deadline_rel) if deadline_rel is not None else None
                            ),
                            "latest_arrival_allowed_minutes_from_departure": (
                                int(deadline_ub_eff) if deadline_ub_eff is not None else None
                            ),
                            "deadline_slack_minutes": (
                                int(deadline_slack) if deadline_slack is not None else None
                            ),
                            "eta_clock": eta_clock,
                            "etd_clock": etd_clock,
                            "wait_minutes": int(wait_here),
                            "opening_minutes_from_departure": (
                                int(extended_opening[node]) if extended_opening[node] is not None else None
                            ),
                            "open_clock": (
                                _fmt_hhmm(departure_minutes0 + extended_opening[node])
                                if (departure_minutes0 is not None and extended_opening[node] is not None)
                                else None
                            ),
                            "gap_time_minutes": int(gap_here)
                        },
                        "cumul": {
                            "time_cumul_minutes": int(time_cumul),
                            "drive_cumul_minutes": int(drive_cumul),
                            "stops_cumul": int(stops_cumul)
                        }
                    })

                # Avanzar y acumular distancia
                prev = index
                index = solution.Value(routing.NextVar(index))
                d = extended_distance_matrix[manager.IndexToNode(prev)][manager.IndexToNode(index)]
                dist_v += d
                total_distance += d

            # Tiempos al final del viaje (END)
            time_total = solution.Value(time_dimension.CumulVar(routing.End(v)))   # viaje + esperas + reload
            time_drive = solution.Value(drive_dimension.CumulVar(routing.End(v)))  # SOLO viaje
            stops_count = solution.Value(stops_dimension.CumulVar(routing.End(v)))

            total_time_minutes_total += int(time_total)
            total_time_minutes_drive += int(time_drive)
            total_stops_global += int(stops_count)

            # Consumo
            fuel = dist_v / max(vehicle_consume[v], 1e-9)
            total_fuel_liters += fuel

            # Totales por viaje
            if not deliveries:
                continue
            trip_kg = sum((d["totals"]["kg"] for d in deliveries if d.get("totals")), 0.0)
            trip_units_vals = [d["totals"].get("units") for d in deliveries if d.get("totals")]
            trip_units_sum = sum((u for u in trip_units_vals if u is not None), 0.0)
            trip_palets_sum = sum(int(d["totals"].get("palets", 0)) for d in deliveries if d.get("totals"))

            # Ruta por IDs (limpiando dobles 0)
            raw_route = [0] + [
                extended_locations[n].get("id") if n else 0
                for n in [node for node in route_nodes]
            ] + [0]
            cleaned_route = [raw_route[0]]
            for n in raw_route[1:]:
                if not (n == 0 and cleaned_route[-1] == 0):
                    cleaned_route.append(n)

            # Agregar al agregado del vehículo base
            agg = vehicle_trips.setdefault(main_vehicle, {
                "vehicle": main_vehicle,
                "trips": [],
                "total_distance": 0.0,
                "total_fuel_liters": 0.0,
                "total_kg": 0.0,
                "total_units": 0.0,
                "total_palets": 0,
                "total_time_minutes_total": 0,
                "total_time_minutes_drive": 0,
                "total_stops": 0,
                "capacity_kg": float(vehicle_capacities_base[main_vehicle]),
                "capacity_palets": (
                    int(vehicle_palets_base[main_vehicle])
                    if vehicle_palets_base[main_vehicle] < PALLET_INF else None
                ),
                "modes_used": set()
            })

            agg["trips"].append({
                "trip_no": trip_no,
                "mode": mode,  # 0=OTHER,1=WALMART,2=CENCOSUD
                "route": cleaned_route,
                "deliveries": deliveries,
                "num_stops": int(stops_count),
                "time_minutes_total": int(time_total),  # viaje + esperas + reload
                "time_minutes_drive": int(time_drive),  # SOLO viaje
                "distance": float(dist_v),
                "fuel_liters": float(fuel),
                "total_kg": round(trip_kg, 2),
                "total_units": (round(trip_units_sum, 2) if trip_units_sum > 0 else None),
                "total_palets": int(trip_palets_sum)
            })

            # Acumulados por vehículo base
            agg["total_distance"] += float(dist_v)
            agg["total_fuel_liters"] += float(fuel)
            agg["total_kg"] += float(trip_kg)
            agg["total_units"] += float(trip_units_sum)
            agg["total_palets"] += int(trip_palets_sum)

            agg["total_time_minutes_total"] += int(time_total)
            agg["total_time_minutes_drive"] += int(time_drive)
            agg["total_stops"] += int(stops_count)
            agg["modes_used"].add(int(mode))

            # Totales globales
            total_kg += float(trip_kg)
            total_units += float(trip_units_sum)
            total_palets_sum += int(trip_palets_sum)

        # ----------------- Post-procesamiento por vehículo base -----------------
        max_vehicle_time_total = 0
        max_vehicle_time_drive = 0

        for vdata in vehicle_trips.values():
            modes_set = vdata.get("modes_used", set())
            mode_labels = {0: "OTHER", 1: "WALMART", 2: "CENCOSUD"}
            vdata["modes_used"] = {
                "ids": sorted(modes_set),
                "labels": [mode_labels[m] for m in sorted(modes_set) if m in mode_labels]
            }

            max_vehicle_time_total = max(max_vehicle_time_total, vdata["total_time_minutes_total"])
            max_vehicle_time_drive = max(max_vehicle_time_drive, vdata["total_time_minutes_drive"])

        vehicles_used = len(vehicle_trips)
        avg_vehicle_time_total = (total_time_minutes_total / vehicles_used) if vehicles_used > 0 else 0.0
        avg_vehicle_time_drive = (total_time_minutes_drive / vehicles_used) if vehicles_used > 0 else 0.0

        # ------------------------------ Respuesta --------------------------------
        return jsonify({
            "status": "success",
            "meta": {
                "max_vehicles": base_num_vehicles,
                "max_trips_per_vehicle": max_trips_per_vehicle,
                "time_horizon_drive_minutes": HORIZON,
                "max_stops_per_vehicle": maximo_de_paradas,
                "time_multiplier": multiplicador_tiempo,
                "reload_service_time_minutes": reload_service_time,
                "departure_time": departure_time_str,
                "workers": req_workers,
                "vehicles_used": len(vehicle_trips),
                "max_vehicle_time_minutes_total": int(max_vehicle_time_total),
                "max_vehicle_time_minutes_drive": int(max_vehicle_time_drive),
                "avg_vehicle_time_minutes_total": int(round(avg_vehicle_time_total)),
                "avg_vehicle_time_minutes_drive": int(round(avg_vehicle_time_drive))
            },
            "totals": {
                "total_distance": round(total_distance, 2),
                "total_fuel_liters": round(total_fuel_liters, 2),
                "total_kg": round(total_kg, 2),
                "total_units": (round(total_units, 2) if total_units > 0 else None),
                "total_palets": int(total_palets_sum),
                "total_time_minutes_total": int(total_time_minutes_total),
                "total_time_minutes_drive": int(total_time_minutes_drive),
                "total_stops": int(total_stops_global)
            },
            "vehicles_used": len(vehicle_trips),
            "assignments": list(vehicle_trips.values())
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify(error=f"Error interno: {str(e)}", traceback=traceback.format_exc()), 500

# ---------------------------------------------------------------------

if __name__ == "__main__":
    # En producción usa Gunicorn; esto es solo para local/dev.
    app.run(host="0.0.0.0", port=3000, debug=False)
