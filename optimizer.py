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
    try:
        setattr(obj, attr, value)
    except Exception:
        pass

def _parse_departure_minutes(hhmm: str) -> int:
    hh, mm = hhmm.split(":")
    return int(hh) * 60 + int(mm)

def _fmt_hhmm(total_minutes: int) -> str:
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
    Optimiza rutas con:
    - Capacidades (kg y palets)
    - Tiempos (Time y Drive)
    - Ventanas de tiempo
    - Duplicación por modos
    - Refrigeración
    - Penalizaciones
    """
    # ---------------------------------------------------------------
    # Parse inicial
    # ---------------------------------------------------------------
    raw_data = request.get_json()
    if raw_data is None:
        return jsonify(error="No se recibió JSON válido"), 400

    if isinstance(raw_data, list):
        if len(raw_data) != 3:
            return jsonify(error="Se esperaba [data, truck_ids, user_id]"), 400
        data, truck_ids, user_id = raw_data
    else:
        data, truck_ids, user_id = raw_data, [], None

    try:
        locations = data["locations"]
        base_num_vehicles = data["max_vehicles"]
        vehicle_capacities_base = data["vehicle_capacities"]
        distance_matrix = data["distance_matrix"]
        time_matrix = data.get("time_matrix")

        vehicle_palets_base = data.get("vehicle_palets", [0] * base_num_vehicles)
        vehicle_consume_base = data.get("vehicle_consume", [1] * base_num_vehicles)
        vehicle_free_base = data.get("vehicle_free", [0] * base_num_vehicles)
        multiplicador_tiempo = float(data.get("multiplicador_tiempo", 1.0) or 1.0)

        maximo_de_paradas = int(data.get("maximas_paradas_camion", 100))

        tiempo_calculo_min = float(data.get("tiempo_calculo", 2))
        tiempo_calculo = int(tiempo_calculo_min * 60)
        HORIZON = int(data.get("max_time_per_trip", 480))
        reload_service_time = int(data.get("reload_service_time", 0))

        # ---------------------------------------------------------------
        # Manejo horas de salida
        # ---------------------------------------------------------------
        departure_time_global_str = data.get("departure_time_global") or data.get("departure_time")

        departure_times_by_truck_raw = data.get("departure_times_by_truck") or {}
        if not isinstance(departure_times_by_truck_raw, dict):
            departure_times_by_truck_raw = {}

        reference_candidates = []
        departure_minutes_global = None
        if departure_time_global_str:
            departure_minutes_global = _parse_departure_minutes(departure_time_global_str)
            reference_candidates.append(departure_minutes_global)

        vehicle_departure_minutes_base = [None] * base_num_vehicles
        for idx in range(base_num_vehicles):
            dep_str = None
            if truck_ids and idx < len(truck_ids):
                key = str(truck_ids[idx])
                dep_str = departure_times_by_truck_raw.get(key)

            if not dep_str and departure_time_global_str:
                dep_str = departure_time_global_str

            if dep_str:
                minutes = _parse_departure_minutes(dep_str)
                vehicle_departure_minutes_base[idx] = minutes
                reference_candidates.append(minutes)

        reference_departure_minutes = None
        if reference_candidates:
            reference_departure_minutes = min(reference_candidates)

        # ---------------------------------------------------------------
        # Validaciones de listas base
        # ---------------------------------------------------------------
        if len(vehicle_capacities_base) != base_num_vehicles:
            return jsonify(error="capacidades no coinciden con max_vehicles"), 400
        if len(vehicle_consume_base) != base_num_vehicles:
            return jsonify(error="consumos no coinciden con max_vehicles"), 400
        if len(vehicle_free_base) != base_num_vehicles:
            return jsonify(error="vehicle_free no coincide con max_vehicles"), 400
        if len(vehicle_palets_base) != base_num_vehicles:
            return jsonify(error="palets por vehículo no coincide"), 400

        PALLET_INF = 10**9
        vehicle_palets_base = [
            (int(p) if int(p) > 0 else PALLET_INF) for p in vehicle_palets_base
        ]

        # ---------------------------------------------------------------
        # Modos (Walmart, Cencosud, Other)
        # ---------------------------------------------------------------
        MODE_FREE = 0
        MODE_W = 1
        MODE_C = 2
        MODES = (MODE_FREE, MODE_W, MODE_C)

        # Duplicación
        vehicle_capacities = []
        vehicle_consume = []
        vehicle_free = []
        vehicle_palets = []
        vehicle_mapping = {}
        vehicle_trip_no = {}
        vehicle_mode = {}

        max_trips_per_vehicle = 1

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

        # ---------------------------------------------------------------
        # Construcción de nodos extendidos
        # ---------------------------------------------------------------
        all_products = set()
        for loc in locations:
            all_products.update(loc.get("demanda", {}).keys())
        all_products = sorted(all_products)

        extended_locations = [locations[0]]
        extended_demands = [0]
        extended_palets_l = [0]
        extended_wait = [0]
        extended_opening = [None]
        extended_deadline = [None]
        extended_gap = [0]
        extended_refriger = [False]
        split_mapping = {}

        refrigerados = [
            "WALMART CD", "CENCOSUD CD", "SANTA ISABEL LOCAL", "JUMBO LOCAL",
            "TOTTUS CD", "TOTTUS LOCAL", "UNIMARC CD", "UNIMARC LOCAL",
            "ARAMARK", "SODEXO"
        ]

        def _group(idn):
            idn = (idn or "").upper()
            if "WALMART CD" in idn:
                return "WALMART"
            if "CENCOSUD CD" in idn:
                return "CENCOSUD"
            return "OTHER"

        for idx_loc, loc in enumerate(locations[1:], start=1):
            dem = {
                p: int(float(loc.get("demanda", {}).get(p, 0)))
                for p in all_products
            }
            total = sum(dem.values())

            pal = int(float(loc.get("palets_en_suelo", 0) or 0))
            wait_m = float(loc.get("wait_minutes", 0) or 0)
            gap = int(loc.get("gap_time", 0) or 0)

            arrival_iso = loc.get("arrival_time")
            deadline_rel = None
            if reference_departure_minutes is not None and arrival_iso:
                dt = datetime.fromisoformat(arrival_iso)
                mins = dt.hour * 60 + dt.minute
                delta = mins - reference_departure_minutes
                if delta < 0:
                    delta += 1440
                deadline_rel = delta

            open_raw = loc.get("open_time")
            open_rel = None
            if reference_departure_minutes is not None and open_raw:
                if "T" in str(open_raw):
                    dt = datetime.fromisoformat(open_raw)
                    mins = dt.hour * 60 + dt.minute
                else:
                    mins = _parse_departure_minutes(open_raw)
                delta = mins - reference_departure_minutes
                if delta < 0:
                    delta += 1440
                open_rel = delta

            ident = (loc.get("identificador", "") or "").upper()
            refreq = any(r in ident for r in refrigerados)

            # Split si excede capacidad
            if total <= max_vehicle_capacity:
                extended_locations.append(loc)
                extended_demands.append(total)
                extended_palets_l.append(pal)
                extended_wait.append(wait_m)
                extended_opening.append(open_rel)
                extended_deadline.append(deadline_rel)
                extended_gap.append(gap)
                extended_refriger.append(refreq)
                split_mapping[len(extended_locations) - 1] = idx_loc
            else:
                # splitting...
                proportions = {p: (dem[p] / total) for p in all_products}
                rem = total
                rem_prod = dem.copy()
                rem_pal = pal

                while rem > 0:
                    take = min(rem, max_vehicle_capacity)
                    sp = copy.deepcopy(loc)
                    sp_dem = {}
                    if rem - take > 0:
                        for p in all_products:
                            q = int(round(take * proportions[p]))
                            q = min(q, rem_prod[p])
                            sp_dem[p] = q
                            rem_prod[p] -= q
                        frac = take / rem if rem else 0
                        pal_asg = int(round(frac * rem_pal))
                        pal_asg = min(pal_asg, rem_pal)
                    else:
                        sp_dem = rem_prod.copy()
                        pal_asg = rem_pal

                    sp["demanda"] = {
                        p: str(sp_dem[p]) for p in sp_dem if sp_dem[p] > 0
                    }

                    extended_locations.append(sp)
                    extended_demands.append(sum(sp_dem.values()))
                    extended_palets_l.append(pal_asg)
                    extended_wait.append(wait_m)
                    extended_opening.append(open_rel)
                    extended_deadline.append(deadline_rel)
                    extended_gap.append(gap)
                    extended_refriger.append(refreq)
                    split_mapping[len(extended_locations) - 1] = idx_loc

                    rem -= take
                    rem_pal -= pal_asg

        num_nodes = len(extended_locations)

        node_group = [
            _group(loc.get("identificador", "")) for loc in extended_locations
        ]

        # ---------------------------------------------------------------
        # Matrices extendidas
        # ---------------------------------------------------------------
        def extend_matrix(base):
            m = [[0] * num_nodes for _ in range(num_nodes)]
            for i in range(num_nodes):
                for j in range(num_nodes):
                    oi = 0 if i == 0 else split_mapping.get(i, i)
                    oj = 0 if j == 0 else split_mapping.get(j, j)
                    m[i][j] = base[oi][oj]
            return m

        extended_distance_matrix = extend_matrix(distance_matrix)
        extended_time_matrix = extend_matrix(time_matrix)
        if multiplicador_tiempo != 1.0:
            extended_time_matrix = [
                [cell * multiplicador_tiempo for cell in row]
                for row in extended_time_matrix
            ]

        # ---------------------------------------------------------------
        # OR-Tools: Manager y Routing
        # ---------------------------------------------------------------
        manager = pywrapcp.RoutingIndexManager(num_nodes, num_vehicles, depot)
        routing = pywrapcp.RoutingModel(manager)

        # ---------------------------------------------------------------
        # Restricción: Refrigeración
        # ---------------------------------------------------------------
        for node_index in range(1, num_nodes):
            if extended_refriger[node_index]:
                idx = manager.NodeToIndex(node_index)
                for v in range(num_vehicles):
                    if not vehicle_free[v]:
                        routing.VehicleVar(idx).RemoveValue(v)

        # ---------------------------------------------------------------
        # Restricción: Modos
        # ---------------------------------------------------------------
        for node_index in range(1, num_nodes):
            g = node_group[node_index]
            idx = manager.NodeToIndex(node_index)
            if g == "WALMART":
                allowed = {MODE_W}
            elif g == "CENCOSUD":
                allowed = {MODE_C}
            else:
                allowed = {MODE_FREE}
            for v in range(num_vehicles):
                if vehicle_mode[v] not in allowed:
                    routing.VehicleVar(idx).RemoveValue(v)

        # ---------------------------------------------------------------
        # Costes + Penalizaciones
        # ---------------------------------------------------------------
        HIGH_PENALTY = 100000

        prioridad_pa_tag = "PUNTO AZUL"
        prioridad_lv_tag = "LA VEGA"
        prioridad_tottus_tag = "TOTTUS CD"
        prioridad_unimarc_tag = "UNIMARC CD"

        is_pa = []
        is_lv = []
        is_tottus = []
        is_unimarc = []

        for loc in extended_locations:
            ident = (loc.get("identificador", "") or "").upper()
            is_pa.append(prioridad_pa_tag in ident)
            is_lv.append(prioridad_lv_tag in ident)
            is_tottus.append(prioridad_tottus_tag in ident)
            is_unimarc.append(prioridad_unimarc_tag in ident)

        any_pa = any(is_pa[1:])
        any_lv = any(is_lv[1:])
        any_t3 = any(is_tottus[i] or is_unimarc[i] for i in range(1, len(is_tottus)))

        def is_tier3(i):
            return is_tottus[i] or is_unimarc[i]

        START_OTHER_PA = 90000
        START_T3_PA = 60000
        START_LV_PA = 35000
        START_OTHER_LV = 60000
        START_T3_LV = 25000
        START_OTHER_T3 = 45000

        PA_LATE = 100000
        LV_LATE = 70000
        T3_LATE = 45000
        PA_AFTER_T3 = 70000
        LV_AFTER_T3 = 40000

        def make_cost_cb(v_idx):
            def cost_cb(fi, ti):
                fn = manager.IndexToNode(fi)
                tn = manager.IndexToNode(ti)

                base = extended_distance_matrix[fn][tn]
                base = max(0, int(round((base / max(vehicle_consume[v_idx], 1e-9)) * 1000)))

                if node_group[tn] == "OTHER":
                    if tn != depot and fn == depot:
                        if any_pa:
                            if is_pa[tn]:
                                pass
                            elif is_lv[tn]:
                                base += START_LV_PA
                            elif is_tier3(tn):
                                base += START_T3_PA
                            else:
                                base += START_OTHER_PA
                        elif any_lv:
                            if is_lv[tn]:
                                pass
                            elif is_tier3(tn):
                                base += START_T3_LV
                            else:
                                base += START_OTHER_LV
                        elif any_t3:
                            if not is_tier3(tn):
                                base += START_OTHER_T3

                    if fn != depot and tn != depot:
                        if is_pa[tn] and not is_pa[fn]:
                            base += PA_LATE
                        if is_lv[tn] and (
                            not is_lv[fn] and not is_pa[fn] and not is_tier3(fn)
                        ):
                            base += LV_LATE
                        if is_tier3(tn) and (
                            not is_pa[fn] and not is_lv[fn] and not is_tier3(fn)
                        ):
                            base += T3_LATE

                        if is_tier3[fn] and is_pa[tn]:
                            base += PA_AFTER_T3
                        if is_tier3[fn] and is_lv[tn]:
                            base += LV_AFTER_T3

                if fn != depot and tn != depot:
                    g1 = node_group[fn]
                    g2 = node_group[tn]
                    if g1 != g2 and ("WALMART" in (g1, g2) or "CENCOSUD" in (g1, g2)):
                        base += HIGH_PENALTY

                return base
            return cost_cb

        for v in range(num_vehicles):
            idxcb = routing.RegisterTransitCallback(make_cost_cb(v))
            routing.SetArcCostEvaluatorOfVehicle(idxcb, v)

        # ---------------------------------------------------------------
        # Dimension: Capacity
        # ---------------------------------------------------------------
        def demand_cb(fi):
            n = manager.IndexToNode(fi)
            return int(extended_demands[n])

        dem_i = routing.RegisterUnaryTransitCallback(demand_cb)

        routing.AddDimensionWithVehicleCapacity(
            dem_i, 0, vehicle_capacities, True, "Capacity"
        )

        # Palets
        def pal_cb(fi):
            n = manager.IndexToNode(fi)
            return int(extended_palets_l[n])

        pal_i = routing.RegisterUnaryTransitCallback(pal_cb)

        routing.AddDimensionWithVehicleCapacity(
            pal_i, 0, vehicle_palets, True, "Palets"
        )

        # ---------------------------------------------------------------
        # Dimension de tiempo (Time)
        # ---------------------------------------------------------------
        start_idxs = set(routing.Start(v) for v in range(num_vehicles))

        def time_cb(fi, ti):
            fn = manager.IndexToNode(fi)
            tn = manager.IndexToNode(ti)
            travel = int(round(extended_time_matrix[fn][tn]))
            service = 0
            if fn == depot and fi not in start_idxs:
                service += reload_service_time
            if fn != depot:
                service += int(round(extended_wait[fn]))
            return travel + service

        time_i = routing.RegisterTransitCallback(time_cb)
        routing.AddDimension(time_i, 0, 10**7, True, "Time")
        time_dim = routing.GetDimensionOrDie("Time")

        # Offsets por hora de salida
        vehicle_start_offsets = {}

        if reference_departure_minutes is not None:
            for v in range(num_vehicles):
                base_idx = vehicle_mapping[v]

                # offset real de salida del camión v (dependiendo de su camión base)
                dep_abs = vehicle_departure_minutes_base[base_idx]
                if dep_abs is None:
                    offset = 0
                else:
                    offset = dep_abs - reference_departure_minutes
                    if offset < 0:
                        offset += 1440  # normalizar a 24h

                cvar = time_dim.CumulVar(routing.Start(v))
                lb = cvar.Min()
                ub = cvar.Max()

                # validar si el offset es factible
                if lb <= offset <= ub:
                    cvar.SetRange(offset, offset)
                else:
                    # fallback seguro: fijar al mínimo factible
                    cvar.SetRange(lb, lb)

                # guardar offset REAL (no el fallback)
                vehicle_start_offsets[v] = offset
        else:
            for v in range(num_vehicles):
                vehicle_start_offsets[v] = 0


        # Ventanas de tiempo nodo
        if reference_departure_minutes is not None:
            for node in range(1, num_nodes):
                idx = manager.NodeToIndex(node)
                lb = 0
                ub = 10**7
                gap = int(extended_gap[node]) or 0

                if extended_opening[node] is not None:
                    eff_open = extended_opening[node] - gap
                    lb = max(lb, max(0, eff_open))

                if extended_deadline[node] is not None:
                    wait_n = int(round(extended_wait[node]))
                    eff_dead = extended_deadline[node] - gap
                    ub_dead = eff_dead - wait_n
                    ub = min(ub, max(0, ub_dead))

                if lb <= ub:
                    time_dim.CumulVar(idx).SetRange(lb, ub)
                else:
                    time_dim.CumulVar(idx).SetRange(lb, lb)

        # ---------------------------------------------------------------
        # Drive dimension
        # ---------------------------------------------------------------
        def drive_cb(fi, ti):
            fn = manager.IndexToNode(fi)
            tn = manager.IndexToNode(ti)
            return int(round(extended_time_matrix[fn][tn]))

        drive_i = routing.RegisterTransitCallback(drive_cb)
        routing.AddDimension(drive_i, 0, HORIZON, True, "Drive")
        drive_dim = routing.GetDimensionOrDie("Drive")

        for v in range(num_vehicles):
            drive_dim.CumulVar(routing.End(v)).SetMax(HORIZON)

        solver = routing.solver()
        for b in range(base_num_vehicles):
            end_cums = [
                drive_dim.CumulVar(routing.End(v))
                for v in range(num_vehicles)
                if vehicle_mapping[v] == b
            ]
            solver.Add(solver.Sum(end_cums) <= HORIZON)

        # Stops
        def stop_cb(fi, ti):
            tn = manager.IndexToNode(ti)
            return 1 if tn != depot else 0

        stop_i = routing.RegisterTransitCallback(stop_cb)

        routing.AddDimension(stop_i, 0, maximo_de_paradas, True, "Stops")
        stop_dim = routing.GetDimensionOrDie("Stops")

        time_dim.SetGlobalSpanCostCoefficient(50)

        # Costo fijo de reingreso
        costo_reingreso_valor = int(data.get("costo_reingreso_valor", 100000))
        for v in range(num_vehicles):
            if vehicle_trip_no[v] > 0:
                routing.SetFixedCostOfVehicle(costo_reingreso_valor, v)

        # ---------------------------------------------------------------
        # Parámetros de búsqueda
        # ---------------------------------------------------------------
        params = pywrapcp.DefaultRoutingSearchParameters()
        params.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
        params.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
        params.time_limit.FromSeconds(tiempo_calculo)

        req_workers = data.get("search_workers")
        if req_workers is None:
            req_workers = 32
        try:
            req_workers = int(req_workers)
        except:
            req_workers = 0
        if req_workers <= 0:
            req_workers = min(32, os.cpu_count() or 1)

        _safe_set(params, "number_of_workers", req_workers)
        _safe_set(params, "log_search", bool(data.get("log_search", False)))

        solution = routing.SolveWithParameters(params)
        if not solution:
            return jsonify(error="No se pudo encontrar solución"), 400

        # ---------------------------------------------------------------
        # Extracción
        # ---------------------------------------------------------------
        vehicle_trips = {}
        total_dist = 0
        total_fuel = 0
        total_kg = 0
        total_units = 0
        total_pal = 0
        total_time_total = 0
        total_time_drive = 0
        total_stops = 0

        def cumul(dim, idx):
            return solution.Value(dim.CumulVar(idx))

        for v in range(num_vehicles):
            st = routing.Start(v)
            if routing.IsEnd(solution.Value(routing.NextVar(st))):
                continue

            base_vid = vehicle_mapping[v]
            trip_no = vehicle_trip_no[v]
            mode = vehicle_mode[v]
            start_offset = vehicle_start_offsets[v]

            deliveries = []
            dist_v = 0
            route_nodes = []

            index = st
            while not routing.IsEnd(index):
                node = manager.IndexToNode(index)
                route_nodes.append(node)

                if node != depot:
                    loc = extended_locations[node]
                    loc_id = loc.get("id")
                    dem = loc.get("demanda", {}) or {}
                    prices = loc.get("precios", {}) or {}
                    weights = loc.get("pesos", {}) or {}
                    packs = loc.get("unidades", {}) or {}

                    prod_detail = []
                    stop_kg = 0
                    stop_units = 0
                    for pid_key, val in dem.items():
                        pid_str = str(pid_key)
                        kg = float(val)
                        price = float(prices.get(pid_str, 0) or 0)
                        w = float(weights.get(pid_str, 0) or 0)
                        pack_u = int(packs.get(pid_str, 0) or 0)

                        units = kg / w if w > 0 else None
                        subtotal = kg * price
                        stop_kg += kg
                        if units is not None:
                            stop_units += units

                        prod_detail.append({
                            "product_id": int(pid_str) if pid_str.isdigit() else pid_str,
                            "kg": round(kg, 2),
                            "price_unit": price,
                            "unit_weight_kg": (w if w > 0 else None),
                            "units": (round(units, 2) if units is not None else None),
                            "pack_units": (pack_u if pack_u > 0 else None),
                            "subtotal": round(subtotal, 2)
                        })

                    idxr = index
                    t_cum = cumul(time_dim, idxr)
                    d_cum = cumul(drive_dim, idxr)
                    s_cum = cumul(stop_dim, idxr)

                    wait_n = int(round(extended_wait[node]))
                    gap_n = int(extended_gap[node]) if extended_gap[node] else 0

                    arr_from_dep = t_cum - start_offset
                    dep_from_dep = t_cum + wait_n - start_offset
                    if arr_from_dep < 0:
                        arr_from_dep = 0
                    if dep_from_dep < 0:
                        dep_from_dep = 0

                    deadline_rel = extended_deadline[node]
                    deadline_from_dep = None
                    latest_arr = None
                    slack = None

                    if deadline_rel is not None:
                        eff_dead = deadline_rel - gap_n
                        latest = eff_dead - wait_n
                        deadline_from_dep = eff_dead - start_offset
                        latest_arr = latest - start_offset
                        slack = latest_arr - arr_from_dep if latest_arr is not None else None

                    eta_clock = None
                    etd_clock = None
                    open_clock = None
                    opening_clock = None
                    closing_clock = None

                    if reference_departure_minutes is not None:
                        eta_clock = _fmt_hhmm(reference_departure_minutes + t_cum)
                        etd_clock = _fmt_hhmm(reference_departure_minutes + t_cum + wait_n)

                        if extended_opening[node] is not None:
                            open_clock = _fmt_hhmm(reference_departure_minutes + extended_opening[node])
                            opening_clock = open_clock

                        if extended_deadline[node] is not None:
                            closing_clock = _fmt_hhmm(reference_departure_minutes + extended_deadline[node])

                    opening_from_dep = (
                        extended_opening[node] - start_offset
                        if extended_opening[node] is not None else None
                    )

                    deliveries.append({
                        "location_id": loc_id,
                        "identificador": loc.get("identificador"),
                        "node_index": node,
                        "group": node_group[node],
                        "requires_refrigeration": bool(extended_refriger[node]),
                        "products": dem,
                        "products_detail": prod_detail,
                        "totals": {
                            "kg": round(stop_kg, 2),
                            "units": (round(stop_units, 2) if stop_units > 0 else None),
                            "palets": int(extended_palets_l[node])
                        },
                        "timing": {
                            "arrival_minutes_from_departure": int(arr_from_dep),
                            "departure_minutes_from_departure": int(dep_from_dep),
                            "opening_clock": opening_clock,
                            "closing_clock": closing_clock,

                            "deadline_minutes_from_departure": (
                                int(deadline_from_dep) if deadline_from_dep is not None else None
                            ),
                            "latest_arrival_allowed_minutes_from_departure": (
                                int(latest_arr) if latest_arr is not None else None
                            ),
                            "deadline_slack_minutes": (
                                int(slack) if slack is not None else None
                            ),

                            "eta_clock": eta_clock,
                            "etd_clock": etd_clock,
                            "wait_minutes": int(wait_n),

                            "opening_minutes_from_departure": (
                                int(opening_from_dep) if opening_from_dep is not None else None
                            ),
                            "open_clock": open_clock,
                            "gap_time_minutes": int(gap_n)
                        },
                        "cumul": {
                            "time_cumul_minutes": int(t_cum),
                            "drive_cumul_minutes": int(d_cum),
                            "stops_cumul": int(s_cum)
                        }
                    })

                prev = index
                index = solution.Value(routing.NextVar(index))
                d = extended_distance_matrix[manager.IndexToNode(prev)][manager.IndexToNode(index)]
                dist_v += d
                total_dist += d

            time_total = solution.Value(time_dim.CumulVar(routing.End(v)))
            time_drive = solution.Value(drive_dim.CumulVar(routing.End(v)))
            stops_count = solution.Value(stop_dim.CumulVar(routing.End(v)))

            total_time_total += time_total
            total_time_drive += time_drive
            total_stops += stops_count

            fuel = dist_v / max(vehicle_consume[v], 1e-9)
            total_fuel += fuel

            if not deliveries:
                continue

            trip_kg = sum(d["totals"]["kg"] for d in deliveries)
            units_list = [d["totals"]["units"] for d in deliveries if d["totals"]["units"] is not None]
            trip_units = sum(units_list) if units_list else 0
            trip_pal = sum(int(d["totals"]["palets"]) for d in deliveries)

            raw_route = [0] + [
                extended_locations[n].get("id") if n else 0
                for n in route_nodes
            ] + [0]

            cleaned_route = [raw_route[0]]
            for n in raw_route[1:]:
                if not (n == 0 and cleaned_route[-1] == 0):
                    cleaned_route.append(n)

            # ---------------------------------------------------------------
            # Hora de salida absoluta del camión (corregido)
            # ---------------------------------------------------------------
            truck_departure_clock = None
            if reference_departure_minutes is not None:
                truck_departure_clock = _fmt_hhmm(reference_departure_minutes + start_offset)

            # ---------------------------------------------------------------
            # Agregado a vehículo base
            # ---------------------------------------------------------------
            agg = vehicle_trips.setdefault(base_vid, {
                "vehicle": base_vid,
                "trips": [],
                "total_distance": 0.0,
                "total_fuel_liters": 0.0,
                "total_kg": 0.0,
                "total_units": 0.0,
                "total_palets": 0,
                "total_time_minutes_total": 0,
                "total_time_minutes_drive": 0,
                "total_stops": 0,
                "capacity_kg": float(vehicle_capacities_base[base_vid]),
                "capacity_palets": (
                    int(vehicle_palets_base[base_vid])
                    if vehicle_palets_base[base_vid] < PALLET_INF else None
                ),
                "modes_used": set()
            })

            agg["trips"].append({
                "trip_no": trip_no,
                "mode": mode,
                "route": cleaned_route,
                "deliveries": deliveries,
                "num_stops": int(stops_count),
                "time_minutes_total": int(time_total),
                "time_minutes_drive": int(time_drive),
                "distance": float(dist_v),
                "fuel_liters": float(fuel),
                "total_kg": round(trip_kg, 2),
                "total_units": (round(trip_units, 2) if trip_units > 0 else None),
                "total_palets": int(trip_pal),
                "departure_clock": truck_departure_clock,
                "departure_offset_minutes": int(start_offset)
            })

            agg["total_distance"] += dist_v
            agg["total_fuel_liters"] += fuel
            agg["total_kg"] += trip_kg
            agg["total_units"] += trip_units
            agg["total_palets"] += trip_pal
            agg["total_time_minutes_total"] += time_total
            agg["total_time_minutes_drive"] += time_drive
            agg["total_stops"] += stops_count
            agg["modes_used"].add(int(mode))

            total_kg += trip_kg
            total_units += trip_units
            total_pal += trip_pal

        # ---------------------------------------------------------------
        # Post-proceso
        # ---------------------------------------------------------------
        max_time_total = 0
        max_time_drive = 0

        for vdata in vehicle_trips.values():
            modes_set = vdata["modes_used"]
            mode_labels = {0: "OTHER", 1: "WALMART", 2: "CENCOSUD"}
            vdata["modes_used"] = {
                "ids": sorted(modes_set),
                "labels": [mode_labels[m] for m in sorted(modes_set)]
            }

            max_time_total = max(max_time_total, vdata["total_time_minutes_total"])
            max_time_drive = max(max_time_drive, vdata["total_time_minutes_drive"])

        num_used = len(vehicle_trips)
        avg_total = total_time_total / num_used if num_used else 0
        avg_drive = total_time_drive / num_used if num_used else 0

        reference_departure_time_str = (
            _fmt_hhmm(reference_departure_minutes)
            if reference_departure_minutes is not None else None
        )

        # ---------------------------------------------------------------
        # Respuesta final
        # ---------------------------------------------------------------
        return jsonify({
            "status": "success",
            "meta": {
                "max_vehicles": base_num_vehicles,
                "max_trips_per_vehicle": max_trips_per_vehicle,
                "time_horizon_drive_minutes": HORIZON,
                "max_stops_per_vehicle": maximo_de_paradas,
                "time_multiplier": multiplicador_tiempo,
                "reload_service_time_minutes": reload_service_time,

                "departure_time": departure_time_global_str,
                "departure_time_global": departure_time_global_str,
                "departure_times_by_truck": departure_times_by_truck_raw,
                "reference_departure_time": reference_departure_time_str,

                "workers": req_workers,
                "vehicles_used": len(vehicle_trips),
                "max_vehicle_time_minutes_total": int(max_time_total),
                "max_vehicle_time_minutes_drive": int(max_time_drive),
                "avg_vehicle_time_minutes_total": int(round(avg_total)),
                "avg_vehicle_time_minutes_drive": int(round(avg_drive))
            },
            "totals": {
                "total_distance": round(total_dist, 2),
                "total_fuel_liters": round(total_fuel, 2),
                "total_kg": round(total_kg, 2),
                "total_units": (round(total_units, 2) if total_units > 0 else None),
                "total_palets": int(total_pal),
                "total_time_minutes_total": int(total_time_total),
                "total_time_minutes_drive": int(total_time_drive),
                "total_stops": int(total_stops)
            },
            "vehicles_used": len(vehicle_trips),
            "assignments": list(vehicle_trips.values())
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify(
            error=f"Error interno: {str(e)}",
            traceback=traceback.format_exc()
        ), 500

# ---------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000, debug=False)
