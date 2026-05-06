from flask import Flask, request, jsonify
from ortools.constraint_solver import pywrapcp, routing_enums_pb2
import copy
import os
import json
import redis
from datetime import datetime

app = Flask(__name__)

# ---------------------------------------------------------------------
# Conexión a Redis y Función de Estado
# ---------------------------------------------------------------------
# Buscamos la URL de Redis en las variables de entorno. 
redis_url = os.environ.get("REDIS_URL")

redis_client = None
if redis_url:
    try:
        # Railway usa URLs seguras (rediss://) que requieren desactivar la verificación estricta de SSL
        if redis_url.startswith("rediss://"):
            import ssl
            redis_client = redis.from_url(redis_url, ssl_cert_reqs=ssl.CERT_NONE)
        else:
            redis_client = redis.from_url(redis_url)
        
        # Hacemos un ping para asegurar que de verdad conectó
        redis_client.ping()
        print("✅ Conectado a Redis exitosamente.")
    except Exception as e:
        print(f"❌ Error conectando a Redis: {e}")
        redis_client = None
else:
    print("⚠️ ADVERTENCIA: Variable REDIS_URL no configurada.")

def update_job_status(job_id: str, status: str, message: str, progress: int):
    """Guarda el progreso en Redis. Falla silenciosamente para no quebrar el optimizador."""
    if not job_id or not redis_client:
        return
    try:
        estado = {
            "status": status,
            "message": message,
            "progress": progress,
            "updated_at": datetime.utcnow().isoformat()
        }
        redis_client.setex(f"opt_status_{job_id}", 3600, json.dumps(estado))
    except Exception as e:
        print(f"Error escribiendo en Redis: {e}")

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
      - Hora de salida independiente por camión
      - Ventanas en nodos (Corregidas)
      - Límite de paradas por vehículo (Stops)
      - NUEVA REGLA: LA VEGA puede ir en rutas CENCOSUD solo si es la primera parada
    """
    job_id = None
    
    # ---------------------- Datos fijos de negocio ---------------------
    ubicaciones_refrigeradas = [
        "WALMART CD", "CENCOSUD CD", "SANTA ISABEL LOCAL", "JUMBO LOCAL",
        "TOTTUS CD", "TOTTUS LOCAL", "UNIMARC CD", "UNIMARC LOCAL",
        "ARAMARK", "SODEXO"
    ]

    # ------------------------- Parse de entrada ------------------------
    try:
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

        # --- NUEVO: Extraer Job ID e iniciar progreso ---
        job_id = data.get("job_id")
        update_job_status(job_id, "preparacion", "Recibiendo datos y calculando matrices...", 10)

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

        # -----------------------------------------------------------------
        # Horas de salida por camión
        # -----------------------------------------------------------------
        vehicle_departure_times_raw = data.get("vehicle_departure_times") or []
        if not isinstance(vehicle_departure_times_raw, list):
            vehicle_departure_times_raw = []

        departure_time_global_str = data.get("departure_time_global") or data.get("departure_time")

        departure_times_by_truck_raw = {}
        for idx in range(base_num_vehicles):
            dep_str = None
            if idx < len(vehicle_departure_times_raw):
                dep_str = vehicle_departure_times_raw[idx] or None
            if dep_str and truck_ids and idx < len(truck_ids):
                departure_times_by_truck_raw[str(truck_ids[idx])] = dep_str

        reference_candidates = []
        vehicle_departure_minutes_base = [None] * base_num_vehicles

        for idx in range(base_num_vehicles):
            dep_str = None
            if idx < len(vehicle_departure_times_raw):
                dep_str = vehicle_departure_times_raw[idx] or None
            if not dep_str and departure_time_global_str:
                dep_str = departure_time_global_str

            if dep_str:
                try:
                    minutes = _parse_departure_minutes(dep_str)
                except Exception:
                    return jsonify(error=f"Formato inválido en hora de salida para camión {idx}; esperado 'HH:MM'"), 400
                vehicle_departure_minutes_base[idx] = minutes
                reference_candidates.append(minutes)

        reference_departure_minutes = None
        if reference_candidates:
            reference_departure_minutes = min(reference_candidates)

        if len(vehicle_capacities_base) != base_num_vehicles:
            return jsonify(error="capacidades no coinciden con max_vehicles"), 400
        if len(vehicle_consume_base) != base_num_vehicles:
            return jsonify(error="consumos no coinciden con max_vehicles"), 400
        if len(vehicle_free_base) != base_num_vehicles:
            return jsonify(error="vehicle_free no coincide con max_vehicles"), 400
        if len(vehicle_palets_base) != base_num_vehicles:
            return jsonify(error="palets por vehículo no coincide con max_vehicles"), 400

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

        vehicle_capacities, vehicle_consume, vehicle_free, vehicle_palets = [], [], [], []
        vehicle_mapping, vehicle_trip_no, vehicle_mode = {}, {}, {}
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

        # --------------------- Construcción de nodos extendidos -------------------
        update_job_status(job_id, "construyendo_modelo", "Agrupando ubicaciones y dividiendo demanda de carga...", 25)
        
        all_products = set()
        for loc in locations:
            all_products.update(loc.get("demanda", {}).keys())
        all_products = sorted(all_products)

        extended_locations   = [locations[0]]
        extended_demands     = [0]        
        extended_palets      = [0.0]      
        extended_wait        = [0.0]      
        extended_deadline    = [None]     
        extended_opening     = [None]     
        extended_opening_gap = [0]        
        extended_closing_gap = [0]        
        extended_refrigerate = [False]    
        split_mapping        = {}         

        def _group(identificador: str) -> str:
            ident = (identificador or "").upper()
            if "WALMART CD" in ident:
                return "WALMART"
            if "CENCOSUD CD" in ident:
                return "CENCOSUD"
            return "OTHER"

        for idx_loc, loc in enumerate(locations[1:], start=1):
            prod_quantities = {
                p: int(float(loc.get("demanda", {}).get(p, 0)))
                for p in all_products
            }
            total_demand = sum(prod_quantities.values())

            palets_total = float(loc.get("palets_en_suelo", 0) or 0)
            palets_total = max(0.0, palets_total)

            wait_minutes = float(loc.get("wait_minutes", 0) or 0.0)
            wait_minutes = max(0.0, wait_minutes)
            # Limitar tiempo de espera a máximo 3 horas (180 minutos)
            MAX_WAIT_MINUTES = 180
            if wait_minutes > MAX_WAIT_MINUTES:
                wait_minutes = MAX_WAIT_MINUTES

            opening_gap = loc.get("opening_gap")
            closing_gap = loc.get("closing_gap")
            
            if opening_gap is None or opening_gap == "":
                opening_gap = 0
            else:
                opening_gap = int(opening_gap)
                if opening_gap < 0:
                    opening_gap = 0
            
            if closing_gap is None or closing_gap == "":
                closing_gap = 0
            else:
                closing_gap = int(closing_gap)
                if closing_gap < 0:
                    closing_gap = 0

            # --- NUEVO: Cálculo corregido de cierre sin salto de día ---
            close_time_raw = loc.get("close_time")
            deadline_minutes_rel = None
            if reference_departure_minutes is not None and close_time_raw:
                try:
                    if "T" in str(close_time_raw):
                        close_dt = datetime.fromisoformat(close_time_raw)
                        close_minutes = close_dt.hour * 60 + close_dt.minute
                    else:
                        close_minutes = _parse_departure_minutes(str(close_time_raw))
                    delta = close_minutes - reference_departure_minutes
                    # SE ELIMINÓ EL IF DELTA < 0 AQUI
                    deadline_minutes_rel = int(delta)
                except Exception:
                    deadline_minutes_rel = None

            # --- NUEVO: Cálculo corregido de apertura sin salto de día ---
            open_time_raw = loc.get("open_time")
            opening_minutes_rel = None
            if reference_departure_minutes is not None and open_time_raw:
                try:
                    if "T" in str(open_time_raw):
                        open_dt = datetime.fromisoformat(open_time_raw)
                        open_minutes = open_dt.hour * 60 + open_dt.minute
                    else:
                        open_minutes = _parse_departure_minutes(str(open_time_raw))
                    delta_open = open_minutes - reference_departure_minutes
                    # SE ELIMINÓ EL IF DELTA_OPEN < 0 AQUI
                    opening_minutes_rel = int(delta_open)
                except Exception:
                    opening_minutes_rel = None

            identificador = (loc.get("identificador", "") or "").upper()
            requires_refrigeration = any(
                name in identificador for name in [s.upper() for s in ubicaciones_refrigeradas]
            )

            if total_demand <= max_vehicle_capacity:
                extended_locations.append(loc)
                extended_demands.append(int(total_demand))
                extended_palets.append(palets_total)           
                extended_wait.append(wait_minutes)
                extended_deadline.append(deadline_minutes_rel)
                extended_opening.append(opening_minutes_rel)
                extended_opening_gap.append(opening_gap)
                extended_closing_gap.append(closing_gap)
                extended_refrigerate.append(requires_refrigeration)
                split_mapping[len(extended_locations) - 1] = idx_loc
            else:
                proportions = {
                    p: (q / total_demand) if total_demand > 0 else 0
                    for p, q in prod_quantities.items()
                }
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
                        palets_assigned = frac * remaining_palets   
                        palets_assigned = min(palets_assigned, remaining_palets)
                    else:
                        split_demand = remaining_per_product.copy()
                        palets_assigned = remaining_palets           

                    split_loc["demanda"] = {
                        p: str(split_demand[p]) for p in all_products if split_demand[p] > 0
                    }

                    extended_locations.append(split_loc)
                    extended_demands.append(int(sum(split_demand.values())))
                    extended_palets.append(palets_assigned)          
                    extended_wait.append(wait_minutes)
                    extended_deadline.append(deadline_minutes_rel)
                    extended_opening.append(opening_minutes_rel)
                    extended_opening_gap.append(opening_gap)         
                    extended_closing_gap.append(closing_gap)         
                    extended_refrigerate.append(requires_refrigeration)
                    split_mapping[len(extended_locations) - 1] = idx_loc

                    remaining -= amount
                    remaining_palets -= palets_assigned

        num_nodes = len(extended_locations)
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

        if multiplicador_tiempo != 1.0:
            extended_time_matrix = [
                [cell * multiplicador_tiempo for cell in row]
                for row in extended_time_matrix
            ]

        # ------------------------------- OR-Tools ---------------------------------
        update_job_status(job_id, "construyendo_modelo", "Aplicando restricciones de tiempo y reglas de exclusividad...", 40)
        
        manager = pywrapcp.RoutingIndexManager(num_nodes, num_vehicles, depot)
        routing = pywrapcp.RoutingModel(manager)

        for node_index in range(1, num_nodes):
            if extended_refrigerate[node_index]:
                node_idx = manager.NodeToIndex(node_index)
                for vehicle_id in range(num_vehicles):
                    if not vehicle_free[vehicle_id]:
                        routing.VehicleVar(node_idx).RemoveValue(vehicle_id)

        # Identificación de nodos especiales ANTES de asignar modos
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

        # MODIFICACIÓN: Permitir LA VEGA en rutas CENCOSUD
        MODE_FREE, MODE_W, MODE_C = 0, 1, 2
        for node_index in range(1, num_nodes):
            g = node_group[node_index]
            node_idx = manager.NodeToIndex(node_index)
            if g == "WALMART":
                allowed = {MODE_W}
            elif g == "CENCOSUD":
                allowed = {MODE_C}
            else:
                # LA VEGA puede ir en rutas MODE_C (CENCOSUD) además de MODE_FREE
                if is_lv_node[node_index]:
                    allowed = {MODE_FREE, MODE_C}
                else:
                    allowed = {MODE_FREE}
            for v in range(num_vehicles):
                if vehicle_mode[v] not in allowed:
                    routing.VehicleVar(node_idx).RemoveValue(v)
        
        # NUEVA RESTRICCIÓN: Si una ruta CENCOSUD incluye LA VEGA, debe ser la primera parada
        # Se implementa como PENALIZACIÓN SUAVE por defecto (más robusto)
        # Para activar restricción dura: enforce_lv_first_hard=true en el request
        solver = routing.solver()
        
        # Flag para activar/desactivar la restricción dura (desactivado por defecto)
        enforce_lv_first_hard = data.get("enforce_lv_first_hard", False)
        
        if enforce_lv_first_hard:
            # ADVERTENCIA: Esta restricción dura puede causar infactibilidad
            # Solo usarla cuando estés seguro de que la ruta es factible
            for v in range(num_vehicles):
                if vehicle_mode[v] == MODE_C:  # Solo para vehículos en modo CENCOSUD
                    start_idx = routing.Start(v)
                    
                    # Encontrar índices de LA VEGA y nodos CENCOSUD
                    lv_indices = [manager.NodeToIndex(i) for i in range(1, num_nodes) if is_lv_node[i]]
                    cencosud_indices = [manager.NodeToIndex(i) for i in range(1, num_nodes) 
                                       if node_group[i] == "CENCOSUD"]
                    
                    # Para cada nodo LA VEGA
                    for lv_idx in lv_indices:
                        lv_node = manager.IndexToNode(lv_idx)
                        
                        # Para cada nodo CENCOSUD (que no sea LA VEGA)
                        for cenc_idx in cencosud_indices:
                            cenc_node = manager.IndexToNode(cenc_idx)
                            if cenc_node == lv_node:
                                continue
                            
                            # Si ambos están en la misma ruta de este vehículo MODE_C,
                            # entonces LA VEGA debe venir directamente del start
                            lv_active = routing.ActiveVar(lv_idx)
                            cenc_active = routing.ActiveVar(cenc_idx)
                            
                            # Si ambos activos => LA VEGA es next(start)
                            # Usamos: lv_active + cenc_active <= 1 + lv_is_first
                            # Equivale a: (lv_active AND cenc_active) => lv_is_first
                            lv_next_var = routing.NextVar(start_idx)
                            solver.Add((lv_active + cenc_active - 1) <= 
                                     solver.IsEqualCstVar(lv_next_var, lv_idx))

        # Continuación del código original...
        any_pa_exists = any(is_pa_node[1:])
        any_lv_exists = any(is_lv_node[1:])
        any_tier3_exists = any(
            (is_tottus_node[i] or is_unimarc_node[i]) for i in range(1, len(is_tottus_node))
        )

        def is_tier3(i):
            return is_tottus_node[i] or is_unimarc_node[i]

        HIGH_PENALTY = 100_000
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

                    # MODIFICACIÓN: No penalizar mezcla LA VEGA + CENCOSUD
                    if from_node != depot and to_node != depot:
                        g_from, g_to = node_group[from_node], node_group[to_node]
                        
                        # EXCEPCIÓN: LA VEGA puede mezclarse con CENCOSUD
                        is_lv_exception = (
                            (is_lv_node[from_node] and g_to == "CENCOSUD") or
                            (g_from == "CENCOSUD" and is_lv_node[to_node])
                        )
                        
                        if not is_lv_exception:
                            if g_from != g_to and ("WALMART" in (g_from, g_to) or "CENCOSUD" in (g_from, g_to)):
                                base += HIGH_PENALTY
                    
                    # PENALIZACIÓN SUAVE: Si en modo CENCOSUD, penalizar ir a LA VEGA después de otro nodo CENCOSUD
                    if vehicle_mode[v_idx] == MODE_C:
                        if to_node != depot and is_lv_node[to_node]:
                            if from_node != depot and node_group[from_node] == "CENCOSUD":
                                # LA VEGA viene después de un nodo CENCOSUD (no es primera)
                                base += 50_000  # Penalización moderada
                    
                    return base
                return distance
            callback_idx = routing.RegisterTransitCallback(make_vehicle_callback(v))
            routing.SetArcCostEvaluatorOfVehicle(callback_idx, v)

        def demand_callback(from_index):
            node = manager.IndexToNode(from_index)
            return int(extended_demands[node])
        demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
        routing.AddDimensionWithVehicleCapacity(
            demand_callback_index, 0, vehicle_capacities, True, "Capacity"
        )

        PALET_SCALE = 100
        vehicle_palets_scaled = [int(round(p * PALET_SCALE)) if p < PALLET_INF else PALLET_INF
                                 for p in vehicle_palets]
        extended_palets_scaled = [int(round(p * PALET_SCALE)) for p in extended_palets]

        def palet_demand_callback(from_index):
            node = manager.IndexToNode(from_index)
            return extended_palets_scaled[node]
        palet_cb_idx = routing.RegisterUnaryTransitCallback(palet_demand_callback)
        routing.AddDimensionWithVehicleCapacity(
            palet_cb_idx, 0, vehicle_palets_scaled, True, "Palets"
        )

        start_indices = set(routing.Start(v) for v in range(num_vehicles))

        def time_callback(from_index, to_index):
            from_node = manager.IndexToNode(from_index)
            to_node   = manager.IndexToNode(to_index)
            travel = int(round(extended_time_matrix[from_node][to_node]))
            service = 0
            if from_node == depot and from_index not in start_indices:
                service += reload_service_time 
            if from_node != depot:
                service += int(round(extended_wait[from_node]))
            return travel + service

        time_cb = routing.RegisterTransitCallback(time_callback)
        routing.AddDimension(time_cb, 0, 10**7, False, "Time")
        time_dimension = routing.GetDimensionOrDie("Time")

        vehicle_start_offsets = {}
        if reference_departure_minutes is not None:
            for v in range(num_vehicles):
                base_idx = vehicle_mapping[v]
                dep_abs  = vehicle_departure_minutes_base[base_idx]
                if dep_abs is None:
                    offset = 0
                else:
                    offset = dep_abs - reference_departure_minutes
                    if offset < 0:
                        offset += 24 * 60
                start_idx = routing.Start(v)
                time_dimension.CumulVar(start_idx).SetRange(offset, offset)
                vehicle_start_offsets[v] = offset
        else:
            for v in range(num_vehicles):
                start_idx = routing.Start(v)
                time_dimension.CumulVar(start_idx).SetRange(0, 0)
                vehicle_start_offsets[v] = 0

        if reference_departure_minutes is not None:
            for node in range(1, num_nodes):
                idx = manager.NodeToIndex(node)
                lb = 0     
                ub = 10**7  

                op_gap = int(extended_opening_gap[node]) if extended_opening_gap[node] is not None else 0
                cl_gap = int(extended_closing_gap[node]) if extended_closing_gap[node] is not None else 0
                if op_gap < 0: op_gap = 0
                if cl_gap < 0: cl_gap = 0

                wait_here = int(round(extended_wait[node]))

                # CAMBIO IMPORTANTE: Las ventanas se aplican a la LLEGADA, no a la salida
                # Por eso NO restamos wait_time del deadline
                
                if extended_opening[node] is not None:
                    # Hora más temprana de LLEGADA permitida (con gap)
                    eff_open_arrival = int(extended_opening[node]) - op_gap
                    lb = max(lb, max(0, eff_open_arrival))
                    
                    # NUEVA LÓGICA: Si hay gap, el camión puede llegar temprano pero debe esperar
                    # hasta la apertura real para poder salir
                    if op_gap > 0:
                        opening_real = int(extended_opening[node])  # Sin gap
                        # El camión NO puede partir antes de: opening_real + wait_time
                        # Esto se modela aumentando el lower bound de tiempo acumulado
                        # PERO esto se maneja mejor con SlackVar...
                        
                        # Solución simple: ajustar el lb mínimo considerando que
                        # si llegas en eff_open_arrival, debes esperar hasta opening_real,
                        # luego esperar wait_here, y entonces partir
                        # Entonces el "tiempo efectivo" mínimo es opening_real + wait_here
                        # PERO esto no es el tiempo de llegada, es el tiempo de salida
                        
                        # La forma correcta: usar el SlackVar
                        pass  # Lo haremos después del SetRange

                if extended_deadline[node] is not None:
                    # Hora más tardía de LLEGADA permitida (NO restamos wait_time)
                    eff_deadline = int(extended_deadline[node]) - cl_gap
                    ub = min(ub, max(0, eff_deadline))

                # VALIDACIÓN: Verificar si la ventana es factible
                if extended_opening[node] is not None and extended_deadline[node] is not None:
                    if ub < lb:
                        loc_id = extended_locations[node].get('id', node)
                        loc_name = extended_locations[node].get('identificador', 'unknown')
                        print(f"⚠️ ADVERTENCIA: Ubicación {loc_id} ({loc_name}) tiene ventana imposible:")
                        print(f"   - Debe llegar después de: {lb} min desde salida (open={extended_opening[node]}, gap={op_gap})")
                        print(f"   - Debe llegar antes de: {ub} min desde salida (close={extended_deadline[node]}, gap={cl_gap})")
                        print(f"   - Tiempo de espera: {wait_here} min")

                if extended_opening[node] is not None or extended_deadline[node] is not None:
                    if lb <= ub:
                        time_dimension.CumulVar(idx).SetRange(lb, ub)
                    else:
                        # Si la ventana es imposible, usar solo el límite inferior
                        time_dimension.CumulVar(idx).SetRange(lb, lb)
                
                # IMPLEMENTACIÓN: Forzar espera hasta apertura real si hay opening_gap
                if extended_opening[node] is not None:
                    op_gap = int(extended_opening_gap[node]) if extended_opening_gap[node] is not None else 0
                    if op_gap > 0:
                        opening_real = int(extended_opening[node])  # Apertura real (SIN gap)
                        wait_here = int(round(extended_wait[node]))
                        
                        # SlackVar(idx) = tiempo que el vehículo espera en el nodo
                        # = CumulVar(next) - CumulVar(current) - TransitTime
                        #
                        # El TransitTime ya incluye wait_here (por el time_callback)
                        # Necesitamos agregar espera adicional si llegamos antes de opening_real
                        #
                        # Queremos: Si Cumul(idx) < opening_real, entonces debemos esperar
                        # hasta opening_real antes de contar wait_here
                        #
                        # Total de espera = max(0, opening_real - Cumul(idx)) + wait_here
                        #
                        # Como el time_callback ya suma wait_here al transit,
                        # necesitamos que Slack >= max(0, opening_real - Cumul(idx))
                        #
                        # SetSlackVarSoftLowerBound no acepta expresiones, solo valores fijos
                        # La solución es usar una constraint adicional:
                        
                        slack_var = time_dimension.SlackVar(idx)
                        arrival_var = time_dimension.CumulVar(idx)
                        
                        # Constraint: slack_var >= opening_real - arrival_var
                        # O sea: arrival_var + slack_var >= opening_real
                        solver.Add(arrival_var + slack_var >= opening_real)

        def drive_callback(from_index, to_index):
            from_node = manager.IndexToNode(from_index)
            to_node   = manager.IndexToNode(to_index)
            return int(round(extended_time_matrix[from_node][to_node]))

        drive_cb = routing.RegisterTransitCallback(drive_callback)
        routing.AddDimension(drive_cb, 0, HORIZON, True, "Drive")
        drive_dimension = routing.GetDimensionOrDie("Drive")

        for v in range(num_vehicles):
            drive_dimension.CumulVar(routing.End(v)).SetMax(HORIZON)
        
        for base in range(base_num_vehicles):
            end_cumuls = [
                drive_dimension.CumulVar(routing.End(v))
                for v in range(num_vehicles) if vehicle_mapping[v] == base
            ]
            solver.Add(solver.Sum(end_cumuls) <= HORIZON)

        def stop_callback(from_index, to_index):
            to_node = manager.IndexToNode(to_index)
            return 1 if to_node != depot else 0

        stop_cb = routing.RegisterTransitCallback(stop_callback)
        routing.AddDimension(stop_cb, 0, maximo_de_paradas, True, "Stops")
        stops_dimension = routing.GetDimensionOrDie("Stops")
        for v in range(num_vehicles):
            stops_dimension.CumulVar(routing.End(v)).SetMax(maximo_de_paradas)

        time_dimension.SetGlobalSpanCostCoefficient(50)

        costo_varias_rutas = True
        costo_reingreso_valor = int(data.get("costo_reingreso_valor", 100_000))
        if costo_varias_rutas:
            for v in range(num_vehicles):
                if vehicle_trip_no[v] > 0:
                    routing.SetFixedCostOfVehicle(costo_reingreso_valor, v)

        # ------------------------ Resolución del modelo ----------------------------
        update_job_status(job_id, "optimizando", "Buscando la primera ruta factible (esto puede tomar un momento)...", 50)
        
        search_parameters = pywrapcp.DefaultRoutingSearchParameters()
        search_parameters.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
        )
        search_parameters.local_search_metaheuristic = (
            routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
        )
        search_parameters.time_limit.FromSeconds(tiempo_calculo)

        req_workers = data.get("search_workers")
        if req_workers is None:
            req_workers = 32
        try:
            req_workers = int(req_workers) if req_workers is not None else 0
        except Exception:
            req_workers = 0
        if req_workers <= 0:
            req_workers = min(32, os.cpu_count() or 1)
        _safe_set(search_parameters, "number_of_workers", req_workers)
        _safe_set(search_parameters, "log_search", bool(data.get("log_search", False)))

        # --- NUEVO: Monitor de soluciones intermedias ---
        class RoutingMonitor:
            def __init__(self, routing_model, current_job_id):
                self.routing = routing_model
                self.job_id = current_job_id
                self.best_cost = float('inf')

            def __call__(self):
                cost = self.routing.CostVar().Min()
                if cost < self.best_cost:
                    self.best_cost = cost
                    update_job_status(self.job_id, "optimizando", "¡Mejora encontrada! Ajustando eficiencia de la ruta...", 75)

        monitor = RoutingMonitor(routing, job_id)
        routing.AddAtSolutionCallback(monitor)
        # -------------------------------------------------

        solution = routing.SolveWithParameters(search_parameters)
        if not solution:
            update_job_status(job_id, "fallo", "No se pudo encontrar ninguna ruta que cumpla todas las restricciones.", 100)
            return jsonify(error="No se pudo encontrar solución."), 400

        update_job_status(job_id, "procesando_resultados", "Ruta definitiva calculada. Extrayendo itinerarios...", 90)

        # ----------------------------- Extracción -------------------------------
        vehicle_trips = {}
        total_distance, total_fuel_liters = 0.0, 0.0
        total_kg, total_units, total_palets_sum = 0.0, 0.0, 0.0

        total_time_minutes_total = 0      
        total_time_minutes_drive = 0      
        total_stops_global = 0

        def cumul(dim, idx):
            return solution.Value(dim.CumulVar(idx))

        for v in range(num_vehicles):
            start = routing.Start(v)
            if routing.IsEnd(solution.Value(routing.NextVar(start))):
                continue

            main_vehicle = vehicle_mapping[v]
            trip_no = vehicle_trip_no[v]
            mode = vehicle_mode[v]
            start_offset = int(vehicle_start_offsets.get(v, 0))

            route_nodes = []
            deliveries = []
            dist_v = 0.0

            index = start
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

                    idx = index
                    time_cumul = cumul(time_dimension, idx)     
                    drive_cumul = cumul(drive_dimension, idx)   
                    stops_cumul = cumul(stops_dimension, idx)
                    wait_here = int(round(extended_wait[node]))
                    op_gap = int(extended_opening_gap[node]) if extended_opening_gap[node] is not None else 0
                    cl_gap = int(extended_closing_gap[node]) if extended_closing_gap[node] is not None else 0
                    if op_gap < 0: op_gap = 0
                    if cl_gap < 0: cl_gap = 0

                    arrival_from_departure = time_cumul - start_offset
                    departure_from_departure = time_cumul + wait_here - start_offset

                    if arrival_from_departure < 0:
                        arrival_from_departure = 0
                    if departure_from_departure < 0:
                        departure_from_departure = 0

                    deadline_rel = extended_deadline[node]          
                    deadline_ub_eff = None
                    deadline_slack = None
                    latest_arrival_from_departure = None
                    deadline_from_departure = None

                    if deadline_rel is not None:
                        # El deadline ahora es la hora límite de LLEGADA (no de salida)
                        eff_deadline = int(deadline_rel) - cl_gap
                        deadline_ub_eff = max(0, eff_deadline)  # Ya no restamos wait_time
                        deadline_from_departure = eff_deadline - start_offset
                        latest_arrival_from_departure = deadline_ub_eff - start_offset

                        if latest_arrival_from_departure is not None:
                            # Slack = cuánto tiempo tiene de margen antes del cierre
                            deadline_slack = latest_arrival_from_departure - arrival_from_departure

                    truck_departure_abs = (
                        (reference_departure_minutes + start_offset)
                        if reference_departure_minutes is not None
                        else None
                    )

                    eta_clock  = None
                    etd_clock  = None
                    open_clock = None      
                    close_clock = None     
                    open_eff_clock  = None   
                    close_eff_clock = None   

                    if truck_departure_abs is not None:
                        eta_clock  = _fmt_hhmm(truck_departure_abs + arrival_from_departure)
                        etd_clock  = _fmt_hhmm(truck_departure_abs + departure_from_departure)

                    if reference_departure_minutes is not None:
                        if extended_opening[node] is not None:
                            open_abs        = reference_departure_minutes + extended_opening[node]
                            open_clock      = _fmt_hhmm(open_abs)
                            open_eff_clock  = _fmt_hhmm(open_abs - op_gap)
                        if extended_deadline[node] is not None:
                            close_abs        = reference_departure_minutes + extended_deadline[node]
                            close_clock      = _fmt_hhmm(close_abs)
                            close_eff_clock  = _fmt_hhmm(close_abs - cl_gap)

                    opening_from_departure = None
                    closing_from_departure = None
                    if extended_opening[node] is not None:
                        opening_from_departure = int(extended_opening[node]) - start_offset
                    if extended_deadline[node] is not None:
                        closing_from_departure = int(extended_deadline[node]) - start_offset

                    margin_open = None
                    if opening_from_departure is not None:
                        eff_open_from_dep = opening_from_departure - op_gap
                        margin_open = int(eff_open_from_dep) - int(arrival_from_departure)

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
                            "palets": round(extended_palets[node], 2)
                        },
                        "timing": {
                            "eta_clock":  eta_clock,   
                            "etd_clock":  etd_clock,   
                            "wait_minutes": int(wait_here),
                            "open_clock":       open_clock,       
                            "close_clock":      close_clock,      
                            "open_eff_clock":   open_eff_clock,   
                            "close_eff_clock":  close_eff_clock,  
                            "opening_gap_minutes": int(op_gap),
                            "closing_gap_minutes": int(cl_gap),
                            "arrival_minutes_from_departure":   int(arrival_from_departure),
                            "departure_minutes_from_departure": int(departure_from_departure),
                            "opening_minutes_from_departure": (
                                int(opening_from_departure) if opening_from_departure is not None else None
                            ),
                            "closing_minutes_from_departure": (
                                int(closing_from_departure) if closing_from_departure is not None else None
                            ),
                            "margin_open_minutes": (
                                int(margin_open) if margin_open is not None else None
                            ),
                            "deadline_slack_minutes": (
                                int(deadline_slack) if deadline_slack is not None else None
                            ),
                            "deadline_minutes_from_departure": (
                                int(deadline_from_departure) if deadline_from_departure is not None else None
                            ),
                            "latest_arrival_allowed_minutes_from_departure": (
                                int(latest_arrival_from_departure)
                                if latest_arrival_from_departure is not None else None
                            ),
                        },
                        "cumul": {
                            "time_cumul_minutes":    int(time_cumul),
                            "time_from_departure":   int(arrival_from_departure),
                            "drive_cumul_minutes":   int(drive_cumul),
                            "stops_cumul":           int(stops_cumul)
                        }
                    })

                prev = index
                index = solution.Value(routing.NextVar(index))
                d = extended_distance_matrix[manager.IndexToNode(prev)][manager.IndexToNode(index)]
                dist_v += d
                total_distance += d

            time_total  = solution.Value(time_dimension.CumulVar(routing.End(v)))  
            time_drive  = solution.Value(drive_dimension.CumulVar(routing.End(v))) 
            stops_count = solution.Value(stops_dimension.CumulVar(routing.End(v)))

            duration_end = int(time_total) - start_offset
            if duration_end < 0:
                duration_end = 0

            total_time_minutes_total += duration_end   
            total_time_minutes_drive += int(time_drive)
            total_stops_global += int(stops_count)

            fuel = dist_v / max(vehicle_consume[v], 1e-9)
            total_fuel_liters += fuel

            if not deliveries:
                continue
            trip_kg = sum((d["totals"]["kg"] for d in deliveries if d.get("totals")), 0.0)
            trip_units_vals = [d["totals"].get("units") for d in deliveries if d.get("totals")]
            trip_units_sum = sum((u for u in trip_units_vals if u is not None), 0.0)
            trip_palets_sum = sum(d["totals"].get("palets", 0.0) for d in deliveries if d.get("totals"))

            raw_route = [0] + [
                extended_locations[n].get("id") if n else 0
                for n in [node for node in route_nodes]
            ] + [0]
            cleaned_route = [raw_route[0]]
            for n in raw_route[1:]:
                if not (n == 0 and cleaned_route[-1] == 0):
                    cleaned_route.append(n)

            dep_abs_v = (
                (reference_departure_minutes + start_offset)
                if reference_departure_minutes is not None else None
            )
            departure_clock_v = _fmt_hhmm(dep_abs_v) if dep_abs_v is not None else None

            return_minutes_from_departure = int(time_total) - start_offset
            if return_minutes_from_departure < 0:
                return_minutes_from_departure = 0
            return_clock_v = (
                _fmt_hhmm(dep_abs_v + return_minutes_from_departure)
                if dep_abs_v is not None else None
            )
            trip_duration_minutes = return_minutes_from_departure

            agg = vehicle_trips.setdefault(main_vehicle, {
                "vehicle": main_vehicle,
                "departure_clock": departure_clock_v,   
                "trips": [],
                "total_distance": 0.0,
                "total_fuel_liters": 0.0,
                "total_kg": 0.0,
                "total_units": 0.0,
                "total_palets": 0.0,
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
                "mode": mode,  
                "route": cleaned_route,
                "deliveries": deliveries,
                "num_stops": int(stops_count),
                "departure_clock": departure_clock_v,      
                "return_clock": return_clock_v,            
                "duration_minutes": trip_duration_minutes, 
                "time_minutes_total": duration_end,        
                "time_minutes_drive": int(time_drive),     
                "distance": float(dist_v),
                "fuel_liters": float(fuel),
                "total_kg": round(trip_kg, 2),
                "total_units": (round(trip_units_sum, 2) if trip_units_sum > 0 else None),
                "total_palets": round(trip_palets_sum, 2)
            })

            agg["total_distance"] += float(dist_v)
            agg["total_fuel_liters"] += float(fuel)
            agg["total_kg"] += float(trip_kg)
            agg["total_units"] += float(trip_units_sum)
            agg["total_palets"] += float(trip_palets_sum)

            agg["total_time_minutes_total"] += duration_end
            agg["total_time_minutes_drive"] += int(time_drive)
            agg["total_stops"] += int(stops_count)
            agg["modes_used"].add(int(mode))

            total_kg += float(trip_kg)
            total_units += float(trip_units_sum)
            total_palets_sum += float(trip_palets_sum)

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

        reference_departure_time_str = (
            _fmt_hhmm(reference_departure_minutes)
            if reference_departure_minutes is not None else None
        )

        update_job_status(job_id, "completado", "Optimización finalizada exitosamente.", 100)

        return jsonify({
            "status": "success",
            "meta": {
                "max_vehicles": base_num_vehicles,
                "max_trips_per_vehicle": max_trips_per_vehicle,
                "time_horizon_drive_minutes": HORIZON,
                "max_stops_per_vehicle": maximo_de_paradas,
                "time_multiplier": multiplicador_tiempo,
                "reload_service_time_minutes": reload_service_time,
                "vehicle_departure_times": vehicle_departure_times_raw,
                "departure_times_by_truck": departure_times_by_truck_raw,
                "reference_departure_time": reference_departure_time_str,
                "departure_time_global": departure_time_global_str or reference_departure_time_str,
                "departure_time":        departure_time_global_str or reference_departure_time_str,
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
                "total_palets": round(total_palets_sum, 2),
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
        if job_id:
            update_job_status(job_id, "fallo", f"Error interno: {str(e)}", 100)
        return jsonify(error=f"Error interno: {str(e)}", traceback=traceback.format_exc()), 500

# ---------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000, debug=False)