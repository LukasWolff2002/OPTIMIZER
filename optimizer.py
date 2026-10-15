from flask import Flask, Response, request, jsonify
from ortools.constraint_solver import pywrapcp, routing_enums_pb2
import copy
import os
import json
import queue
import redis
import threading
import time
import uuid
from datetime import datetime

app = Flask(__name__)

# ---------------------------------------------------------------------
# Conexión a Redis y Función de Estado
# ---------------------------------------------------------------------
redis_url = os.environ.get("REDIS_URL")

redis_client = None
if redis_url:
    try:
        if redis_url.startswith("rediss://"):
            import ssl
            redis_client = redis.from_url(redis_url, ssl_cert_reqs=ssl.CERT_NONE)
        else:
            redis_client = redis.from_url(redis_url)
        
        redis_client.ping()
        print("✅ Conectado a Redis exitosamente.")
    except Exception as e:
        print(f"❌ Error conectando a Redis: {e}")
        redis_client = None
else:
    print("⚠️ ADVERTENCIA: Variable REDIS_URL no configurada.")

# Oyentes de progreso por job_id. Los registra /optimize/stream para mandar el
# progreso por la MISMA conexión HTTP en que después va el resultado. Redis sólo
# llegaba a la app si ambos servicios compartían la misma instancia (y la app
# además tenía que poder conectarse a ella con TLS), cosa que casi nunca se
# cumplía: la pantalla "Procesando" quedaba en "Enviando datos..." hasta el final.
_oyentes_progreso = {}
_oyentes_lock = threading.Lock()


def _registrar_oyente(job_id, oyente):
    with _oyentes_lock:
        _oyentes_progreso[str(job_id)] = oyente


def _quitar_oyente(job_id):
    with _oyentes_lock:
        _oyentes_progreso.pop(str(job_id), None)


def update_job_status(job_id: str, status: str, message: str, progress: int,
                     detalle=None, codigo=None, sugerencias=None, **extra):
    """Publica el progreso: al oyente de la conexión (si la hay) y en Redis (si
    está configurado). Falla en silencio para no quebrar el optimizador.

    `detalle`, `codigo` y `sugerencias` viajan hasta la pantalla de "Procesando"
    de la app: sin ellos, un fallo sólo se veía como "Error en la optimización".
    `extra` lleva las cifras de la búsqueda (mejoras, camiones, km, paradas,
    segundos transcurridos y tope) para que la pantalla muestre algo real.
    """
    if not job_id:
        return
    estado = {
        "status": status,
        "message": message,
        "progress": progress,
        "updated_at": datetime.utcnow().isoformat()
    }
    if detalle:
        estado["detalle"] = list(detalle)[:20]
    if codigo:
        estado["error_code"] = codigo
    if sugerencias:
        estado["sugerencias"] = list(sugerencias)[:10]
    estado.update({k: v for k, v in extra.items() if v is not None})

    with _oyentes_lock:
        oyente = _oyentes_progreso.get(str(job_id))
    if oyente:
        try:
            oyente(dict(estado))
        except Exception as e:
            print(f"Error avisando progreso: {e}")

    if not redis_client:
        return
    try:
        redis_client.setex(f"opt_status_{job_id}", 3600, json.dumps(estado))
    except Exception as e:
        print(f"Error escribiendo en Redis: {e}")

# ---------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------

# Multa por dejar un local fuera de la ruta. Sólo se usa en el diagnóstico de
# infactibilidad: tiene que ser mucho mayor que cualquier costo de arco real
# para que el solver descarte un local únicamente cuando NO existe forma de
# incluirlo.
PENALIZACION_DESCARTE = 10**9


def _safe_set(obj, attr, value, diag=None):
    """Setea un atributo protobuf si existe. Devuelve si se aplicó de verdad.

    Antes fallaba en silencio y la respuesta seguía informando el valor pedido
    como si se hubiera aplicado: `number_of_workers` NO existe en OR-Tools 9.x,
    así que la meta decía "workers: 32" sobre un parámetro que nunca se seteó.
    """
    try:
        setattr(obj, attr, value)
        return True
    except Exception as e:
        if diag is not None:
            diag.aviso("PARAMETRO_IGNORADO",
                       f"El solver no acepta '{attr}' en esta versión de OR-Tools; se ignora.",
                       parametro=attr, valor=str(value), motivo=str(e))
        return False

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

def _add_minutes_to_clock(clock_str: str, minutes) -> str:
    """Agrega minutos a un string 'HH:MM'."""
    if not clock_str or ":" not in str(clock_str):
        return clock_str
    try:
        hh, mm = str(clock_str).split(":")
        total = int(hh) * 60 + int(mm) + int(round(float(minutes)))
        total %= (24 * 60)
        return f"{total // 60:02d}:{total % 60:02d}"
    except Exception:
        return clock_str

# ---------------------------------------------------------------------
# Diagnóstico: por qué falló, en lenguaje de quien arma las rutas
# ---------------------------------------------------------------------
#
# Antes, cualquier problema devolvía "No se pudo encontrar solución." y nada
# más. Con 40 locales, 8 camiones, ventanas horarias, palets y refrigeración,
# eso no le dice a nadie qué cambiar. Aquí se registra cada decisión del armado
# del modelo y, cuando algo falla, se identifica la causa concreta.

class ErrorOptimizacion(Exception):
    """Fallo con causa identificada.

    `codigo` es estable y pensado para que la UI pueda reaccionar; `mensaje` es
    la frase que ve el usuario; `detalle` son las líneas concretas (qué local,
    cuántos kilos faltan) y `sugerencias` qué hacer al respecto.
    """

    def __init__(self, codigo, mensaje, detalle=None, sugerencias=None, datos=None, http=400):
        super().__init__(mensaje)
        self.codigo = codigo
        self.mensaje = mensaje
        self.detalle = list(detalle or [])
        self.sugerencias = list(sugerencias or [])
        self.datos = datos or {}
        self.http = http


class Diagnostico:
    """Bitácora de la corrida. Se imprime (Railway) y se devuelve en el JSON."""

    ICONOS = {"info": "•", "aviso": "⚠️", "error": "❌"}

    def __init__(self, job_id=None):
        self.job_id = job_id
        self.eventos = []
        self.avisos = 0
        self.errores = 0

    def registrar(self, nivel, codigo, mensaje, **datos):
        evento = {"nivel": nivel, "codigo": codigo, "mensaje": mensaje}
        if datos:
            evento["datos"] = datos
        self.eventos.append(evento)
        if nivel == "aviso":
            self.avisos += 1
        elif nivel == "error":
            self.errores += 1
        print(f"{self.ICONOS.get(nivel, '•')} [{codigo}] {mensaje}", flush=True)
        return evento

    def info(self, codigo, mensaje, **d):
        return self.registrar("info", codigo, mensaje, **d)

    def aviso(self, codigo, mensaje, **d):
        return self.registrar("aviso", codigo, mensaje, **d)

    def error(self, codigo, mensaje, **d):
        return self.registrar("error", codigo, mensaje, **d)

    # Códigos que son detalle técnico del servicio: quedan en la bitácora pero no
    # se le muestran a quien arma la ruta, que no puede hacer nada con ellos.
    CODIGOS_TECNICOS = {"PARAMETRO_IGNORADO", "DIAGNOSTICO_RELAJACION_FALLO",
                        "DIAGNOSTICO_DESCARTES_FALLO"}

    def mensajes(self, nivel, incluir_tecnicos=False):
        return [e["mensaje"] for e in self.eventos
                if e["nivel"] == nivel
                and (incluir_tecnicos or e["codigo"] not in self.CODIGOS_TECNICOS)]

    def resumen(self, limite=300):
        return {
            "eventos": self.eventos[-limite:],
            "total_eventos": len(self.eventos),
            "avisos": self.avisos,
            "errores": self.errores,
        }


def _num(x, dec=0):
    """Formato chileno: 1.234,5 — el mismo que usa la app."""
    try:
        s = f"{float(x):,.{dec}f}"
    except Exception:
        return str(x)
    return s.replace(",", "@").replace(".", ",").replace("@", ".")


def _nombre_local(loc, idx=None):
    ident = (loc.get("identificador") or "").strip()
    lid = loc.get("id")
    if ident and lid is not None:
        return f"{ident} (ID {lid})"
    if ident:
        return ident
    if lid is not None:
        return f"Local ID {lid}"
    return f"Local #{idx}"


# ---------------------------------------------------------------------
# Revisiones ANTES de resolver: lo que se puede descartar sin el solver
# ---------------------------------------------------------------------

# Grupo excluyente de un local. Walmart y Cencosud no comparten viaje con otros
# clientes, y cada camión sólo puede atender UN grupo por viaje.
#
# Está a nivel de módulo porque hace falta ANTES de armar la flota virtual, para
# saber qué modos se necesitan de verdad (ver `modos_necesarios`).
def _group(identificador: str) -> str:
    ident = (identificador or "").upper()
    if "WALMART CD" in ident:
        return "WALMART"
    if "CENCOSUD CD" in ident:
        return "CENCOSUD"
    return "OTHER"


def _es_la_vega(identificador: str) -> bool:
    # La misma regla que `is_lv_node` en construir_modelo.
    return "LA VEGA" in (identificador or "").upper()


# ── Zonas (pedido del usuario, 13-10-2026) ─────────────────────────────────
# Con «mezclar_zonas» en falso, un camión que lleva algo a un local de la
# Región Metropolitana solo reparte a locales de la Región Metropolitana: los
# de otras zonas (Quinta Región, Zona Sur, sin zona) van en otro camión. Es de
# la Región Metropolitana todo local cuya zona dice METROPOLITANA («REGION
# METROPOLITANA», «… ZONA ORIENTE», «… ZONA PONIENTE», «… ZONA SUR»). El
# packing (el depósito) no cuenta.
ZONA_METRO = "METRO"
ZONA_OTRA = "OTRA"
ZONA_ETIQUETAS = {ZONA_METRO: "Región Metropolitana", ZONA_OTRA: "otras zonas"}


def _zona_clase(zona) -> str:
    return ZONA_METRO if "METROPOLITANA" in str(zona or "").upper() else ZONA_OTRA


def _leer_bool(valor, por_defecto):
    """true/false del JSON, o «1»/«0» y «true»/«false» como texto."""
    if valor is None:
        return por_defecto
    return bool(valor) and str(valor).strip().lower() not in ("false", "0", "")


def _etiqueta_grupo(g, resto="el resto de los locales"):
    """Nombre de un grupo de `_grupos_por_camion` para los mensajes."""
    etiquetas = {"WALMART": "Walmart CD", "CENCOSUD": "Cencosud CD", "OTHER": resto}
    if isinstance(g, tuple):
        grupo, zona = g
        return f"{etiquetas.get(grupo, grupo)} ({ZONA_ETIQUETAS.get(zona, zona)})"
    return etiquetas.get(g, g)


def _grupos_por_camion(nodos):
    """Grupos de locales que necesitan cada uno su propio camión, con sus nodos.

    `nodos` es [(índice, identificador)] o, si no se mezclan zonas,
    [(índice, identificador, zona)]: entonces el mismo reparto se hace dentro
    de cada zona y la clave de cada grupo es (grupo, zona), porque un camión no
    puede tomar locales de dos zonas.
    """
    por_zona = {}
    for nodo in nodos:
        zona = nodo[2] if len(nodo) > 2 else None
        por_zona.setdefault(zona, []).append((nodo[0], nodo[1]))
    grupos = {}
    for zona, lista in por_zona.items():
        for g, idxs in _grupos_de_clientes(lista).items():
            grupos[g if zona is None else (g, zona)] = idxs
    return grupos


def _grupos_de_clientes(nodos):
    """Grupos por cliente de una lista de nodos [(índice, identificador)].

    Walmart CD y Cencosud CD van solos y
    el resto junto ("OTHER"), como en el modelo. La Vega es la excepción: el
    modelo la deja ir en modo libre O en el de Cencosud (ver `allowed` en
    construir_modelo), así que no abre un grupo propio si ya hay resto o
    Cencosud; sólo lo abre, como resto, cuando no hay ninguno de los dos.

    Antes La Vega contaba siempre como resto, y La Vega + Cencosud CD con un
    solo camión se rechazaba con GRUPOS_EXCEDEN_CAMIONES aunque el modelo sí
    la resolvía (las dos en el mismo viaje, en modo Cencosud).
    """
    grupos, la_vega = {}, []
    for i, ident in nodos:
        if _group(ident) == "OTHER" and _es_la_vega(ident):
            la_vega.append(i)
        else:
            grupos.setdefault(_group(ident), []).append(i)
    if la_vega:
        destino = "CENCOSUD" if ("CENCOSUD" in grupos and "OTHER" not in grupos) else "OTHER"
        grupos.setdefault(destino, []).extend(la_vega)
    return grupos


def modos_necesarios(locations, modo_free, modo_w, modo_c):
    """Qué modos hacen falta para ESTA solicitud.

    Cada camión se modelaba siempre como tres vehículos virtuales (libre,
    Walmart, Cencosud) aunque la ruta no tuviera un solo local de esos grupos:
    el modelo era 3× más grande de lo necesario y el solver gastaba su tiempo
    descartando vehículos que nunca podían usarse.

    Un modo sólo se necesita si hay locales que lo exijan:
      · Walmart  → hay locales del grupo WALMART
      · Cencosud → hay locales del grupo CENCOSUD
      · libre    → hay locales del resto
    (La Vega cuenta aquí como resto y abre el modo libre aunque también puede
    ir en el de Cencosud: a lo más sobra un modo, que el solver no usa. Para
    contar cuántos camiones hacen falta sí se la junta con Cencosud: ver
    `_grupos_por_camion`.)
    """
    grupos = {_group(loc.get("identificador", "")) for loc in locations[1:]}
    modos = []
    if "OTHER" in grupos:
        modos.append(modo_free)
    if "WALMART" in grupos:
        modos.append(modo_w)
    if "CENCOSUD" in grupos:
        modos.append(modo_c)
    return tuple(modos) or (modo_free,), grupos


def revisar_estructura(data, truck_ids, diag):
    """Valida que el JSON traiga lo mínimo y con la forma correcta.

    Antes, una clave faltante reventaba con KeyError y el usuario recibía
    'Error interno: locations' sin más contexto.
    """
    faltantes = [k for k in ("locations", "max_vehicles", "vehicle_capacities", "distance_matrix")
                 if k not in data]
    if faltantes:
        raise ErrorOptimizacion(
            "PAYLOAD_INCOMPLETO",
            "La solicitud llegó incompleta al optimizador.",
            detalle=[f"Faltan los campos: {', '.join(faltantes)}."],
            sugerencias=["Es un error de la aplicación, no de los datos que cargaste. Reporta el problema."],
            datos={"campos_faltantes": faltantes},
        )

    locations = data.get("locations") or []
    if len(locations) < 2:
        raise ErrorOptimizacion(
            "SIN_LOCALES",
            "No hay locales que visitar.",
            detalle=[f"Llegaron {len(locations)} ubicaciones y hace falta al menos el depósito más un local."],
            sugerencias=["Vuelve al optimizador y selecciona al menos un local además del packing."],
        )

    n = len(locations)
    etiquetas_matriz = {"distance_matrix": "matriz de distancias", "time_matrix": "matriz de tiempos"}
    for nombre in ("distance_matrix", "time_matrix"):
        matriz = data.get(nombre)
        if matriz is None:
            continue
        if len(matriz) != n or any(len(fila) != n for fila in matriz):
            filas = len(matriz)
            cols = sorted({len(f) for f in matriz})
            raise ErrorOptimizacion(
                "MATRIZ_INCONSISTENTE",
                f"La {etiquetas_matriz[nombre]} no calza con la cantidad de locales.",
                detalle=[f"Se esperaba una matriz de {n}×{n} y llegó de {filas}×{cols}."],
                sugerencias=["Recalcula las matrices desde Ubicaciones y vuelve a intentarlo."],
                datos={"matriz": nombre, "esperado": n, "filas": filas, "columnas": cols},
            )

    if data.get("time_matrix") is None:
        raise ErrorOptimizacion(
            "SIN_MATRIZ_TIEMPO",
            "Falta la matriz de tiempos de viaje.",
            detalle=["Sin tiempos de viaje no se pueden respetar los horarios de atención de los locales."],
            sugerencias=["Recalcula las matrices desde Ubicaciones (botón de recalcular) y reintenta."],
        )

    faltan_coord = [_nombre_local(l, i) for i, l in enumerate(locations)
                    if l.get("lat") in (None, "") or l.get("lng") in (None, "")]
    if faltan_coord:
        diag.aviso("LOCAL_SIN_COORDENADAS",
                   f"{len(faltan_coord)} ubicación(es) sin coordenadas: {', '.join(faltan_coord[:5])}")

    diag.info("PAYLOAD_OK",
              f"Entrada válida: {n - 1} local(es) + depósito, {data.get('max_vehicles')} camión(es).")


def revisar_factibilidad(ctx, diag):
    """Chequeos de capacidad, palets, refrigeración, paradas y ventanas.

    Todos son condiciones que hacen imposible la ruta y que se pueden detectar
    con aritmética, sin gastar minutos de solver para terminar en un 'no se
    encontró solución'.
    """
    problemas = []

    # ── Capacidad en kilos ────────────────────────────────────────────────
    demanda_total = sum(ctx["extended_demands"][1:])
    capacidad_total = sum(ctx["vehicle_capacities_base"])
    if demanda_total > capacidad_total:
        faltan = demanda_total - capacidad_total
        problemas.append(ErrorOptimizacion(
            "CAPACIDAD_KG_INSUFICIENTE",
            "La carga no cabe en los camiones seleccionados.",
            detalle=[
                f"Demanda total: {_num(demanda_total)} kg.",
                f"Capacidad de la flota: {_num(capacidad_total)} kg "
                f"({len(ctx['vehicle_capacities_base'])} camión(es)).",
                f"Faltan {_num(faltan)} kg de capacidad.",
            ],
            sugerencias=[
                "Agrega otro camión a la selección.",
                "Baja las cantidades pedidas o saca algún local de esta ruta.",
            ],
            datos={"demanda_kg": demanda_total, "capacidad_kg": capacidad_total, "faltan_kg": faltan},
        ))

    # ── Palets ────────────────────────────────────────────────────────────
    palets_total = sum(ctx["extended_palets"][1:])
    palets_flota = sum(p for p in ctx["vehicle_palets_base"] if p < ctx["PALLET_INF"])
    camiones_con_limite = sum(1 for p in ctx["vehicle_palets_base"] if p < ctx["PALLET_INF"])
    if camiones_con_limite == len(ctx["vehicle_palets_base"]) and palets_total > palets_flota:
        problemas.append(ErrorOptimizacion(
            "PALETS_INSUFICIENTES",
            "Los palets en suelo no caben en los camiones seleccionados.",
            detalle=[
                f"Palets pedidos: {_num(palets_total, 1)}.",
                f"Palets disponibles en la flota: {_num(palets_flota)}.",
                f"Faltan {_num(palets_total - palets_flota, 1)} palets.",
            ],
            sugerencias=[
                "Agrega un camión con más capacidad de palets.",
                "Revisa los palets en suelo cargados en la pantalla de demandas.",
            ],
            datos={"palets_pedidos": palets_total, "palets_flota": palets_flota},
        ))

    # ── Refrigeración ─────────────────────────────────────────────────────
    nodos_refrigerados = [i for i in range(1, ctx["num_nodes"]) if ctx["extended_refrigerate"][i]]
    if nodos_refrigerados:
        camiones_refri = [i for i, f in enumerate(ctx["vehicle_free_base"]) if f]
        if not camiones_refri:
            nombres = [_nombre_local(ctx["extended_locations"][i], i) for i in nodos_refrigerados]
            problemas.append(ErrorOptimizacion(
                "SIN_CAMION_REFRIGERADO",
                "Hay locales que exigen camión refrigerado y no seleccionaste ninguno.",
                detalle=[f"Locales que lo exigen: {', '.join(sorted(set(nombres))[:10])}."],
                sugerencias=["Selecciona al menos un camión refrigerado, o saca esos locales de la ruta."],
                datos={"locales": sorted(set(nombres))},
            ))
        else:
            kg_refri = sum(ctx["extended_demands"][i] for i in nodos_refrigerados)
            cap_refri = sum(ctx["vehicle_capacities_base"][i] for i in camiones_refri)
            if kg_refri > cap_refri:
                problemas.append(ErrorOptimizacion(
                    "CAPACIDAD_REFRIGERADA_INSUFICIENTE",
                    "La carga que necesita frío no cabe en los camiones refrigerados.",
                    detalle=[
                        f"Carga refrigerada: {_num(kg_refri)} kg.",
                        f"Capacidad refrigerada: {_num(cap_refri)} kg en {len(camiones_refri)} camión(es).",
                    ],
                    sugerencias=["Suma otro camión refrigerado o reparte la carga en otra fecha."],
                    datos={"kg_refrigerado": kg_refri, "capacidad_refrigerada": cap_refri},
                ))

    # ── Grupos exclusivos: cada camión sólo puede tomar UN grupo por viaje ──
    # Sin mezclar zonas, cada grupo además se parte por zona (Región
    # Metropolitana / otras zonas): un camión no toma locales de las dos.
    def _ident(i):
        return ctx["extended_locations"][i].get("identificador", "")

    zonas = ctx.get("node_zona_clase")  # None: se pueden mezclar zonas

    def _nodo(i):
        return (i, _ident(i)) if zonas is None else (i, _ident(i), zonas[i])

    nota_zonas = ("No se mezclan zonas: un camión que va a la Región Metropolitana no reparte en otras "
                  "zonas, así que cada zona necesita su propio camión.")
    grupos = _grupos_por_camion([_nodo(i) for i in range(1, ctx["num_nodes"])])
    if len(grupos) > ctx["base_num_vehicles"]:
        detalle = [f"{_etiqueta_grupo(g)}: {len(v)} local(es)." for g, v in sorted(grupos.items())]
        sin_zonas = len(_grupos_por_camion([(i, _ident(i)) for i in range(1, ctx["num_nodes"])]))
        por_zonas = zonas is not None and sin_zonas <= ctx["base_num_vehicles"]
        problemas.append(ErrorOptimizacion(
            "ZONAS_EXCEDEN_CAMIONES" if por_zonas else "GRUPOS_EXCEDEN_CAMIONES",
            ("Sin mezclar zonas, hacen falta más camiones de los seleccionados." if por_zonas
             else "Hay más grupos excluyentes de locales que camiones disponibles."),
            detalle=([nota_zonas] if zonas is not None else []) + [
                "Walmart y Cencosud no pueden ir en el mismo viaje que otros clientes "
                "(La Vega sí puede ir con Cencosud), así que cada grupo necesita su propio camión.",
                f"Grupos en esta ruta: {len(grupos)}. Camiones seleccionados: {ctx['base_num_vehicles']}.",
            ] + detalle,
            sugerencias=[
                f"Selecciona al menos {len(grupos)} camiones.",
            ] + (["O permite mezclar zonas en los ajustes del algoritmo."] if zonas is not None else []) + [
                "O deja los locales de un grupo para otra ruta.",
            ],
            datos={"grupos": {_etiqueta_grupo(g): len(v) for g, v in grupos.items()},
                   "camiones": ctx["base_num_vehicles"]},
        ))

    # ── Refrigerados vs grupos ────────────────────────────────────────────
    # Cada camión atiende UN grupo por viaje (Walmart, Cencosud o el resto), y
    # sólo un refrigerado puede llevar a un local que exige frío. Si los locales
    # que exigen frío forman más grupos que refrigerados hay, uno queda sin
    # camión. Antes esto terminaba en "no encaja junto con el resto de las
    # paradas", que no dice nada: pasó con Walmart + Cencosud + Jumbo y dos
    # refrigerados (el tercero de la flota era de carga seca).
    if nodos_refrigerados:
        camiones_refri = [i for i, f in enumerate(ctx["vehicle_free_base"]) if f]
        grupos_frio = {
            g: {_nombre_local(ctx["extended_locations"][i], i) for i in nodos}
            for g, nodos in _grupos_por_camion([_nodo(i) for i in nodos_refrigerados]).items()
        }
        if camiones_refri and len(grupos_frio) > len(camiones_refri):
            faltan = len(grupos_frio) - len(camiones_refri)
            problemas.append(ErrorOptimizacion(
                "REFRIGERADOS_INSUFICIENTES_POR_GRUPO",
                "Faltan camiones refrigerados: los locales que exigen frío no pueden ir todos juntos.",
                detalle=[
                    "Walmart y Cencosud no comparten camión con otros clientes, y cada camión "
                    "atiende un solo grupo por viaje: cada grupo que exige frío necesita su propio refrigerado.",
                    f"Refrigerados seleccionados: {len(camiones_refri)} de {len(ctx['vehicle_free_base'])} camiones. "
                    f"Grupos que exigen frío: {len(grupos_frio)}.",
                ] + ([nota_zonas] if zonas is not None else [])
                  + [f"{_etiqueta_grupo(g, 'el resto de los clientes')}: {', '.join(sorted(n))}."
                     for g, n in sorted(grupos_frio.items())],
                sugerencias=[
                    f"Selecciona {faltan} camión(es) refrigerado(s) más.",
                ] + (["O permite mezclar zonas en los ajustes del algoritmo."] if zonas is not None else []) + [
                    "O deja los locales de uno de esos grupos para otra ruta.",
                ],
                datos={"grupos_frio": {_etiqueta_grupo(g, 'el resto de los clientes'): sorted(n)
                                       for g, n in grupos_frio.items()},
                       "refrigerados": len(camiones_refri)},
            ))

    # ── Máximo de paradas ─────────────────────────────────────────────────
    paradas_necesarias = ctx["num_nodes"] - 1
    paradas_posibles = ctx["maximo_de_paradas"] * ctx["base_num_vehicles"]
    if paradas_necesarias > paradas_posibles:
        problemas.append(ErrorOptimizacion(
            "PARADAS_INSUFICIENTES",
            "El máximo de paradas por camión no alcanza para todos los locales.",
            detalle=[
                f"Paradas a repartir: {paradas_necesarias} "
                f"(algunos locales se dividen en dos si su carga no cabe en un camión).",
                f"Tope actual: {ctx['maximo_de_paradas']} paradas × {ctx['base_num_vehicles']} camión(es) "
                f"= {paradas_posibles}.",
            ],
            sugerencias=[
                f"Sube 'Máx. paradas / camión' a {-(-paradas_necesarias // max(ctx['base_num_vehicles'], 1))} o más.",
                "O agrega otro camión.",
            ],
            datos={"paradas_necesarias": paradas_necesarias, "paradas_posibles": paradas_posibles},
        ))

    return problemas


def _descarga_mas_larga_que_ventana(apertura, cierre, cl_gap, servicio):
    """La descarga no cabe entre la apertura y el cierre (menos la tolerancia de cierre).

    Desde el 15-10-2026 esos locales se aceptan si el camión llega antes de que
    abra: la descarga empieza al abrir y termina después del cierre. Antes
    dejaban la ruta entera sin solución.
    """
    if apertura is None or cierre is None or cierre <= apertura:
        return False
    largo = cierre - max(0, int(cl_gap or 0)) - apertura
    return int(round(servicio)) > largo


# ---------------------------------------------------------------------
# Hora de salida de cada camión
# ---------------------------------------------------------------------
#
# El modelo minimiza la jornada de la FLOTA (de la primera salida al último
# regreso): la hora en que arranca cada camión le da lo mismo, salvo la del que
# sale primero. Así todos quedaban con el mismo atraso (cuatro camiones pedidos
# a las 22:00 salían los cuatro a las 23:00: el del CD llegaba sin margen y los
# otros tres esperaban cinco horas a que abriera su primer local), y el ajuste
# de después atrasaba la salida aunque no hubiera espera que ahorrar. Desde el
# 15-10-2026 la salida de cada camión se calcula sobre SU recorrido.

SEGURIDAD_LLEGADA_MINUTOS = 30


def _recorrer_viaje(paradas, salida, vuelta):
    """Horario de un viaje que sale a `salida`: si llega antes de que pueda
    estar en el andén, espera; descarga desde que abre. Devuelve (llegadas,
    inicios de descarga, salidas, regreso, minutos de espera en cada parada)."""
    t = salida
    llegadas, inicios, salidas, esperas = [], [], [], []
    for p in paradas:
        en_el_lugar = t + p["viaje"]
        llegada = en_el_lugar if p["desde"] is None else max(en_el_lugar, p["desde"])
        inicio = llegada if p["abre"] is None else max(llegada, p["abre"])
        t = inicio + p["descarga"]
        llegadas.append(llegada)
        inicios.append(inicio)
        salidas.append(t)
        esperas.append(inicio - en_el_lugar)
    return llegadas, inicios, salidas, t + vuelta, esperas


def horario_del_viaje(paradas, salida_pedida, margen, vuelta, seguridad=SEGURIDAD_LLEGADA_MINUTOS):
    """La salida de UN camión y su horario, sin mirar a los demás.

    Sale a la hora pedida y se atrasa sólo lo que le ahorra espera (a que abra
    un local), sin pasar del margen y sin acercar ninguna llegada a su tope a
    menos de `seguridad` minutos (la que ya estaba más cerca no se mueve).

    paradas: [{"viaje", "desde", "abre", "tope", "descarga"}] en orden, en
    minutos desde la salida de referencia; desde, abre y tope pueden ser None.
    Devuelve None si a la hora pedida ya no se cumple algún tope (el solver lo
    asegura, así que no debería pasar): entonces vale el horario del solver.
    """
    llegadas0, _, _, _, esperas0 = _recorrer_viaje(paradas, salida_pedida, vuelta)
    espera0 = sum(esperas0)
    if any(p["tope"] is not None and llegada > p["tope"] for p, llegada in zip(paradas, llegadas0)):
        return None

    def cabe(atraso):
        llegadas = _recorrer_viaje(paradas, salida_pedida + atraso, vuelta)[0]
        return all(p["tope"] is None or ahora - antes <= max(0, p["tope"] - antes - seguridad)
                   for p, antes, ahora in zip(paradas, llegadas0, llegadas))

    # Atrasar sirve hasta comerse la espera: más allá corre todo el viaje. Lo
    # que cabe disminuye con el atraso, así que se busca el mayor por mitades.
    bajo, alto = 0, max(0, int(espera0))
    while bajo < alto:
        medio = (bajo + alto + 1) // 2
        if cabe(medio):
            bajo = medio
        else:
            alto = medio - 1
    sin_espera = bajo
    atraso = min(sin_espera, max(0, int(margen)))

    llegadas, inicios, salidas, regreso, esperas = _recorrer_viaje(paradas, salida_pedida + atraso, vuelta)
    return {
        "salida": salida_pedida + atraso,
        "atraso": atraso,
        "llegadas": llegadas,
        "inicios": inicios,
        "salidas": salidas,
        "regreso": regreso,
        "esperas": esperas,
        "espera": sum(esperas),
        "esperas_sin_atraso": esperas0,
        "espera_sin_atraso": espera0,
        # Lo que se atrasaría con margen de sobra, y la espera que quedaría.
        "atraso_sin_tope": sin_espera,
        "espera_sin_tope": espera0 - sin_espera,
    }


def revisar_ventanas(ctx, diag):
    """Locales cuya ventana horaria es imposible de cumplir.

    Devuelve (problemas_duros, avisos). Un problema duro es un local al que no
    se puede llegar a tiempo ni saliendo directo del packing: mientras todos los
    locales sean obligatorios, uno solo así deja la ruta entera sin solución.

    Para un local cuya descarga dura más que su horario, «a tiempo» es antes de
    que abra (ver _descarga_mas_larga_que_ventana).
    """
    duros, avisos = [], []
    ref = ctx["reference_departure_minutes"]
    if ref is None:
        return duros, avisos

    tiempos_desde_deposito = ctx["extended_time_matrix"][0] if ctx["extended_time_matrix"] else None
    salida_min = min([o for o in ctx["vehicle_start_offsets_base"] if o is not None], default=0)
    antes_de_abrir = ctx.get("antes_de_abrir") or [False] * ctx["num_nodes"]

    for i in range(1, ctx["num_nodes"]):
        loc = ctx["extended_locations"][i]
        nombre = _nombre_local(loc, i)
        apertura = ctx["extended_opening"][i]
        cierre = ctx["extended_deadline"][i]
        servicio = int(round(ctx["extended_wait"][i]))
        cl_gap = int(ctx["extended_closing_gap"][i] or 0)

        if apertura is not None and cierre is not None and cierre <= apertura:
            duros.append((i, nombre, "VENTANA_INVERTIDA",
                          f"{nombre}: el horario de atención cierra ({_fmt_hhmm(ref + cierre)}) "
                          f"antes de abrir ({_fmt_hhmm(ref + apertura)})."))
            continue

        horario = (f"{_fmt_hhmm(ref + apertura)} a {_fmt_hhmm(ref + cierre - cl_gap)}"
                   if apertura is not None and cierre is not None else "")
        if cierre is not None and tiempos_desde_deposito is not None:
            # La matriz ya está extendida: se indexa con el nodo, no con el local original.
            viaje = int(round(tiempos_desde_deposito[i]))
            llegada_mas_temprana = salida_min + viaje
            if antes_de_abrir[i]:
                tope_llegada = apertura
                exigencia = (f"y como la descarga ({servicio} min) dura más que su horario ({horario}), "
                             f"hay que llegar antes de que abra, a las {_fmt_hhmm(ref + apertura)}.")
            else:
                tope_llegada = cierre - cl_gap - servicio
                exigencia = (f"y hay que estar antes de las {_fmt_hhmm(ref + tope_llegada)} "
                             f"(cierre {_fmt_hhmm(ref + cierre)} − {cl_gap} min de tolerancia "
                             f"− {servicio} min de descarga).")
            if llegada_mas_temprana > tope_llegada:
                duros.append((i, nombre, "VENTANA_INALCANZABLE",
                              f"{nombre}: saliendo del packing a las {_fmt_hhmm(ref + salida_min)} "
                              f"se llega a las {_fmt_hhmm(ref + llegada_mas_temprana)} como muy pronto, "
                              f"{exigencia}"))
                continue

            if antes_de_abrir[i]:
                diag.aviso("LLEGAR_ANTES_DE_ABRIR",
                           f"{nombre}: la descarga ({servicio} min) dura más que su horario ({horario}): "
                           f"el camión tiene que llegar antes de que abra y la descarga termina "
                           f"después del cierre.")
            margen = tope_llegada - llegada_mas_temprana
            if margen < 30:
                avisos.append(f"{nombre}: quedan sólo {margen} min de margen para llegar "
                              f"{'antes de que abra' if antes_de_abrir[i] else 'a tiempo'}.")

    return duros, avisos


# ---------------------------------------------------------------------
# Diagnóstico DESPUÉS de que el solver no encontró solución
# ---------------------------------------------------------------------
#
# La técnica es la estándar para explicar una infactibilidad: volver a resolver
# soltando UNA restricción por vez. La primera que hace aparecer una solución es
# la que estaba bloqueando la ruta. Cada intento usa un presupuesto corto: no se
# busca la mejor ruta, sólo saber si existe alguna.

RELAJACIONES = [
    # Solo se prueba si se pidió no mezclar zonas (ver diagnosticar_infactibilidad).
    ("zonas", "ZONAS_SEPARADAS",
     "La regla de no mezclar la Región Metropolitana con otras zonas impide armar la ruta.",
     ["Permite mezclar zonas en los ajustes del algoritmo.",
      "O selecciona otro camión para que cada zona tenga el suyo."]),
    ("ventanas", "VENTANAS_HORARIAS",
     "Los horarios de atención de los locales hacen imposible la ruta.",
     ["Revisa los horarios de apertura y cierre de los locales de esta ruta.",
      "Adelanta o atrasa la hora de salida de los camiones.",
      "Marca el local como festivo si hoy atiende en otro horario."]),
    ("espera", "TIEMPOS_DE_ESPERA",
     "Los tiempos de descarga estimados no caben en la jornada.",
     ["Activa el límite de tiempo de espera por local, o bájalo.",
      "Revisa en Análisis de Transportes si algún local tiene una espera desproporcionada."]),
    ("horizonte", "HORIZONTE_CONDUCCION",
     "La ruta no cabe en el máximo de horas de conducción por camión.",
     ["Agrega otro camión para repartir los locales.",
      "Saca de la ruta los locales más lejanos."]),
    ("paradas", "MAXIMO_DE_PARADAS",
     "El máximo de paradas por camión impide cubrir todos los locales.",
     ["Sube 'Máx. paradas / camión' en los ajustes del algoritmo.",
      "O agrega otro camión."]),
    ("palets", "PALETS",
     "Los palets en suelo no caben en los camiones.",
     ["Agrega un camión con más palets.",
      "Revisa los palets en suelo cargados por local."]),
    ("capacidad", "CAPACIDAD_KG",
     "La carga en kilos no cabe en los camiones.",
     ["Agrega otro camión o baja las cantidades pedidas."]),
    ("refrigeracion", "REFRIGERACION",
     "La exigencia de camión refrigerado impide armar la ruta.",
     ["Selecciona más camiones refrigerados.",
      "O deja los locales que exigen frío para otra ruta."]),
    ("grupos", "GRUPOS_EXCLUSIVOS",
     "La regla de que Walmart y Cencosud viajen separados impide armar la ruta.",
     ["Selecciona un camión más para que cada grupo tenga el suyo.",
      "O arma rutas distintas para Walmart y Cencosud."]),
]


def _parametros_rapidos(segundos):
    p = pywrapcp.DefaultRoutingSearchParameters()
    p.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    p.time_limit.FromSeconds(max(1, int(segundos)))
    return p


def _motivo_probable_del_descarte(i, ctx):
    """Por qué un local concreto quedó fuera. Se responde con las mismas
    comprobaciones de las revisiones previas, pero aplicadas a ese nodo."""
    loc = ctx["extended_locations"][i]
    ref = ctx["reference_departure_minutes"]
    servicio = int(round(ctx["extended_wait"][i]))
    apertura = ctx["extended_opening"][i]
    cierre = ctx["extended_deadline"][i]
    cl_gap = int(ctx["extended_closing_gap"][i] or 0)

    if ctx["extended_demands"][i] > max(ctx["vehicle_capacities_base"], default=0):
        return (f"pide {_num(ctx['extended_demands'][i])} kg y el camión más grande "
                f"lleva {_num(max(ctx['vehicle_capacities_base'], default=0))} kg")

    if ctx["extended_refrigerate"][i] and not any(ctx["vehicle_free_base"]):
        return "exige camión refrigerado y no hay ninguno seleccionado"

    antes = (ctx.get("antes_de_abrir") or [False] * ctx["num_nodes"])[i]

    # La matriz ya está extendida: se indexa con el nodo, no con el local original.
    if ref is not None and cierre is not None:
        tiempos = ctx["extended_time_matrix"][0] if ctx["extended_time_matrix"] else None
        if tiempos is not None:
            viaje = int(round(tiempos[i]))
            salida_min = min([o for o in ctx["vehicle_start_offsets_base"] if o is not None], default=0)
            tope = apertura if antes else cierre - cl_gap - servicio
            if salida_min + viaje > tope:
                if antes:
                    return (f"la descarga ({servicio} min) dura más que su horario, así que hay que llegar "
                            f"antes de que abra ({_fmt_hhmm(ref + apertura)}), y el viaje desde el packing "
                            f"toma {viaje} min: no se alcanza")
                return (f"cierra a las {_fmt_hhmm(ref + cierre)} y el viaje desde el packing "
                        f"toma {viaje} min: no se alcanza a llegar")

    tiempos = ctx["extended_time_matrix"][0] if ctx["extended_time_matrix"] else None
    if tiempos is not None:
        ida_vuelta = int(round(tiempos[i])) + int(round(ctx["extended_time_matrix"][i][0]))
        if ida_vuelta > ctx["HORIZON"]:
            return (f"ir y volver toma {ida_vuelta} min de conducción y el tope por camión "
                    f"es de {ctx['HORIZON']} min")

    palet_i = ctx["extended_palets"][i]
    max_palets = max([p for p in ctx["vehicle_palets_base"] if p < ctx["PALLET_INF"]], default=None)
    if max_palets is not None and palet_i > max_palets:
        return f"ocupa {_num(palet_i, 1)} palets y el camión con más espacio lleva {max_palets}"

    if antes and ref is not None:
        return (f"la descarga ({servicio} min) dura más que su horario y el camión tiene que llegar "
                f"antes de que abra ({_fmt_hhmm(ref + apertura)}): no calza con el resto de las paradas")

    if ctx.get("node_zona_clase") is not None:
        zona = {ZONA_METRO: "de la Región Metropolitana", ZONA_OTRA: "de otras zonas"}.get(
            ctx["node_zona_clase"][i], "de su zona")
        return (f"no encaja junto con el resto de las paradas {zona} dentro de los tiempos disponibles "
                f"(no se mezclan zonas: permitirlo en los ajustes del algoritmo puede destrabarlo)")

    return "no encaja junto con el resto de las paradas dentro de los tiempos disponibles"


def diagnosticar_infactibilidad(construir_modelo, ctx, diag, segundos_por_intento=8,
                                estado_solver=None, tiempo_calculo=None):
    """Identifica QUÉ restricción bloquea la ruta y devuelve un ErrorOptimizacion.

    `estado_solver` sirve para el caso en que NINGUNA restricción explique el
    fallo: si además se agotó el tiempo, lo más probable es que el problema sea
    grande y no imposible.
    """

    diag.info("DIAGNOSTICO_INICIO",
              "No hubo solución: probando qué restricción la impide "
              f"({segundos_por_intento}s por prueba).")
    update_job_status(diag.job_id, "diagnosticando",
                      "No hubo ruta posible: buscando qué locales no se pueden atender...", 82)

    # ── Paso 1: ¿qué locales son los imposibles? ──────────────────────────
    # Con disyunciones el solver puede dejar locales fuera pagando una multa.
    # Los que deje fuera son, literalmente, los que no se pueden atender.
    try:
        modelo = construir_modelo(permitir_descartes=True, silencioso=True)
        sol = modelo["routing"].SolveWithParameters(_parametros_rapidos(segundos_por_intento * 2))
    except Exception as e:
        diag.aviso("DIAGNOSTICO_DESCARTES_FALLO", f"No se pudo probar con descartes: {e}")
        sol, modelo = None, None

    if sol and modelo:
        routing_d, manager_d = modelo["routing"], modelo["manager"]
        visitados = set()
        for v in range(routing_d.vehicles()):
            idx = routing_d.Start(v)
            while not routing_d.IsEnd(idx):
                visitados.add(manager_d.IndexToNode(idx))
                idx = sol.Value(routing_d.NextVar(idx))
        fuera = [i for i in range(1, ctx["num_nodes"]) if i not in visitados]

        # Si quedan fuera TODOS los locales, el problema no es de un local
        # concreto sino una restricción global (horizonte, paradas, capacidad):
        # eso lo responde mejor la escalera de relajaciones de más abajo.
        if fuera and len(fuera) == ctx["num_nodes"] - 1:
            diag.info("DESCARTE_TOTAL",
                      "Ningún local es alcanzable: la causa es global, no de un local puntual.")
            fuera = []

        if fuera:
            nombres = []
            vistos = set()
            for i in fuera:
                nombre = _nombre_local(ctx["extended_locations"][i], i)
                if nombre in vistos:
                    continue
                vistos.add(nombre)
                nombres.append(f"{nombre}: {_motivo_probable_del_descarte(i, ctx)}.")
            diag.error("LOCALES_IMPOSIBLES",
                       f"{len(vistos)} local(es) no se pueden atender con esta configuración.")
            return ErrorOptimizacion(
                "LOCALES_IMPOSIBLES",
                (f"No se puede armar la ruta porque {len(vistos)} "
                 f"{'local queda' if len(vistos) == 1 else 'locales quedan'} fuera de alcance."),
                detalle=nombres,
                sugerencias=[
                    "Saca esos locales de la ruta y vuelve a optimizar.",
                    "O corrige lo que los deja fuera (horario, camión, cantidades) y reintenta.",
                ],
                datos={"locales_sin_solucion": nombres},
            )

    # ── Paso 2: soltar una restricción a la vez ───────────────────────────
    # «zonas» solo si se pidió no mezclarlas: si no, soltarla no cambia nada.
    relajaciones = [r for r in RELAJACIONES if r[0] != "zonas" or ctx.get("node_zona_clase") is not None]
    for n_prueba, (clave, codigo, mensaje, sugerencias) in enumerate(relajaciones, start=1):
        update_job_status(diag.job_id, "diagnosticando",
                          f"Buscando la causa: probando sin «{clave}» ({n_prueba} de {len(relajaciones)})...",
                          84 + int(12 * n_prueba / len(relajaciones)))
        try:
            modelo = construir_modelo(relajar=frozenset([clave]), silencioso=True)
            sol = modelo["routing"].SolveWithParameters(_parametros_rapidos(segundos_por_intento))
        except Exception as e:
            diag.aviso("DIAGNOSTICO_RELAJACION_FALLO", f"Prueba '{clave}' falló: {e}")
            continue

        if sol:
            diag.error(f"BLOQUEA_{codigo}", f"Sin la restricción «{clave}» sí hay solución: es la que bloquea.")
            return ErrorOptimizacion(
                codigo,
                mensaje,
                detalle=[
                    "Se probó a armar la ruta ignorando esa restricción y ahí sí hubo solución, "
                    "así que es la que está impidiendo el cálculo.",
                ],
                sugerencias=sugerencias,
                datos={"restriccion_que_bloquea": clave},
            )
        diag.info(f"DIAGNOSTICO_{codigo}", f"Soltar «{clave}» tampoco alcanza.")

    # ── Paso 3: ninguna restricción por sí sola lo explica ────────────────
    # Si además se acabó el tiempo, el diagnóstico honesto es "es grande", no
    # "es imposible": son problemas con soluciones opuestas.
    if estado_solver == routing_enums_pb2.RoutingSearchStatus.ROUTING_FAIL_TIMEOUT:
        minutos = max(1, int(round((tiempo_calculo or 0) / 60)))
        return ErrorOptimizacion(
            "TIEMPO_DE_CALCULO_INSUFICIENTE",
            "Se acabó el tiempo de cálculo antes de encontrar una ruta.",
            detalle=[
                f"Se usaron {minutos} min de cálculo para {ctx['num_nodes'] - 1} paradas "
                f"y {ctx['base_num_vehicles']} camión(es).",
                "Se probó soltando cada restricción por separado y ninguna destraba el cálculo "
                "por sí sola, así que no parece haber una condición imposible: el problema es "
                "grande y no alcanzó el tiempo.",
            ],
            sugerencias=[
                "Sube el 'Tiempo de cálculo' en los ajustes del algoritmo.",
                "O divide la carga en dos optimizaciones con menos locales.",
            ],
            datos={"solver_status": int(estado_solver)},
        )

    return ErrorOptimizacion(
        "SIN_SOLUCION",
        "No se encontró ninguna ruta que cumpla las restricciones, y no hay una sola causa.",
        detalle=[
            "Se probó soltando cada restricción por separado (horarios, paradas, palets, "
            "capacidad, refrigeración, grupos) y ninguna por sí sola destraba el cálculo: "
            "hay varias condiciones apretadas a la vez.",
            f"Locales: {ctx['num_nodes'] - 1} · camiones: {ctx['base_num_vehicles']} · "
            f"carga: {_num(sum(ctx['extended_demands'][1:]))} kg de "
            f"{_num(sum(ctx['vehicle_capacities_base']))} kg disponibles.",
        ],
        sugerencias=[
            "Divide la carga en dos rutas con menos locales.",
            "Agrega camiones o sube el tiempo de cálculo.",
            "Revisa los horarios de los locales más restrictivos.",
        ],
    )


# ---------------------------------------------------------------------
# API
# ---------------------------------------------------------------------

@app.route("/optimize", methods=["POST"])
def optimize():
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

        if isinstance(raw_data, list):
            if len(raw_data) != 3:
                return jsonify(error="Se esperaba [data, truck_ids, user_id]"), 400
            data, truck_ids, user_id = raw_data
        else:
            data, truck_ids, user_id = raw_data, [], None

        job_id = data.get("job_id")
        diag = Diagnostico(job_id)
        update_job_status(job_id, "preparacion", "Recibiendo datos y revisando que la ruta sea posible...", 10)

        # Revisión de estructura ANTES de tocar nada: una clave faltante reventaba
        # con KeyError y el usuario recibía "Error interno: locations".
        revisar_estructura(data, truck_ids, diag)

        _fecha_str = data.get("fecha") or data.get("date")
        if _fecha_str:
            try:
                if "T" in str(_fecha_str):
                    _dia_semana = datetime.fromisoformat(str(_fecha_str)).weekday()
                else:
                    from datetime import date as _date_cls
                    _dia_semana = _date_cls.fromisoformat(str(_fecha_str)).weekday()
            except Exception:
                _dia_semana = datetime.now().weekday()
        else:
            _dia_semana = datetime.now().weekday()
        
        _DIA_NOMBRES = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
        es_dia_segunda_ventana_np = _dia_semana in (3, 4, 5) 
        print(f"📆 Día detectado: {_DIA_NOMBRES[_dia_semana]} "
              f"(weekday={_dia_semana}) — "
              f"{'✅ Aplica 2da ventana Nicolas Palma' if es_dia_segunda_ventana_np else '⏭ Sin 2da ventana'}")

        locations = data["locations"]
        base_num_vehicles = data["max_vehicles"]
        vehicle_capacities_base = data["vehicle_capacities"]
        distance_matrix = data["distance_matrix"]
        time_matrix = data.get("time_matrix")

        vehicle_palets_base = data.get("vehicle_palets", [0] * base_num_vehicles)
        vehicle_consume_base = data.get("vehicle_consume", [1] * base_num_vehicles)
        vehicle_free_base = data.get("vehicle_free", [0] * base_num_vehicles)
        multiplicador_tiempo = float(data.get("multiplicador_tiempo", 1.0) or 1.0)
        
        if multiplicador_tiempo <= 0:
            return jsonify(error="multiplicador_tiempo debe ser > 0"), 400

        maximo_de_paradas = int(data.get("maximas_paradas_camion", 100))
        if maximo_de_paradas <= 0:
            return jsonify(error="maximas_paradas_camion debe ser > 0"), 400

        tiempo_calculo_min = float(data.get("tiempo_calculo", 2))
        tiempo_calculo = int(tiempo_calculo_min * 60)
        HORIZON = int(data.get("max_time_per_trip", 480))
        reload_service_time = int(data.get("reload_service_time", 0))

        # ─────────────────────────────────────────────────────────────────────
        # TOPE DEL TIEMPO DE ESPERA POR LOCAL (minutos)
        # ─────────────────────────────────────────────────────────────────────
        # Lo decide quien arma la ruta, desde la pantalla de demandas:
        #   limitar_espera=False → se usa la espera real estimada de cada local,
        #                          sin recortar (rutas más realistas, pero un
        #                          local con espera enorme puede dejar el
        #                          problema sin solución).
        #   limitar_espera=True  → se recorta a max_wait_minutes (por defecto 30).
        # Antes era una constante en el código y hubo que editarla y desplegar
        # cada vez que se quería probar otro valor (ver historial del repo).
        limitar_espera = data.get("limitar_espera")
        if limitar_espera is None:
            limitar_espera = True
        limitar_espera = bool(limitar_espera) and str(limitar_espera).lower() not in ("false", "0")

        try:
            max_wait_minutes_cfg = float(data.get("max_wait_minutes", 30) or 30)
        except Exception:
            return jsonify(error="max_wait_minutes debe ser un número"), 400
        if max_wait_minutes_cfg <= 0:
            limitar_espera = False

        max_wait_minutes_cap = max_wait_minutes_cfg if limitar_espera else float("inf")
        esperas_recortadas = []

        # ─────────────────────────────────────────────────────────────────────
        # MEZCLAR ZONAS (pedido del usuario, 13-10-2026)
        # ─────────────────────────────────────────────────────────────────────
        # Lo decide quien arma la ruta, junto al tope de espera. Sin el dato se
        # mezclan, como siempre. En falso, un camión que lleva algo a la Región
        # Metropolitana solo reparte en la Región Metropolitana (ver _zona_clase).
        mezclar_zonas = _leer_bool(data.get("mezclar_zonas"), por_defecto=True)

        # ─────────────────────────────────────────────────────────────────────
        # HOLGURA DE LA HORA DE SALIDA (minutos)
        # ─────────────────────────────────────────────────────────────────────
        # La hora de salida que se elige por camión es un compromiso operativo
        # (turnos, carga), no una sugerencia. El modelo la trataba como un piso
        # con 12 HORAS de holgura, y encima el ajuste fino posterior la corría
        # más: alguien ponía 07:00 y podía recibir una ruta que sale mucho
        # después, sin que nada lo explicara. Ahora el margen lo decide quien
        # arma la ruta y el resultado dice cuánto se movió y por qué.
        try:
            max_departure_slack = int(float(data.get("max_departure_slack_minutes", 60)))
        except Exception:
            return jsonify(error="max_departure_slack_minutes debe ser un número"), 400
        max_departure_slack = max(0, min(max_departure_slack, 12 * 60))

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

        # DESPUÉS
        reference_departure_minutes = None
        if reference_candidates:
            # Si hay salidas que cruzan medianoche (ej: 22:00 y 00:00 en el mismo ciclo),
            # min() elegiría 00:00 siendo que 22:00 es la salida más temprana del ciclo real.
            # Se normalizan los tiempos < 12h sumando 24h para resolverlo.
            if max(reference_candidates) - min(reference_candidates) > 12 * 60:
                adjusted = [t + 24 * 60 if t < 12 * 60 else t for t in reference_candidates]
                reference_departure_minutes = min(adjusted) % (24 * 60)
            else:
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

        MODE_FREE = 0
        MODE_W    = 1
        MODE_C    = 2

        # Sólo los modos que esta solicitud necesita: con rutas de un solo grupo
        # el modelo pasa de 3 vehículos virtuales por camión a 1.
        MODES, _grupos_presentes = modos_necesarios(locations, MODE_FREE, MODE_W, MODE_C)
        _nombres_modo = {MODE_FREE: "libre", MODE_W: "Walmart", MODE_C: "Cencosud"}

        # Cada vehículo virtual es un camión en un modo y, sin mezclar zonas,
        # además en una zona: el modelo ya obliga a que cada camión use uno
        # solo, así que con la zona en el vehículo un camión queda en UNA zona
        # sin otra restricción. Mezclando zonas la flota es la de siempre.
        if mezclar_zonas:
            COMBOS = [(m, None) for m in MODES]
        else:
            COMBOS = []
            for _clase in (ZONA_METRO, ZONA_OTRA):
                _de_la_zona = [loc for loc in locations[1:] if _zona_clase(loc.get("zona")) == _clase]
                if _de_la_zona:
                    _modos_zona, _ = modos_necesarios([locations[0]] + _de_la_zona, MODE_FREE, MODE_W, MODE_C)
                    COMBOS.extend((m, _clase) for m in _modos_zona)
            COMBOS = COMBOS or [(MODE_FREE, ZONA_OTRA)]

        diag.info("FLOTA_VIRTUAL",
                  f"Grupos en la ruta: {', '.join(sorted(_grupos_presentes))}. "
                  f"Modos activos: {', '.join(_nombres_modo[m] + (f' ({ZONA_ETIQUETAS[z]})' if z else '') for m, z in COMBOS)} "
                  f"→ {base_num_vehicles * len(COMBOS)} vehículos en el modelo "
                  f"(antes siempre {base_num_vehicles * 3}).")

        vehicle_capacities, vehicle_consume, vehicle_free, vehicle_palets = [], [], [], []
        vehicle_mapping, vehicle_trip_no, vehicle_mode, vehicle_zona = {}, {}, {}, {}
        max_trips_per_vehicle = 1 

        for idx in range(base_num_vehicles):
            for trip in range(max_trips_per_vehicle):
                for mode, zona_v in COMBOS:
                    vehicle_capacities.append(vehicle_capacities_base[idx])
                    vehicle_consume.append(vehicle_consume_base[idx])
                    vehicle_free.append(vehicle_free_base[idx])
                    vehicle_palets.append(vehicle_palets_base[idx])
                    v_idx = len(vehicle_capacities) - 1
                    vehicle_mapping[v_idx] = idx
                    vehicle_trip_no[v_idx] = trip
                    vehicle_mode[v_idx] = mode
                    vehicle_zona[v_idx] = zona_v

        num_vehicles = len(vehicle_capacities)
        max_vehicle_capacity = max(vehicle_capacities)
        depot = 0

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
            if wait_minutes > max_wait_minutes_cap:
                esperas_recortadas.append((_nombre_local(loc, idx_loc), wait_minutes, max_wait_minutes_cap))
                wait_minutes = max_wait_minutes_cap

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
                    if delta_open < 0:
                        delta_open += 24 * 60
                    opening_minutes_rel = int(delta_open)
                except Exception:
                    opening_minutes_rel = None

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
                    if delta < 0:
                        delta += 24 * 60
                    deadline_minutes_rel = int(delta)
                except Exception:
                    deadline_minutes_rel = None

            if (opening_minutes_rel is not None and deadline_minutes_rel is not None
                    and 0 < deadline_minutes_rel < opening_minutes_rel):
                opening_minutes_rel -= 24 * 60
                _loc_ident_debug = (loc.get("identificador") or "?")
                print(f"🌙 Ventana cross-midnight corregida — {_loc_ident_debug}: "
                      f"apertura={_fmt_hhmm((reference_departure_minutes + opening_minutes_rel) % (24 * 60))} "
                      f"(ya abierta al salir), "
                      f"cierre={_fmt_hhmm((reference_departure_minutes + deadline_minutes_rel) % (24 * 60))}")

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
        node_zona_clase = [_zona_clase(loc.get("zona")) for loc in extended_locations]
        # Locales cuya descarga dura más que su horario: se atienden si el camión
        # llega antes de que abran (el tope de llegada pasa a ser la apertura).
        antes_de_abrir = [False] + [
            _descarga_mas_larga_que_ventana(extended_opening[i], extended_deadline[i],
                                            extended_closing_gap[i], extended_wait[i])
            for i in range(1, num_nodes)
        ]

        # -------------------------------------------------------------------------
        # FUNCIÓN DE PARES DE BODEGA COMPARTIDA (POR ID)
        # -------------------------------------------------------------------------
        def is_shared_warehouse_pair(from_node, to_node):
            if from_node == depot or to_node == depot:
                return False
            
            # Extraemos los IDs de manera segura y los convertimos a string 
            # para evitar problemas si vienen como integer (ej. 6 vs "6")
            loc_from = extended_locations[from_node]
            id_from = str(loc_from.get("id") or loc_from.get("location_id") or "")
            
            loc_to = extended_locations[to_node]
            id_to = str(loc_to.get("id") or loc_to.get("location_id") or "")
            
            # Par 1: Jumbo Costanera + Dark Store (IDs 6 y 232)
            if (id_from == "6" and id_to == "232") or (id_from == "232" and id_to == "6"):
                return True
                
            # Par 2: Jumbo 1 Norte + Dark Store (IDs 115 y 116)
            if (id_from == "115" and id_to == "116") or (id_from == "116" and id_to == "115"):
                return True
                
            return False

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

        # ─────────────────────────────────────────────────────────────────
        # REVISIONES DE FACTIBILIDAD (antes de gastar minutos de solver)
        # ─────────────────────────────────────────────────────────────────
        # Todas estas condiciones hacen imposible la ruta y se detectan con
        # aritmética. Antes se descubrían igual, pero después de esperar el
        # tiempo de cálculo completo y con el mensaje "No se pudo encontrar
        # solución", que no dice qué cambiar.
        vehicle_start_offsets_base = []
        for _i in range(base_num_vehicles):
            _dep = vehicle_departure_minutes_base[_i]
            if _dep is None or reference_departure_minutes is None:
                vehicle_start_offsets_base.append(0)
            else:
                _off = _dep - reference_departure_minutes
                if _off < 0:
                    _off += 24 * 60
                elif _off > 12 * 60:
                    _off -= 24 * 60
                vehicle_start_offsets_base.append(max(0, _off))

        ctx = {
            "extended_locations": extended_locations,
            "extended_demands": extended_demands,
            "extended_palets": extended_palets,
            "extended_wait": extended_wait,
            "extended_opening": extended_opening,
            "extended_deadline": extended_deadline,
            "extended_opening_gap": extended_opening_gap,
            "extended_closing_gap": extended_closing_gap,
            "extended_refrigerate": extended_refrigerate,
            "extended_time_matrix": extended_time_matrix,
            "split_mapping": split_mapping,
            "node_group": node_group,
            # None si se pueden mezclar zonas: las revisiones lo usan para saberlo.
            "node_zona_clase": (None if mezclar_zonas else node_zona_clase),
            "antes_de_abrir": antes_de_abrir,
            "num_nodes": num_nodes,
            "base_num_vehicles": base_num_vehicles,
            "vehicle_capacities_base": vehicle_capacities_base,
            "vehicle_palets_base": vehicle_palets_base,
            "vehicle_free_base": vehicle_free_base,
            "vehicle_start_offsets_base": vehicle_start_offsets_base,
            "reference_departure_minutes": reference_departure_minutes,
            "maximo_de_paradas": maximo_de_paradas,
            "PALLET_INF": PALLET_INF,
            "HORIZON": HORIZON,
        }

        diag.info("CARGA",
                  f"Carga total {_num(sum(extended_demands[1:]))} kg de "
                  f"{_num(sum(vehicle_capacities_base))} kg disponibles "
                  f"({len(extended_locations) - 1} paradas).")

        if esperas_recortadas:
            for nombre, original, tope in esperas_recortadas[:10]:
                diag.aviso("ESPERA_RECORTADA",
                           f"{nombre}: espera estimada {int(original)} min, recortada al tope de {int(tope)} min.")
        if not limitar_espera:
            diag.info("ESPERA_SIN_TOPE", "Sin tope de espera: se usa la espera real estimada de cada local.")
        if not mezclar_zonas:
            _n_metro = sum(1 for z in node_zona_clase[1:] if z == ZONA_METRO)
            diag.info("ZONAS_SEPARADAS",
                      f"No se mezclan zonas: {_n_metro} parada(s) de la Región Metropolitana y "
                      f"{num_nodes - 1 - _n_metro} de otras zonas van en camiones distintos.")

        problemas = revisar_factibilidad(ctx, diag)
        ventanas_duras, ventanas_avisos = revisar_ventanas(ctx, diag)
        for aviso in ventanas_avisos[:10]:
            diag.aviso("MARGEN_JUSTO", aviso)

        if ventanas_duras:
            for _i, _nombre, _cod, _msg in ventanas_duras:
                diag.error(_cod, _msg)
            problemas.append(ErrorOptimizacion(
                "VENTANAS_IMPOSIBLES",
                (f"{len(ventanas_duras)} "
                 f"{'local tiene un horario imposible' if len(ventanas_duras) == 1 else 'locales tienen horarios imposibles'} "
                 f"de cumplir."),
                detalle=[m for _, _, _, m in ventanas_duras],
                sugerencias=[
                    "Corrige el horario del local en Ubicaciones, o adelanta la hora de salida del camión.",
                    "Si hoy atiende en horario especial, marca el local como festivo.",
                    "O saca esos locales de esta ruta.",
                ],
                datos={"locales": [n for _, n, _, _ in ventanas_duras]},
            ))

        if problemas:
            principal = problemas[0]
            for extra in problemas[1:]:
                principal.detalle.append("")
                principal.detalle.append(f"Además — {extra.mensaje}")
                principal.detalle.extend(extra.detalle)
                principal.sugerencias.extend(extra.sugerencias)
            principal.datos["problemas"] = [p.codigo for p in problemas]
            raise principal

        diag.info("FACTIBILIDAD_OK", "Las restricciones básicas se pueden cumplir; se arma el modelo.")

        # ------------------------------- OR-Tools ---------------------------------
        update_job_status(job_id, "construyendo_modelo", "Aplicando restricciones de tiempo y reglas de exclusividad...", 40)
        
        # ─────────────────────────────────────────────────────────────────
        # CONSTRUCCIÓN DEL MODELO
        # ─────────────────────────────────────────────────────────────────
        # Está en una función para poder volver a armarlo RELAJANDO una
        # restricción a la vez cuando no hay solución: comparar qué versión sí
        # es factible es lo que permite decir *cuál* restricción bloquea la ruta
        # en vez de un "no se pudo encontrar solución".
        #
        #   relajar: conjunto de restricciones a desactivar. Valores posibles:
        #     "ventanas" · "horizonte" · "paradas" · "palets" · "capacidad"
        #     "refrigeracion" · "grupos" · "espera"
        #   permitir_descartes: agrega disyunciones para que el solver pueda
        #     DEJAR FUERA locales (con penalización). Sirve para identificar
        #     exactamente qué locales son los imposibles.
        def construir_modelo(relajar=frozenset(), permitir_descartes=False, silencioso=False):
            _p = (lambda *a, **k: None) if silencioso else print
            manager = pywrapcp.RoutingIndexManager(num_nodes, num_vehicles, depot)
            routing = pywrapcp.RoutingModel(manager)

            if "refrigeracion" not in relajar:
                for node_index in range(1, num_nodes):
                    if extended_refrigerate[node_index]:
                        node_idx = manager.NodeToIndex(node_index)
                        for vehicle_id in range(num_vehicles):
                            if not vehicle_free[vehicle_id]:
                                routing.VehicleVar(node_idx).RemoveValue(vehicle_id)

            prioridad_pa_tag = "PUNTO AZUL"
            prioridad_lv_tag = "LA VEGA"
            prioridad_tottus_tag = "TOTTUS CD"
            prioridad_unimarc_tag = "UNIMARC CD"

            is_pa_node, is_lv_node, is_tottus_node, is_unimarc_node, is_nicolas_palma_node = [], [], [], [], []
            for loc in extended_locations:
                ident = (loc.get("identificador", "") or "").upper()
                is_pa_node.append(prioridad_pa_tag in ident)
                is_lv_node.append(prioridad_lv_tag in ident)
                is_tottus_node.append(prioridad_tottus_tag in ident)
                is_unimarc_node.append(prioridad_unimarc_tag in ident)
                is_nicolas_palma_node.append(
                    "NICOLAS PALMA" in ident or "NICOLÁS PALMA" in ident
                )

            MODE_FREE, MODE_W, MODE_C = 0, 1, 2
            if "grupos" not in relajar:
                for node_index in range(1, num_nodes):
                    g = node_group[node_index]
                    node_idx = manager.NodeToIndex(node_index)
                    if g == "WALMART":
                        allowed = {MODE_W}
                    elif g == "CENCOSUD":
                        allowed = {MODE_C}
                    else:
                        if is_lv_node[node_index]:
                            allowed = {MODE_FREE, MODE_C}
                        else:
                            allowed = {MODE_FREE}
                    for v in range(num_vehicles):
                        if vehicle_mode[v] not in allowed:
                            routing.VehicleVar(node_idx).RemoveValue(v)

            # ── No mezclar zonas: cada local solo en los vehículos de su zona ──
            # (un vehículo con zona None es de la flota de siempre: acepta todo)
            if not mezclar_zonas and "zonas" not in relajar:
                for node_index in range(1, num_nodes):
                    node_idx = manager.NodeToIndex(node_index)
                    for v in range(num_vehicles):
                        if vehicle_zona[v] is not None and vehicle_zona[v] != node_zona_clase[node_index]:
                            routing.VehicleVar(node_idx).RemoveValue(v)

            solver = routing.solver()

            # ── Un camión físico solo puede activar UN modo virtual por trip ──
            for base in range(base_num_vehicles):
                for trip in range(max_trips_per_vehicle):
                    trip_vs = [
                        v for v in range(num_vehicles)
                        if vehicle_mapping[v] == base and vehicle_trip_no[v] == trip
                    ]
                    n = len(trip_vs)
                    if n > 1:
                        # is_empty[v] = 1 si el vehículo virtual no tiene ruta (next_start == end)
                        is_empty_list = [
                            solver.IsEqualCstVar(
                                routing.NextVar(routing.Start(v)), routing.End(v)
                            )
                            for v in trip_vs
                        ]
                        # "a lo sumo 1 activo" ≡ "al menos N-1 vacíos"
                        solver.Add(solver.Sum(is_empty_list) >= n - 1)



            enforce_lv_first_hard = data.get("enforce_lv_first_hard", False)

            if enforce_lv_first_hard:
                for v in range(num_vehicles):
                    if vehicle_mode[v] == MODE_C: 
                        start_idx = routing.Start(v)
                        lv_indices = [manager.NodeToIndex(i) for i in range(1, num_nodes) if is_lv_node[i]]
                        cencosud_indices = [manager.NodeToIndex(i) for i in range(1, num_nodes) 
                                           if node_group[i] == "CENCOSUD"]

                        for lv_idx in lv_indices:
                            lv_node = manager.IndexToNode(lv_idx)
                            for cenc_idx in cencosud_indices:
                                cenc_node = manager.IndexToNode(cenc_idx)
                                if cenc_node == lv_node:
                                    continue

                                lv_active = routing.ActiveVar(lv_idx)
                                cenc_active = routing.ActiveVar(cenc_idx)
                                lv_next_var = routing.NextVar(start_idx)
                                solver.Add((lv_active + cenc_active - 1) <= 
                                         solver.IsEqualCstVar(lv_next_var, lv_idx))

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

                        if from_node != depot and to_node != depot:
                            g_from, g_to = node_group[from_node], node_group[to_node]
                            is_lv_exception = (
                                (is_lv_node[from_node] and g_to == "CENCOSUD") or
                                (g_from == "CENCOSUD" and is_lv_node[to_node])
                            )

                            if not is_lv_exception:
                                if g_from != g_to and ("WALMART" in (g_from, g_to) or "CENCOSUD" in (g_from, g_to)):
                                    base += HIGH_PENALTY

                        if vehicle_mode[v_idx] == MODE_C:
                            if to_node != depot and is_lv_node[to_node]:
                                if from_node != depot and node_group[from_node] == "CENCOSUD":
                                    base += 50_000 

                        return base
                    return distance
                callback_idx = routing.RegisterTransitCallback(make_vehicle_callback(v))
                routing.SetArcCostEvaluatorOfVehicle(callback_idx, v)

            def demand_callback(from_index):
                node = manager.IndexToNode(from_index)
                return int(extended_demands[node])
            demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
            capacidades_eff = ([10**9] * num_vehicles) if "capacidad" in relajar else vehicle_capacities
            routing.AddDimensionWithVehicleCapacity(
                demand_callback_index, 0, capacidades_eff, True, "Capacity"
            )

            PALET_SCALE = 100
            vehicle_palets_scaled = [int(round(p * PALET_SCALE)) if p < PALLET_INF else PALLET_INF
                                     for p in vehicle_palets]
            extended_palets_scaled = [int(round(p * PALET_SCALE)) for p in extended_palets]

            def palet_demand_callback(from_index):
                node = manager.IndexToNode(from_index)
                return extended_palets_scaled[node]
            palet_cb_idx = routing.RegisterUnaryTransitCallback(palet_demand_callback)
            palets_eff = ([PALLET_INF] * num_vehicles) if "palets" in relajar else vehicle_palets_scaled
            routing.AddDimensionWithVehicleCapacity(
                palet_cb_idx, 0, palets_eff, True, "Palets"
            )

            start_indices = set(routing.Start(v) for v in range(num_vehicles))

            # -------------------------------------------------------------------------
            # CALLBACK DE TIEMPO (Ahora el servicio se gestiona en el SlackVar)
            # -------------------------------------------------------------------------
            def time_callback(from_index, to_index):
                from_node = manager.IndexToNode(from_index)
                to_node   = manager.IndexToNode(to_index)
                travel = int(round(extended_time_matrix[from_node][to_node]))
                service = 0
                if from_node == depot and from_index not in start_indices:
                    service += reload_service_time 
                # El tiempo de espera se maneja ahora dinámicamente con SlackVar
                return travel + service

            time_cb = routing.RegisterTransitCallback(time_callback)

            # -------------------------------------------------------------------------
            # DIMENSIÓN DE TIEMPO CON SLACK DINÁMICO
            # -------------------------------------------------------------------------
            _max_opening_rel = max((int(v) for v in extended_opening[1:] if v is not None), default=0)
            max_wait_val = max((int(round(w)) for w in extended_wait[1:]), default=0)

            slack_tiempo_espera = max(_max_opening_rel + 120, 120) + max_wait_val
            _p(f"⏳ Slack de espera habilitado: {slack_tiempo_espera} min")

            routing.AddDimension(time_cb, slack_tiempo_espera, 10**7, False, "Time")
            time_dimension = routing.GetDimensionOrDie("Time")

            for node in range(1, num_nodes):
                idx = manager.NodeToIndex(node)
                w_here = 0 if "espera" in relajar else int(round(extended_wait[node]))
                if w_here > 0:
                    pair_prev_indices = []
                    for prev_node in range(1, num_nodes):
                        if is_shared_warehouse_pair(prev_node, node):
                            pair_prev_indices.append(manager.NodeToIndex(prev_node))

                    if pair_prev_indices:
                        is_preceded_expr = solver.Sum([
                            solver.IsEqualCstVar(routing.NextVar(p_idx), idx) 
                            for p_idx in pair_prev_indices
                        ])
                        is_active = routing.ActiveVar(idx)
                        solver.Add(time_dimension.SlackVar(idx) >= w_here * (is_active - is_preceded_expr))
                    else:
                        solver.Add(time_dimension.SlackVar(idx) >= w_here * routing.ActiveVar(idx))

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
                        elif offset > 12 * 60:
                            offset -= 24 * 60
                            _p(f"🌙 Offset cross-midnight corregido para vehículo base {base_idx}: "
                                  f"{offset + 24*60} min → {offset} min "
                                  f"({_fmt_hhmm(dep_abs)} salió antes que referencia {_fmt_hhmm(reference_departure_minutes)})")
                    start_idx = routing.Start(v)
                    effective_start = max(0, offset)

                    time_dimension.CumulVar(start_idx).SetRange(
                        effective_start,
                        effective_start + max_departure_slack
                    )
                    vehicle_start_offsets[v] = offset 
            else:
                for v in range(num_vehicles):
                    start_idx = routing.Start(v)
                    time_dimension.CumulVar(start_idx).SetRange(0, 0)
                    vehicle_start_offsets[v] = 0

            NP_SV_OPEN_ABS  = 20 * 60        
            NP_SV_CLOSE_ABS = 23 * 60 + 59   

            if reference_departure_minutes is not None and "ventanas" not in relajar:
                for node in range(1, num_nodes):
                    idx       = manager.NodeToIndex(node)
                    wait_here = int(round(extended_wait[node]))
                    loc_id    = extended_locations[node].get('id', node)
                    loc_name  = extended_locations[node].get('identificador', 'unknown')

                    use_double_window = is_nicolas_palma_node[node] and es_dia_segunda_ventana_np

                    if use_double_window:
                        arrival_var = time_dimension.CumulVar(idx)

                        sv_open_rel  = NP_SV_OPEN_ABS  - reference_departure_minutes
                        sv_close_rel = NP_SV_CLOSE_ABS - reference_departure_minutes
                        if sv_open_rel  < -12 * 60: sv_open_rel  += 24 * 60
                        if sv_close_rel < -12 * 60: sv_close_rel += 24 * 60

                        w1_ub        = None   
                        w1_open_real = None   

                        if extended_deadline[node] is not None:
                            cl_gap = max(0, int(extended_closing_gap[node] or 0))
                            _w1_raw = int(extended_deadline[node]) - cl_gap - wait_here
                            w1_ub  = max(0, _w1_raw if _w1_raw >= 0 else int(extended_deadline[node]) - cl_gap)  # arrival deadline

                        if extended_opening[node] is not None:
                            w1_open_real = int(extended_opening[node])

                        if w1_ub is not None:
                            overall_ub = max(w1_ub, sv_close_rel)
                            arrival_var.SetRange(0, overall_ub)

                            if w1_ub < sv_open_rel:
                                solver.Add(
                                    solver.Max(w1_ub - arrival_var, arrival_var - sv_open_rel) >= 0
                                )

                            if w1_open_real is not None:
                                sv_earliest = max(0, sv_open_rel - wait_here)
                                solver.Add(
                                    solver.Max(
                                        arrival_var + wait_here - w1_open_real,
                                        arrival_var - sv_earliest
                                    ) >= 0
                                )

                            v1_open_str  = (_fmt_hhmm((reference_departure_minutes + w1_open_real) % (24 * 60))
                                            if w1_open_real is not None else "sin apertura")
                            v1_close_str = _fmt_hhmm((reference_departure_minutes + w1_ub) % (24 * 60))

                        else:
                            arrival_var.SetRange(0, sv_close_rel)
                            v1_open_str  = "—"
                            v1_close_str = "sin cierre"

                        sv_open_clock  = _fmt_hhmm((reference_departure_minutes + sv_open_rel)  % (24 * 60))
                        sv_close_clock = _fmt_hhmm((reference_departure_minutes + sv_close_rel) % (24 * 60))
                        dia_str        = _DIA_NOMBRES[_dia_semana] if _dia_semana < 7 else "?"

                        _p(f"🌙 DOBLE VENTANA ({dia_str}) — {loc_name} (ID {loc_id}):")
                        _p(f"   Ventana 1 (JSON):   {v1_open_str} → {v1_close_str}")
                        _p(f"   Ventana 2 (fija):   {sv_open_clock} → {sv_close_clock}")

                    else:
                        if antes_de_abrir[node]:
                            # La descarga dura más que el horario: el camión tiene que
                            # llegar antes de que abra (la restricción 1 de abajo dice
                            # desde cuándo). La descarga empieza al abrir y termina
                            # después del cierre, por eso no rige «cierre − descarga».
                            apertura_rel = int(extended_opening[node])
                            time_dimension.CumulVar(idx).SetRange(0, max(0, apertura_rel))
                            _p(f"🚪 {loc_name} (ID {loc_id}): la descarga ({wait_here} min) dura más que "
                               f"su horario → llegada ≤ apertura "
                               f"{_fmt_hhmm((reference_departure_minutes + apertura_rel) % (24 * 60))}")
                        elif extended_deadline[node] is not None:
                            cl_gap = int(extended_closing_gap[node]) if extended_closing_gap[node] is not None else 0
                            if cl_gap < 0:
                                cl_gap = 0

                            # El camión debe COMPLETAR el servicio antes de close_eff.
                            # deadline_llegada = close_clock - closing_gap - service_time
                            close_eff_rel = int(extended_deadline[node]) - cl_gap
                            eff_deadline  = close_eff_rel - wait_here
                            close_eff_display = _fmt_hhmm((reference_departure_minutes + close_eff_rel) % (24 * 60))

                            if eff_deadline > 0:
                                # Caso normal: hay margen para llegar y hacer el servicio.
                                time_dimension.CumulVar(idx).SetRange(0, eff_deadline)
                                deadline_clock = _fmt_hhmm((reference_departure_minutes + eff_deadline) % (24 * 60))
                                _p(f"📅 {loc_name} (ID {loc_id}): llegada ≤ {deadline_clock} "
                                      f"(cierre_eff={close_eff_display}, gap={cl_gap} min, servicio={wait_here} min)")
                            else:
                                # El tiempo de servicio es mayor que la ventana disponible.
                                # No se aplica upper bound de llegada para evitar infactibilidad;
                                # la restricción de salida (departure >= opening + wait) del bloque
                                # siguiente se encarga de mantener la coherencia temporal.
                                _p(f"⚠️  {loc_name} (ID {loc_id}): servicio ({wait_here} min) ≥ "
                                      f"ventana hasta cierre_eff={close_eff_display} ({close_eff_rel} min) "
                                      f"— omitiendo upper bound de llegada")

                        if extended_opening[node] is not None:
                            opening_real = int(extended_opening[node])
                            arrival_var  = time_dimension.CumulVar(idx)

                            op_gap = int(extended_opening_gap[node]) if extended_opening_gap[node] is not None else 0

                            # ── Restricción 1: llegada mínima ─────────────────────────────────
                            # El camión no puede llegar antes de (apertura - opening_gap).
                            # El opening_gap define desde cuándo puede estar en el muelle/cola.
                            # Si opening - op_gap <= 0 (ventana comienza antes de la referencia),
                            # no se aplica límite inferior (el camión puede llegar en cualquier momento).
                            earliest_arrival_rel = opening_real - op_gap
                            if earliest_arrival_rel > 0:
                                solver.Add(
                                    arrival_var >= earliest_arrival_rel * routing.ActiveVar(idx)
                                )

                            # ── Restricción 2: salida mínima ──────────────────────────────────
                            # El camión debe SALIR después de (apertura + servicio).
                            # Si llega antes de que abra, espera idle y el servicio empieza
                            # en la apertura. departure = arrival + SlackVar >= opening + wait.
                            solver.Add(
                                arrival_var + time_dimension.SlackVar(idx) >=
                                (opening_real + wait_here) * routing.ActiveVar(idx)
                            )

                            opening_clock          = _fmt_hhmm((reference_departure_minutes + opening_real) % (24 * 60))
                            earliest_arrival_clock = _fmt_hhmm((reference_departure_minutes + opening_real - op_gap) % (24 * 60))
                            earliest_departure_clock = _fmt_hhmm((reference_departure_minutes + opening_real + wait_here) % (24 * 60))

                            _p(f"📍 {loc_name} (ID {loc_id}):")
                            _p(f"   ✓ Llegada mínima: {earliest_arrival_clock} (gap={op_gap} min, {'aplicada' if earliest_arrival_rel > 0 else 'sin restricción'})")
                            _p(f"   ✓ Apertura real:  {opening_clock}")
                            _p(f"   ✓ Salida mínima:  {earliest_departure_clock} (apertura + {wait_here} min servicio)")

            def drive_callback(from_index, to_index):
                from_node = manager.IndexToNode(from_index)
                to_node   = manager.IndexToNode(to_index)
                return int(round(extended_time_matrix[from_node][to_node]))

            drive_cb = routing.RegisterTransitCallback(drive_callback)
            horizonte_eff = 10**6 if "horizonte" in relajar else HORIZON
            routing.AddDimension(drive_cb, 0, horizonte_eff, True, "Drive")
            drive_dimension = routing.GetDimensionOrDie("Drive")

            for v in range(num_vehicles):
                drive_dimension.CumulVar(routing.End(v)).SetMax(horizonte_eff)

            for base in range(base_num_vehicles):
                end_cumuls = [
                    drive_dimension.CumulVar(routing.End(v))
                    for v in range(num_vehicles) if vehicle_mapping[v] == base
                ]
                solver.Add(solver.Sum(end_cumuls) <= horizonte_eff)

            # -------------------------------------------------------------------------
            # CALLBACK DE PARADAS (Omite el límite para nodos emparejados)
            # -------------------------------------------------------------------------
            def stop_callback(from_index, to_index):
                from_node = manager.IndexToNode(from_index)
                to_node = manager.IndexToNode(to_index)
                if to_node == depot: 
                    return 0
                if is_shared_warehouse_pair(from_node, to_node):
                    return 0  
                return 1

            stop_cb = routing.RegisterTransitCallback(stop_callback)
            paradas_eff = 10**5 if "paradas" in relajar else maximo_de_paradas
            routing.AddDimension(stop_cb, 0, paradas_eff, True, "Stops")
            stops_dimension = routing.GetDimensionOrDie("Stops")
            for v in range(num_vehicles):
                stops_dimension.CumulVar(routing.End(v)).SetMax(paradas_eff)

            time_dimension.SetGlobalSpanCostCoefficient(50)

            costo_varias_rutas = True
            costo_reingreso_valor = int(data.get("costo_reingreso_valor", 100_000))
            if costo_varias_rutas:
                for v in range(num_vehicles):
                    if vehicle_trip_no[v] > 0:
                        routing.SetFixedCostOfVehicle(costo_reingreso_valor, v)

            # Disyunciones: permiten dejar un local fuera pagando una multa. Sólo
            # se usan en el diagnóstico, nunca en la corrida real, para no
            # devolver en silencio una ruta a la que le faltan locales.
            if permitir_descartes:
                for _n in range(1, num_nodes):
                    routing.AddDisjunction([manager.NodeToIndex(_n)], PENALIZACION_DESCARTE)

            return {
                "manager": manager, "routing": routing, "solver": solver,
                "time": time_dimension, "drive": drive_dimension, "stops": stops_dimension,
            }

        _modelo = construir_modelo()
        manager        = _modelo["manager"]
        routing        = _modelo["routing"]
        solver         = _modelo["solver"]
        time_dimension = _modelo["time"]
        drive_dimension = _modelo["drive"]
        stops_dimension = _modelo["stops"]


        # ------------------------ Resolución del modelo ----------------------------
        update_job_status(job_id, "buscando", "Buscando la primera ruta posible...", 50,
                          limite_s=tiempo_calculo, t=0)
        
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
        workers_aplicado = _safe_set(search_parameters, "number_of_workers", req_workers, diag)
        _safe_set(search_parameters, "log_search", bool(data.get("log_search", False)), diag)
        _tope_txt = (f"{tiempo_calculo / 60:.0f} min" if tiempo_calculo >= 60
                     else f"{tiempo_calculo} s")
        diag.info("BUSQUEDA",
                  f"Búsqueda guiada con {_tope_txt} de tope"
                  + (f", {req_workers} hilos." if workers_aplicado else "."))

        class RoutingMonitor:
            """Informa el progreso, pero como mucho una vez cada 2 segundos.

            Antes escribía en Redis en CADA mejora: con búsqueda guiada son
            cientos de llamadas de red dentro del solver, que se come parte del
            tiempo de cálculo en I/O."""

            INTERVALO_SEGUNDOS = 2.0

            def __init__(self, routing_model, current_job_id, manager_model, distancias, limite_s):
                self.routing = routing_model
                self.manager = manager_model
                self.distancias = distancias
                self.job_id = current_job_id
                self.limite_s = limite_s
                self.best_cost = float('inf')
                self.mejoras = 0
                self.ultimo_aviso = 0.0
                self.inicio = time.monotonic()

            def cifras(self):
                """Camiones, paradas y km de la solución que se acaba de encontrar."""
                camiones, paradas, km = 0, 0, 0.0
                for v in range(self.routing.vehicles()):
                    idx = self.routing.Start(v)
                    siguiente = self.routing.NextVar(idx).Value()
                    if self.routing.IsEnd(siguiente):
                        continue
                    camiones += 1
                    while not self.routing.IsEnd(idx):
                        siguiente = self.routing.NextVar(idx).Value()
                        a = self.manager.IndexToNode(idx)
                        b = self.manager.IndexToNode(siguiente)
                        km += float(self.distancias[a][b] or 0)
                        if b != 0:
                            paradas += 1
                        idx = siguiente
                return camiones, paradas, round(km, 1)

            def __call__(self):
                cost = self.routing.CostVar().Min()
                if cost >= self.best_cost:
                    return
                self.best_cost = cost
                self.mejoras += 1
                ahora = time.monotonic()
                # La primera solución se informa siempre: es el salto de "buscando"
                # a "mejorando". Las siguientes, como mucho cada 2 segundos.
                if self.mejoras > 1 and ahora - self.ultimo_aviso < self.INTERVALO_SEGUNDOS:
                    return
                self.ultimo_aviso = ahora
                try:
                    camiones, paradas, km = self.cifras()
                except Exception:
                    camiones = paradas = km = None
                t = round(ahora - self.inicio, 1)
                avance = min(1.0, t / self.limite_s) if self.limite_s else 0
                texto = ("Primera ruta encontrada; ahora se busca una mejor..." if self.mejoras == 1
                         else f"Mejorando la ruta ({self.mejoras} mejoras encontradas)...")
                update_job_status(self.job_id, "mejorando", texto, 50 + int(40 * avance),
                                  mejoras=self.mejoras, camiones=camiones, paradas=paradas,
                                  km=km, t=t, limite_s=self.limite_s)

        monitor = RoutingMonitor(routing, job_id, manager, extended_distance_matrix, tiempo_calculo)
        routing.AddAtSolutionCallback(monitor)

        solution = routing.SolveWithParameters(search_parameters)
        if not solution:
            # `status()` distingue "no existe ruta posible" de "se acabó el
            # tiempo de cálculo", que son dos problemas con soluciones opuestas.
            estado_solver = routing.status()
            _nombres_estado = {0: "no resuelto", 1: "éxito", 2: "óptimo local no alcanzado",
                               3: "sin solución", 4: "sin solución y se acabó el tiempo",
                               5: "modelo inválido", 6: "infactible", 7: "óptimo"}
            diag.error("SOLVER_SIN_SOLUCION",
                       f"El solver terminó sin solución: {_nombres_estado.get(estado_solver, estado_solver)}.")

            update_job_status(job_id, "optimizando",
                              "No hubo solución: analizando qué restricción la impide...", 80)
            # Presupuesto del diagnóstico: proporcional al del cálculo, acotado.
            _seg_diag = int(max(8, min(30, tiempo_calculo / 4)))
            raise diagnosticar_infactibilidad(construir_modelo, ctx, diag,
                                              segundos_por_intento=_seg_diag,
                                              estado_solver=estado_solver,
                                              tiempo_calculo=tiempo_calculo)

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

        def _paradas_del_viaje(nodos):
            """Las paradas de un recorrido para horario_del_viaje, con las
            reglas de tiempo del modelo (ventana, tolerancias, descarga)."""
            paradas, previo = [], depot
            for node in nodos:
                abre = extended_opening[node]
                op_gap = max(0, int(extended_opening_gap[node] or 0))
                servicio = int(round(extended_wait[node]))
                if antes_de_abrir[node]:
                    tope = max(0, int(abre))
                elif extended_deadline[node] is not None:
                    _cl_gap = max(0, int(extended_closing_gap[node] or 0))
                    tope = int(extended_deadline[node]) - _cl_gap - servicio
                    tope = tope if tope > 0 else None   # el modelo no pone tope en ese caso
                else:
                    tope = None
                paradas.append({
                    "viaje": int(round(extended_time_matrix[previo][node])),
                    "desde": (int(abre) - op_gap) if abre is not None and int(abre) - op_gap > 0 else None,
                    "abre": int(abre) if abre is not None else None,
                    "tope": tope,
                    "descarga": 0 if is_shared_warehouse_pair(previo, node) else servicio,
                })
                previo = node
            return paradas, int(round(extended_time_matrix[previo][depot]))

        def _es_nicolas_palma(node):
            ident = (extended_locations[node].get("identificador", "") or "").upper()
            return "NICOLAS PALMA" in ident or "NICOLÁS PALMA" in ident

        for v in range(num_vehicles):
            start = routing.Start(v)
            if routing.IsEnd(solution.Value(routing.NextVar(start))):
                continue

            main_vehicle = vehicle_mapping[v]
            trip_no = vehicle_trip_no[v]
            mode = vehicle_mode[v]
            actual_start = solution.Value(time_dimension.CumulVar(routing.Start(v)))
            start_offset = actual_start  # en vez del offset calculado del input

            # Horario propio del camión (ver horario_del_viaje): sale a SU hora
            # pedida y se atrasa sólo si eso le ahorra espera. Queda el del
            # solver sin hora de referencia, en viajes de recarga y con la doble
            # ventana de Nicolás Palma, que este cálculo no conoce.
            horario = None
            _nodos_viaje = []
            _idx = solution.Value(routing.NextVar(start))
            while not routing.IsEnd(_idx):
                _nodos_viaje.append(manager.IndexToNode(_idx))
                _idx = solution.Value(routing.NextVar(_idx))
            if (reference_departure_minutes is not None and trip_no == 0
                    and not (es_dia_segunda_ventana_np and any(_es_nicolas_palma(n) for n in _nodos_viaje))):
                _paradas, _vuelta = _paradas_del_viaje(_nodos_viaje)
                horario = horario_del_viaje(_paradas, vehicle_start_offsets_base[main_vehicle],
                                            max_departure_slack, _vuelta)
            if horario is not None:
                start_offset = horario["salida"]
                llegada_al_nodo = dict(zip(_nodos_viaje, horario["llegadas"]))

            # La hora de salida real del camión:
            if reference_departure_minutes is not None:
                actual_departure_abs = reference_departure_minutes + start_offset
                departure_clock_v = _fmt_hhmm(actual_departure_abs)

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
                    time_cumul = (llegada_al_nodo[node] if horario is not None
                                  else cumul(time_dimension, idx))
                    drive_cumul = cumul(drive_dimension, idx)   
                    stops_cumul = cumul(stops_dimension, idx)
                    
                    # -----------------------------------------------------------------
                    # APLICAR LECTURA DE ESPERA DE BODEGA COMPARTIDA EN LA EXTRACCIÓN
                    # -----------------------------------------------------------------
                    prev_node_in_route = route_nodes[-2] if len(route_nodes) > 1 else depot
                    if is_shared_warehouse_pair(prev_node_in_route, node):
                        wait_here = 0
                    else:
                        wait_here = int(round(extended_wait[node]))
                        
                    op_gap = int(extended_opening_gap[node]) if extended_opening_gap[node] is not None else 0
                    cl_gap = int(extended_closing_gap[node]) if extended_closing_gap[node] is not None else 0
                    if op_gap < 0: op_gap = 0
                    if cl_gap < 0: cl_gap = 0

                    arrival_from_departure = time_cumul - start_offset
                    if arrival_from_departure < 0:
                        arrival_from_departure = 0

                    # Calcular la espera idle ANTES del ETD: si el camión llega antes de que
                    # abra el local, la espera empieza desde la llegada (no desde la apertura).
                    if extended_opening[node] is not None:
                        waiting_at_node_minutes = max(0, int(extended_opening[node]) - int(time_cumul))
                    else:
                        waiting_at_node_minutes = 0

                    # ETD real = llegada + espera idle (por apertura) + tiempo de servicio
                    departure_from_departure = time_cumul + waiting_at_node_minutes + wait_here - start_offset
                    if departure_from_departure < 0:
                        departure_from_departure = 0

                    deadline_rel = extended_deadline[node]          
                    deadline_ub_eff = None
                    deadline_slack = None
                    latest_arrival_from_departure = None
                    deadline_from_departure = None

                    llega_antes_de_abrir = bool(antes_de_abrir[node])
                    if deadline_rel is not None:
                        if llega_antes_de_abrir:
                            # La descarga dura más que el horario: el tope de llegada es la apertura.
                            eff_deadline = int(extended_opening[node])
                        else:
                            # deadline de llegada = close - closing_gap - service_time
                            _raw_eff_ext = int(deadline_rel) - cl_gap - wait_here
                            eff_deadline = _raw_eff_ext if _raw_eff_ext >= 0 else int(deadline_rel) - cl_gap
                        deadline_ub_eff = max(0, eff_deadline)
                        deadline_from_departure = eff_deadline - start_offset
                        latest_arrival_from_departure = deadline_ub_eff - start_offset

                        if latest_arrival_from_departure is not None:
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
                            # tope de llegada: cierre − tolerancia − descarga, o la apertura
                            # cuando la descarga dura más que el horario
                            close_eff_clock  = (open_clock if llega_antes_de_abrir
                                                else _fmt_hhmm(close_abs - cl_gap - wait_here))

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
                        "zona": ext.get("zona"),
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
                            "waiting_at_node_minutes": int(waiting_at_node_minutes),
                            "open_clock":       open_clock,       
                            "close_clock":      close_clock,      
                            "open_eff_clock":   open_eff_clock,   
                            "close_eff_clock":  close_eff_clock,  
                            "opening_gap_minutes": int(op_gap),
                            "closing_gap_minutes": int(cl_gap),
                            # La descarga dura más que el horario: llega antes de que abra
                            # y termina después del cierre.
                            "llega_antes_de_abrir": llega_antes_de_abrir,
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

            time_total  = (horario["regreso"] if horario is not None
                           else solution.Value(time_dimension.CumulVar(routing.End(v))))
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

            # Hora de salida pedida para ESTE camión y cuánto se atrasó (nunca
            # más que `max_departure_slack`; ver horario_del_viaje).
            _salida_pedida_rel = vehicle_start_offsets_base[main_vehicle] if reference_departure_minutes is not None else 0
            _salida_pedida_clock = (_fmt_hhmm(reference_departure_minutes + _salida_pedida_rel)
                                    if reference_departure_minutes is not None else None)
            _retraso = max(0, int(start_offset) - int(_salida_pedida_rel))
            _motivo_retraso = (f"el cálculo atrasó la salida {_retraso} min para calzar con los "
                               f"horarios de atención de los locales" if _retraso > 0 else None)
            # Si el margen no alcanzó para no esperar, cuánto haría falta: la
            # app lo avisa en la pre-ruta con la patente del camión.
            _espera_por_margen = None
            if horario is not None:
                def _donde(esperas):
                    """El local donde más espera el camión."""
                    k = esperas.index(max(esperas))
                    return _nombre_local(extended_locations[_nodos_viaje[k]], _nodos_viaje[k])

                if _retraso > 0:
                    _local = _donde(horario["esperas_sin_atraso"])
                    if horario["espera"] == 0:
                        _motivo_retraso = f"se atrasó la salida {_retraso} min para no esperar a que abra {_local}"
                    elif _retraso >= max_departure_slack:
                        _motivo_retraso = (f"se atrasó la salida {_retraso} min, todo el margen permitido, "
                                           f"para esperar menos a que abra {_local}")
                    else:
                        _motivo_retraso = (f"se atrasó la salida {_retraso} min para esperar menos a que abra "
                                           f"{_local}: más atraso dejaría otra parada con menos de "
                                           f"{SEGURIDAD_LLEGADA_MINUTOS} min de margen")
                if (horario["atraso"] < horario["atraso_sin_tope"]
                        and horario["espera"] >= SEGURIDAD_LLEGADA_MINUTOS):
                    _k = horario["esperas"].index(max(horario["esperas"]))
                    _espera_por_margen = {
                        # Toda la espera del viaje, también la de antes de poder
                        # entrar al andén, y dónde y desde qué hora descarga.
                        "minutos": int(horario["espera"]),
                        "local": _donde(horario["esperas"]),
                        "descarga_desde": _fmt_hhmm(reference_departure_minutes + horario["inicios"][_k]),
                        "salida": _fmt_hhmm(reference_departure_minutes + horario["salida"]),
                        "margen": int(max_departure_slack),
                        "margen_necesario": int(horario["atraso_sin_tope"]),
                        "salida_sin_espera": _fmt_hhmm(reference_departure_minutes + _salida_pedida_rel
                                                       + horario["atraso_sin_tope"]),
                        "espera_con_margen_necesario": int(horario["espera_sin_tope"]),
                    }

            agg = vehicle_trips.setdefault(main_vehicle, {
                "vehicle": main_vehicle,
                "departure_clock": departure_clock_v,
                "departure_clock_solicitado": _salida_pedida_clock,
                "departure_delay_minutes": _retraso,
                "departure_delay_motivo": _motivo_retraso,
                "espera_por_margen": _espera_por_margen,
                "max_departure_slack_minutes": max_departure_slack,
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
                # METRO / OTRA si no se mezclan zonas; None si se pueden mezclar.
                "zona_clase": vehicle_zona.get(v),
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

        # ─────────────────────────────────────────────────────────────────────
        # POST-PROCESAMIENTO: MODOS
        # ─────────────────────────────────────────────────────────────────────
        # La hora de salida ya no se ajusta aquí: la calcula horario_del_viaje
        # para cada camión en la extracción. El ajuste de antes atrasaba hasta
        # dejar 30 min de holgura en la parada más justa aunque no hubiera
        # espera que ahorrar, y con el mismo margen para todos.
        for vdata in vehicle_trips.values():
            modes_set = vdata.get("modes_used", set())
            mode_labels = {0: "OTHER", 1: "WALMART", 2: "CENCOSUD"}
            vdata["modes_used"] = {
                "ids": sorted(modes_set),
                "labels": [mode_labels[m] for m in sorted(modes_set) if m in mode_labels]
            }

        # Max/avg de las jornadas
        max_vehicle_time_total = 0
        max_vehicle_time_drive = 0
        for vdata in vehicle_trips.values():
            max_vehicle_time_total = max(max_vehicle_time_total, vdata["total_time_minutes_total"])
            max_vehicle_time_drive = max(max_vehicle_time_drive, vdata["total_time_minutes_drive"])
        vehicles_used = len(vehicle_trips)
        avg_vehicle_time_total = (total_time_minutes_total / vehicles_used) if vehicles_used > 0 else 0.0
        avg_vehicle_time_drive = (total_time_minutes_drive / vehicles_used) if vehicles_used > 0 else 0.0

        reference_departure_time_str = (
            _fmt_hhmm(reference_departure_minutes)
            if reference_departure_minutes is not None else None
        )

        # Locales que quedaron sin ninguna entrega: con todos los nodos
        # obligatorios no debería pasar, pero si pasa hay que decirlo en vez de
        # devolver una ruta a la que le faltan paradas.
        _servidos = {d.get("location_id")
                     for vd in vehicle_trips.values()
                     for t in vd.get("trips", [])
                     for d in t.get("deliveries", [])}
        _sin_servir = sorted({_nombre_local(l, i) for i, l in enumerate(extended_locations)
                              if i > 0 and l.get("id") not in _servidos})
        if _sin_servir:
            diag.aviso("LOCAL_SIN_VISITA",
                       f"Quedaron sin visita: {', '.join(_sin_servir[:10])}")

        diag.info("RESULTADO",
                  f"Ruta lista: {len(vehicle_trips)} camión(es), {total_stops_global} paradas, "
                  f"{_num(total_distance, 1)} km, {_num(total_kg)} kg.")
        update_job_status(job_id, "completado", "Optimización finalizada exitosamente.", 100)

        return jsonify({
            "status": "success",
            "avisos": diag.mensajes("aviso"),
            "log": diag.resumen(),
            "locales_sin_visita": _sin_servir,
            "meta": {
                "max_vehicles": base_num_vehicles,
                "max_trips_per_vehicle": max_trips_per_vehicle,
                "time_horizon_drive_minutes": HORIZON,
                "max_stops_per_vehicle": maximo_de_paradas,
                "time_multiplier": multiplicador_tiempo,
                "reload_service_time_minutes": reload_service_time,
                "max_departure_slack_minutes": max_departure_slack,
                "limitar_espera": bool(limitar_espera),
                "max_wait_minutes": (max_wait_minutes_cfg if limitar_espera else None),
                "mezclar_zonas": bool(mezclar_zonas),
                "esperas_recortadas": len(esperas_recortadas),
                "vehicle_departure_times": vehicle_departure_times_raw,
                "departure_times_by_truck": departure_times_by_truck_raw,
                "reference_departure_time": reference_departure_time_str,
                "departure_time_global": departure_time_global_str or reference_departure_time_str,
                "departure_time":        departure_time_global_str or reference_departure_time_str,
                "workers": (req_workers if workers_aplicado else 1),
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

    except ErrorOptimizacion as err:
        # Fallo CON causa identificada: es lo que ve el usuario en la app.
        _diag = locals().get("diag") or Diagnostico(job_id)
        _diag.error(err.codigo, err.mensaje)
        print(f"❌ [{err.codigo}] {err.mensaje}", flush=True)
        for linea in err.detalle:
            print(f"   · {linea}", flush=True)
        update_job_status(job_id, "fallo", err.mensaje, 100,
                          detalle=err.detalle, codigo=err.codigo, sugerencias=err.sugerencias)
        return jsonify({
            "status": "error",
            "error": err.mensaje,
            "error_code": err.codigo,
            "detalle": err.detalle,
            "sugerencias": err.sugerencias,
            "diagnostico": err.datos,
            "log": _diag.resumen(),
        }), err.http

    except Exception as e:
        import traceback
        traceback.print_exc()
        _diag = locals().get("diag")
        mensaje = f"Error interno del optimizador: {type(e).__name__}: {e}"
        if job_id:
            update_job_status(job_id, "fallo", mensaje, 100,
                              codigo="ERROR_INTERNO",
                              detalle=["Es un fallo del optimizador, no de los datos que cargaste.",
                                       "El detalle técnico quedó en el registro del servidor."])
        return jsonify({
            "status": "error",
            "error": mensaje,
            "error_code": "ERROR_INTERNO",
            "detalle": ["Es un fallo del optimizador, no de los datos que cargaste."],
            "sugerencias": ["Reintenta; si vuelve a pasar, reporta el problema con la hora exacta."],
            "log": _diag.resumen() if _diag else {},
            "traceback": traceback.format_exc(),
        }), 500

# ---------------------------------------------------------------------
# Progreso en vivo por la misma conexión
# ---------------------------------------------------------------------
#
# Mismo cálculo que /optimize, pero la respuesta es NDJSON (una línea JSON por
# evento) y va llegando mientras se calcula:
#   {"tipo": "inicio",    "job_id": ...}
#   {"tipo": "progreso",  "etapa", "mensaje", "porcentaje", + cifras de la búsqueda}
#   {"tipo": "latido",    "t": segundos}            ← cada 5 s sin novedades
#   {"tipo": "resultado", "http": 200|400|500, "cuerpo": <lo que devuelve /optimize>}
# El latido mantiene viva la conexión a través de los proxies durante los
# minutos de cálculo en que el solver no informa nada.

LATIDO_SEGUNDOS = 5


def _linea(evento):
    return json.dumps(evento, ensure_ascii=False) + "\n"


@app.route("/optimize/stream", methods=["POST"])
def optimize_stream():
    payload = request.get_json(silent=True)
    if payload is None:
        return jsonify(error="No se recibió JSON válido"), 400

    datos = payload[0] if isinstance(payload, list) and payload else payload
    if not isinstance(datos, dict):
        return jsonify(error="Se esperaba [data, truck_ids, user_id]"), 400
    job_id = str(datos.get("job_id") or uuid.uuid4().hex)
    datos["job_id"] = job_id

    cola = queue.Queue()
    _registrar_oyente(job_id, lambda estado: cola.put(("progreso", estado)))

    def calcular():
        # optimize() lee `request`: se le da un contexto propio con el mismo JSON.
        try:
            with app.test_request_context("/optimize", method="POST", json=payload):
                respuesta = optimize()
                if isinstance(respuesta, tuple):
                    cuerpo, codigo = respuesta[0], respuesta[1]
                else:
                    cuerpo, codigo = respuesta, 200
                cola.put(("fin", cuerpo.get_json(silent=True) or {}, int(codigo)))
        except Exception as e:
            cola.put(("fin", {"status": "error", "error": f"Error interno del optimizador: {e}",
                              "error_code": "ERROR_INTERNO"}, 500))

    threading.Thread(target=calcular, name=f"optimizar-{job_id}", daemon=True).start()

    def eventos():
        inicio = time.monotonic()
        try:
            yield _linea({"tipo": "inicio", "job_id": job_id})
            while True:
                try:
                    tipo, *resto = cola.get(timeout=LATIDO_SEGUNDOS)
                except queue.Empty:
                    yield _linea({"tipo": "latido", "t": round(time.monotonic() - inicio, 1)})
                    continue
                if tipo == "fin":
                    cuerpo, codigo = resto
                    yield _linea({"tipo": "resultado", "http": codigo, "cuerpo": cuerpo})
                    return
                estado = resto[0]
                evento = {"tipo": "progreso", "etapa": estado.pop("status", None),
                          "mensaje": estado.pop("message", None),
                          "porcentaje": estado.pop("progress", None)}
                estado.pop("updated_at", None)
                evento.update(estado)
                yield _linea(evento)
        finally:
            _quitar_oyente(job_id)

    return Response(eventos(), mimetype="application/x-ndjson",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000, debug=False)