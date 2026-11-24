"""Prioridad de zona de un camión en el optimizador (24-11-2026).

Pedido del usuario: «Hay camiones con prioridad de zona, donde la zona puede ser
REGION METROPOLITANA o QUINTA REGION, de esta forma, el optimizador si el camion
tiene esa prioridad de zona, la debe respetar». Decisiones del usuario:
  · un camión con prioridad NUNCA reparte fuera de su zona;
  · los camiones sin prioridad pueden ayudar en esa zona si hace falta, pero el
    de la prioridad va primero;
  · la prioridad está en la ficha del camión (bd_teruel: trucks.zona_prioritaria)
    y llega en `vehicle_zona_prioridad`, una por camión.

Parte del payload real de data_input.json (4 locales, 2 camiones refrigerados de
700 kg). Los kilos de cada local: Tottus CD (70) 392 · Santa Isabel (89) 80,86 ·
Jumbo (106) 428,2 · Jumbo (109) 187,28. Las zonas se ponen en cada prueba.
"""
import json
import os
import sys

import pytest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import optimizer  # noqa: E402
from test_retiros import agregar_local, optimizar, payload_de_ejemplo, paradas, wenco  # noqa: E402

RM, QUINTA = "REGION METROPOLITANA", "QUINTA REGION"


@pytest.fixture
def payload():
    return payload_de_ejemplo()


def con_zonas(payload, **zonas):
    """zonas: {"z70": RM, ...} por id de local."""
    for loc in payload[0]["locations"][1:]:
        loc["zona"] = zonas.get(f"z{loc['id']}")
    return payload


def con_prioridades(payload, *prioridades):
    payload[0]["vehicle_zona_prioridad"] = list(prioridades)
    return payload


def locales_por_camion(r):
    return {a["vehicle"]: sorted(d["location_id"] for t in a["trips"] for d in t["deliveries"] if d["tipo"] == "entrega")
            for a in r["assignments"]}


# ── Las ayudas ──────────────────────────────────────────────────────────────

def test_la_zona_de_un_local_y_la_prioridad_de_un_camion():
    assert optimizer._zona_de_prioridad("REGION METROPOLITANA ZONA ORIENTE") == optimizer.PRIORIDAD_METRO
    assert optimizer._zona_de_prioridad("quinta region") == optimizer.PRIORIDAD_QUINTA
    assert optimizer._zona_de_prioridad("ZONA SUR") is None
    assert optimizer._zona_de_prioridad(None) is None
    assert optimizer._leer_prioridades(None, 2) == ([None, None], None)
    assert optimizer._leer_prioridades([RM, ""], 2) == ([optimizer.PRIORIDAD_METRO, None], None)
    assert optimizer._leer_prioridades([RM], 2)[1] == "vehicle_zona_prioridad no coincide con max_vehicles"
    assert "no es una zona con prioridad" in optimizer._leer_prioridades(["ZONA SUR", None], 2)[1]
    assert optimizer._resumen_de_prioridades(["METRO", "METRO"]) == \
        "Los 2 camiones elegidos tienen prioridad Región Metropolitana."
    assert optimizer._resumen_de_prioridades(["QUINTA", None, None]) == \
        "Camiones elegidos: 1 camión con prioridad Quinta Región, 2 camiones sin prioridad."


# ── Sin prioridades, como siempre ───────────────────────────────────────────

def test_sin_prioridad_todo_sigue_igual_y_la_respuesta_lo_dice(payload):
    con_zonas(payload, z70=RM, z89=RM, z106=QUINTA, z109=QUINTA)
    http, r = optimizar(payload)
    assert http == 200 and r["status"] == "success"
    assert sorted(sum(locales_por_camion(r).values(), [])) == [70, 89, 106, 109]
    assert all(a["zona_prioridad"] is None for a in r["assignments"])


def test_un_valor_que_no_es_una_zona_o_un_largo_distinto_es_un_error_de_la_aplicacion(payload):
    http, r = optimizar(con_prioridades(payload, "ZONA SUR", None))
    assert http == 400 and "no es una zona con prioridad" in r["error"]
    http, r = optimizar(con_prioridades(payload_de_ejemplo(), RM))
    assert http == 400 and r["error"] == "vehicle_zona_prioridad no coincide con max_vehicles"


# ── La regla y la preferencia ───────────────────────────────────────────────

def local_quinta_junto_a_jumbo(payload):
    """Un local de la Quinta (sin carga ni horario) a 3 min y 1 km de Jumbo 106,
    que en la ruta de ejemplo va en el camión 1."""
    return agregar_local(payload, {"id": 80, "identificador": "LOCAL QUINTA TEST", "lat": -33.0, "lng": -71.5,
                                   "demanda": {}, "wait_minutes": 15, "zona": QUINTA}, como=3, minutos=3.0, km=1.0)


def test_el_camion_con_prioridad_solo_va_a_su_zona_y_la_toma_primero(payload, monkeypatch):
    # Quinta: Tottus (70) y el local de prueba (80); el resto, Región Metropolitana.
    con_prioridades(con_zonas(payload, z70=QUINTA, z89=RM, z106=RM, z109=RM), QUINTA, None)
    local_quinta_junto_a_jumbo(payload)
    http, r = optimizar(payload)
    assert http == 200 and r["status"] == "success"
    por_camion = locales_por_camion(r)
    assert por_camion[0] == [70, 80], "la Quinta en el camión con prioridad Quinta: va primero"
    assert por_camion[1] == [89, 106, 109], "el camión con prioridad no sale de su zona"
    assert {a["vehicle"]: a["zona_prioridad"] for a in r["assignments"]} == {0: "Quinta Región", 1: None}
    eventos = {e["codigo"]: e["mensaje"] for e in r["log"]["eventos"]}
    assert eventos["PRIORIDAD_DE_ZONA"].startswith("Camiones elegidos: 1 camión con prioridad Quinta Región, "
                                                   "1 camión sin prioridad.")

    # Sin la multa, el local de la Quinta junto a Jumbo se lo lleva el camión
    # sin prioridad (le queda de paso): la multa es lo que hace ir primero al otro.
    monkeypatch.setattr(optimizer, "PRIORIDAD_ZONA_PENALTY", 0)
    sin_multa = payload_de_ejemplo()
    con_prioridades(con_zonas(sin_multa, z70=QUINTA, z89=RM, z106=RM, z109=RM), QUINTA, None)
    local_quinta_junto_a_jumbo(sin_multa)
    http, r = optimizar(sin_multa)
    assert http == 200 and 80 in locales_por_camion(r)[1]


def test_si_al_de_la_prioridad_no_le_alcanza_el_otro_ayuda_en_su_zona(payload):
    # Todo en la Quinta (1.088 kg): el camión con prioridad lleva 700 kg; el otro ayuda.
    con_prioridades(con_zonas(payload, z70=QUINTA, z89=QUINTA, z106=QUINTA, z109=QUINTA), QUINTA, None)
    http, r = optimizar(payload)
    assert http == 200 and r["status"] == "success"
    por_camion = locales_por_camion(r)
    assert por_camion[0] and por_camion[1], "los dos camiones reparten en la Quinta"
    assert sorted(por_camion[0] + por_camion[1]) == [70, 89, 106, 109]


def test_la_region_metropolitana_incluye_sus_zonas_oriente_poniente_y_sur(payload):
    con_prioridades(con_zonas(payload, z70=RM, z89="REGION METROPOLITANA ZONA ORIENTE",
                              z106="REGION METROPOLITANA ZONA SUR", z109="REGION METROPOLITANA ZONA PONIENTE"),
                    RM, RM)
    http, r = optimizar(payload)
    assert http == 200 and r["status"] == "success"
    assert sorted(sum(locales_por_camion(r).values(), [])) == [70, 89, 106, 109]


# ── Lo que no se puede: dicho antes de calcular ─────────────────────────────

def test_un_local_al_que_ningun_camion_puede_ir_lo_dice_con_su_zona(payload):
    con_prioridades(con_zonas(payload, z70=QUINTA, z89=RM, z106=RM, z109="ZONA SUR"), RM, RM)
    http, r = optimizar(payload)
    assert http == 400 and r["error_code"] == "PRIORIDAD_DE_ZONA"
    assert r["error"] == "Hay locales a los que no puede ir ningún camión elegido: todos tienen prioridad de otra zona."
    assert "Quinta Región: TOTTUS CD (ID 70)." in r["detalle"]
    assert "ZONA SUR: JUMBO LOCAL (ID 109)." in r["detalle"]
    assert "Los 2 camiones elegidos tienen prioridad Región Metropolitana." in r["detalle"]
    assert r["sugerencias"][0] == "Agrega un camión sin prioridad de zona, o uno con la prioridad de esa zona."


def test_la_carga_de_una_zona_que_no_cabe_en_los_camiones_que_pueden_ir(payload):
    # Todo en la Región Metropolitana (1.086 kg de carga) y solo el camión 1 (700 kg) puede ir.
    con_prioridades(con_zonas(payload, z70=RM, z89=RM, z106=RM, z109=RM), QUINTA, None)
    http, r = optimizar(payload)
    assert http == 400 and r["error_code"] == "PRIORIDAD_DE_ZONA_CAPACIDAD"
    assert r["error"] == "La carga de Región Metropolitana no cabe en los camiones que pueden ir ahí."
    assert r["detalle"][0] == "Carga: 1.086 kg. Camiones que pueden ir: 1, con 700 kg."


# ── Los retiros también respetan la prioridad ───────────────────────────────

def test_un_retiro_fuera_de_la_zona_de_todos_los_camiones_queda_sin_planificar(payload):
    con_prioridades(con_zonas(payload, z70=RM, z89=RM, z106=RM, z109=RM), RM, RM)
    agregar_local(payload, dict(wenco(), zona=QUINTA))
    http, r = optimizar(payload)
    assert http == 200 and r["status"] == "success", "un retiro es opcional: la ruta sale igual"
    assert paradas(r, "retiro") == []
    (fuera,) = r["retiros_sin_planificar"]
    assert fuera["motivo"] == "ningún camión de su zona pasa por ahí y no queda un camión libre para ir a retirar"
    assert sorted(p["location_id"] for p in paradas(r, "entrega")) == [70, 89, 106, 109]
