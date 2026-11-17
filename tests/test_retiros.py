"""Paradas de RETIRO en el optimizador (17-11-2026).

El plan está en bd_teruel: docs/PLAN_PARADAS_DE_RETIRO.md, §4 (el servicio) y
las decisiones D1 (primero entregar), D5 (grupos y zonas), D6 (cuenta como
parada) y D7 (opcional: el que no cabe queda fuera con su motivo).

Cada prueba parte del payload real de data_input.json (4 locales, 2 camiones
refrigerados de 700 kg y 3 palets, salidas 04:00 y 05:30) con 1 segundo de
búsqueda: con tan pocos locales sobra. Lo que se comprueba son reglas, no una
ruta exacta: el solver puede elegir otra igual de buena.
"""
import json
import os
import sys

import pytest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

import optimizer  # noqa: E402

FLOTA = ("vehicle_capacities", "vehicle_palets", "vehicle_consume", "vehicle_free",
         "vehicle_departure_times", "vehicle_interno")


# ── Ayudas ────────────────────────────────────────────────────────────────

def payload_de_ejemplo():
    texto = open(os.path.join(RAIZ, "data_input.json"), encoding="utf-8").read()
    payload = json.loads(texto[texto.index("-d '") + 4:texto.rindex("'")])
    payload[0]["tiempo_calculo"] = 0.02  # 1 s
    return payload


def agregar_local(payload, local, como=3, minutos=10.0, km=8.0):
    """Suma un local al final, a `minutos` y `km` más que el local `como`."""
    datos = payload[0]
    n = len(datos["locations"])
    datos["locations"].append(local)
    for nombre, extra in (("time_matrix", minutos), ("distance_matrix", km)):
        matriz = datos[nombre]
        for fila in matriz:
            fila.append(fila[como] + extra)
        matriz.append([matriz[como][j] + extra for j in range(n)] + [0])
    return n


def wenco(id_=74, nombre="WENCO PAINE", **retiro):
    """Un local sin productos que sólo va a retirar, sin horario."""
    return {"id": id_, "identificador": nombre, "lat": -33.8, "lng": -70.7, "demanda": {}, "wait_minutes": 30,
            "retiro": {"kg": 300, "palets": 1, "minutos": 40,
                       "lineas": [{"clase": "otro", "nombre": "Pallets", "cantidad": 20, "kg": 300}], **retiro}}


def con_camiones(payload, n):
    datos = payload[0]
    actual = datos["max_vehicles"]
    datos["max_vehicles"] = n
    for clave in FLOTA:
        valores = datos.get(clave) or []
        datos[clave] = (valores * n)[:n] if n > actual else valores[:n]
    payload[1] = (payload[1] + [900 + k for k in range(n)])[:n]
    return payload


def optimizar(payload):
    with optimizer.app.test_client() as cliente:
        respuesta = cliente.post("/optimize", json=payload)
        return respuesta.status_code, respuesta.get_json()


def viajes(resultado):
    return [viaje for asignacion in resultado["assignments"] for viaje in asignacion["trips"]]


def paradas(resultado, tipo=None):
    return [p for viaje in viajes(resultado) for p in viaje["deliveries"] if tipo is None or p["tipo"] == tipo]


def entregas_en_orden(viaje):
    """Ninguna entrega después de un retiro (D1)."""
    tipos = [p["tipo"] for p in viaje["deliveries"]]
    return "entrega" not in tipos[tipos.index("retiro"):] if "retiro" in tipos else True


@pytest.fixture
def payload():
    return payload_de_ejemplo()


# ── Sin retiros: como siempre ───────────────────────────────────────────────

def test_sin_retiros_cada_parada_es_una_entrega_y_lo_nuevo_viene_en_cero(payload):
    http, r = optimizar(payload)
    assert http == 200 and r["status"] == "success"
    assert {p["tipo"] for p in paradas(r)} == {"entrega"}
    assert all("retiro" not in p for p in paradas(r))
    assert sorted(p["location_id"] for p in paradas(r)) == [70, 89, 106, 109]
    assert r["retiros_sin_planificar"] == []
    assert r["meta"]["retiros"] == {"pedidos": 0, "planificados": 0}
    assert r["totals"]["total_kg"] == pytest.approx(1088.34)
    assert r["totals"]["total_kg_retiro"] == 0 and r["totals"]["total_palets_retiro"] == 0
    assert all(v["num_retiros"] == 0 for v in viajes(r))


def test_un_local_sin_demanda_y_sin_retiro_se_sigue_visitando(payload):
    payload[0]["locations"][2]["demanda"] = {}
    http, r = optimizar(payload)
    assert http == 200
    santa_isabel = [p for p in paradas(r) if p["location_id"] == 89]
    assert len(santa_isabel) == 1 and santa_isabel[0]["tipo"] == "entrega"
    assert santa_isabel[0]["totals"]["kg"] == 0


# ── Con retiros ─────────────────────────────────────────────────────────────

def test_entregas_y_un_retiro_el_retiro_va_al_final_y_no_suma_a_la_carga(payload):
    agregar_local(payload, wenco())
    http, r = optimizar(payload)
    assert http == 200 and r["status"] == "success"
    (retiro,) = paradas(r, "retiro")
    assert retiro["location_id"] == 74
    assert retiro["retiro"] == {"kg": 300.0, "palets": 1.0, "minutos": 40.0,
                                "lineas": [{"clase": "otro", "nombre": "Pallets", "cantidad": 20, "kg": 300}]}
    assert retiro["products"] == {} and retiro["products_detail"] == []
    assert retiro["totals"]["kg"] == 0 and retiro["totals"]["palets"] == 0
    assert retiro["timing"]["wait_minutes"] == 40
    assert retiro["group"] == "RETIRO" and retiro["requires_refrigeration"] is False
    assert all(entregas_en_orden(v) for v in viajes(r))
    viaje = next(v for v in viajes(r) if v["num_retiros"])
    assert viaje["deliveries"][-1]["tipo"] == "retiro"
    assert viaje["total_kg_retiro"] == 300 and viaje["total_palets_retiro"] == 1
    assert r["totals"]["total_kg"] == pytest.approx(1088.34), "lo retirado nunca se suma a lo entregado"
    assert r["totals"]["total_kg_retiro"] == 300
    assert r["meta"]["retiros"] == {"pedidos": 1, "planificados": 1}
    assert r["locales_sin_visita"] == [] and r["retiros_sin_planificar"] == []


def test_entrega_y_retiro_en_el_mismo_local(payload):
    payload[0]["locations"][4]["retiro"] = {"kg": 50.5, "palets": 0.5, "minutos": 15, "lineas": []}  # JUMBO 109
    http, r = optimizar(payload)
    assert http == 200
    del_109 = [p for p in paradas(r) if p["location_id"] == 109]
    assert sorted(p["tipo"] for p in del_109) == ["entrega", "retiro"]
    assert all(entregas_en_orden(v) for v in viajes(r))
    assert r["totals"]["total_kg"] == pytest.approx(1088.34)
    assert r["totals"]["total_kg_retiro"] == pytest.approx(50.5)


def test_solo_retiros_no_da_nodos_de_entrega_vacios_ni_exige_frio(payload):
    for local in payload[0]["locations"][1:]:
        local["demanda"] = {}
        local["retiro"] = {"kg": 100, "palets": 0.5, "minutos": 20,
                           "lineas": [{"clase": "caja", "caja_de_despacho_id": 1, "nombre": "IFCO ALTA", "cantidad": 50}]}
    payload[0]["vehicle_free"] = [0, 0]  # TOTTUS CD y JUMBO exigen frío para ENTREGAR, no para retirar
    http, r = optimizar(payload)
    assert http == 200 and r["status"] == "success"
    assert {p["tipo"] for p in paradas(r)} == {"retiro"}
    assert sorted(p["location_id"] for p in paradas(r)) == [70, 89, 106, 109]
    assert r["totals"]["total_kg"] == 0 and r["totals"]["total_kg_retiro"] == 400


def test_un_camion_de_walmart_retira_a_la_vuelta(payload):
    datos = payload[0]
    datos["locations"] = datos["locations"][:2]
    datos["locations"][1]["identificador"] = "WALMART CD"
    for matriz in ("time_matrix", "distance_matrix"):
        datos[matriz] = [fila[:2] for fila in datos[matriz][:2]]
    con_camiones(payload, 1)
    agregar_local(payload, wenco(), como=1)
    http, r = optimizar(payload)
    assert http == 200
    (asignacion,) = r["assignments"]
    assert asignacion["modes_used"]["labels"] == ["WALMART"], "el retiro es neutro: no abre el modo libre"
    assert [p["tipo"] for p in asignacion["trips"][0]["deliveries"]] == ["entrega", "retiro"]
    assert r["retiros_sin_planificar"] == []


def test_los_minutos_del_retiro_no_se_recortan_con_el_tope_de_espera(payload):
    agregar_local(payload, wenco(minutos=90))
    http, r = optimizar(payload)
    assert http == 200
    (retiro,) = paradas(r, "retiro")
    assert retiro["timing"]["wait_minutes"] == 90
    assert not any("WENCO" in a and "recortada" in a for a in r["avisos"])


# ── Lo que no cabe ──────────────────────────────────────────────────────────

def test_un_retiro_mas_grande_que_cualquier_camion_es_un_error_con_el_nombre_del_local(payload):
    agregar_local(payload, wenco(kg=900))
    http, r = optimizar(payload)
    assert http == 400 and r["error_code"] == "RETIRO_MAS_GRANDE_QUE_EL_CAMION"
    assert "WENCO PAINE (ID 74): 900 kg y 1 palets." in r["detalle"]
    assert "El camión más grande lleva 700 kg; el de más palets, 3 palets." in r["detalle"]


def test_un_retiro_que_no_cabe_en_palets_tambien_es_un_error(payload):
    agregar_local(payload, wenco(kg=100, palets=4))
    http, r = optimizar(payload)
    assert http == 400 and r["error_code"] == "RETIRO_MAS_GRANDE_QUE_EL_CAMION"


def test_un_retiro_sin_espacio_queda_fuera_y_el_resto_de_la_ruta_sale_igual(payload):
    for k, id_ in enumerate((74, 75, 76)):
        agregar_local(payload, wenco(id_=id_, nombre=f"WENCO {id_}", kg=650), minutos=10 + 5 * k)
    http, r = optimizar(payload)
    assert http == 200 and r["status"] == "success"
    assert len(paradas(r, "retiro")) == 2
    (fuera,) = r["retiros_sin_planificar"]
    assert fuera["motivo"].startswith("no queda espacio: el retiro ocupa 650 kg y 1 palets")
    assert fuera["retiro"]["kg"] == 650
    assert sorted(p["location_id"] for p in paradas(r, "entrega")) == [70, 89, 106, 109]
    assert r["totals"]["total_kg"] == pytest.approx(1088.34)
    assert r["meta"]["retiros"] == {"pedidos": 3, "planificados": 2}
    assert any(a.startswith(f"{fuera['local']}: el retiro quedó fuera de la ruta") for a in r["avisos"])
    assert r["locales_sin_visita"] == [], "un retiro que quedó fuera no es un local sin visita"


def test_un_retiro_en_un_local_que_cierra_antes_queda_fuera_con_la_hora(payload):
    local = wenco()
    local["open_time"], local["close_time"] = "03:00", "05:00"
    agregar_local(payload, local)
    http, r = optimizar(payload)
    assert http == 200
    (fuera,) = r["retiros_sin_planificar"]
    assert fuera["motivo"].startswith("cierra a las 05:00: con 40 min de carga hay que llegar antes de las 04:20")
    assert len(paradas(r, "entrega")) == 4


def test_el_motivo_del_cierre_dice_la_tolerancia_del_local(payload):
    local = wenco()
    local["open_time"], local["close_time"], local["closing_gap"] = "03:00", "05:00", 10
    agregar_local(payload, local)
    http, r = optimizar(payload)
    assert http == 200
    (fuera,) = r["retiros_sin_planificar"]
    assert fuera["motivo"].startswith(
        "cierra a las 05:00: con 40 min de carga y 10 min de tolerancia hay que llegar antes de las 04:10")


def test_nunca_se_retira_antes_de_entregar_aunque_convenga(payload):
    # A 5 min del packing y cerrando a las 05:00: sólo cabría ANTES de las
    # entregas (los camiones salen a las 04:00). Con «primero entregar» (D1)
    # no se puede, así que queda fuera.
    local = wenco()
    local["open_time"], local["close_time"] = "03:00", "05:00"
    agregar_local(payload, local, como=0, minutos=5, km=2)
    http, r = optimizar(payload)
    assert http == 200
    assert paradas(r, "retiro") == []
    (fuera,) = r["retiros_sin_planificar"]
    assert "después de sus entregas" in fuera["motivo"]


def test_el_retiro_cuenta_como_parada(payload):
    payload[0]["maximas_paradas_camion"] = 2  # las 4 entregas llenan los 2 camiones
    agregar_local(payload, wenco())
    http, r = optimizar(payload)
    assert http == 200
    (fuera,) = r["retiros_sin_planificar"]
    assert "máximo de 2 paradas" in fuera["motivo"]


# ── Zonas ───────────────────────────────────────────────────────────────────

def _zonas_separadas(payload):
    for local in payload[0]["locations"][1:]:
        local["zona"] = "REGION METROPOLITANA"
    local = wenco()
    local["zona"] = "QUINTA REGION"
    agregar_local(payload, local)
    payload[0]["mezclar_zonas"] = False
    return payload


def test_sin_mezclar_zonas_el_retiro_va_en_un_camion_de_su_zona(payload):
    con_camiones(_zonas_separadas(payload), 3)
    http, r = optimizar(payload)
    assert http == 200
    viaje = next(v for v in viajes(r) if v["num_retiros"])
    assert viaje["zona_clase"] == "OTRA"
    assert [p["tipo"] for p in viaje["deliveries"]] == ["retiro"]


def test_sin_mezclar_zonas_y_sin_camion_libre_el_retiro_queda_fuera(payload):
    http, r = optimizar(_zonas_separadas(payload))
    assert http == 200
    (fuera,) = r["retiros_sin_planificar"]
    assert fuera["motivo"].startswith("ningún camión de su zona pasa por ahí")


# ── Datos que no se entienden ───────────────────────────────────────────────

def test_un_retiro_con_kilos_que_no_son_numero_se_rechaza(payload):
    agregar_local(payload, wenco(kg="mucho"))
    http, r = optimizar(payload)
    assert http == 400 and r["error_code"] == "RETIRO_INVALIDO"
    assert r["detalle"] == ["WENCO PAINE (ID 74): «kg» del retiro es «mucho» y tiene que ser un número."]
