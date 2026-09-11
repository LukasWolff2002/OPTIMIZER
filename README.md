# Optimizador de rutas — servicio externo

Servicio Flask + Google OR-Tools que arma las rutas de reparto. Lo llama
`OptimizerJob` de la app `bd_teruel` (`POST /optimize`), que le manda locales,
camiones y matrices y recibe las rutas ya resueltas.

```
bd_teruel                          este servicio
─────────                          ─────────────
OptimizerController#create
   └─ OptimizerJob ──POST /optimize──► revisar_estructura
                                        revisar_factibilidad   ← rechaza antes de calcular
                                        construir_modelo(...)
                                        SolveWithParameters
                                        └─ sin solución → diagnosticar_infactibilidad
   ◄── 200 + assignments  |  400 + error_code/detalle/sugerencias
```

## Contrato de entrada

`POST /optimize` con `[data, truck_ids, user_id]`. Campos de `data` que importan:

| Campo | Qué hace |
|---|---|
| `locations` | `[0]` es el depósito. Cada uno: `id`, `lat`, `lng`, `identificador`, `demanda {producto_id: kg}`, `palets_en_suelo`, `wait_minutes`, `open_time`, `close_time`, `opening_gap`, `closing_gap` |
| `distance_matrix` / `time_matrix` | Cuadradas y del tamaño de `locations`. **La de tiempos es obligatoria** |
| `max_vehicles`, `vehicle_capacities`, `vehicle_palets`, `vehicle_consume`, `vehicle_free` | Flota. `vehicle_free` = refrigerado |
| `vehicle_departure_times` | `"HH:MM"` por camión, en el orden de `truck_ids` |
| `tiempo_calculo` | Minutos de búsqueda |
| `maximas_paradas_camion` | Tope de paradas por camión |
| `multiplicador_tiempo` | Margen sobre los tiempos de viaje (tráfico) |
| `limitar_espera` | **Nuevo.** `true` recorta la espera de cada local a `max_wait_minutes`; `false` usa la estimada real |
| `max_wait_minutes` | **Nuevo.** Tope en minutos (por defecto 30) |
| `max_departure_slack_minutes` | **Nuevo.** Cuánto puede el cálculo atrasar la hora de salida de un camión (0–720, por defecto 60). Con 0 sale exactamente a la hora pedida |
| `fecha` | **Nuevo.** Fecha de la ruta. Sin ella se usaba la del servidor (UTC), y las reglas por día de la semana salían mal |
| `max_time_per_trip` | Horizonte de conducción por camión. Por defecto 480 min — hoy la app **no lo envía** |
| `job_id` | Id de la `OptimizerRequest`, para publicar el progreso en Redis |

## Contrato de salida

**Éxito** (200): `status: "success"`, `assignments`, `totals`, `meta`, más
`avisos` (cosas que conviene mirar: esperas recortadas, márgenes justos),
`locales_sin_visita` y `log` (la bitácora completa).

Cada asignación trae además, cuando el cálculo movió la salida:
`departure_clock_solicitado` (la hora que se pidió), `departure_delay_minutes` y
`departure_delay_motivo`. La app lo muestra como "pediste 07:00 · +35 min".

**Fallo** (400): `status: "error"` con

```json
{ "error": "La carga no cabe en los camiones seleccionados.",
  "error_code": "CAPACIDAD_KG_INSUFICIENTE",
  "detalle": ["Demanda total: 5.240 kg.", "Capacidad de la flota: 3.000 kg."],
  "sugerencias": ["Agrega otro camión a la selección."],
  "diagnostico": { "demanda_kg": 5240, "capacidad_kg": 3000 },
  "log": { "eventos": [ … ] } }
```

`OptimizerJob` guarda todo eso en `optimizer_requests.error_details` y lo
muestran la pantalla de "Procesando", el índice del optimizador y "Última
solicitud".

## Cómo se explica un fallo

Tres capas, de la más barata a la más cara:

1. **`revisar_estructura`** — el payload llegó incompleto, sin matriz de tiempos
   o con matrices que no calzan. No se toca el solver.
2. **`revisar_factibilidad` + `revisar_ventanas`** — aritmética pura: kilos vs
   capacidad, palets, refrigeración, grupos excluyentes vs camiones, tope de
   paradas, y por local: ventana invertida, servicio más largo que la ventana,
   local inalcanzable saliendo directo del packing. Todo esto antes se descubría
   después de esperar el tiempo de cálculo completo, con el mensaje "No se pudo
   encontrar solución".
3. **`diagnosticar_infactibilidad`** — sólo si el solver no encontró nada:
   - Primero rearma el modelo **con disyunciones** (se pueden dejar locales fuera
     pagando una multa). Los que quedan fuera son los imposibles, y de cada uno
     se explica por qué.
   - Si quedan fuera *todos*, la causa es global: se rearma el modelo **soltando
     una restricción a la vez** (ventanas, esperas, horizonte, paradas, palets,
     capacidad, refrigeración, grupos). La primera que hace aparecer una solución
     es la que bloqueaba.

Además, si el solver se quedó sin tiempo (`ROUTING_FAIL_TIMEOUT`) se dice eso y
no "es imposible": son problemas distintos con soluciones opuestas.

### Códigos de error

`PAYLOAD_INCOMPLETO` · `SIN_LOCALES` · `MATRIZ_INCONSISTENTE` · `SIN_MATRIZ_TIEMPO` ·
`CAPACIDAD_KG_INSUFICIENTE` · `CAPACIDAD_REFRIGERADA_INSUFICIENTE` ·
`PALETS_INSUFICIENTES` · `SIN_CAMION_REFRIGERADO` · `REFRIGERADOS_INSUFICIENTES_POR_GRUPO` · `GRUPOS_EXCEDEN_CAMIONES` ·
`PARADAS_INSUFICIENTES` · `VENTANAS_IMPOSIBLES` · `LOCALES_IMPOSIBLES` ·
`TIEMPO_DE_CALCULO_INSUFICIENTE` · `VENTANAS_HORARIAS` · `TIEMPOS_DE_ESPERA` ·
`HORIZONTE_CONDUCCION` · `MAXIMO_DE_PARADAS` · `PALETS` · `CAPACIDAD_KG` ·
`REFRIGERACION` · `GRUPOS_EXCLUSIVOS` · `SIN_SOLUCION` · `ERROR_INTERNO`

## Cómo probarlo local

```bash
pip install -r requirements.txt
python3 -c "
import json, sys; sys.path.insert(0, '.')
import optimizer
payload = json.load(open('payload.json'))   # [data, truck_ids, user_id]
with optimizer.app.test_client() as c:
    print(c.post('/optimize', json=payload).get_json())
"
```

`data_input.json` trae un `curl` con un payload real de ejemplo (4 locales,
2 camiones) del que se puede extraer el JSON.

## Puntos críticos y plan de mejora

El desarrollo completo —qué falla, por qué importa y qué proponemos hacer, con
orden sugerido— está en [`docs/PUNTOS_CRITICOS.md`](docs/PUNTOS_CRITICOS.md).
Resumen de lo más urgente: `gunicorn -w 32` en un contenedor acotado, el endpoint
sin autenticación, y que un camión con `palets = 0` desactiva la restricción de
palets en silencio.

## Puntos que conviene tener presentes

- **Todos los locales son obligatorios.** No hay disyunciones en la corrida real:
  si un solo local es imposible, no hay ruta. Por eso el diagnóstico las usa
  para señalarlo.
- **Los vehículos virtuales se generan por grupo presente** (`modos_necesarios`):
  una ruta sin locales de Walmart no crea el modo Walmart. Antes eran siempre 3
  por camión. Medido: misma ruta y mismo tiempo, ~10–17% menos memoria del modelo.
- **`max_trips_per_vehicle = 1`**: el código soporta varios viajes por camión
  (`costo_reingreso_valor`, `reload_service_time`) pero está desactivado.
- **`number_of_workers` no existe en OR-Tools 9.x**: la búsqueda es de un hilo.
  Antes se seteaba en silencio y la respuesta informaba "32 workers".
- **El horizonte de conducción son 480 min fijos** porque la app no manda
  `max_time_per_trip`. Si una ruta no cabe, ahora al menos se dice.
- **Reglas por identificador de local escritas en el código**: los locales
  refrigerados, los pares de bodega compartida (IDs 6/232 y 115/116), la
  prioridad de Punto Azul / La Vega / Tottus / Unimarc y la segunda ventana de
  Nicolás Palma. Cambiar cualquiera de esas reglas exige editar y desplegar.
