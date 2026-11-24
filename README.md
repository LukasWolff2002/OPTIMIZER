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
| `locations[].retiro` | **Nuevo (17-11-2026).** Lo que el camión va a BUSCAR al local: `{"kg", "palets", "minutos", "lineas"}`. Ver «Retiros» |

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

## Retiros (17-11-2026)

Una parada puede ser de **retiro**: el camión va a buscar cajas de despacho,
envases u otra cosa. El plan completo está en la app (`bd_teruel`,
`docs/PLAN_PARADAS_DE_RETIRO.md`, §4 y las decisiones D1, D5, D6 y D7).

**Entrada.** Cada local de `locations[]` puede traer, además de `demanda`:

```json
"retiro": {"kg": 750.0, "palets": 4, "minutos": 40,
           "lineas": [{"clase": "envase", "envase_id": 3, "nombre": "Clamshell 250 g",
                       "cantidad": 30000, "kg": 750.0, "palets": 4.0}]}
```

El servicio usa `kg`, `palets` y `minutos` (los de carga: el tope de espera
no los recorta) y devuelve `lineas` tal como llegaron. Un local con `retiro` y
sin productos es **sólo de retiro**: no da una parada de entrega vacía (un
local sin productos y sin retiro se sigue visitando, como siempre).

**Reglas.**

- **Un nodo de retiro por local**, con la fila del local en las matrices, su
  horario y sus tolerancias. Va después de todos los nodos de entrega.
- **Primero se entrega, después se retira (D1).** No existe el arco de un
  retiro a una entrega: en un camión, ninguna entrega va después de un retiro.
  Si un local tiene entrega y retiro, el retiro va después de TODAS las
  entregas del camión, no necesariamente justo después de la suya.
- **Espacio aparte.** Dos dimensiones, `RetiroKg` y `RetiroPalets`, con los kg
  y los palets del camión: cuando empieza a cargar, lo entregado ya bajó.
- **Neutro (D5).** Entra en cualquier modo (libre, Walmart, Cencosud), no abre
  uno propio, no paga la multa de mezclar grupos ni las de primera parada.
  Un camión de Walmart puede retirar a la vuelta. Sin mezclar zonas, respeta
  la zona de su local. Nunca exige frío.
- **Cuenta como parada (D6)** y su desvío entra en las horas de conducción.
- **Opcional (D7).** Cada retiro va en una disyunción con la multa de descarte
  (10⁹): el solver lo deja fuera sólo si no cabe. Las entregas siguen siendo
  obligatorias.
- **Un retiro no se parte.** Si no cabe entero (kg y palets) en ningún camión,
  es el error `RETIRO_MAS_GRANDE_QUE_EL_CAMION`, antes de calcular.

**Salida.**

- Cada parada de `trips[].deliveries[]` trae `"tipo": "entrega"` o `"retiro"`.
  La de retiro trae además `"retiro"` (el eco), `products: {}`,
  `products_detail: []`, `totals.kg = 0`, `group: "RETIRO"` y sus horas como
  cualquier parada (`timing.wait_minutes` = los minutos de carga).
- Por viaje: `num_retiros`, `total_kg_retiro`, `total_palets_retiro`; por
  camión y en `totals`: `total_kg_retiro` y `total_palets_retiro`. Nunca se
  suman a `total_kg` ni a `total_palets`.
- `retiros_sin_planificar`: los que quedaron fuera, con `location_id`,
  `identificador`, `local`, `zona`, el eco de `retiro` y el `motivo` (el
  horario, el espacio, el máximo de paradas, la conducción o la zona). Cada uno
  va también en `avisos`. **Viene siempre, vacía si no hay**: así la app sabe
  que el servicio conoce los retiros.
- `meta.retiros`: `{"pedidos": n, "planificados": m}`.

**Despliegue.** Este servicio va ANTES que la app que manda retiros: uno viejo
ignoraría `retiro` y visitaría el local como una entrega vacía. Sin retiros en
la solicitud, la respuesta es la de siempre más las claves nuevas en cero.

## Prioridad de zona de un camión (24-11-2026)

Pedido del usuario: «Hay camiones con prioridad de zona, donde la zona puede ser
REGION METROPOLITANA o QUINTA REGION, de esta forma, el optimizador si el camion
tiene esa prioridad de zona, la debe respetar». Se marca en la ficha del camión
(`bd_teruel`: `trucks.zona_prioritaria`).

**Entrada.** `vehicle_zona_prioridad`: una por camión, en el orden de los demás
`vehicle_*`; `"REGION METROPOLITANA"`, `"QUINTA REGION"` o `null` (sin
prioridad). Sin la clave, ningún camión tiene prioridad. Otro valor o un largo
distinto de `max_vehicles` es un 400.

**Reglas** (decisiones del usuario):

- **Nunca fuera de su zona.** Un camión con prioridad solo reparte en su zona:
  ni un local de otra zona o sin zona, ni su retiro (regla dura, `RemoveValue`).
  La Región Metropolitana es toda zona que dice METROPOLITANA (también oriente,
  poniente y sur); la Quinta Región, la que dice QUINTA. Vale se mezclen o no
  las zonas.
- **Va primero, los demás ayudan si hace falta.** Un camión sin prioridad paga
  `PRIORIDAD_ZONA_PENALTY` (60.000, del orden de las multas de primera parada)
  por cada parada de una zona que tiene un camión con prioridad en la selección.

**Antes de calcular.** `PRIORIDAD_DE_ZONA`: hay locales a los que no puede ir
ningún camión elegido (todos con prioridad de otra zona), con los locales por
zona. `PRIORIDAD_DE_ZONA_CAPACIDAD`: la carga de una zona (o de varias juntas)
no cabe en los camiones que pueden ir ahí. Si el solver no encuentra solución,
el diagnóstico prueba soltar `prioridad_zona` (sólo si algún camión la tiene) y
el motivo de un local o de un retiro que queda fuera la menciona.

**Salida.** Cada camión de `assignments[]` trae `zona_prioridad`: «Región
Metropolitana», «Quinta Región» o `null`.

**Despliegue.** Este servicio va ANTES que la app que manda la prioridad: uno
viejo ignora `vehicle_zona_prioridad` y repartiría sin respetarla.

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
`REFRIGERACION` · `GRUPOS_EXCLUSIVOS` · `SIN_SOLUCION` · `ERROR_INTERNO` ·
`RETIRO_MAS_GRANDE_QUE_EL_CAMION` · `RETIRO_INVALIDO`

Avisos (la ruta sale igual): `RETIROS_EXCEDEN_LA_FLOTA` (los retiros juntos
pasan de la flota: alguno va a quedar fuera) y `RETIRO_SIN_PLANIFICAR`.

Las revisiones previas y el diagnóstico miran sólo las ENTREGAS: un retiro es
opcional, así que nunca es lo que impide la ruta. Un retiro con un horario
imposible no es `VENTANAS_IMPOSIBLES`: queda fuera con ese motivo.

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

### Pruebas

```bash
pip install pytest
python3 -m pytest tests -q
```

`tests/test_retiros.py` (16 pruebas, ~15 s) parte de ese mismo payload con 1 s
de búsqueda: sin retiros sale como siempre; el retiro va al final del camión,
no suma a la carga y vuelve con su eco; entrega y retiro en el mismo local;
sólo retiros con camiones sin frío; un camión de Walmart que retira; los
minutos de carga sin recortar; el retiro más grande que el camión (kg y
palets); los que quedan fuera por espacio, por horario, por el máximo de
paradas, por «primero entregar» y por la zona; y un retiro mal formado.

`tests/test_prioridad_zona.py` (9 pruebas): la zona de un local y la prioridad
de un camión; sin prioridad, como siempre; un valor o un largo inválido; el
camión con prioridad no sale de su zona y la toma primero (sin la multa, el
local de su zona se lo llevaría el otro); si no le alcanza, el otro ayuda; la
Región Metropolitana con sus zonas; un local al que ningún camión puede ir; la
carga de una zona que no cabe; y un retiro fuera de la zona de todos.

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
- **Grupos excluyentes por viaje**: Walmart CD solo; Cencosud CD solo o con
  **Punto Azul y/o La Vega, siempre antes que el CD** (Punto Azul + Cencosud,
  La Vega + Cencosud, Punto Azul + La Vega + Cencosud); el resto, mezclable.
  El orden es regla dura: no existe el arco de un Cencosud CD a Punto Azul o
  La Vega (`ACOMPANANTES_DE_CENCOSUD`). Antes solo La Vega podía ir con
  Cencosud, y quedar después del CD costaba una multa, no lo impedía. Se quitó
  `enforce_lv_first_hard`, que la app no enviaba.
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
