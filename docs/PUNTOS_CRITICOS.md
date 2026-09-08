# Optimizador de rutas — puntos críticos y qué propongo hacer

Revisión del algoritmo y del servicio (`optimizer.py`, `Procfile`, `gunicorn.conf.py`)
y de cómo lo llama `bd_teruel`. Cada punto dice **qué pasa**, **por qué importa**
y **qué propongo**, con una estimación de esfuerzo.

Lo que verifiqué ejecutando el servicio localmente va marcado con **✔**.
Los puntos ya resueltos en la última tanda están en el
[README](../README.md) y en `docs/TRANSPORTES_PLAN_FK_NORMALIZACION.md` §14 de la app.

## Resumen

| # | Punto | Riesgo | Esfuerzo |
|---|---|---|---|
| C1 | `gunicorn -w 32` en un contenedor de Railway | **Alto** — caídas por memoria | 1 línea |
| C2 | El endpoint no pide autenticación | **Alto** | ~15 líneas |
| A2 | `palets = 0` se interpreta como "sin límite" | **Alto** — rutas mal armadas en silencio | ~10 líneas + validación |
| A1 | Todos los locales son obligatorios | **Alto** — un local imposible deja la ruta sin solución | Medio |
| A3 | ~~La hora de salida admite 12 h de holgura~~ · **HECHO** | Medio | hecho |
| A4 | Ventanas que cruzan medianoche se asumen del día siguiente | Medio | Medio |
| A5 | Dividir un local duplica su tiempo de descarga | Medio | ~20 líneas |
| B1–B4 | Reglas de negocio escritas en el código | Medio — cambiarlas exige desplegar | Medio-alto |
| B5 | ~~Tres vehículos virtuales por camión, siempre~~ · **HECHO** (rinde menos de lo que supuse) | Bajo | hecho |
| C3 | El candado de concurrencia vive sólo en Rails | Medio | ~20 líneas |
| D1 | Las demandas se truncan a kilos enteros | Bajo | ~5 líneas |
| D3 | Horizonte de conducción fijo en 480 min | Bajo | ~10 líneas |
| D2, D4, D5 | Contrato y código muerto | Bajo | Bajo |

---

## A. Cómo modela el problema

### A1. Todos los locales son obligatorios: uno imposible deja la ruta entera sin solución

**Qué pasa.** El modelo no declara ninguna disyunción (`AddDisjunction`), así que
cada local es de visita obligatoria. Si un solo local no se puede atender —cierra
antes de que se llegue, exige frío y no hay camión refrigerado, su carga no cabe—
el solver no devuelve nada **para toda la ruta**.

**Por qué importa.** Es el modo de fallo más frecuente y el más caro: se pierde la
optimización completa de 30 locales por culpa de uno. El diagnóstico nuevo ya
señala cuál es el culpable, pero el usuario igual tiene que sacarlo a mano y
volver a empezar.

**Qué propongo.** Una casilla en *Ajustes del algoritmo*: **"Si algún local no se
puede atender, armar la ruta con el resto"**. Activada, el modelo agrega
`AddDisjunction([nodo], PENALIZACION_DESCARTE)` por local, y el resultado trae
`locales_omitidos` con el motivo de cada uno, que la pre-ruta muestra antes de
confirmar. La penalización tiene que ser mucho mayor que cualquier costo de arco
real para que sólo omita cuando de verdad no hay forma.

La infraestructura ya está: `construir_modelo(permitir_descartes=True)` se usa hoy
en el diagnóstico. Falta exponerla, propagar `locales_omitidos` a la respuesta y
mostrarlo en la UI. **Esfuerzo: medio.** Es la mejora de mayor impacto de la lista.

### A2. Un camión con `palets = 0` desactiva la restricción de palets ✔

**Qué pasa.**

```python
vehicle_palets_base = [(p if p > 0 else PALLET_INF) for p in vehicle_palets_base]
#                                        ↑ 0 = ilimitado
```

y en la app `trucks.palets` es `integer, default: 0, null: false`. Un camión dado
de alta sin declarar palets llega con 0 y el optimizador le asigna **10⁹ palets**.

**Por qué importa.** La restricción de palets desaparece para ese camión sin que
nada lo diga: la ruta se arma y al cargar no cabe. Es peor que fallar, porque el
error se descubre en el andén. Hoy los cinco camiones de la base tienen palets
declarados, así que está latente, pero el default de la columna lo hace probable.

**Qué propongo.** Separar "sin límite" de "no declarado":
1. En la app, mandar `null` cuando `palets` es 0 y **avisar en la selección de
   flota** ("KLKL11 no tiene palets declarados: no se controlará esa restricción").
2. En el optimizador, cuando llegue `null`/0, registrar un aviso de nivel `aviso`
   —que ya viaja hasta el log de la app y el correo— en vez de asumir en silencio.
3. Idealmente, exigir `palets > 0` al crear o editar un camión.

**Esfuerzo: bajo.**

### A3. La hora de salida que se elige es sólo un piso, con 12 horas de holgura

**Qué pasa.** `MAX_DEPARTURE_SLACK = 720`: el modelo puede arrancar el camión hasta
12 h después de la hora indicada. Encima, el post-proceso (`SAFETY_MARGIN_MINUTES`)
vuelve a correr la salida hacia adelante para reducir la espera ociosa.

**Por qué importa.** La hora de salida es un compromiso operativo (turnos, carga
del camión), no una sugerencia. Alguien pone 07:00 y puede recibir una ruta que
sale mucho después, sin que nada explique por qué.

**HECHO** — la holgura es ahora `max_departure_slack_minutes` (60 min por defecto,
rango 0–720), con su campo en *Ajustes del algoritmo*, junto a las horas de salida
de la flota. El tope acota **el total**: lo que mueve el solver más lo que mueve el
ajuste fino posterior, que antes se sumaban sin control.

La respuesta trae por camión `departure_clock_solicitado`, `departure_delay_minutes`
y `departure_delay_motivo`; la pre-ruta y el detalle de la ruta muestran un aviso
—"pediste 07:00 · +35 min"— cuando la salida se movió. Verificado con el payload de
ejemplo:

| Margen | Camión 0 | Camión 1 |
|---|---|---|
| 0 min | 04:00 → **04:00** | 05:30 → **05:30** |
| 15 min | 04:00 → 04:15 | 05:30 → 05:44 |
| 60 min (por defecto) | 04:00 → 05:00 | 05:30 → 06:30 |
| 720 min (lo que había) | 04:00 → **06:00** | 05:30 → **06:47** |

La última fila es el comportamiento anterior: dos horas de diferencia sobre la hora
elegida, sin aviso.

**El margen es una decisión con costo, y ahora se puede tomar a conciencia.** Sobre
el mismo payload, con 8 s de cálculo:

| Margen | Kilómetros | Atrasos aplicados |
|---|---|---|
| 0 min | 579,0 km | ninguno — salen a la hora pedida |
| 15 min | 579,0 km | +15 / +14 min |
| 60 min | 557,0 km | +60 / +60 min |
| 720 min | 556,0 km | +120 / +77 min |

Exigir la salida exacta cuesta aquí un ~4% más de kilómetros. Es poco, y a cambio la
hora de salida vuelve a ser un dato con el que se puede planificar el turno. Quien
arma la ruta ahora elige de qué lado quiere estar; antes la decisión estaba tomada
de fábrica, en el extremo de las 12 horas.

### A4. Un local que ya cerró se interpreta como que cierra al día siguiente

**Qué pasa.** Los horarios se pasan a minutos relativos a la salida y, si dan
negativo, se les suma 24 h:

```python
delta = close_minutes - reference_departure_minutes
if delta < 0: delta += 24 * 60
```

Un local que cierra a las 03:00 con salida a las 04:00 queda modelado como si
cerrara a las 03:00 **del día siguiente**: 23 horas de ventana.

**Por qué importa.** Es una ventana inventada. La ruta se arma como si el local
estuviera disponible todo el día y el camión llega cerrado. No hay forma de
distinguir "es un local nocturno" de "ya cerró para esta salida".

**Qué propongo.** Un flag explícito por local. Lo más barato: `close_time <
open_time` en el maestro ya significa "cruza medianoche" (Lo Valledor abre 04:00 y
cierra 12:00; un local nocturno sería 22:00–06:00). Con eso, la regla pasa a ser:
sumar 24 h **sólo** si la ventana del maestro cruza medianoche; si no, la ventana
ya pasó y hay que decirlo con un error del tipo `VENTANA_YA_CERRADA`, que el
diagnóstico nuevo sabe mostrar. **Esfuerzo: medio** (hay que revisar los tres
lugares donde se hace la corrección).

### A5. Partir un local en dos duplica su tiempo de descarga

**Qué pasa.** Cuando la demanda de un local supera la capacidad del camión más
grande, se divide en varios nodos. Cada nodo hereda **el `wait_minutes` completo**
y cuenta como una parada más en el tope de `maximas_paradas_camion`.

**Por qué importa.** Un local con 40 min de descarga y demanda partida en dos
consume 80 min de jornada modelada, cuando en la práctica la segunda descarga
—muchas veces en el mismo andén, seguida de la primera— no cuesta lo mismo. Se
subestima cuánto cabe en el día y se gastan paradas del tope.

**Qué propongo.** Reusar el mecanismo que ya existe para los pares de bodega
compartida: si el nodo anterior de la ruta es **otro trozo del mismo local**, el
servicio del segundo trozo se reduce (a un tiempo de recarga configurable) y no
suma parada. El código de `is_shared_warehouse_pair` hace exactamente eso, sólo
hay que generalizarlo a "mismo `location_id`". **Esfuerzo: bajo.**

---

## B. Reglas de negocio escritas en el código

Todas comparten diagnóstico y solución: **son datos, no lógica**. Hoy cambiar
cualquiera de ellas exige editar Python y desplegar el servicio, y quedan fuera del
alcance de quien administra los locales.

### B1. Qué locales exigen frío ✔

Una lista de diez cadenas al inicio de `optimize()`, comparada por substring contra
`identificador`. En la base actual hay locales fuera de la lista (**Lo Valledor**,
**IFCO Santiago**): si alguno necesitara frío, nadie se enteraría — sencillamente
se le asignaría cualquier camión.

**Propongo** una columna `locations.requiere_refrigeracion` (booleana), enviada en
el payload; el optimizador la usa y deja la lista sólo como respaldo para los
registros antiguos. La UI de Ubicaciones ya tiene dónde ponerla (junto a
`es_deposito`).

### B2. Pares de bodega compartida por ID ✔

```python
if (id_from == "6" and id_to == "232") or (id_from == "115" and id_to == "116"): …
```

Son ids de producción escritos a mano. **En la base de desarrollo el id 6 es
"Tottus CD San Bernardo"**, no Jumbo Costanera; los otros tres no existen. La regla
es correcta sólo mientras esos ids no cambien y sólo en ese ambiente.

**Propongo** una tabla puente (`location_pairs`, o `locations.comparte_bodega_con_id`
autorreferencial) que viaje en el payload como lista de pares. Se administra desde
Ubicaciones y deja de depender de ids literales.

### B3. Prioridades de visita (Punto Azul, La Vega, Tottus, Unimarc)

Diez constantes de penalización (`START_PENALTY_*`, `*_LATE_ENTRY_PENALTY`) y
etiquetas por substring. Son números mágicos sin unidad declarada: el costo de arco
es **litros × 1000**, así que `HIGH_PENALTY = 100_000` equivale a 100 litros de
combustible.

**Propongo** (a) documentar la unidad —basta un comentario, ya está en el README—,
(b) mover las etiquetas a `locations.prioridad_visita` (entero: 1 primero, 2
después…) y derivar las penalizaciones de una sola constante
`COSTO_LITRO = 1000`, de modo que la escala se entienda y se ajuste en un lugar.

### B4. La segunda ventana de Nicolás Palma (jue–sáb, 20:00–23:59)

Un caso particular de un local, con su propio bloque de restricciones.

**Propongo** un segundo horario por día en `locations` (`monday_open_2`,
`monday_close_2`, …). Encaja con el modelo de horarios que ya existe, elimina el
caso especial del código y sirve para cualquier local que atienda en dos tramos.
**Esfuerzo: medio-alto** (migración + formulario + payload), pero es el que más
código particular borra.

### B5. Los grupos excluyentes (Walmart / Cencosud) salen del `identificador`

`_group()` compara substrings. Cada camión se modela como **tres vehículos
virtuales** (libre / Walmart / Cencosud) con la restricción de que sólo uno se
active, lo que **triplica el tamaño del modelo**.

**HECHO** — los vehículos virtuales se generan **sólo para los grupos presentes en
la solicitud** (`modos_necesarios()`): si en la ruta no hay locales de Walmart, no
se crea el modo Walmart. Con rutas de un solo grupo, cada camión pasa de 3
vehículos virtuales a 1.

**Lo que midió, contra lo que yo había anticipado.** Dije que era "la optimización
de rendimiento más grande disponible sin tocar el algoritmo". **No lo es.**
Comparando la misma instancia con y sin la reducción, mismo presupuesto de cálculo:

| Instancia | Vehículos en el modelo | Ruta obtenida | Memoria del modelo |
|---|---|---|---|
| 20 locales · 4 camiones | 12 → 4 | 747,3 km en ambos | 10,6 → 9,5 MB (−10%) |
| 40 locales · 8 camiones | 24 → 8 | 1.234,4 km en ambos | 15,6 → 13,0 MB (−17%) |
| 60 locales · 10 camiones | 30 → 10 | 1.882,8 km en ambos | 17,1 → 15,0 MB (−12%) |

(memoria = RSS máximo del proceso menos el de arranque, un proceso por variante;
`tracemalloc` no sirve aquí porque OR-Tools reserva en C++.)

**La ruta es idéntica y el tiempo también**: los modos sobrantes ya estaban
prohibidos por `VehicleVar.RemoveValue`, así que el solver nunca los exploraba de
verdad. Lo que se gana es memoria —un par de megas— y un modelo que dice la verdad
sobre su tamaño. Vale la pena tenerlo, pero **no era la palanca de rendimiento**
que supuse: si hace falta más velocidad hay que buscarla en el punto C4.

Queda pendiente la otra mitad: mover el grupo del `identificador` a una columna
`locations.grupo_exclusivo`.

---

## C. Servicio y despliegue

### C1. `gunicorn -w 32` ✔

```
web: gunicorn -c gunicorn.conf.py -w 32 -b 0.0.0.0:5000 optimizer:app
```

Treinta y dos procesos, cada uno con su copia del intérprete, de OR-Tools y —cuando
le toca una petición— de un modelo que con 40 locales y 8 camiones ocupa cientos de
MB.

**Por qué importa.** El contenedor de Railway tiene memoria acotada: es la receta
para que el proceso muera por OOM justo cuando se lanza una optimización grande. Y
no compra nada, porque **sólo puede correr una optimización a la vez** (el candado
está en Rails) y la búsqueda de OR-Tools es de un solo hilo.

**Propongo** `-w 2` (uno atendiendo, otro para el healthcheck y el `status`), con el
`timeout = 3600` que ya está. **Esfuerzo: una línea.** Es lo primero que haría.

### C2. El endpoint es público ✔

`POST /optimize` no valida nada: quien tenga la URL puede lanzar optimizaciones,
consumir la CPU del servicio y ver la respuesta, que incluye los locales con
nombre, las cantidades y los precios.

**Propongo** un token compartido: `X-Optimizer-Token` contra una variable de
entorno, en el servicio y en `OptimizerJob`. Sin token, 401. **Esfuerzo: ~15 líneas
en cada lado.** (Es el mismo hallazgo que en su momento cerró los cuatro endpoints
públicos de `/optimizer` en la app; este quedó fuera porque vive en otro repo.)

### C3. El candado de concurrencia vive sólo en el lado que llama

`ProcessingLock` está en Rails. El servicio no sabe si ya está resolviendo: dos
llamadas simultáneas (un reintento, otra instancia de la app, una prueba manual)
se solapan y compiten por la CPU, alargando las dos.

**Propongo** un candado en el propio servicio con Redis (`SET … NX EX 3600`, que ya
está conectado para el progreso) que devuelva **409** con un mensaje claro
—"ya hay una optimización en curso, espera a que termine"— que el diagnóstico nuevo
sabe mostrar. **Esfuerzo: bajo.**

### C4. La búsqueda es de un solo hilo ✔

`number_of_workers` no existe en OR-Tools 9.x (verificado sobre el descriptor del
protobuf); el helper lo tragaba en silencio. Ya está corregido para que avise y
para que la respuesta informe lo real.

**Qué se puede hacer de verdad si hace falta más calidad:** subir el tiempo de
cálculo, o lanzar en paralelo dos o tres procesos con `first_solution_strategy`
distintas (`PATH_CHEAPEST_ARC`, `PARALLEL_CHEAPEST_INSERTION`, `SAVINGS`) y quedarse
con la mejor. Antes de eso conviene B5, que reduce el modelo a un tercio.

---

## D. Precisión y contrato

### D1. Las demandas se truncan a kilos enteros ✔

```python
prod_quantities = { p: int(float(loc.get("demanda", {}).get(p, 0))) … }
```

`int()` trunca, no redondea. En el payload de ejemplo se pierden 2,34 kg sobre
1.088; con 40 locales y tres productos cada uno la pérdida esperada ronda los 60 kg
—un palet— siempre **a favor** de creer que cabe más de lo que cabe.

**Propongo** escalar por 100 como ya se hace con los palets (kg × 100 en la
dimensión de capacidad) o, como mínimo, `round()` en vez de `int()`. **Esfuerzo:
bajo.**

### D2. `vehicle_interno` se envía y no se usa ✔

La app lo manda en cada petición y el optimizador no lo lee nunca.

**Propongo** decidir: o se usa (por ejemplo, preferir camiones internos sobre
externos con un costo fijo por vehículo, que es una decisión de negocio real) o se
saca del payload. Dejarlo sugiere una regla que no existe.

### D3. El horizonte de conducción es fijo en 480 minutos

`HORIZON = data.get("max_time_per_trip", 480)` y la app **nunca lo envía**. Ocho
horas de conducción por camión, siempre.

**Propongo** un campo en *Ajustes del algoritmo* junto al tope de espera, o
derivarlo de la jornada del chofer. Mientras tanto el diagnóstico ya lo señala
cuando es lo que bloquea (`HORIZONTE_CONDUCCION`).

### D4. Números mágicos sin unidad

El costo de arco es litros × 1000; `SetGlobalSpanCostCoefficient(50)` cuesta 0,05
litros por minuto de duración total; las penalizaciones van de 25.000 a 100.000
(25 a 100 litros). Nada de eso está escrito en el código.

**Propongo** una constante `COSTO_LITRO = 1000` y expresar todo en múltiplos de
ella (`HIGH_PENALTY = 100 * COSTO_LITRO`), para que se lea qué se está cambiando al
tocar un número.

### D5. Código muerto que confunde

`max_trips_per_vehicle = 1` desactiva el soporte de varios viajes por camión, pero
quedan vivos `costo_reingreso_valor`, `reload_service_time` y toda la maquinaria de
`vehicle_trip_no`.

**Propongo** decidirlo explícitamente. Si el negocio hace reingresos al packing
—camión que vuelve, recarga y sale otra vez—, **activarlo es probablemente la mejora
de kilómetros más grande disponible**, y el código ya está. Si no se hace, borrarlo.

---

## Orden que propongo

**Primero, lo que evita caídas y datos mal armados (una tarde de trabajo):**

1. **C1** `-w 32` → `-w 2`. Una línea.
2. **C2** token en el endpoint.
3. **A2** palets 0 = ilimitado.
4. **D1** truncado de kilos.

**Después, lo que más cambia la experiencia de uso:**

5. **A1** permitir armar la ruta dejando locales fuera, con el motivo de cada uno.
6. **A3** acotar la holgura de la hora de salida y mostrar el ajuste.
7. **B5** generar sólo los modos de grupo presentes (modelo 3× → 1×).

**Luego, sacar las reglas del código a la base:**

8. **B1** refrigeración por columna.
9. **B2** pares de bodega por tabla.
10. **A4** ventanas que cruzan medianoche.
11. **A5** partición de locales sin duplicar la descarga.
12. **B4** segundo horario por local (elimina el caso Nicolás Palma).

**Y decisiones de negocio, no técnicas:**

13. **D5** ¿se hacen reingresos al packing? Si sí, activar multi-viaje.
14. **D2** ¿los camiones internos tienen preferencia sobre los externos?
15. **D3** ¿ocho horas de conducción es el número correcto?
