# Tail Latency con Adaptive Hedged Requests — V2

PoC local en Go para comparar la latencia de cola de dos arquitecturas que
necesitan agregar las respuestas de diez servicios independientes:

- **Baseline:** diez instancias primarias sin respaldo.
- **Propuesta:** diez primarias más dos workers compartidos para respaldos
  selectivos.

Como la respuesta final espera los diez resultados, basta que uno de ellos sea
lento para elevar la latencia completa. El proyecto mide si reducir ese efecto
en p95 y p99 compensa un incremento simulado de capacidad del 20%. No representa
una implementación de producción ni un precio real de infraestructura.

## Arquitecturas

```text
Baseline
Cliente -> Orquestador -> [P1][P2][P3] ... [P10] -> Agregado de 10 respuestas

Propuesta
                         +-> [P1][P2][P3] ... [P10] -+
Cliente -> Orquestador --+                              +-> primer resultado válido
                         +-> [Worker R1][Worker R2] ----+
                               pool compartido
```

Cada bloque representa una instancia lógica con capacidad limitada por un
semáforo. En la configuración V2 predeterminada, un servicio pendiente después
de 150 ms puede obtener un respaldo del pool; la primera respuesta válida gana
y cancela la redundante.

## Configuración final

| Parámetro | Valor |
|---|---:|
| Servicios | 10 |
| Instancias primarias | 10 |
| Workers compartidos de respaldo | 2 |
| Capacidad por instancia | 5 |
| Respaldos máximos por solicitud | 3 |
| Umbral de Hedged Requests | **150 ms** |
| Probabilidad de latencia lenta | 3% |
| Latencia normal | 10–30 ms |
| Latencia lenta | 400–1500 ms |
| Timeout | 5000 ms |
| Semilla | 2026 |

El umbral de 150 ms corresponde a V2 y es el valor predeterminado del código y
del CLI. No es necesario indicarlo al ejecutar los escenarios predefinidos.

## Plan de latencias

Antes de iniciar trabajo concurrente se generan las latencias primaria y de
rescate para cada combinación de request y servicio. Los request IDs del plan
van de `1` a `RequestCount`, ambos inclusive.

Las muestras primarias y de rescate usan streams pseudoaleatorios separados,
derivados de la semilla configurada. Por ello, baseline y propuesta pueden
compartir exactamente las mismas latencias primarias sin que el scheduling de
goroutines altere el workload. Las esperas de los handlers aceptan cancelación
mediante `context.Context`.

## Baseline

El baseline mantiene una instancia primaria por servicio. Cada instancia usa
un channel con búfer como semáforo y admite, por defecto, cinco ejecuciones
simultáneas. Para cada solicitud, el orquestador despacha los diez servicios en
paralelo y solo completa el agregado al recibir una respuesta válida de todos.

Al vencer el timeout, el contexto compartido cancela tanto los intentos que ya
comenzaron como los que aún esperan capacidad. `Execute` recoge el estado final
de sus diez intentos antes de retornar, por lo que no deja trabajo de esa
solicitud ejecutándose en segundo plano.

## Métricas y percentiles

El baseline registra automáticamente la latencia terminal de cada solicitud y
los estados finales de todos sus intentos. El recolector está protegido para
aceptar solicitudes concurrentes y entrega snapshots que no exponen su memoria
interna.

p50, p95 y p99 usan nearest-rank: se ordena una copia de las `N` latencias y se
selecciona el índice `ceil(p × N) - 1`. Se incluyen tanto solicitudes completadas
como timeouts; estos últimos también conservan un contador separado. Las
latencias agregadas son tiempos totales del cliente y no duraciones aisladas de
servicios.

## Shared Rescue Pool

El pool crea el número configurado de workers, cada uno con un semáforo propio.
`TryReserve` revisa los workers en orden numérico y retorna inmediatamente si
todos están llenos. Una reserva puede ejecutar cualquiera de los diez handlers
y devuelve su slot después de éxito, error o cancelación.

El snapshot del pool incluye ocupación actual y máxima por worker, ocupación
total, reservas aceptadas y rechazadas y utilización ponderada por tiempo. El
estado puede reiniciarse entre experimentos únicamente cuando la ocupación es
cero.

## Adaptive Hedged Requests

La propuesta inicia las mismas diez operaciones primarias que el baseline. Un
servicio que sigue pendiente al superar el umbral se convierte una sola vez en
candidato a rescate. Los candidatos se ordenan por mayor tiempo pendiente; los
empates usan una clave reproducible derivada de la semilla, el request ID y el
servicio.

Cada solicitud puede activar como máximo el presupuesto configurado de
respaldos. Una reserva fallida por falta de capacidad se registra, pero no crea
un intento. Para cada servicio, el primer resultado válido se acepta y cancela
al competidor mediante un contexto propio. El orquestador recoge todos los
intentos cancelados antes de retornar y deja en cero la ocupación primaria y de
rescate.

## Ejecución de experimentos

La prueba funcional usa V2 de forma predeterminada:

```sh
go run . -output run-results/functional
```

Para ejecutar consecutivamente los escenarios predefinidos:

```sh
go run . -scenario all -output run-results/all
```

Los escenarios representan niveles de carga; no arquitecturas:

| Escenario | Solicitudes | Concurrencia | Comparación ejecutada |
|---|---:|---:|---|
| `functional` | 100 | 5 | Baseline y Propuesta V2 |
| `main` | 2000 | 10 | Baseline y Propuesta V2 |
| `load` | 2000 | 30 | Baseline y Propuesta V2 |
| `extended` | 5000 | 10 | Baseline y Propuesta V2 |

`-scenario` admite `functional`, `main`, `load`, `extended`, `custom` y `all`.
El escenario `custom` permite ajustar solicitudes, concurrencia, distribución
de latencias, umbral, workers, capacidad, presupuesto, timeout y semilla con
los flags mostrados por `go run . -help`.

La salida contiene `baseline.csv`, `proposal.csv`, `attempts.csv` y
`summary.csv`. Las duraciones se expresan en nanosegundos y las marcas de tiempo
se escriben en RFC 3339 con la zona `America/Lima`. Para proteger resultados
existentes, la exportación falla si un archivo ya existe; `-overwrite` habilita
su reemplazo explícito.

## Logging visual

El modo normal conserva una salida compacta. Para observar el ciclo de una
solicitud concreta, se puede activar el logging y filtrar por request ID:

```sh
go run . -scenario main -verbose -log-request 5 \
  -output run-results/main-visual -overwrite
```

La traza diferencia Baseline y Propuesta y muestra estos eventos:

- `PRIMARY_DISPATCHED`: envío de una operación a su instancia primaria.
- `HEDGE_THRESHOLD_EXCEEDED`: la operación continúa pendiente después de 150 ms.
- `RESCUE_ASSIGNED`: el rescate obtiene un slot de un worker compartido.
- `RESCUE_SKIPPED`: no se crea el rescate por presupuesto o falta de capacidad.
- `RESULT_ACCEPTED`: respuesta primaria o de rescate aceptada como ganadora.
- `ATTEMPT_CANCELLED`: intento redundante cancelado.
- `REQUEST_COMPLETED`: agregado final con los diez resultados.

`-verbose` sin `-log-request` registra todas las solicitudes y genera una salida
grande en los escenarios de 2000 requests. El filtro solo afecta el logging; no
modifica la carga, las métricas ni los CSV.

## Resultados y recomendación

La versión V2, con umbral de 150 ms, es la configuración final y predeterminada.
En el escenario principal de 2000 solicitudes y concurrencia 10 redujo el p99
de 1462.06 a 181.85 ms (87.56%), con 2.79% más intentos iniciados. Con
concurrencia 30 redujo el p99 de 1478.74 a 489.12 ms (66.92%), limitó el
incremento de intentos a 4.03% y omitió solo 8 rescates por falta de capacidad.

Los CSV finales y la comparación completa están en [`results`](results), con el
análisis resumido en [`results/README.md`](results/README.md).

## Requisitos

- Go 1.25 o compatible.
- No se requieren servicios ni dependencias externas.

## Verificación actual

```sh
go version
gofmt -w *.go
go test ./...
go test -race ./...
go test -count=10 ./...
go run . -scenario main -output run-results/main
go run . -scenario load -output run-results/load
```

Los resultados finales ejecutados con semilla `2026` están disponibles en
[`results`](results). Las nuevas ejecuciones en `run-results/` se ignoran
para evitar subir artefactos locales por accidente.
