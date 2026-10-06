# Conceptos explicados

Cada concepto: **qué es**, **analogía** y **en nuestro proyecto**. Las zonas del Data Lake, Parquet y particiones están en [`data_lake.md`](data_lake.md).

## Índice

1. [Big Data y las 5V](#1-big-data-y-las-5v)
2. [Batch vs. streaming](#2-batch-vs-streaming)
3. [ETL vs. ELT](#3-etl-vs-elt)
4. [Grano, clave y cardinalidad](#4-grano-clave-y-cardinalidad)
5. [Evolución de esquema](#5-evolución-de-esquema)
6. [Tiempo de evento vs. tiempo de llegada](#6-tiempo-de-evento-vs-tiempo-de-llegada)
7. [Watermark](#7-watermark)
8. [Checkpoint](#8-checkpoint)
9. [Idempotencia, append y upsert](#9-idempotencia-append-y-upsert)
10. [Lambda, Kappa e híbrido](#10-lambda-kappa-e-híbrido)
11. [Modelo estrella: hechos y dimensiones](#11-modelo-estrella-hechos-y-dimensiones)
12. [Join stream-static](#12-join-stream-static)
13. [MapReduce](#13-mapreduce)
14. [Data skew](#14-data-skew)
15. [Archivos chicos y compactación](#15-archivos-chicos-y-compactación)
16. [Calidad de datos y quarantine](#16-calidad-de-datos-y-quarantine)
17. [Linaje y trazabilidad](#17-linaje-y-trazabilidad)
18. [PII y enmascarado](#18-pii-y-enmascarado)
19. [Cassandra y el modelado query-first](#19-cassandra-y-el-modelado-query-first)
20. [Percentiles y detección de anomalías](#20-percentiles-y-detección-de-anomalías)
21. [Métricas de negocio: SLA, CSAT, NPS](#21-métricas-de-negocio-sla-csat-nps)
22. [Orquestación](#22-orquestación)

---

## 1. Big Data y las 5V

**Qué es:** datos que, por alguna de sus características, **no se pueden manejar con herramientas tradicionales** (una sola computadora, Excel, una base de datos común). Las 5V describen esas características:

| V | Pregunta | En nuestro proyecto |
|---|---|---|
| **Volumen** | ¿Cuánto? | Muestra de 12,9 MB; escala real proyectada ~4 TB/mes |
| **Velocidad** | ¿Qué tan rápido llega y cuánto tarda en servir? | Micro-lotes cada minuto, **desordenados** |
| **Variedad** | ¿Cuántos formatos y estructuras? | CSV, JSONL, 2 versiones de esquema, 3 monedas |
| **Veracidad** | ¿Qué tan confiable es? | Nulos, texto en vez de números, negativos, NPS fuera de escala |
| **Valor** | ¿Para qué sirve? | Detectar sobrecostos; GenAI es el 20 % del costo |

**Analogía:** un kiosco anota las ventas en un cuaderno. Una cadena de 10.000 kioscos que vende cada segundo, con cajas de distintas marcas y algunas que anotan mal, ya no puede usar un cuaderno.

**La clave para el profesor:** las 5V **no se recitan**; se usan para **justificar decisiones**. Cada V de la tabla de §2 termina en una decisión de diseño.

---

## 2. Batch vs. streaming

| | **Batch** | **Streaming** |
|---|---|---|
| Qué es | Procesar un **bloque** de datos de una vez, en un horario fijo | Procesar los datos **a medida que llegan**, en lotes chicos |
| Latencia | Horas o días | Segundos o minutos |
| Analogía | El cartero que reparte una vez por día | WhatsApp: el mensaje llega al instante |
| Cuándo conviene | Datos que cambian poco o donde no urge la respuesta | Datos continuos que se necesitan ya |
| En el proyecto | Maestros (02:00 diario), billing (día 1 del mes) | Eventos de uso (cada 1 minuto) |

**Micro-lote (*micro-batch*):** Spark Structured Streaming no procesa evento por evento, sino en **lotes muy chicos** cada pocos segundos o minutos. En el dataset, cada archivo `events_part_XXXX.jsonl` simula un micro-lote de 360 eventos.

**Regla:** el camino se elige por **frecuencia y latencia requerida**, no por formato del archivo.

---

## 3. ETL vs. ELT

| | **ETL** | **ELT** |
|---|---|---|
| Orden | **E**xtraer → **T**ransformar → **C**argar (*Load*) | Extraer → **Cargar** → Transformar |
| Qué se guarda | Solo el dato ya transformado | **Primero el crudo**, después se transforma |
| Analogía | Lavar y cortar la verdura en el mercado y llevar a casa solo lo cortado | Llevar la verdura entera a casa, guardarla y cortarla cuando haga falta |
| Ventaja | Ocupa menos espacio | Si una regla estaba mal, **se reprocesa desde el crudo** |

**En nuestro proyecto:** es **ELT**. Landing y Bronze guardan el dato tal como llegó, y Silver y Gold lo transforman. Si mañana descubrimos que la regla de NPS estaba mal, se recalcula desde Bronze sin pedir los datos otra vez.

---

## 4. Grano, clave y cardinalidad

**Grano:** qué representa **una fila** de una tabla.
- `billing_monthly`: una factura de **una org en un mes** (80 × 3 = 240 filas).
- `usage_events`: **un evento** de uso.
- `org_daily_usage_by_service`: **una org en un día en un servicio**.

**Analogía:** en un ticket de supermercado, ¿una fila es un producto o una compra entera? Si mezclas granos, sumas peras con manzanas.

**Clave:** la columna (o combinación de columnas) que identifica **cada fila de forma única**: `event_id`, `invoice_id`, `(org_id, fecha, service)`.

**Cardinalidad:** cuántos valores **distintos** tiene una columna.
- **Baja:** `service` (6 valores), `region` (7). Sirven para particionar.
- **Alta:** `event_id` (43.200), `org_id` (80 hoy, miles en producción). **No** sirven para particionar: generarían demasiadas carpetas.

---

## 5. Evolución de esquema

**Qué es:** el **esquema** es la lista de columnas y sus tipos. **Evoluciona** cuando la fuente agrega, quita o cambia columnas con el tiempo.

**Analogía:** un formulario en papel al que a mitad de año le agregan un casillero nuevo. Los formularios viejos no lo tienen y no por eso están mal.

**En nuestro proyecto:** desde el 18/07/2025 los eventos pasan a la **v2**, que agrega `carbon_kg` (todos) y `genai_tokens` (solo genai).

**Cómo se resuelve:** en Silver se arma un **esquema unificado** con todas las columnas. En los eventos v1, las columnas nuevas quedan en `null`. Se usa un **esquema explícito** (declarado por nosotros), no inferido: si aparece una v3 con columnas desconocidas, se detecta y se alerta en vez de fallar en silencio.

---

## 6. Tiempo de evento vs. tiempo de llegada

| | **Tiempo de evento** (*event time*) | **Tiempo de llegada** (*processing / ingest time*) |
|---|---|---|
| Qué es | Cuándo **ocurrió** | Cuándo **lo recibimos** |
| Columna | `timestamp` | `ingest_ts` |
| Analogía | La fecha del matasellos de una carta | El día que la carta llega a tu buzón |

**En nuestro proyecto:** la diferencia es enorme. **Cada archivo trae eventos de los 60 días**, así que un evento del 03/07 puede llegar junto con uno del 31/08.

**Consecuencia:** todo lo que importa al negocio (costo **del día**) se organiza por **tiempo de evento**. Por eso particionamos por `event_date`.

---

## 7. Watermark

**Qué es:** el límite que usa Spark Streaming para decidir **cuándo un evento llegó "demasiado tarde"**.

> **watermark = hora del evento más nuevo visto − margen tolerado**
> Todo evento anterior al watermark se descarta.

**Para qué existe:** si Spark suma ventas **por día** dentro del stream, necesita saber cuándo "cerrar" un día y liberar memoria. Sin watermark esperaría para siempre.

**Analogía:** el kiosco cierra la caja del lunes a las 2 de la mañana del martes. Una venta del lunes que aparece el miércoles ya no entra.

**En nuestro proyecto (notebook 04):**

| Margen | 2 h | 1 día | 7 días | 30 días | 60 días |
|---|---|---|---|---|---|
| Descartado | 99 % | 97,5 % | 87,6 % | 49,6 % | 0 % |

**Conclusión:** no se agrega por ventanas dentro del stream. El watermark se usa **solo para limitar la memoria de la deduplicación** por `event_id`.

---

## 8. Checkpoint

**Qué es:** una carpeta donde el stream anota **qué archivos ya procesó** y en qué estado quedó.

**Analogía:** el señalador de un libro. Si cierras el libro (o se corta la luz), lo abres donde te quedaste y no vuelves a leer desde la página 1.

**En nuestro proyecto:** si Colab corta la sesión y el stream se reinicia, el checkpoint evita volver a leer los 120 archivos y **duplicar** todos los costos. **Si se borra el checkpoint, el stream reprocesa todo desde cero.**

---

## 9. Idempotencia, append y upsert

**Idempotencia:** procesar lo mismo **una o varias veces da el mismo resultado**.

**Analogía:** el botón del ascensor. Apretarlo una vez o diez veces lo llama igual. Lo contrario es una máquina expendedora: si apretás dos veces, te cobra dos veces.

**Por qué importa:** en sistemas reales los procesos fallan y se reintentan. Si el reintento duplica los costos, FinOps ve el doble.

| Modo de escritura | Qué hace | Idempotente | Uso en el proyecto |
|---|---|---|---|
| **Append** | Solo **agrega** filas al final | No por sí solo (necesita dedupe) | Bronze y Silver de eventos, más dedupe por `event_id` |
| **Overwrite de partición** | **Reemplaza** una partición completa | Sí | Batch diario: reemplaza el día |
| **Upsert** (*update + insert*) | Si la clave existe la **reemplaza**; si no, la inserta | Sí | Gold y Cassandra: recalcular claves (org, día, servicio) |

---

## 10. Lambda, Kappa e híbrido

| Patrón | Idea | Analogía | Problema típico |
|---|---|---|---|
| **Lambda** | Dos caminos: uno **rápido** (streaming, aproximado) y uno **batch** que recalcula y corrige | El diario publica en la web al instante y la versión revisada en papel al día siguiente | Hay que escribir la **misma lógica dos veces** y puede divergir |
| **Kappa** | Un solo camino: **todo es streaming**; para recalcular se hace *replay* desde el inicio | Solo web: si hubo un error, se republica todo desde el archivo histórico | Obliga a tratar como stream datos que llegan una vez por mes |
| **Híbrido** | Combinación justificada | — | Requiere disciplina para no duplicar lógica |

**En nuestro proyecto:** híbrido. Los eventos van por streaming, los maestros y billing por batch, y un cierre D+1 recalcula. Se parece a Lambda, **pero el camino rápido y el cierre usan la misma función y escriben en las mismas tablas**: no hay dos lógicas que puedan divergir.

---

## 11. Modelo estrella: hechos y dimensiones

**Qué es:** una forma de organizar datos analíticos con una tabla central de **hechos** rodeada de tablas de **dimensiones**.

| | **Hechos** | **Dimensiones** |
|---|---|---|
| Qué guardan | **Qué pasó** y **cuánto**: medidas numéricas | **Quién, dónde, qué**: contexto descriptivo |
| Tamaño | Muchísimas filas | Pocas filas |
| Cambian | Todo el tiempo | Poco |
| En el proyecto | `usage_events` (costo, requests…) | `customers_orgs`, `resources`, `users` |

**Analogía:** el ticket de compra es el **hecho** (compraste 2 kg de pan a $3000). El catálogo de productos y la ficha del cliente son las **dimensiones**.

Se llama "estrella" porque al dibujarlo los hechos quedan en el centro y las dimensiones alrededor, como puntas.

---

## 12. Join stream-static

**Qué es:** cruzar un **stream** (datos que llegan todo el tiempo) con una **tabla estática** (que cambia poco).

**Analogía:** el cajero que escanea productos (stream) y consulta la lista de precios (estática), que se actualiza una vez por día.

**En nuestro proyecto:** cada evento que llega por streaming se enriquece con los datos de su organización (industria, plan, región) y de su recurso, que vienen del camino batch. Es la flecha punteada "join stream-static" del diagrama. Es posible porque **no hay huérfanos**: todo `org_id` y `resource_id` existe en las dimensiones.

---

## 13. MapReduce

**Qué es:** un modelo para **dividir un cálculo gigante entre muchas máquinas** que trabajan en paralelo.

**Analogía:** el escrutinio de una elección nacional.

| Fase | Elección | Nuestro proyecto |
|---|---|---|
| **Split** | Los votos están en miles de urnas | Los eventos están en 120 archivos (bloques) |
| **Map** | Cada mesa anota "partido X: 1" por cada voto | Cada evento → `(org, día, servicio) → {costo, métricas}` |
| **Combiner** | Cada mesa suma sus propios votos antes de enviarlos | Cada archivo suma sus pares localmente |
| **Shuffle** | Todos los resultados del partido X van al mismo centro | Todos los pares con la misma clave van al mismo reducer (`crc32 % 4`) |
| **Reduce** | El centro suma los parciales | El reducer suma el total de cada clave |

**Combiner:** es una "mini reducción" local. Solo se puede usar si la operación es **asociativa**, como la suma: (a + b) + c = a + (b + c). En la muestra redujo solo un 1,5 % porque cada archivo mezcla los 60 días y casi no repite claves; a escala real reduce mucho.

**Relación con Spark:** un `groupBy(...).agg(sum(...))` de Spark (o de pandas) hace exactamente esto por dentro. En el notebook 05 los dos dieron 11.050 filas y 147.433,98 USD.

---

## 14. Data skew

**Qué es:** cuando, en el shuffle, **un reducer recibe muchos más datos que los demás**.

**Analogía:** un supermercado con 4 cajas donde todos hacen la fila en la misma. Las otras 3 cajeras esperan sin hacer nada y el súper es tan lento como esa única fila.

**Causa típica:** una clave muy frecuente, por ejemplo un cliente gigante que genera la mitad de los eventos.

**En nuestro proyecto:** los 4 reducers recibieron entre 10.535 y 10.857 pares (< 3 % de diferencia): **no hay skew**, porque la clave combina tres columnas y reparte bien.

---

## 15. Archivos chicos y compactación

**Qué es el problema:** cada vez que el stream escribe, crea archivos nuevos. Muchos archivos diminutos hacen lenta la lectura, porque abrir cada archivo tiene un costo fijo.

**Analogía:** leer un libro de 1.000 páginas contra leer 1.000 hojas sueltas guardadas en 1.000 sobres distintos.

**En nuestro proyecto:** cada micro-lote pesa ~107 KB. Solución:
- **`coalesce`** al escribir, para generar menos archivos por lote.
- **Compactación diaria** (en el cierre D+1): se juntan los archivos de cada partición en archivos de 128–256 MB.

---

## 16. Calidad de datos y quarantine

**Qué es:** reglas verificables que decide qué hacer con cada dato sospechoso.

| Tratamiento | Cuándo | Ejemplo |
|---|---|---|
| **Convertir** | Dato correcto, tipo equivocado | `"8.94"` → 8,94 |
| **Deducir** | Falta, pero se calcula con certeza | `unit` desde `metric` |
| **Marcar (flag)** | Sospechoso, pero puede ser real | Costo negativo |
| **Quarantine** | Inválido, no se puede usar | CSAT = 7 en escala 1–5 |
| **Aceptar y documentar** | Raro, pero tiene explicación | Ticket abierto |

**Quarantine:** una tabla aparte donde van los registros rechazados **con el motivo**. **Analogía:** la aduana. El paquete sospechoso no se tira: se aparta, se etiqueta y alguien lo revisa.

**Regla de oro:** nunca se borra un dato en silencio.

---

## 17. Linaje y trazabilidad

**Qué es:** poder responder **"¿de dónde salió este número?"** recorriendo el camino hacia atrás.

**Analogía:** el código de lote en un envase de leche. Si hay un problema, se rastrea hasta la planta y el día de producción.

**En nuestro proyecto:** cada fila de Bronze guarda `source_file` (de qué archivo vino), `ingest_ts` (cuándo entró) y `batch_id` (en qué ejecución). Así se puede ir de Gold → Silver → Bronze → el archivo exacto de Landing.

**Conciliación:** Landing = Bronze = Silver + Quarantine. Si no cierra, algo se perdió.

---

## 18. PII y enmascarado

**PII** (*Personally Identifiable Information*): datos que identifican a una persona, como email, nombre o teléfono.

**Enmascarado:** reemplazarlos por algo que no identifique. Por ejemplo, un *hash*: `user_9tze5r5u@example.com` → `a3f9…`. Sigue sirviendo para contar usuarios distintos, pero no revela quién es.

**En nuestro proyecto:** `users.email` y los recursos con tag `pii:true` se enmascaran **antes de Gold**, porque Gold y Cassandra los consultan muchos usuarios.

---

## 19. Cassandra y el modelado query-first

**Qué es Cassandra:** una base de datos distribuida pensada para **leer y escribir muy rápido a gran escala**. AstraDB es Cassandra administrada en la nube.

**La gran diferencia con una base relacional:** **no hay joins**. Cada tabla se diseña para responder **una consulta específica**: eso es *query-first*. Primero se define la pregunta y después la tabla.

**Analogía:** una biblioteca relacional tiene un solo catálogo y buscas cruzando fichas. Cassandra tiene **un estante armado para cada pregunta frecuente**: "libros de Borges por año" tiene su propio estante, ya ordenado.

**Conceptos clave:**
- **Partition key:** decide **en qué máquina** vive el dato. Todo lo que se consulta junto debe compartirla.
- **Clustering key:** decide **el orden** dentro de la partición.

**Ejemplo para la entrega 2** (consulta: "costos diarios de una org en un rango de fechas"):
```sql
CREATE TABLE org_daily_usage_by_service (
    org_id     text,
    usage_date date,
    service    text,
    costo_usd  double,
    requests   double,
    PRIMARY KEY ((org_id), usage_date, service)
);
-- partition key: org_id      → todos los datos de una org juntos
-- clustering:    usage_date  → ordenados por fecha, ideal para rangos
```

---

## 20. Percentiles y detección de anomalías

**Percentil 99 (p99):** el valor por debajo del cual está el **99 %** de los datos.

**Analogía:** si en una clase de 100 alumnos el p99 de altura es 1,90 m, solo 1 alumno mide más.

**En nuestro proyecto:** el p99 del costo por evento es 16,72 USD y la mediana es 1,00. Definimos **pico = más de 5 × p99** (> 83,6 USD): 49 eventos.

**Por qué percentiles y no el promedio:** el promedio se distorsiona con los propios picos. La mediana y los percentiles son **robustos**.

**Otros métodos (para la entrega 2):**
- **z-score:** cuántas desviaciones estándar se aleja un valor del promedio. Sensible a los mismos picos que busca.
- **MAD** (desviación absoluta mediana): como el z-score, pero con la mediana. Más robusto.

---

## 21. Métricas de negocio: SLA, CSAT, NPS

| Métrica | Qué mide | Escala | En el dataset |
|---|---|---|---|
| **SLA** (*Service Level Agreement*) | Si se cumplió el tiempo de respuesta prometido al cliente | Cumplido / incumplido | 9,5 % incumplido |
| **CSAT** (*Customer Satisfaction*) | Satisfacción con **una interacción** puntual (un ticket) | 1 a 5 | 29 valores fuera de escala (6 y 7) |
| **NPS** (*Net Promoter Score*) | "¿Nos recomendarías?" | Individual: 0 a 10 · Agregado de cuenta: −100 a 100 | Valores de −38 a 101: escala ambigua (decisión abierta D2) |

**Cómo se calcula el NPS agregado:** % de promotores (9–10) − % de detractores (0–6). Por eso puede ir de −100 a 100, y por eso un valor de −38 podría ser un NPS de cuenta y no un error.

---

## 22. Orquestación

**Qué es:** la herramienta que decide **qué job corre, cuándo y en qué orden**, y qué hacer si falla.

**Analogía:** el director de una orquesta. Cada músico sabe tocar, pero alguien tiene que decir cuándo entra cada uno.

**En nuestro proyecto:**
- **Airflow** (referencia de producción): define flujos como gráficos de tareas (*DAGs*): "a las 02:00 cargar maestros → después calidad → después Gold → después cargar Cassandra". Si un paso falla, reintenta o alerta.
- **Cron / notebook** (en Colab): una versión simple que ejecuta los jobs en orden.
- El **streaming no se orquesta por horario**: corre permanentemente y procesa cada micro-lote apenas llega.
