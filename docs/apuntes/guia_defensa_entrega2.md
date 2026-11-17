# Guía de defensa · Entrega 2

Para estudiar en equipo antes de la muestra del 18/11. Tiene los números que hay que saber, un guion para la demo y las preguntas probables con su respuesta corta. Los conceptos que aparecen están explicados en [`conceptos_entrega2.md`](conceptos_entrega2.md).

> **Lo que pidió el profesor:** *"No agreguen más arquitectura. Demuestren que este diseño funciona."* Toda la defensa gira alrededor de eso: cada afirmación se respalda con un número o una prueba que corrió.
>
> **El medio punto que faltó en la entrega 1** dependía de que **los cuatro** pudieran explicar las decisiones. Cada integrante tiene que poder responder cualquier pregunta de esta guía, no solo la de su parte.

---

## 1. Los números que hay que saber de memoria

| Número | Qué es | Dónde se ve |
|---|---|---|
| **43.200** | Eventos únicos en Bronze, Silver y Gold (120 archivos × 360) | `E2_02`, `E2_03`, `E2_04` |
| **15 lotes** | Micro-lotes del stream (0 a 14). Los lotes **4, 9 y 14 no traen datos**: solo avanzan el watermark | `E2_02` |
| **3.600** | Eventos del lote 5 que se reprocesaron después de la falla: `nuevos 0 · ya_existian 3600` | `E2_02` |
| **211** | Filas válidas de `resources.csv` que iban a quarantine por las comillas. También son los 211 costos negativos marcados en Silver (casualidad: son cosas distintas) | `E2_01`, `E2_03` |
| **83,59 USD** | Umbral de pico: 5 × p99 (16,72 USD). 49 eventos lo superan | `E2_03` |
| **11.050 filas · 147.433,98 USD** | Gold, idéntico al MapReduce de la entrega 1 | `E2_04` |
| **+1,00 · +12,3456 · +7,00 · 0** | Cuánto cambió Gold en las pruebas 1, 2, 3 y 4 | `E2_05` |
| **147 de 186** | Partición más grande de Cassandra Q1 contra su tope teórico (31 días × 6 servicios) | `E2_06` |
| **419,1986 USD** | Q1 en Astra (`org_53lc58dr`, 1 al 7/08, 29 filas) = Gold | `E2_06`, captura |
| **49/49 + 33** | El método MAD encuentra todos los picos de la regla y 33 más; 28 de esas 33 son de storage y networking | `E2_07` |

---

## 2. Guion de la demo (10 a 12 minutos)

**Regla de oro: no correr el pipeline en vivo.** El stream y las restauraciones de Drive tardan minutos y dependen de Colab. Se muestran los notebooks con sus salidas guardadas, y lo único en vivo son las consultas en Astra, que tardan segundos.

| Bloque | Quién | Qué mostrar | Frase clave |
|---|---|---|---|
| **1. Contexto y correcciones** (2 min) | Integrante A | README → diagrama v2 → tabla del plan de correcciones | "El feedback pedía demostrar que el diseño funciona. Estas son las pruebas que lo demuestran, y el diagrama muestra solo lo que está implementado." |
| **2. Ingesta** (3 min) | Integrante B | `E2_01`: tabla con `cuadra = True` e idempotencia. `E2_02`: log de lotes, la falla del lote 5 y la recuperación | "El lote 5 se cayó después de escribir en Bronze. Al reiniciar con el mismo checkpoint, el anti-join encontró los 3.600 eventos como `ya_existian` y no duplicó ninguno." |
| **3. Silver, Gold y pruebas** (3 min) | Integrante C | `E2_03`: casos inyectados en quarantine. `E2_04`: assert contra el MapReduce. `E2_05`: tabla con las 4 pruebas OK | "Cada prueba la frena una barrera distinta. La cuarta salta todas las anteriores a propósito, y Gold igual no cambia." |
| **4. Serving y analítica** (3 min) | Integrante D | **En vivo en el CQL Console:** Q1, Q2 y la consulta rechazada. `E2_07`: tabla de métodos y el gráfico de networking | "Cassandra solo responde las preguntas para las que diseñamos la tabla. Un umbral único en USD es ciego a los servicios baratos: este pico de networking es 18 veces lo normal y la regla no lo ve." |

**Antes de la clase:**
- Entrar a Astra y verificar que la base esté **Active**. Si dice *Hibernated*, tocar *Resume*; tarda un par de minutos.
- Tener abiertos en pestañas: el README en GitHub, `E2_02`, `E2_05`, `E2_07` y el CQL Console con las consultas de `cql/02_consultas.cql` copiadas.
- **Plan B si falla internet o Astra:** las capturas están en `evidence/e2/capturas/`.

---

## 3. Las 3 pruebas del feedback (+1)

| Prueba | Qué pasa | Qué la frena | Resultado |
|---|---|---|---|
| **1. Duplicado** | Llegan 3 eventos ya cargados y 1 nuevo repetido 2 veces | **Barrera 2** (`dropDuplicatesWithinWatermark`): todo llegó dentro de la ventana de 1 h, así que al lote entró **1 sola fila** | Gold: 1 fila cambia, +1 evento, +1,00 USD |
| **2. Tardío** | Un evento del 03/07 llega último, 59 días tarde | **Nada lo descarta**: el watermark es sobre la llegada, no sobre la fecha del evento. Gold recalcula solo el 03/07 | +12,3456 USD exactos; `updated_at` de los otros 59 días sin cambios |
| **3. Fallo** | El job se cae **después** de escribir en Bronze | **Checkpoint**: el lote no quedó confirmado y se repite. **Barrera 3** (anti-join): `ya_existian 1` | Primer intento: Bronze sí, Gold no. Segundo intento: Gold +7,00 una sola vez |
| **4. Duplicado en Silver** | Se inyecta a mano una copia en Silver (se saltan todas las barreras) | **Barrera 4**: `dropDuplicates(['event_id'])` en Gold antes de agregar | Silver tiene 2 copias; Gold no cambia |

**La corrección que salió de la prueba 3:** en el reintento el evento ya está en Bronze, así que `nuevos = 0`. Si las fechas a recalcular salieran solo de los eventos nuevos, Silver y Gold **nunca** se habrían actualizado. Por eso salen de **todos** los eventos válidos del lote (decisión B06).

---

## 4. Preguntas probables

### Spark y entorno
- **¿Por qué Spark 4 y no 3.5?** → Colab trae Python 3.13 y Java 21. Spark 3.5 no soporta Python 3.13 y no arrancaba (`JAVA_GATEWAY_EXITED`). Spark 4.0 soporta los dos y además trae `dropDuplicatesWithinWatermark`.
- **¿Por qué trabajan en el disco local de Colab y no directo en Drive?** → Drive tiene consistencia eventual: el archivo aparece antes de que termine de subirse su contenido. El stream leyó Parquet de 0 bytes. En local las escrituras son atómicas, y al final se sincroniza a Drive con `rsync`.
- **¿Por qué "un notebook a la vez"?** → Nos pasó: `E2_04` restauró Silver mientras `E2_03` todavía sincronizaba, y leyó 17 días en lugar de 60. El assert contra el MapReduce lo detectó.

### Ingesta
- **¿Por qué esquema explícito y no `inferSchema`?** → `inferSchema` adivina. Si un archivo trae `value` como texto, cambia el tipo de toda la columna. Con esquema explícito, lo que no cumple va a `_corrupt_record` y se manda a quarantine.
- **¿Qué es `PERMISSIVE`?** → Spark no se cae ante una fila mala: la carga con nulos y guarda el texto original en `_corrupt_record`. Así no se pierde y se puede auditar.
- **¿Cómo saben que no perdieron filas?** → Cada tabla concilia `landing = bronze + inválidos + duplicados` (columna `cuadra`), y Silver concilia `bronze = silver + quarantine + duplicados`.
- **¿Por qué `availableNow` si el caso es streaming?** → Procesa lo que hay y termina, así la demo es reproducible y la falla se puede inyectar en un lote concreto. En producción solo cambia el `trigger` a `processingTime='1 minute'`; el resto del código es el mismo.

### Deduplicación y watermark (la advertencia del profesor)
- **¿El watermark acota el estado de la deduplicación?** → Sí, con `dropDuplicatesWithinWatermark`: Spark recuerda cada `event_id` hasta que el watermark (llegada máxima − 1 h) pasa su `ingest_ts`, y después lo borra. Los lotes 4, 9 y 14 no traen datos y sirven para eso: avanzan el watermark y limpian el estado.
- **¿Y `dropDuplicates` común con watermark?** → Esa es la trampa que marcó el profesor. Con la clave solo en `event_id` y sin la columna del watermark, `dropDuplicates` **no** borra estado nunca: crece sin límite. Por eso usamos `dropDuplicatesWithinWatermark`.
- **¿Por qué el watermark es sobre `ingest_ts` y no sobre `timestamp`?** → Cada lote trae eventos de 60 días. Con watermark sobre el tiempo del evento, o se descartan casi todos (99 % con 2 h, entrega 1) o hay que guardar 60 días de estado. Sobre la llegada, el estado dura 1 h y alcanza para los reintentos.
- **¿Y si un duplicado llega dos días después?** → Pasa la barrera 2, porque el estado ya lo olvidó. Lo frena la barrera 3: el anti-join contra Bronze. Y si igual llegara a Silver, lo frena la 4 en Gold (prueba 4). **La garantía no depende del estado del stream.**
- **¿`foreachBatch` es exactly-once?** → No, es *at-least-once*: si falla, el lote se repite. Lo que lo hace efectivamente exactly-once es que todo lo que escribe es idempotente: anti-join en Bronze, overwrite por partición en Silver y Gold, upsert en Cassandra.
- **¿El anti-join escala?** → Hoy compara contra todo Bronze. A escala se limita a las particiones `event_date` que trae el lote, porque Bronze está particionado por esa columna. Es una mejora conocida.

### Silver y calidad
- **¿Por qué quarantine quedó en 0 con datos reales? ¿Las reglas sirven?** → Los datos tienen problemas, pero se pueden tratar: 1.309 valores llegan como texto numérico y se convierten, 2.075 unidades se deducen, los negativos y picos se marcan. Ninguno es **inválido**. Las reglas se probaron con 7 casos inyectados: 4 fueron a quarantine con su motivo y 2 se marcaron.
- **¿Regla dura vs. blanda?** → Dura: el dato no se puede usar (sin `event_id`, `value = 'abc'`, org inexistente) → quarantine. Blanda: el dato es sospechoso pero puede ser real (costo negativo = crédito, pico) → flag. FinOps decide.
- **¿Por qué el umbral es −0,01 y no 0?** → Lo fija la consigna: tolera redondeos. De 216 negativos, 5 están entre −0,01 y 0 y no se marcan.
- **¿Por qué `try_cast`?** → Si el texto no es número, devuelve nulo en lugar de romper el job. Ese nulo dispara la regla `value_no_numerico`.
- **¿Por qué broadcast join?** → Las dimensiones (80 orgs, 400 recursos) son chicas: se copian enteras a cada ejecutor y se evita mover los 43.200 eventos por la red (shuffle).
- **¿Por qué el último snapshot de los maestros?** → Bronze guarda una foto por día de ingesta. Si se juntaran dos fotos, cada org aparecería dos veces y el join duplicaría eventos.

### Gold
- **¿Cómo validaron Gold?** → Contra el MapReduce de la entrega 1, que es un cálculo independiente en Python puro: mismas claves, mismo costo, mismos eventos.
- **¿Cómo manejan el dato tardío en Gold?** → No suman incrementos: recalculan la **fecha completa** desde Silver y sobrescriben solo esa partición (`partitionOverwriteMode = dynamic`). Con overwrite estático, se borrarían las otras 59 fechas.
- **¿Para qué el `repartition('event_date')`?** → Para que cada día quede en 1 archivo: 60 archivos en total. Es la compactación del mart.

### Cassandra / AstraDB
- **¿Partition key vs. clustering key?** → La partition key decide **en qué nodo** vive la fila y es obligatoria en el `WHERE`. La clustering key decide **el orden dentro de la partición** y permite rangos.
- **¿Por qué `(org_id, usage_month)` y no solo `org_id`?** → Para acotar la partición: como máximo 186 filas, aunque pasen años. Solo con `org_id` crecería para siempre.
- **¿Y si quiero un rango que cruza meses?** → Una consulta por mes, que la aplicación junta. Los reportes de FinOps son mensuales, así que es raro.
- **¿Por qué `usage_date DESC`?** → Lo más reciente sale primero y el rango de fechas es una lectura secuencial en disco.
- **¿Qué consistencia usan?** → `LOCAL_QUORUM`: la mayoría de las réplicas de la región confirma. Astra la exige para escribir, y al leer garantiza ver lo último escrito.
- **¿Replicación?** → La maneja Astra: replica en varias zonas de disponibilidad de la región. No se declara en el CQL.
- **¿Por qué rechazó la consulta por `service`?** → Porque `service` es clustering y viene después de `usage_date`: sin la partición y la fecha, Cassandra tendría que recorrer todo el cluster. `ALLOW FILTERING` no lo arregla: hace ese mismo recorrido. Si hiciera falta, se crea otra tabla con esa clave.
- **¿Por qué recargar no duplica?** → En Cassandra `INSERT` es upsert: si la clave existe, sobrescribe. Lo mostramos cargando dos veces: 11.050 → 11.050.
- **¿Por qué en Q2 borran y reinsertan el día?** → Si al recalcular una anomalía desaparece, un upsert no la borraría. Es lo mismo que el overwrite por partición de Gold.
- **¿Qué es un tombstone y por qué `UNSET_VALUE`?** → Cassandra no borra: escribe una marca de borrado (tombstone), y escribir `null` también genera una. Muchas tombstones hacen lentas las lecturas. Con `UNSET_VALUE`, la celda directamente no se escribe.
- **¿Por qué no el conector Spark-Cassandra?** → No soporta Spark 4. Usamos el driver de Python con escrituras concurrentes; en producción la misma función se llama desde `foreachBatch`.

### Analítica
- **¿Por qué no ML?** → No hay anomalías etiquetadas y la historia es de 60 días. Un modelo supervisado no tiene con qué entrenar, y la consigna pide no forzar ML. MAD es explicable ("18 veces su mediana") y no necesita etiquetas.
- **¿Por qué MAD y no z-score?** → El z-score usa media y desvío, y los propios picos los inflan: el pico se "esconde" (enmascaramiento). La mediana y el MAD casi no se mueven con los picos.
- **¿Por qué el umbral 5?** → Por la tabla de sensibilidad: con 3,5 aparecen 75 "nuevas" (ruido); entre 5 y 7 se estabiliza; con 10 se pierde un pico real.
- **¿Por qué por serie org × servicio?** → Cada servicio y cada cliente vive en su propia escala. 27 USD en networking es un pico enorme para quien gasta 1,5 por día, y nada para compute.
- **¿Cuál es la limitación?** → La mediana usa los 60 días, incluidos los posteriores al día evaluado. Sirve para auditoría D+1, no para alertas en vivo. En el backlog: ventana móvil de 28 días hacia atrás.

### Gobierno
- **¿Cómo rastrean una cifra de Gold hasta su origen?** → Se filtra Silver por org, día y servicio, y la columna `source_file` dice de qué archivo vino cada evento.
- **¿Dónde está el token?** → En Colab Secrets. El bundle está en Drive (`cpa_secrets/`). Ninguno de los dos está en el repo; `.gitignore` bloquea `secure-connect-*.zip`.
- **¿Hay PII?** → `users.email`, en Bronze. No se propaga a Silver, Gold ni Astra. Enmascararlo está en el backlog como obligatorio.

---

## 5. Lo que no hicimos (y cómo decirlo)

Si preguntan, se responde **directo y con el motivo**. Está documentado en `plan_correcciones.md` §4 y en el backlog.

| Pregunta | Respuesta |
|---|---|
| ¿Y los marts de Soporte y Producto? | La consigna exige el de FinOps para esta entrega. Soporte y Producto son M1–M3 del backlog para el final. |
| ¿Y Airflow? | Hoy los notebooks corren en orden con una regla documentada. Un `run_all` u orquestador está en el backlog (S3). |
| ¿Silver y Gold corren dentro del stream? | En el pipeline principal corren como cierre batch. La cadena completa por micro-lote se demuestra en el sandbox de `E2_05`, con la misma lógica. Integrarla es M7. |
| ¿Billing se usa? | Está en Bronze. La conversión de moneda depende de D1, que no está confirmada. Es M6. |
| ¿Funciones repetidas entre notebooks? | Sí, a propósito para que cada notebook sea autocontenido. Pasarlas a `src/` es M8. |
