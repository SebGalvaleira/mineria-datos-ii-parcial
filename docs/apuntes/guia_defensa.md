# Guía de defensa · Entrega 1

Para cada sección: la idea en una frase, los números que hay que saber y las preguntas probables.

---

## Los 5 números que tienes que saber de memoria

| Número | Qué es | Dónde sale |
|---|---|---|
| **43.200** | Eventos de uso (120 archivos × 360) | notebook 01 |
| **59 días** | Lo que cubre **cada** micro-lote: llegan desordenados | notebook 02, celda 6 |
| **99 %** | Eventos que descartaría un watermark de 2 h | notebook 04 |
| **20 %** | Costo que concentra GenAI con solo el 9,6 % de los eventos | notebook 03 |
| **6,3 %** | Costo que concentran 49 picos (0,1 % de los eventos) | notebook 02 y 03 |

---

## §1 · Problema y objetivos

**En una frase:** un proveedor de nube recibe datos sucios y tiene que ordenarlos para que FinOps, Soporte y Producto tomen decisiones.

**Preguntas probables**
- *¿Por qué esos objetivos?* → Cada uno sale de una pregunta de un usuario y tiene métrica y meta. Por ejemplo, O1 (≤ 15 min) sale de que la consigna pide near real-time para el costo.
- *¿Qué hace "medible" a un objetivo?* → Que tenga un número verificable. "Que ande rápido" no se puede evaluar; "≤ 15 min" sí.
- *¿Para qué numeraste O1 a O8?* → Para trazarlos en la matriz de §6: cada componente dice qué objetivo cumple.

---

## §2 · 5V

**En una frase:** las V que dominan son **velocidad y veracidad**, y son las que definen la arquitectura.

**Preguntas probables**
- *¿Esto es Big Data con 13 MB?* → La muestra no; el problema sí. Un proveedor real con 500 mil recursos genera ~432 M eventos/día (~4 TB/mes). Está marcado como supuesto y la cuenta está en el notebook 03.
- *¿Por qué velocidad es "alta" si llegan 720 eventos por día?* → Por el **desorden**, no por la cantidad: cada lote trae eventos de 60 días, y eso obliga a diseñar el streaming de otra forma.
- *¿Cuál es el "valor"?* → Detectar dónde se va la plata: GenAI es el 20 % del costo con el 9,6 % de los eventos, y 49 picos son el 6,3 % del costo.

---

## §3 · Perfil de fuentes

**En una frase:** 8 fuentes sin huérfanos, pero con 12 tipos de problemas de calidad, cada uno con su tratamiento.

**Los 5 tratamientos:** convertir · deducir · marcar · quarantine · aceptar y documentar.

**Preguntas probables**
- *¿Qué es el grano?* → Qué representa una fila. Billing: una factura por org y mes (80 × 3 = 240).
- *¿Por qué no borras los costos negativos?* → Pueden ser créditos reales. Se marcan con un flag y FinOps decide. Borrarlos sesga el costo.
- *¿Cómo completas `unit` vacía?* → Cada `metric` tiene siempre la misma unidad (cpu_hours → hours), así que se deduce con certeza.
- *¿Qué es un huérfano?* → Una fila que apunta a algo inexistente. No hay ninguno, así que los joins no pierden filas.
- *¿Qué es una decisión abierta?* → Algo que no podemos decidir solos (por ejemplo, si el subtotal en ARS ya está en USD). Es mejor declararlo que inventar.

---

## §4 · Arquitectura

**En una frase:** todo entra a Landing y se divide en dos caminos según la frecuencia: streaming para eventos, batch para maestros y billing.

**El camino de un evento:** JSONL → Landing → streaming → Bronze → Silver → `foreachBatch` → Gold → Cassandra → FinOps.

**Preguntas probables**
- *¿Por qué la factura va por batch?* → No por ser CSV, sino porque cambia una vez por mes y no necesita latencia baja. **El camino se elige por frecuencia y latencia, no por formato.**
- *¿Por qué no usaste ventanas con watermark?* → Porque lo medí: con 2 h se pierde el 99 % de los eventos, porque cada lote trae datos de 60 días (notebook 04).
- *¿Entonces cómo agregas?* → (A) Guardo todo en modo append. (B) Por cada lote recalculo desde Silver solo las claves (org, día, servicio) que cambiaron y hago upsert en Gold. (C) Cada noche, un cierre D+1 recalcula el día completo como control.
- *¿Para qué usas el watermark entonces?* → Solo para limitar la memoria de la deduplicación por `event_id`.
- *¿Qué es el join stream-static?* → Enriquecer cada evento (hecho) con los datos de su organización (dimensión), que vienen del camino batch. Es un modelo estrella.

---

## §5 · Patrón

**En una frase:** híbrido, porque los datos tienen dos ritmos, y evito el defecto de Lambda usando la misma lógica en los dos caminos.

**Preguntas probables**
- *¿Diferencia entre Lambda y Kappa?* → Lambda: dos caminos (rápido y batch que corrige). Kappa: todo es streaming y se recalcula haciendo replay.
- *¿Por qué no Kappa?* → Obligaría a tratar como stream datos que llegan una vez por mes (billing), y la conciliación mensual es naturalmente batch.
- *¿Cuál es el problema clásico de Lambda?* → Escribir la misma lógica dos veces y que diverja. Lo evito: `foreachBatch` y el cierre D+1 llaman a la **misma** función y escriben en las **mismas** tablas.

---

## §6 · Matriz

**En una frase:** cada objetivo de §1 apunta a un componente de §4 y a una forma de comprobarlo.

**Pregunta probable**
- *¿Cómo compruebas O3 (sin pérdida)?* → Contando filas: Landing = Bronze = Silver + Quarantine. Si no cierra, se perdió algo.

---

## §7 · Data Lake

**En una frase:** Landing (original intocable) → Bronze (ordenado) → Silver (limpio) → Gold (resumido para cada usuario), más quarantine.

**Preguntas probables**
- *¿Por qué Parquet?* → Es columnar y comprimido: se leen solo las columnas necesarias. FinOps casi siempre consulta 3 o 4 columnas.
- *¿Por qué particionas por fecha del evento y no de llegada?* → Porque llegan desordenados: lo que se consulta es cuándo ocurrió.
- *¿Por qué no particionas por `org_id`?* → Alta cardinalidad: muchas carpetas con archivos diminutos.
- *¿Por qué los maestros no tienen partición?* → Son tablas chicas (80 orgs); particionarlas generaría archivos diminutos.
- *¿Qué es el problema de archivos chicos?* → Cada micro-lote es de ~107 KB; miles de archivos así hacen lenta la lectura. Se compacta una vez por día.

---

## §8 · Flujos

**Preguntas probables**
- *¿Qué hace el checkpoint?* → Registra qué archivos ya se leyeron. Si el stream se reinicia, no los vuelve a procesar ni duplica.
- *¿Por qué el batch escribe en modo overwrite de la partición?* → Si el job corre dos veces, reemplaza el día en vez de duplicarlo. Es idempotencia en batch.

---

## §9 · MapReduce

**En una frase:** map (evento → clave, valor) · combiner (suma local) · shuffle (misma clave → mismo reducer) · reduce (suma final). Verificado contra pandas: 11.050 filas y 147.433,98 USD en ambos.

**Preguntas probables**
- *¿Cuál es la clave?* → (org_id, fecha del evento, service): es el grano del mart.
- *¿Por qué el combiner redujo solo un 1,5 %?* → Porque cada lote mezcla los 60 días y casi no repite claves. A escala real, con bloques de 128 MB, reduciría mucho más.
- *¿Por qué se puede usar combiner?* → Porque la suma es asociativa: (a + b) + c = a + (b + c).
- *¿Qué es el data skew?* → Que un reducer reciba mucho más que los otros y todos lo esperen. Medí menos del 3 % de diferencia: no hay skew.
- *¿Por qué `crc32 % 4`?* → La misma clave siempre da el mismo número, así que todos sus parciales llegan al mismo reducer.

---

## §10 · Riesgos

**Pregunta probable**
- *¿Cuál es el riesgo principal?* → Los eventos tardíos. Lo comprobé con datos (notebook 04) y todo el diseño del streaming responde a eso.

---

## §11 · Esfuerzo

**Pregunta probable**
- *¿Qué sigue en la entrega 2?* → Implementar en PySpark: ingesta a Bronze, streaming con checkpoint, Silver con quarantine, el mart diario y la carga en AstraDB.
