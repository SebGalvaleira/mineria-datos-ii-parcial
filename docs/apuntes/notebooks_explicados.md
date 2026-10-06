# Notebooks explicados

## Qué hace cada uno

| Notebook | Pregunta que responde | Resultado clave |
|---|---|---|
| 01 · Mirar los datos | ¿Qué hay en el dataset? | 7 CSV + 120 JSONL; grano de cada tabla; la v2 agrega `carbon_kg` y `genai_tokens` |
| 02 · Perfil de calidad | ¿Cuántos problemas hay? | 12 tipos de problema contados; cada archivo cubre 59 días; 0 huérfanos |
| 03 · Cifras 5V | ¿Con qué números justifico las 5V? | 432 M eventos/día proyectados; GenAI 20 % del costo; 9,5 % SLA incumplido |
| 04 · Watermark | ¿Sirve un watermark con estos datos? | 2 h → 99 % descartado; 60 días → 0 % |
| 05 · MapReduce | ¿Cómo se calcula el mart diario en paralelo? | 11.050 filas; coincide con pandas |

## Trucos de código que usamos

| Código | Qué hace | Ejemplo |
|---|---|---|
| `Path('../data/...')` | Ruta relativa: `..` sube un nivel (de `notebooks/` a la raíz) | `LANDING = Path('../data/datalake/landing')` |
| `glob.glob('*.jsonl')` | Lista archivos que cumplen un patrón | Los 120 archivos de eventos |
| `pd.read_json(a, lines=True)` | Lee un JSONL (un JSON por línea) | Un archivo de eventos |
| `pd.concat([...])` | Pega varias tablas una debajo de otra | Los 120 archivos en una sola tabla |
| `json.loads(linea)` | Convierte texto JSON en diccionario **sin cambiar tipos** | Para ver que `value` llega como texto |
| `df.isna().sum()` | Cuenta los vacíos por columna | Nulos de cada tabla |
| `columna.is_unique` | ¿La clave se repite? | `event_id` es único |
| `(condición).mean()` | **Porcentaje** de filas que cumplen la condición (True = 1, False = 0) | `genai.mean()` → 9,6 % |
| `(condición).sum()` | **Cantidad** de filas que la cumplen | `(costo < 0).sum()` → 216 |
| `quantile(0.99)` | Percentil 99: el valor bajo el cual está el 99 % | Umbral de picos |
| `groupby(col).sum()` | Agrupa y suma, como una tabla dinámica | Costo por servicio |
| `isin([...])` | ¿El valor está en la lista? | Severidad high o critical |
| `cummax()` | Máximo acumulado: "lo más grande que vi hasta acá" | Reloj del watermark |
| `shift(1)` | Corre los valores un lugar | Lo que Spark sabía **antes** de abrir el archivo |
| `.map(serie)` | Le pone a cada fila el valor que corresponde a su clave | El watermark de cada evento según su archivo |
| `pd.Timedelta('2h')` | Una duración que se puede restar a una fecha | Margen del watermark |
| `timestamp[:10]` | Los primeros 10 caracteres: el día | `"2025-08-17T01:55:00Z"` → `"2025-08-17"` |
| `zlib.crc32(...) % 4` | Convierte una clave en un número del 0 al 3, siempre igual para la misma clave | Elegir el reducer |
| `defaultdict(list)` | Diccionario que crea una lista vacía si la clave no existe | Los buzones de cada reducer |

## Entorno (por si algo falla)

```bash
cd "/home/sebastian/Documentos/GitHub/facultad-main/mineria de datos II - miercoles"
source .venv/bin/activate      # siempre antes de jupyter
jupyter lab                    # siempre desde la carpeta principal
```
- `No module named 'pandas'` → Jupyter se abrió sin el entorno. Cerrar, activar y volver a abrir.
- `FileNotFoundError` en `../data/...` → El notebook no está en `notebooks/` o Jupyter se abrió desde otra carpeta.
- **Kernel → Restart Kernel and Run All Cells** corre todo en orden: úsalo antes de entregar.
