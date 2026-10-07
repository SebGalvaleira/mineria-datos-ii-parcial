# Cloud Provider Analytics — Proyecto Integrador

**Minería de Datos II · ISTEA 2C 2026** · Prof. Diego Mosquera

## Equipo

| Integrante | GitHub |
|---|---|
| David Riveros | [@Davoo0o](https://github.com/Davoo0o) |
| Maria Lopez | [@MariaLopez1999](https://github.com/MariaLopez1999) |
| Ramon Ojea Espil | [@Reimonoo](https://github.com/Reimonoo) |
| Sebastián Gonzalez | [@SebGalvaleira](https://github.com/SebGalvaleira) |

Diseño de un pipeline de datos para un proveedor de nube: ingesta batch y streaming, Data Lake (Landing → Bronze → Silver → Gold) en Parquet y marts para FinOps, Soporte y Producto servidos en Cassandra/AstraDB.

## Entrega 1 · Diseño y fundación de datos

📄 **Documento principal:** [`docs/diseno_entrega1.md`](docs/diseno_entrega1.md)

| # | Punto | Sección |
|---|---|---|
| 1 | Problema, usuarios y objetivos medibles | §1 |
| 2 | Justificación Big Data (5V) | §2 |
| 3 | Inventario y perfil de fuentes | §3 |
| 4 | Arquitectura de alto nivel | §4 |
| 5 | Patrón (híbrido) | §5 |
| 6 | Matriz requisito → componente | §6 |
| 7 | Diseño del Data Lake | §7 |
| 8 | Flujos batch y streaming | §8 |
| 9 | MapReduce | §9 |
| 10 | Supuestos, riesgos y decisiones abiertas | §10 |
| 11 | Esfuerzo, roles y recursos | §11 |
| 12 | Repositorio y evidencia | este README |

## Hallazgo principal

Cada uno de los 120 micro-lotes de eventos trae datos de los **60 días** del período. Un watermark típico de 2 h descartaría el **99 %** de los eventos (`notebooks/04`). Por eso el streaming **no agrega por ventanas**: guarda todo y recalcula solo las claves (org, fecha, servicio) que cada lote modifica.

## Estructura

```
README.md              este archivo
DECISIONS.md           registro de decisiones
requirements.txt       dependencias de Python
docs/
  diseno_entrega1.md   documento de diseño
  arquitectura_v1.dot  fuente del diagrama
  build_diagrama.py    genera docs/img/arquitectura_v1.png
  img/                 diagrama exportado
notebooks/             evidencia de exploración (ver tabla abajo)
evidence/              salidas de los notebooks (CSV y gráficos)
data/datalake/landing/ dataset original (inmutable)
```

## Notebooks

| Notebook | Qué demuestra | Sección |
|---|---|---|
| `01_mirar_los_datos.ipynb` | Primer contacto: archivos, filas, grano, esquema v1/v2 | §3 |
| `02_perfil_de_calidad.ipynb` | Conteo de problemas de calidad, desorden de micro-lotes, integridad referencial | §2, §3 |
| `03_cifras_5v.ipynb` | Volumen proyectado, concentración del costo, KPIs de soporte | §2 |
| `04_arquitectura_watermark.ipynb` | Simulación del watermark: % de eventos descartados según el margen | §4 |
| `05_mapreduce.ipynb` | MapReduce manual de `org_daily_usage_by_service`, verificado contra pandas | §9 |

## Cómo reproducir

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
jupyter lab
```
Abrir cada notebook y ejecutar **Kernel → Restart Kernel and Run All Cells**.

Para regenerar el diagrama: `sudo apt install graphviz` y luego `python docs/build_diagrama.py`.

## Convenciones

- **Landing es inmutable:** los archivos originales no se modifican.
- **Rutas del lake:** `datalake/<zona>/<dominio>/<tabla>/<particion>=<valor>/`.
- **Nombres:** `snake_case`; marts nombrados por su grano (`org_daily_usage_by_service`).
- **Columnas técnicas** desde Bronze: `source_file`, `ingest_ts`, `batch_id`. **Flags** con prefijo `flag_`.
- **Fechas** en UTC (ISO 8601). **Montos** en USD.
- **Nunca se borra un dato en silencio:** lo inválido va a quarantine con su motivo.
