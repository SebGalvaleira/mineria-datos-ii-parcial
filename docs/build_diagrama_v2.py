"""Genera docs/img/arquitectura_v2.png a partir de arquitectura_v2.dot (solo lo implementado en la entrega 2).
Requiere Graphviz (comando `dot`) y Pillow.  Uso:  python docs/build_diagrama_v2.py
"""
import subprocess
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

def _font(sz, bold=False):
    for name in (['DejaVuSans-Bold.ttf', 'arialbd.ttf'] if bold else ['DejaVuSans.ttf', 'arial.ttf']):
        try: return ImageFont.truetype(name, sz)
        except OSError: pass
    return ImageFont.load_default()

HERE = Path(__file__).parent
(HERE / 'img').mkdir(exist_ok=True)
subprocess.run(['dot', '-Tpng', '-Gdpi=150', str(HERE / 'arquitectura_v2.dot'), '-o', str(HERE / 'img' / '_main.png')], check=True)
m = Image.open(HERE / 'img' / '_main.png').convert('RGB'); W = m.width
items = [("Linaje y metadatos", "source_file · ingest_ts\nbatch_id · stream_batch_id\nupdated_at en Gold"),
         ("Calidad", "5 reglas duras → quarantine\n2 flags (negativo, pico)\nconciliación de conteos"),
         ("Seguridad", "token en Colab Secrets\nbundle fuera del repo\nLanding inmutable"),
         ("Observabilidad", "log JSONL por lote y corrida\numbrales.json versionado\nasserts en cada notebook"),
         ("Idempotencia", "checkpoint + 3 barreras\n+ dedupe final en Gold\noverwrite por partición · upsert")]
F = lambda sz, b=False: _font(sz, b)
H = 330; bar = Image.new('RGB', (W, H), 'white'); d = ImageDraw.Draw(bar)
pad = 40; d.rounded_rectangle([pad, 10, W - pad, H - 10], 18, outline='#8a8984', width=3)
d.text((pad + 25, 26), "Capacidades transversales implementadas · aplican a todas las capas", font=F(30, True), fill='#0b0b0b')
cw = (W - 2 * pad - 50) / 5
for i, (t, s) in enumerate(items):
    x = pad + 25 + i * cw; d.rounded_rectangle([x, 80, x + cw - 20, H - 28], 12, outline='#b9b8b2', width=2, fill='#fafaf8')
    d.text((x + 18, 96), t, font=F(28, True), fill='#0b0b0b'); d.multiline_text((x + 18, 142), s, font=F(25), fill='#52514e', spacing=8)
out = Image.new('RGB', (W, m.height + H), 'white'); out.paste(m, (0, 0)); out.paste(bar, (0, m.height))
out.save(HERE / 'img' / 'arquitectura_v2.png'); (HERE / 'img' / '_main.png').unlink(); print(out.size)
