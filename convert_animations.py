#!/usr/bin/env python3
"""Pre-converts all animated GIF/WebP in static/uploads to WebM."""
import os
import sys
import subprocess
import tempfile

BASE = os.path.dirname(__file__)
UPLOADS = os.path.join(BASE, 'static', 'uploads')

def webp_to_tmp_gif(webp_path):
    from PIL import Image, ImageSequence
    img = Image.open(webp_path)
    frames, durations = [], []
    for frame in ImageSequence.Iterator(img):
        rgba = frame.convert('RGBA')
        frames.append(rgba.convert('P', palette=Image.ADAPTIVE, colors=256))
        durations.append(frame.info.get('duration', 50))
    tmp = tempfile.NamedTemporaryFile(suffix='.gif', delete=False)
    frames[0].save(tmp.name, save_all=True, append_images=frames[1:],
                   loop=0, duration=durations, optimize=False)
    tmp.close()
    return tmp.name

def to_webm(src, dst):
    ext = src.rsplit('.', 1)[-1].lower()
    tmp_gif = None
    ffmpeg_input = src

    if ext == 'webp':
        print('  (webp→gif intermediate)', end=' ', flush=True)
        tmp_gif = webp_to_tmp_gif(src)
        ffmpeg_input = tmp_gif

    cmd = [
        'ffmpeg', '-y', '-i', ffmpeg_input,
        '-c:v', 'libvpx-vp9', '-b:v', '0', '-crf', '35',
        '-cpu-used', '5', '-deadline', 'realtime',
        '-auto-alt-ref', '0', '-an', dst
    ]
    result = subprocess.run(cmd, capture_output=True, timeout=300)
    if tmp_gif:
        try: os.unlink(tmp_gif)
        except OSError: pass
    return result.returncode == 0, result.stderr.decode()[-200:] if result.returncode != 0 else ''

converted = 0
skipped = 0
failed = 0

for root, dirs, files in os.walk(UPLOADS):
    for fname in files:
        ext = fname.rsplit('.', 1)[-1].lower() if '.' in fname else ''
        if ext not in ('gif', 'webp'):
            continue
        src = os.path.join(root, fname)
        dst = src + '.webm'

        if os.path.exists(dst):
            skipped += 1
            continue

        rel = os.path.relpath(src, BASE)
        print(f'Converting {rel}', end=' ... ', flush=True)
        ok, err = to_webm(src, dst)
        if ok:
            orig = os.path.getsize(src)
            new = os.path.getsize(dst)
            pct = new * 100 // orig if orig else 0
            print(f'OK  {orig//1024}KB → {new//1024}KB ({pct}%)')
            converted += 1
        else:
            print(f'FAILED: {err}')
            failed += 1

print(f'\nДone: {converted} converted, {skipped} skipped, {failed} failed.')
