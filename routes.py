# routes.py — All Flask routes extracted from main.py as a Blueprint.
# Non-Flask imports from main are done lazily inside route bodies (import main as m)
# to avoid circular imports. Decorators/constants imported at module level are safe
# because main.py does `from routes import bp` at the very END of the file, after
# all names are defined.

import os
import re
import json
import hmac
import hashlib
import asyncio
import math
import urllib.parse
import threading
from datetime import datetime, timedelta
from functools import wraps
from flask import (
    Blueprint, render_template, request, session, jsonify,
    redirect, url_for, Response, make_response,
    send_from_directory, send_file
)
from database import get_db, _USE_PG, _to_dt
from config import (
    ADMIN_TELEGRAM_IDS, SITE_URL, COIN_PACKAGES, PREMIUM_PACKAGES,
    TELEGRAM_BOT_TOKEN,
    YOOKASSA_SHOP_ID, YOOKASSA_SECRET,
    CRYPTOCLOUD_API_KEY, CRYPTOCLOUD_SECRET_KEY, CRYPTOCLOUD_SHOP_ID,
)
import logging

logger = logging.getLogger(__name__)
bp = Blueprint('main', __name__)

# These imports are safe: main.py does `from routes import bp` at the very bottom,
# so all these names in main are already defined when Python evaluates this.
from main import (
    rate_limit, _rate_limit_check,
    _allowed_file, schedule_webm_conversion,
    UPLOAD_FOLDER, THUMB_CACHE_DIR,
    get_user_reading, get_user_subscriptions,
    award_xp, get_or_create_user_stats, get_level_from_xp, get_xp_for_level,
    is_subscribed, get_manga_chapters_api,
    check_quests, create_site_notification,
    increment_manga_views,
    _manga_loading,
    _stats_cache, _stats_cache_lock,
    _suggest_similar_manga,
    _BOT_UA_KEYWORDS, _is_bot_request,
    cache, socketio,
)
import bot as _bot_module

def _m():
    """Lazy reference to main module for items not imported at top level."""
    import main
    return main


# ==================== STATIC FILE ROUTES ====================

@bp.route('/static/uploads/<path:filename>')
def serve_upload(filename):
    """Раздаёт загруженные файлы без кеширования браузером."""
    resp = send_from_directory(UPLOAD_FOLDER, filename)
    resp.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp


@bp.route('/thumb')
def serve_thumb():
    """Отдаёт сжатую миниатюру изображения. Поддерживает анимированные GIF."""
    import io as _io
    from PIL import Image, ImageSequence

    src = request.args.get('src', '')
    try:
        max_w = min(int(request.args.get('w', 200)), 800)
    except ValueError:
        return '', 400

    # Только файлы из uploads
    if not src.startswith('/static/uploads/'):
        return '', 400
    abs_path = os.path.join(os.path.dirname(__file__), src.lstrip('/'))
    if not os.path.isfile(abs_path):
        return '', 404

    ext = abs_path.rsplit('.', 1)[-1].lower()
    mtime = str(int(os.path.getmtime(abs_path)))
    cache_key = hashlib.md5(f'{src}:{max_w}:{mtime}'.encode()).hexdigest()
    cache_path = os.path.join(THUMB_CACHE_DIR, f'{cache_key}.{ext}')

    if os.path.exists(cache_path):
        resp = send_file(cache_path)
        resp.headers['Cache-Control'] = 'public, max-age=86400'
        return resp

    try:
        img = Image.open(abs_path)
        # Не увеличиваем, если уже меньше
        if img.width <= max_w:
            resp = send_file(abs_path)
            resp.headers['Cache-Control'] = 'public, max-age=86400'
            return resp

        ratio = max_w / img.width
        new_size = (max_w, max(1, int(img.height * ratio)))

        if ext == 'gif':
            frames, durations = [], []
            for frame in ImageSequence.Iterator(img):
                rgba = frame.convert('RGBA').resize(new_size, Image.LANCZOS)
                frames.append(rgba.convert('P', palette=Image.ADAPTIVE, colors=256))
                durations.append(frame.info.get('duration', 100))
            buf = _io.BytesIO()
            frames[0].save(buf, format='GIF', save_all=True,
                           append_images=frames[1:],
                           loop=img.info.get('loop', 0),
                           duration=durations, optimize=False)
            with open(cache_path, 'wb') as f:
                f.write(buf.getvalue())
        else:
            out = img.copy()
            out.thumbnail(new_size, Image.LANCZOS)
            save_kwargs = {'optimize': True}
            if ext in ('jpg', 'jpeg'):
                save_kwargs['quality'] = 82
            out.save(cache_path, **save_kwargs)

        resp = send_file(cache_path)
        resp.headers['Cache-Control'] = 'public, max-age=86400'
        return resp
    except Exception as e:
        logger.error(f'Thumb error {src}: {e}')
        return send_file(abs_path)


@bp.route('/anim')
def serve_anim():
    """Конвертирует анимацию (GIF/WebP) в WebM через ffmpeg и отдаёт как <video>."""
    import subprocess
    import tempfile
    from PIL import Image, ImageSequence

    src = request.args.get('src', '')
    try:
        max_w = min(int(request.args.get('w', 600)), 800)
    except ValueError:
        return '', 400

    if not src.startswith('/static/uploads/'):
        return '', 400
    abs_path = os.path.join(os.path.dirname(__file__), src.lstrip('/'))
    if not os.path.isfile(abs_path):
        return '', 404

    mtime = str(int(os.path.getmtime(abs_path)))
    cache_key = hashlib.md5(f'anim:{src}:{max_w}:{mtime}'.encode()).hexdigest()
    cache_path = os.path.join(THUMB_CACHE_DIR, f'{cache_key}.webm')

    if os.path.exists(cache_path):
        resp = send_file(cache_path, mimetype='video/webm')
        resp.headers['Cache-Control'] = 'public, max-age=86400'
        return resp

    ext = abs_path.rsplit('.', 1)[-1].lower()
    ffmpeg_input = abs_path
    tmp_gif = None

    try:
        # Animated WebP ffmpeg не читает напрямую — конвертируем через Pillow в GIF
        if ext == 'webp':
            img = Image.open(abs_path)
            frames, durations = [], []
            ratio = max_w / img.width if img.width > max_w else 1.0
            new_size = (max(1, int(img.width * ratio)), max(1, int(img.height * ratio)))
            for frame in ImageSequence.Iterator(img):
                rgba = frame.convert('RGBA').resize(new_size, Image.LANCZOS)
                frames.append(rgba.convert('P', palette=Image.ADAPTIVE, colors=256))
                durations.append(frame.info.get('duration', 50))
            tmp = tempfile.NamedTemporaryFile(suffix='.gif', delete=False)
            frames[0].save(tmp.name, save_all=True, append_images=frames[1:],
                           loop=0, duration=durations, optimize=False)
            tmp.close()
            tmp_gif = tmp.name
            ffmpeg_input = tmp_gif

        cmd = [
            'ffmpeg', '-y', '-i', ffmpeg_input,
            '-vf', f'scale={max_w}:-2' if ext != 'webp' else 'scale=trunc(iw/2)*2:trunc(ih/2)*2',
            '-c:v', 'libvpx-vp9', '-b:v', '0', '-crf', '35',
            '-cpu-used', '5', '-deadline', 'realtime',
            '-auto-alt-ref', '0', '-an',
            cache_path
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=120)
        if tmp_gif:
            try: os.unlink(tmp_gif)
            except OSError: pass

        if result.returncode == 0 and os.path.exists(cache_path):
            resp = send_file(cache_path, mimetype='video/webm')
            resp.headers['Cache-Control'] = 'public, max-age=86400'
            return resp
        logger.error(f'ffmpeg failed for {src}: {result.stderr.decode()[:300]}')
    except Exception as e:
        logger.error(f'Anim convert error {src}: {e}')
        if tmp_gif:
            try: os.unlink(tmp_gif)
            except OSError: pass

    return send_file(abs_path)


# ==================== FLASK ROUTES ====================

@bp.route('/')
def index():
    user_id = session.get('user_id')

    reading = []
    subscriptions = []

    if user_id:
        reading = get_user_reading(user_id, 12)
        subscriptions = get_user_subscriptions(user_id, 12)

    # Жанры/теги для секции "Все лейблы"
    genres = [
        {'icon': '❤️', 'name': 'Романтика'},
        {'icon': '🔮', 'name': 'Фэнтези'},
        {'icon': '🌀', 'name': 'Исекай'},
        {'icon': '👊', 'name': 'Экшен'},
        {'icon': '🤣', 'name': 'Комедия'},
        {'icon': '🎭', 'name': 'Драма'},
        {'icon': '⚡', 'name': 'Система'},
        {'icon': '👻', 'name': 'Ужасы'},
        {'icon': '🔎', 'name': 'Детектив'},
        {'icon': '💼', 'name': 'Повседневность'},
        {'icon': '🎓', 'name': 'Школа'},
        {'icon': '⚔️', 'name': 'Боевые искусства'},
    ]

    return render_template('index.html',
                          reading=reading,
                          subscriptions=subscriptions,
                          user_id=user_id,
                          genres=genres)


@bp.route('/api/auth/webapp', methods=['POST'])
def api_auth_webapp():
    """Аутентификация через Telegram WebApp initData (HMAC-SHA256)."""
    body = request.get_json(silent=True) or {}
    init_data_raw = (body.get('initData') or '').strip()
    if not init_data_raw:
        return jsonify({'error': 'no initData'}), 400

    try:
        parsed = dict(urllib.parse.parse_qsl(init_data_raw, keep_blank_values=True))
        hash_from_tg = parsed.pop('hash', None)
        if not hash_from_tg:
            return jsonify({'error': 'no hash'}), 400

        check_str = '\n'.join(f'{k}={v}' for k, v in sorted(parsed.items()))
        secret = hmac.new(b'WebAppData', TELEGRAM_BOT_TOKEN.encode(), hashlib.sha256).digest()
        expected = hmac.new(secret, check_str.encode(), hashlib.sha256).hexdigest()

        if not hmac.compare_digest(expected, hash_from_tg):
            return jsonify({'error': 'invalid hash'}), 403
    except Exception as ex:
        logger.warning(f"⚠️ webapp auth parse error: {ex}")
        return jsonify({'error': 'parse error'}), 400

    try:
        tg_user = json.loads(parsed.get('user', '{}'))
    except Exception:
        return jsonify({'error': 'bad user json'}), 400

    tg_id = tg_user.get('id')
    if not tg_id:
        return jsonify({'error': 'no user id'}), 400

    user = get_or_create_user_by_telegram(
        tg_id,
        tg_user.get('username'),
        tg_user.get('first_name'),
        tg_user.get('last_name'),
    )
    if not user:
        return jsonify({'error': 'db error'}), 500

    already = session.get('user_id') == user['id']
    session['user_id'] = user['id']
    session['username'] = (
        user.get('telegram_username') or
        user.get('telegram_first_name') or
        f"User_{user['id']}"
    )
    session.permanent = True
    return jsonify({'ok': True, '_already': already, 'user_id': user['id'], 'username': session['username']})


@bp.route('/api/home/recent')
def api_home_recent():
    data = get_recent_chapters_from_api(21)
    resp = make_response(jsonify(data))
    resp.headers['Cache-Control'] = 'public, max-age=300'
    return resp


@bp.route('/api/home/spotlights')
def api_home_spotlights():
    spotlights_data = get_cached_spotlights(ttl_seconds=1800)
    resp = make_response(jsonify(spotlights_data.get('spotlights', {})))
    resp.headers['Cache-Control'] = 'public, max-age=1800'
    return resp


@bp.route('/api/home/popular')
def api_home_popular():
    period = request.args.get('period', 'MONTH').upper()
    if period not in ('DAY', 'WEEK', 'MONTH'):
        period = 'MONTH'
    raw = get_popular_manga_from_api(period, 12)
    # Нормализуем поля под формат, который ожидает buildSliderItems на фронте
    data = [
        {
            'id':        m.get('manga_id'),
            'slug':      m.get('manga_slug'),
            'title':     m.get('manga_title'),
            'cover_url': m.get('cover_url'),
            'score':     m.get('score', 0),
            'type':      'MANGA',
        }
        for m in raw
    ]
    resp = make_response(jsonify(data))
    resp.headers['Cache-Control'] = 'public, max-age=600'
    return resp


@bp.route('/sw.js')
def service_worker():
    sw_content = """
const CACHE = 'bubblemanga-v1';
const IMG_CACHE = 'bubblemanga-images-v1';

self.addEventListener('install', e => {
    self.skipWaiting();
});

self.addEventListener('activate', e => {
    e.waitUntil(
        caches.keys().then(keys =>
            Promise.all(keys
                .filter(k => k !== CACHE && k !== IMG_CACHE)
                .map(k => caches.delete(k))
            )
        )
    );
    self.clients.claim();
});

async function cacheFirst(req, cacheName) {
    const cached = await caches.match(req);
    if (cached) return cached;
    const resp = await fetch(req);
    if (resp.ok) {
        const cache = await caches.open(cacheName);
        cache.put(req, resp.clone());
    }
    return resp;
}

async function staleWhileRevalidate(req, cacheName) {
    const cache = await caches.open(cacheName);
    const cached = await cache.match(req);
    const fetchPromise = fetch(req).then(resp => {
        if (resp.ok) cache.put(req, resp.clone());
        return resp;
    }).catch(() => null);
    return cached || fetchPromise;
}

async function networkFirst(req, cacheName) {
    try {
        const resp = await fetch(req);
        if (resp.ok) {
            const cache = await caches.open(cacheName);
            cache.put(req, resp.clone());
        }
        return resp;
    } catch {
        const cached = await caches.match(req);
        return cached || new Response('Нет подключения', { status: 503 });
    }
}

self.addEventListener('fetch', e => {
    const { request } = e;
    const url = new URL(request.url);

    // Картинки (обложки, страницы глав) — cache-first
    if (request.destination === 'image') {
        e.respondWith(cacheFirst(request, IMG_CACHE));
        return;
    }

    // API главной — stale-while-revalidate
    if (url.pathname.startsWith('/api/home/')) {
        e.respondWith(staleWhileRevalidate(request, CACHE));
        return;
    }

    // HTML страницы — network-first с fallback
    if (request.mode === 'navigate') {
        e.respondWith(networkFirst(request, CACHE));
        return;
    }
});
""".strip()
    resp = make_response(sw_content, 200)
    resp.headers['Content-Type'] = 'application/javascript'
    resp.headers['Service-Worker-Allowed'] = '/'
    resp.headers['Cache-Control'] = 'no-cache'
    return resp

@bp.route('/login/<token>')
def login_token(token):
    """Вход по токену из Telegram"""
    user = get_user_by_token(token)
    if not user:
        return "Неверный или устаревший токен. Получите новый через Telegram бота.", 403
    session['user_id'] = user['id']
    session['username'] = user['telegram_username'] or user['telegram_first_name'] or f"User_{user['id']}"
    session.permanent = True
    # Не делаем redirect сразу — страница остаётся на /login/<token>.
    # Это позволяет пользователю нажать «Открыть в браузере» в Telegram
    # и попасть в Safari уже с токеном → залогиниться там тоже.
    name = user.get('telegram_first_name') or user.get('telegram_username') or 'Пользователь'
    return render_template_string('''<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Вход — BubbleManga</title>
  <style>
    *{margin:0;padding:0;box-sizing:border-box}
    body{min-height:100dvh;display:flex;align-items:center;justify-content:center;
         background:#0a0a0a;font-family:'Segoe UI',system-ui,sans-serif;color:#f4f4f5;padding:20px}
    .card{text-align:center;max-width:360px;width:100%}
    .icon{font-size:56px;margin-bottom:16px}
    h1{font-size:22px;font-weight:700;margin-bottom:8px}
    .sub{font-size:14px;color:rgba(244,244,245,.55);margin-bottom:28px;line-height:1.5}
    .btn{display:block;padding:14px 24px;border-radius:14px;text-decoration:none;
         background:linear-gradient(135deg,#667eea,#764ba2);color:#fff;
         font-size:15px;font-weight:700;transition:opacity .18s;margin-bottom:12px}
    .btn:hover{opacity:.85}
    .hint{font-size:12px;color:rgba(244,244,245,.35);line-height:1.5}
    .hint b{color:rgba(244,244,245,.6)}
  </style>
</head>
<body>
  <div class="card">
    <div class="icon">✅</div>
    <h1>Добро пожаловать, {{ name }}!</h1>
    <p class="sub">Вы успешно вошли в аккаунт.<br>Нажмите кнопку ниже чтобы перейти на сайт.</p>
    <a href="/" class="btn">Перейти на BubbleManga</a>
    <p class="hint">
      Хотите открыть в браузере (Safari)?<br>
      Нажмите <b>···&nbsp;→&nbsp;Открыть в браузере</b> прямо сейчас —<br>
      вы попадёте туда уже авторизованным.
    </p>
  </div>
</body>
</html>''', name=name)

@bp.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

@bp.route('/search')
def search():
    _PER = 25

    query   = request.args.get('q', '').strip()
    user_id = session.get('user_id')

    if not query or len(query) < 2:
        return render_template('search.html',
                               query=query, results=[],
                               total=0, user_id=user_id)

    if user_id:
        save_search_history(user_id, query)

    like   = f'%{query}%'
    starts = f'{query}%'

    conn = get_db()
    c    = conn.cursor()

    c.execute('''SELECT COUNT(*) FROM manga
                 WHERE manga_title LIKE ? OR original_name LIKE ? OR manga_slug LIKE ?''',
              (like, like, like))
    total = c.fetchone()[0]

    results = []
    if total > 0:
        c.execute('''
            SELECT manga_id, manga_slug, manga_title, manga_type,
                   manga_status, cover_url, rating, score, views, chapters_count, last_updated,
                   CASE
                     WHEN lower(manga_title) = lower(?)    THEN 10
                     WHEN lower(manga_title) LIKE lower(?) THEN 5
                     ELSE 1
                   END AS _rel
            FROM manga
            WHERE manga_title LIKE ? OR original_name LIKE ? OR manga_slug LIKE ?
            ORDER BY _rel DESC, COALESCE(score, 0) DESC
            LIMIT ?
        ''', (query, starts, like, like, like, _PER))
        results = [dict(r) for r in c.fetchall()]
        for r in results:
            r.pop('_rel', None)

    conn.close()

    # Fallback на API только если БД пуста
    if total == 0:
        api_results = search_manga_api(query, _PER)
        total   = len(api_results)
        results = api_results

    return render_template('search.html',
                           query=query, results=results,
                           total=total, user_id=user_id)

@bp.route('/api/search')
def api_search():
    """AJAX-поиск манги с сортировкой и offset-пагинацией."""
    _PER = 25
    _SORT = {
        'relevance': None,                            # специальная логика
        'score':     'COALESCE(score, 0) DESC',
        'views':     'COALESCE(views, 0) DESC',
        'chapters':  'COALESCE(chapters_count, 0) DESC',
        'updated':   "COALESCE(last_updated, '1970') DESC",
    }

    query  = request.args.get('q', '').strip()
    offset = max(0, request.args.get('offset', 0, type=int))
    limit  = min(max(1, request.args.get('limit', _PER, type=int)), 100)
    sort   = request.args.get('sort', 'relevance')
    if sort not in _SORT:
        sort = 'relevance'

    if not query or len(query) < 2:
        return jsonify({'results': [], 'total': 0, 'has_more': False})

    like   = f'%{query}%'
    starts = f'{query}%'

    conn = get_db()
    c    = conn.cursor()

    rows = None
    total = 0

    if sort == 'relevance':
        if _USE_PG:
            # PostgreSQL tsvector поиск
            tsq = query  # plainto_tsquery сам нормализует
            try:
                c.execute('''SELECT COUNT(*) FROM manga
                             WHERE search_vector @@ plainto_tsquery('simple', %s)''', (tsq,))
                total = c.fetchone()[0]
                if total > 0:
                    c.execute('''
                        SELECT manga_id, manga_slug, manga_title, manga_type, manga_status,
                               cover_url, rating, score, views, chapters_count, last_updated
                        FROM manga
                        WHERE search_vector @@ plainto_tsquery('simple', %s)
                        ORDER BY ts_rank(search_vector, plainto_tsquery('simple', %s)) DESC
                        LIMIT %s OFFSET %s
                    ''', (tsq, tsq, limit, offset))
                    rows = [dict(r) for r in c.fetchall()]
            except Exception:
                rows = None
                total = 0
        else:
            # SQLite FTS5 поиск
            fts_query = query.replace('"', '""')
            try:
                c.execute('''SELECT m.manga_id FROM manga_fts
                             JOIN manga m USING(manga_id)
                             WHERE manga_fts MATCH ?
                             ORDER BY rank LIMIT 1''', (fts_query + '*',))
                use_fts = c.fetchone() is not None
            except Exception:
                use_fts = False

            if use_fts:
                c.execute('''SELECT COUNT(*) FROM manga_fts
                             JOIN manga m USING(manga_id)
                             WHERE manga_fts MATCH ?''', (fts_query + '*',))
                total = c.fetchone()[0]
                if total > 0:
                    c.execute('''
                        SELECT m.manga_id, m.manga_slug, m.manga_title, m.manga_type, m.manga_status,
                               m.cover_url, m.rating, m.score, m.views, m.chapters_count, m.last_updated
                        FROM manga_fts
                        JOIN manga m USING(manga_id)
                        WHERE manga_fts MATCH ?
                        ORDER BY rank
                        LIMIT ? OFFSET ?
                    ''', (fts_query + '*', limit, offset))
                    rows = [dict(r) for r in c.fetchall()]

    if rows is None:
        # LIKE fallback (also used for non-relevance sort)
        c.execute('''SELECT COUNT(*) FROM manga
                     WHERE manga_title LIKE ? OR original_name LIKE ? OR manga_slug LIKE ?''',
                  (like, like, like))
        total = c.fetchone()[0]

        if total == 0:
            conn.close()
            return jsonify({'results': [], 'total': 0, 'has_more': False})

        if sort == 'relevance':
            c.execute('''
                SELECT manga_id, manga_slug, manga_title, manga_type, manga_status,
                       cover_url, rating, score, views, chapters_count, last_updated,
                       CASE
                         WHEN lower(manga_title) = lower(?)    THEN 10
                         WHEN lower(manga_title) LIKE lower(?) THEN 5
                         ELSE 1
                       END AS _rel
                FROM manga
                WHERE manga_title LIKE ? OR original_name LIKE ? OR manga_slug LIKE ?
                ORDER BY _rel DESC, COALESCE(score, 0) DESC
                LIMIT ? OFFSET ?
            ''', (query, starts, like, like, like, limit, offset))
        else:
            order_sql = _SORT[sort]
            c.execute(f'''
                SELECT manga_id, manga_slug, manga_title, manga_type, manga_status,
                       cover_url, rating, score, views, chapters_count, last_updated
                FROM manga
                WHERE manga_title LIKE ? OR original_name LIKE ? OR manga_slug LIKE ?
                ORDER BY {order_sql}
                LIMIT ? OFFSET ?
            ''', (like, like, like, limit, offset))
        rows = [dict(r) for r in c.fetchall()]

    conn.close()

    for r in rows:
        r.pop('_rel', None)

    return jsonify({
        'results': rows,
        'total':    total,
        'has_more': offset + len(rows) < total,
    })


@bp.route('/api/search/suggestions')
def search_suggestions():
    query   = request.args.get('q', '').strip()
    user_id = session.get('user_id')

    if len(query) < 2:
        return jsonify([])

    # Сначала — тайтлы из каталога БД
    conn = get_db()
    c    = conn.cursor()
    c.execute('''SELECT manga_title FROM manga
                 WHERE manga_title LIKE ? OR original_name LIKE ?
                 ORDER BY COALESCE(score, 0) DESC
                 LIMIT 8''', (f'{query}%', f'{query}%'))
    from_catalog = [row[0] for row in c.fetchall()]

    # Добавляем из истории поиска (только если мало результатов)
    from_history = []
    if len(from_catalog) < 5:
        c.execute('''SELECT DISTINCT query FROM search_history
                     WHERE query LIKE ?
                     ORDER BY created_at DESC
                     LIMIT ?''', (f'{query}%', 8 - len(from_catalog)))
        from_history = [row[0] for row in c.fetchall()]
    conn.close()

    # Объединяем, дедупликация с сохранением порядка
    seen = set()
    suggestions = []
    for s in from_catalog + from_history:
        if s.lower() not in seen:
            seen.add(s.lower())
            suggestions.append(s)

    return jsonify(suggestions[:8])

_GENRE_GROUPS = {
    'Жанры': [
        'Романтика', 'Фэнтези', 'Драма', 'Комедия', 'Экшен', 'Приключения',
        'Повседневность', 'Психологическое', 'Боевые искусства', 'Фантастика',
        'Ужасы', 'Мистика', 'Триллер', 'Детектив', 'Исторический', 'Спорт',
        'Трагедия', 'Антиутопия', 'Военное', 'Криминал / Преступники',
        'Музыка', 'Иясикэй', 'Пародия', 'Меха', 'Шоу-бизнес',
        'Апокалиптический', 'Постапокалиптический', 'Гэг-юмор',
        'Гурман', 'Философия', 'Медицина', 'Политика', 'Безумие',
        'Сверхъестественное', 'Нуар',
    ],
    'Демография': [
        'Сёнен', 'Сёдзе', 'Сэйнэн', 'Дзёсей',
        'Яой', 'Юри', 'Сёнен-ай', 'Сёдзе-ай', 'Этти', 'Эротика',
    ],
    'Сеттинг': [
        'Школа', 'Исекай', 'Средневековье', 'Школьная жизнь',
        'Магическая академия', 'Учебное заведение', 'Старшая школа',
        'Космос', 'Подземелье', 'Будущее', 'Виртуальная реальность',
        'Япония', 'Армия', 'Мурим', 'Фэнтезийный мир', 'Офисные работники',
    ],
    'Персонажи & расы': [
        'ГГ мужчина', 'ГГ женщина', 'ГГ имба', 'Умный ГГ', 'Тупой ГГ',
        'ГГ не человек', 'Молодой ГГ', 'Антигерой', 'Злодейка',
        'Горничная', 'Рыцарь', 'Самурай', 'Ниндзя', 'Наёмник',
        'Вампир', 'Демон', 'Зверолюди', 'Монстродевушка', 'Монстр',
        'Эльф', 'Драконы', 'Бог', 'Богиня', 'Волшебник / Маг', 'Ведьма',
        'Волшебные существа', 'Разумные расы', 'Призрак', 'Нежить',
        'Ангел', 'Злой дух', 'Владыка демонов', 'Девочки-волшебницы',
        'Гоблин', 'Животные компаньоны', 'Зомби', 'Мифология',
        'В основном взрослые', 'Мужской гарем', 'Женский гарем',
        'Брат и сестра', 'Гяру', 'Лоли',
    ],
    'Сюжет & механики': [
        'Реинкарнация', 'Система', 'Магия', 'Навыки / Способности',
        'Ранги силы', 'Культивация', 'Борьба за власть', 'Сокрытие личности',
        'Воспоминания из другого мира', 'Выживание', 'Управление территорией',
        'Игровые элементы', 'Месть', 'Аристократия',
        'Манипуляция временем / Путешествия', 'Дружба', 'Жестокий мир',
        'Элементы юмора', 'Бои на мечах', 'Супер сила', 'Спасение мира',
        'Любовный многоугольник', 'Шантаж', 'Игра с высокими ставками',
        'Насилие / Жестокость', 'Преступления', 'Мафия', 'Гильдии',
        'Артефакты', 'Алхимия', 'Яндере', 'Империи', 'Героическое фэнтези',
        'Правонарушитель / Хулиган', 'Бессмертный', 'Видеоигры', 'Геймеры',
        'Идол', 'Работа', 'Игры', 'Раб', 'Грузовик-сан',
        'Огнестрельное оружие', 'Холодное оружие', 'Робот', 'Полиция',
        'Амнезия / Потеря памяти', 'Хикикомори', 'Культура Отаку',
        'Командный спорт', 'Спортивное тело', 'Рисование', 'Искусство',
    ],
}


@cache.cached(timeout=600, key_prefix='catalog_genres')
def _get_catalog_genres():
    """Кешированный список жанров/тегов для каталога (TTL 10 мин)."""
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT tags FROM manga WHERE tags IS NOT NULL AND tags != '[]' AND tags != ''")
    genre_freq = {}
    for row in c.fetchall():
        try:
            tags = json.loads(row[0])
            for tag in tags:
                tag = tag.strip()
                if tag:
                    genre_freq[tag] = genre_freq.get(tag, 0) + 1
        except Exception:
            pass
    conn.close()

    # Строим группы: только теги с >= 10 манг
    groups = {}
    placed = set()
    for group_name, group_tags in _GENRE_GROUPS.items():
        members = [t for t in group_tags if genre_freq.get(t, 0) >= 10]
        if members:
            groups[group_name] = members
            placed.update(members)

    # Все оставшиеся теги с >= 10 манг → «Прочее»
    others = sorted(
        [t for t, cnt in genre_freq.items() if cnt >= 10 and t not in placed],
        key=lambda g: -genre_freq[g]
    )
    if others:
        groups['Прочее'] = others

    return groups


@bp.route('/catalog')
def catalog_page():
    """Каталог всех манг с фильтрацией"""
    genre_groups = _get_catalog_genres()
    # Плоский список всех тегов для JS
    all_genres = [tag for tags in genre_groups.values() for tag in tags]
    return render_template('catalog.html', genre_groups=genre_groups, genres=all_genres)


@bp.route('/api/catalog')
def api_catalog():
    """AJAX-каталог с фильтрацией и сортировкой"""
    _PER = 28
    _SORT = {
        'score':    'COALESCE(score, 0) DESC, manga_id ASC',
        'views':    'COALESCE(views, 0) DESC, manga_id ASC',
        'chapters': 'COALESCE(chapters_count, 0) DESC, manga_id ASC',
        'updated':  "COALESCE(last_updated, '1970') DESC, manga_id ASC",
        'title':    'manga_title ASC, manga_id ASC',
    }
    manga_type = request.args.get('type', '').strip().upper()
    manga_status = request.args.get('status', '').strip().upper()
    genres_raw = request.args.get('genres', '').strip()
    sort = request.args.get('sort', 'score')
    offset = max(0, request.args.get('offset', 0, type=int))
    limit = min(max(1, request.args.get('limit', _PER, type=int)), 100)

    if sort not in _SORT:
        sort = 'score'

    selected_genres = [g.strip() for g in genres_raw.split(',') if g.strip()] if genres_raw else []

    where = ['1=1']
    params = []

    if manga_type and manga_type in ('MANGA', 'MANHWA', 'MANHUA', 'OEL', 'NOVEL', 'ONE_SHOT', 'DOUJINSHI', 'COMICS'):
        where.append('manga_type = ?')
        params.append(manga_type)

    if manga_status and manga_status in ('ONGOING', 'FINISHED', 'CANCELLED', 'HIATUS', 'ANNOUNCED'):
        where.append('manga_status = ?')
        params.append(manga_status)

    for genre in selected_genres[:10]:
        safe = genre.replace('"', '').replace('%', '').replace('_', '\\_')
        where.append('tags LIKE ?')
        params.append(f'%"{safe}"%')

    where_sql = ' AND '.join(where)
    order_sql = _SORT[sort]

    conn = get_db()
    c = conn.cursor()
    c.execute(f'SELECT COUNT(*) FROM manga WHERE {where_sql}', params)
    total = c.fetchone()[0]

    c.execute(
        f'''SELECT manga_id, manga_slug, manga_title, manga_type, manga_status,
                   cover_url, rating, score, views, chapters_count, last_updated,
                   SUBSTR(description, 1, 160) AS description, tags
            FROM manga WHERE {where_sql}
            ORDER BY {order_sql}
            LIMIT ? OFFSET ?''',
        params + [limit, offset]
    )
    rows = [dict(r) for r in c.fetchall()]
    conn.close()

    return jsonify({'results': rows, 'total': total, 'has_more': offset + len(rows) < total})


@bp.route('/api/subscribe/<manga_id>', methods=['POST'])
def subscribe(manga_id):
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Not authenticated'}), 401
    
    subscribed = toggle_subscription(user_id, manga_id)
    return jsonify({'subscribed': subscribed})

@bp.route('/read/<manga_slug>/<chapter_slug>')
def read_chapter(manga_slug, chapter_slug):
    # Сначала попробуем найти в БД
    conn = get_db()
    c = conn.cursor()
    c.execute('''SELECT c.*, m.manga_title, m.manga_id, m.manga_slug 
                 FROM chapters c 
                 JOIN manga m ON c.manga_id = m.manga_id 
                 WHERE c.chapter_slug = ?''', (chapter_slug,))
    chapter = c.fetchone()
    
    if not chapter:
        # Если главы нет в БД, получаем через API
        pages = get_chapter_pages(chapter_slug)
        if not pages:
            conn.close()
            return "Глава не найдена", 404
        
        # Ищем manga_id по slug
        c.execute('SELECT manga_id, manga_title, manga_slug FROM manga WHERE manga_slug = ?', (manga_slug,))
        manga_result = c.fetchone()
        
        if not manga_result:
            conn.close()
            return "Манга не найдена", 404
        
        manga_id = manga_result['manga_id']
        manga_title = manga_result['manga_title']
        manga_slug_db = manga_result['manga_slug']  # Получаем manga_slug из БД
        
        # Получаем URL страниц
        page_urls = [p.get("image", {}).get("compress", {}).get("url", "") 
                     for p in pages if p.get("image", {}).get("compress", {}).get("url")]
        
        # Создаем временный объект главы
        chapter_dict = {
            'chapter_id': f"temp_{chapter_slug}",
            'chapter_slug': chapter_slug,
            'chapter_number': '1',
            'chapter_volume': None,
            'chapter_name': 'Глава из API',
            'manga_title': manga_title,
            'manga_id': manga_id,
            'manga_slug': manga_slug_db,  # Используем manga_slug из БД
            'pages_json': json.dumps(page_urls),
            'pages': page_urls,
            'chapter_url': f"{SITE_URL}/read/{manga_slug_db}/{chapter_slug}"
        }
        
        # Показываем главу без сохранения в БД
        subscribed = False
        user_id = session.get('user_id')
        if user_id:
            subscribed = is_subscribed(user_id, manga_id)
        
        conn.close()
        return render_template('chapter.html',
                              chapter=chapter_dict,
                              subscribed=subscribed,
                              user_id=user_id,
                              prev_chapter=None,
                              next_chapter=None)
    
    # Преобразуем результат запроса в словарь
    chapter_dict = dict(chapter)
    
    # Убедимся, что manga_slug присутствует в словаре
    # (он должен быть в запросе из-за JOIN с таблицей manga)
    if 'manga_slug' not in chapter_dict:
        chapter_dict['manga_slug'] = manga_slug
    
    # Проверяем, что pages_json не None и содержит данные
    if chapter_dict.get('pages_json'):
        try:
            chapter_dict['pages'] = json.loads(chapter_dict['pages_json'])
        except (json.JSONDecodeError, TypeError) as e:
            print(f"❌ Ошибка загрузки JSON для главы {chapter_slug}: {e}")
            chapter_dict['pages'] = []
    else:
        chapter_dict['pages'] = []
    
    # Если страниц нет или они пустые, получаем через API
    if not chapter_dict['pages']:
        print(f"📄 Получение страниц через API для главы {chapter_slug}")
        pages = get_chapter_pages(chapter_slug)
        
        if pages:
            # Извлекаем URL страниц
            page_urls = [p.get("image", {}).get("compress", {}).get("url", "") 
                        for p in pages if p.get("image", {}).get("compress", {}).get("url")]
            
            chapter_dict['pages'] = page_urls
            
            # Обновляем в БД
            c.execute('UPDATE chapters SET pages_json = ?, pages_count = ? WHERE chapter_slug = ?',
                      (json.dumps(page_urls), len(page_urls), chapter_slug))
            conn.commit()
            print(f"✅ Обновлено {len(page_urls)} страниц для главы {chapter_slug}")
        else:
            print(f"⚠️ Не удалось получить страницы для главы {chapter_slug}")
    
    # Обновляем счетчик просмотров
    increment_manga_views(chapter_dict['manga_id'])
    
    # Обновляем историю чтения и начисляем XP
    user_id = session.get('user_id')
    if user_id:
        c.execute('''INSERT OR REPLACE INTO reading_history
                     (user_id, manga_id, chapter_id, last_read)
                     VALUES (?, ?, ?, ?)''',
                  (user_id, chapter_dict['manga_id'],
                   chapter_dict['chapter_id'], datetime.now()))
        # Сохраняем каждую прочитанную главу отдельно
        c.execute('''INSERT OR IGNORE INTO chapters_read
                     (user_id, chapter_id, manga_id)
                     VALUES (?, ?, ?)''',
                  (user_id, chapter_dict['chapter_id'], chapter_dict['manga_id']))
        conn.commit()
        # XP / счётчики / стрик — только при реальном прочтении (80%+),
        # обрабатываются в POST /api/chapter/<slug>/complete

    # Предыдущая и следующая главы
    manga_id_nav = chapter_dict['manga_id']
    chapter_num_nav = chapter_dict['chapter_number']

    c.execute('''SELECT chapter_slug, chapter_number FROM chapters
                 WHERE manga_id = ? AND CAST(chapter_number AS FLOAT) < CAST(? AS FLOAT)
                 ORDER BY CAST(chapter_number AS FLOAT) DESC LIMIT 1''',
              (manga_id_nav, chapter_num_nav))
    prev_ch = c.fetchone()

    c.execute('''SELECT chapter_slug, chapter_number FROM chapters
                 WHERE manga_id = ? AND CAST(chapter_number AS FLOAT) > CAST(? AS FLOAT)
                 ORDER BY CAST(chapter_number AS FLOAT) ASC LIMIT 1''',
              (manga_id_nav, chapter_num_nav))
    next_ch = c.fetchone()

    prev_chapter = dict(prev_ch) if prev_ch else None
    next_chapter = dict(next_ch) if next_ch else None

    conn.close()

    # Если пользователь дочитал последнюю доступную главу — предложить похожую мангу
    if user_id and next_chapter is None:
        import threading as _th
        _th.Thread(
            target=_suggest_similar_manga,
            args=(user_id, chapter_dict['manga_id'], chapter_dict.get('manga_title', manga_slug)),
            daemon=True
        ).start()

    subscribed = False
    if user_id:
        subscribed = is_subscribed(user_id, chapter_dict['manga_id'])

    return render_template('chapter.html',
                          chapter=chapter_dict,
                          subscribed=subscribed,
                          user_id=user_id,
                          prev_chapter=prev_chapter,
                          next_chapter=next_chapter)

# ==================== ФИЛЬТРЫ ДЛЯ ШАБЛОНОВ ====================

@bp.app_template_filter('relative_time')
def relative_time_filter(timestamp):
    """Фильтр для преобразования времени в относительный формат"""
    if not timestamp:
        return "Недавно"
    
    try:
        # Пробуем разные форматы дат
        if isinstance(timestamp, str):
            # Убираем временную зону если есть
            timestamp = timestamp.split('+')[0].split('Z')[0]
            
            # Пробуем разные форматы
            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S.%f']:
                try:
                    date = datetime.strptime(timestamp, fmt)
                    break
                except ValueError:
                    continue
            else:
                return timestamp[:10]  # Возвращаем только дату если не распарсилось
        else:
            date = timestamp
        
        now = datetime.now()
        diff = now - date
        
        # Вычисляем разницу в различных единицах
        seconds = diff.total_seconds()
        minutes = seconds / 60
        hours = minutes / 60
        days = hours / 24
        
        if seconds < 60:
            return "Только что"
        elif minutes < 60:
            mins = int(minutes)
            if mins == 1:
                return "1 минуту назад"
            elif 2 <= mins <= 4:
                return f"{mins} минуты назад"
            else:
                return f"{mins} минут назад"
        elif hours < 24:
            hrs = int(hours)
            if hrs == 1:
                return "1 час назад"
            elif 2 <= hrs <= 4:
                return f"{hrs} часа назад"
            else:
                return f"{hrs} часов назад"
        elif days < 7:
            ds = int(days)
            if ds == 1:
                return "Вчера"
            elif ds == 2:
                return "Позавчера"
            else:
                return f"{ds} дней назад"
        elif days < 30:
            weeks = int(days / 7)
            if weeks == 1:
                return "1 неделю назад"
            elif weeks == 2:
                return "2 недели назад"
            else:
                return f"{weeks} недель назад"
        elif days < 365:
            months = int(days / 30)
            if months == 1:
                return "1 месяц назад"
            elif 2 <= months <= 4:
                return f"{months} месяца назад"
            else:
                return f"{months} месяцев назад"
        else:
            years = int(days / 365)
            if years == 1:
                return "1 год назад"
            elif 2 <= years <= 4:
                return f"{years} года назад"
            else:
                return f"{years} лет назад"
                
    except Exception as e:
        logger.error(f"Ошибка в фильтре relative_time: {e}")
        return timestamp[:10] if isinstance(timestamp, str) and len(timestamp) >= 10 else "Недавно"

# Добавьте также другие полезные фильтры
@bp.app_template_filter('format_date')
def format_date_filter(date_str, format='%d.%m.%Y'):
    """Форматирование даты"""
    if not date_str:
        return ""
    
    try:
        if isinstance(date_str, str):
            # Убираем временную зону если есть
            date_str = date_str.split('+')[0].split('Z')[0]
            
            # Пробуем разные форматы
            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d']:
                try:
                    date = datetime.strptime(date_str, fmt)
                    return date.strftime(format)
                except ValueError:
                    continue
        
        return date_str[:10] if len(date_str) >= 10 else date_str
    except Exception as e:
        logger.error(f"Ошибка в фильтре format_date: {e}")
        return date_str
def _bg_load_all_chapters(manga_slug):
    """Фоновый поток: загрузить все главы и сохранить в БД"""
    try:
        logger.info(f"🔄 [BG] Фоновая загрузка всех глав для {manga_slug}")
        get_manga_chapters_api(manga_slug, limit=10000)
        logger.info(f"✅ [BG] Фоновая загрузка завершена для {manga_slug}")
    except Exception as e:
        logger.error(f"❌ [BG] Ошибка фоновой загрузки для {manga_slug}: {e}")
    finally:
        _manga_loading.pop(manga_slug, None)


@bp.route('/api/manga/<manga_slug>/chapters')
def api_manga_chapters(manga_slug):
    """API: получить главы манги с пагинацией (для фоновой подгрузки)"""
    try:
        offset = int(request.args.get('offset', 0))
        limit = min(int(request.args.get('limit', 50)), 5000)
        order = request.args.get('order', 'desc').lower()
        if order not in ('asc', 'desc'):
            order = 'desc'
    except (ValueError, TypeError):
        offset, limit, order = 0, 50, 'desc'

    order_sql = 'ASC' if order == 'asc' else 'DESC'

    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT manga_id FROM manga WHERE manga_slug = ?', (manga_slug,))
    manga_row = c.fetchone()

    if not manga_row:
        conn.close()
        return jsonify({'chapters': [], 'is_loading': manga_slug in _manga_loading, 'total_in_db': 0, 'has_more': False})

    manga_id = manga_row['manga_id']
    c.execute(
        f'''SELECT chapter_id, chapter_slug, chapter_number, chapter_volume,
                   chapter_name, created_at, chapter_url
            FROM chapters
            WHERE manga_id = ?
            ORDER BY CAST(chapter_number AS FLOAT) {order_sql}
            LIMIT ? OFFSET ?''',
        (manga_id, limit, offset)
    )
    chapters = [dict(row) for row in c.fetchall()]

    c.execute('SELECT COUNT(*) as cnt FROM chapters WHERE manga_id = ?', (manga_id,))
    total_in_db = c.fetchone()['cnt']
    conn.close()

    return jsonify({
        'chapters': chapters,
        'is_loading': manga_slug in _manga_loading,
        'total_in_db': total_in_db,
        'has_more': len(chapters) == limit
    })


@bp.route('/api/manga/<manga_slug>/read-chapters')
def api_read_chapters(manga_slug):
    """API: список прочитанных глав пользователя для конкретной манги"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify([])
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT manga_id FROM manga WHERE manga_slug = ?', (manga_slug,))
    row = c.fetchone()
    if not row:
        conn.close()
        return jsonify([])
    manga_id = row['manga_id']
    c.execute(
        'SELECT chapter_id FROM chapters_read WHERE user_id = ? AND manga_id = ?',
        (user_id, manga_id)
    )
    chapter_ids = [r['chapter_id'] for r in c.fetchall()]
    conn.close()
    return jsonify(chapter_ids)


def _calc_chapter_reward(pages_count: int) -> tuple[int, int]:
    """Вернуть (xp, coins) за прочитанную главу в зависимости от длины."""
    if pages_count <= 5:
        return 5, 0
    elif pages_count <= 15:
        return 15, 0
    elif pages_count <= 30:
        return 25, 1
    elif pages_count <= 50:
        return 40, 2
    else:
        return 60, 3


@bp.route('/api/chapter/<chapter_slug>/complete', methods=['POST'])
def api_chapter_complete(chapter_slug):
    """
    Вызывается клиентом когда пользователь прочитал ≥80% страниц главы.
    Начисляет XP/монеты, обновляет стрик и задания.
    """
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'ok': False, 'error': 'not_auth'}), 401

    pages_count = request.json.get('pages_count', 0) if request.is_json else 0
    pages_count = max(0, int(pages_count))

    xp_amount, coins_amount = _calc_chapter_reward(pages_count)

    conn = get_db()
    try:
        # Антиспам: не начислять повторно за одну главу в течение 1 часа
        dup = conn.execute(
            'SELECT id FROM xp_log WHERE user_id=? AND ref_id=? AND reason="chapter_complete"'
            ' AND created_at > datetime("now", "-1 hour")',
            (user_id, chapter_slug)
        ).fetchone()
        if dup:
            return jsonify({'ok': False, 'error': 'already_rewarded'}), 200

        # Счётчики
        conn.execute('INSERT OR IGNORE INTO user_stats (user_id) VALUES (?)', (user_id,))
        conn.execute(
            'UPDATE user_stats SET total_chapters_read = total_chapters_read + 1,'
            ' total_pages_read = total_pages_read + ? WHERE user_id = ?',
            (pages_count, user_id)
        )

        # XP + монеты
        conn.execute(
            'UPDATE user_stats SET xp = xp + ?, coins = coins + ? WHERE user_id = ?',
            (xp_amount, coins_amount, user_id)
        )
        conn.execute(
            'INSERT INTO xp_log (user_id, reason, ref_id, amount) VALUES (?, ?, ?, ?)',
            (user_id, 'chapter_complete', chapter_slug, xp_amount)
        )
        conn.commit()

        # Стрик чтения
        update_reading_streak(user_id, conn)
        # Дневные задания
        update_daily_quest_progress(user_id, 'chapters_today', conn)
        # Сезонные задания
        update_season_quest_progress(user_id, 'chapters_read', 1, conn)

        # Уровень и ачивки
        row = conn.execute('SELECT xp, level FROM user_stats WHERE user_id=?', (user_id,)).fetchone()
        new_xp = row['xp'] if row else xp_amount
        new_level = get_level_from_xp(new_xp)
        old_level = row['level'] if row else 1
        leveled_up = new_level > old_level
        if leveled_up:
            conn.execute('UPDATE user_stats SET level=? WHERE user_id=?', (new_level, user_id))
            conn.commit()
            create_site_notification(user_id, 'level_up', f'Уровень {new_level}!',
                                     f'Поздравляем с {new_level} уровнем!',
                                     f'/profile/{user_id}', conn=conn)
        new_achievements = check_achievements(user_id, conn)

        # Инвалидируем кеш
        with _stats_cache_lock:
            _stats_cache.pop(user_id, None)

        return jsonify({
            'ok': True,
            'xp': xp_amount,
            'coins': coins_amount,
            'total_xp': new_xp,
            'level': new_level,
            'leveled_up': leveled_up,
            'achievements': [a['name'] for a in new_achievements],
        })
    except Exception as e:
        logger.error(f'chapter_complete error: {e}')
        return jsonify({'ok': False, 'error': str(e)}), 500
    finally:
        conn.close()


@bp.route('/api/manga/<manga_slug>/similar')
def api_manga_similar(manga_slug):
    """Похожие манги: минимум 3 совпадающих жанра (tags)."""
    import json as _jsim
    conn = get_db()
    try:
        src = conn.execute(
            'SELECT manga_id, tags FROM manga WHERE manga_slug = ?', (manga_slug,)
        ).fetchone()
        if not src:
            return jsonify({'error': 'Not found'}), 404
        try:
            src_tags = set(_jsim.loads(src['tags'] or '[]'))
        except Exception:
            src_tags = set()
        if not src_tags:
            return jsonify({'results': []})

        rows = conn.execute(
            'SELECT manga_id, manga_title, manga_slug, cover_url, tags FROM manga WHERE manga_id != ?',
            (src['manga_id'],)
        ).fetchall()

        results = []
        for row in rows:
            try:
                other_tags = set(_jsim.loads(row['tags'] or '[]'))
            except Exception:
                other_tags = set()
            common = src_tags & other_tags
            if len(common) >= 3:
                results.append({
                    'title': row['manga_title'],
                    'slug': row['manga_slug'],
                    'cover': row['cover_url'],
                    'common_tags': sorted(common)[:6],
                    'common_count': len(common),
                })

        results.sort(key=lambda x: x['common_count'], reverse=True)
        return jsonify({'results': results[:40]})
    finally:
        conn.close()


def _refresh_manga_worker(slugs):
    """Фоновый поток: загрузить/обновить полные данные манг по slug-ам.

    Обновляем если:
      - манги нет в БД вообще, или
      - описание пустое (данные неполные), или
      - прошло больше 24 ч с последнего обновления.
    """
    threshold = datetime.now() - timedelta(hours=24)
    for slug in slugs:
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute(
                'SELECT manga_id, last_updated, description FROM manga WHERE manga_slug = ?',
                (slug,)
            )
            row = c.fetchone()
            conn.close()

            needs_refresh = True
            if row:
                # Пропускаем только если описание есть и данные свежие
                has_desc = bool((row['description'] or '').strip())
                if has_desc and row['last_updated']:
                    try:
                        last_upd = datetime.fromisoformat(row['last_updated'])
                        if last_upd > threshold:
                            needs_refresh = False
                    except Exception:
                        pass

            if not needs_refresh:
                continue

            fresh = api.fetch_manga(slug)
            if not fresh:
                continue

            # Сохраняем полные данные через общую функцию
            save_manga_details_to_db(fresh)
            logger.info(f"[bulk-refresh] обновлена манга {slug}")
        except Exception as e:
            logger.warning(f"[bulk-refresh] ошибка для {slug}: {e}")


@bp.route('/api/manga/bulk-refresh', methods=['POST'])
def api_manga_bulk_refresh():
    """API: обновить метаданные манг в фоне (TTL 24 ч).
    Body: {"slugs": ["slug1", "slug2", ...]}  (до 20 штук за раз)
    """
    data = request.get_json(silent=True) or {}
    slugs = [s for s in (data.get('slugs') or []) if isinstance(s, str)][:20]
    if slugs:
        t = threading.Thread(target=_refresh_manga_worker, args=(slugs,), daemon=True)
        t.start()
    return jsonify({'queued': len(slugs)})


@bp.route('/manga/<manga_slug>')
def manga_detail(manga_slug):
    """Детальная страница манги"""

    # Сначала пытаемся получить из БД
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT * FROM manga WHERE manga_slug = ?', (manga_slug,))
    manga_db = c.fetchone()
    conn.close()

    # Если манга не в БД вообще — нужен синхронный API-запрос
    if not manga_db:
        logger.info(f"📄 Манга {manga_slug} не в БД, загружаем через API")
        manga_details = get_manga_details_api(manga_slug)
        if not manga_details:
            return "Манга не найдена", 404
        manga_data = manga_details
    else:
        manga_data = dict(manga_db)
        # Фоновое обновление: если описание пустое или данные старше 1 дня
        needs_refresh = not (manga_db['description'] or '').strip()
        if not needs_refresh:
            last_updated = manga_db['last_updated']
            if last_updated:
                try:
                    needs_refresh = datetime.now() - datetime.fromisoformat(last_updated) > timedelta(days=1)
                except Exception:
                    pass
            else:
                needs_refresh = True
        if needs_refresh:
            logger.info(f"🔄 Фоновое обновление {manga_slug}")
            threading.Thread(
                target=get_manga_details_api, args=(manga_slug,), daemon=True
            ).start()

    # Десериализуем JSON-поля если они пришли из БД (строки)
    import json as _json
    for _field in ('tags', 'formats'):
        val = manga_data.get(_field)
        if isinstance(val, str):
            try:
                manga_data[_field] = _json.loads(val)
            except Exception:
                manga_data[_field] = []
        elif val is None:
            manga_data[_field] = []

    manga_id = manga_data.get('manga_id')

    # Берём только первые 50 глав для начального рендера (#15)
    chapters = []
    total_in_db = 0
    first_chapter = None
    last_chapter = None
    if manga_id:
        conn = get_db()
        c = conn.cursor()
        c.execute(
            '''SELECT chapter_id, chapter_slug, chapter_number, chapter_volume,
                      chapter_name, created_at, chapter_url
               FROM chapters
               WHERE manga_id = ?
               ORDER BY CAST(chapter_number AS FLOAT) DESC
               LIMIT 50''',
            (manga_id,)
        )
        chapters = [dict(row) for row in c.fetchall()]
        c.execute('SELECT COUNT(*) as cnt FROM chapters WHERE manga_id = ?', (manga_id,))
        total_in_db = c.fetchone()['cnt']
        # Самая первая и самая последняя глава для кнопок
        row = c.execute(
            '''SELECT chapter_slug, chapter_number FROM chapters WHERE manga_id = ?
               ORDER BY CAST(chapter_number AS FLOAT) ASC LIMIT 1''', (manga_id,)
        ).fetchone()
        if row:
            first_chapter = dict(row)
        row = c.execute(
            '''SELECT chapter_slug, chapter_number FROM chapters WHERE manga_id = ?
               ORDER BY CAST(chapter_number AS FLOAT) DESC LIMIT 1''', (manga_id,)
        ).fetchone()
        if row:
            last_chapter = dict(row)
        conn.close()

    # Запускаем фоновую загрузку глав если они неполные (#14)
    expected_chapters = manga_data.get('chapters_count', 0)
    is_loading_more = manga_slug in _manga_loading
    if manga_id and not is_loading_more and expected_chapters > 0 and total_in_db < expected_chapters:
        _manga_loading[manga_slug] = True
        threading.Thread(
            target=_bg_load_all_chapters,
            args=(manga_slug,),
            daemon=True
        ).start()
        is_loading_more = True
        logger.info(f"🔄 Фоновая загрузка глав: {manga_slug} ({total_in_db}/{expected_chapters})")

    # Проверяем подписку
    subscribed = False
    user_id = session.get('user_id')
    if user_id and manga_id:
        subscribed = is_subscribed(user_id, manga_id)
        # Дневное задание "открой 2 разные манги"
        try:
            _conn_dq = get_db()
            update_daily_quest_progress(user_id, 'manga_today', _conn_dq)
            _conn_dq.close()
        except Exception:
            pass

    # Проверяем историю чтения
    reading_history = None
    if user_id and manga_id:
        conn = get_db()
        c = conn.cursor()
        c.execute(
            '''SELECT rh.*, c.chapter_slug, c.chapter_number
               FROM reading_history rh
               JOIN chapters c ON rh.chapter_id = c.chapter_id
               WHERE rh.user_id = ? AND rh.manga_id = ?
               ORDER BY rh.last_read DESC LIMIT 1''',
            (user_id, manga_id)
        )
        history = c.fetchone()
        conn.close()
        if history:
            reading_history = dict(history)

    # Вишлист + личный статус + рейтинг
    in_wishlist = False
    user_manga_status = None
    user_manga_rating = None
    manga_rating_avg = None
    manga_rating_count = 0
    if manga_id:
        conn2 = get_db()
        if user_id:
            in_wishlist = bool(conn2.execute(
                'SELECT 1 FROM reading_wishlist WHERE user_id=? AND manga_id=?', (user_id, manga_id)
            ).fetchone())
            row = conn2.execute(
                'SELECT status FROM user_manga_status WHERE user_id=? AND manga_id=?', (user_id, manga_id)
            ).fetchone()
            if row:
                user_manga_status = row[0]
            rrow = conn2.execute(
                'SELECT score FROM manga_user_ratings WHERE user_id=? AND manga_id=?', (user_id, manga_id)
            ).fetchone()
            if rrow:
                user_manga_rating = rrow[0]
        arow = conn2.execute(
            'SELECT AVG(score) as avg, COUNT(*) as cnt FROM manga_user_ratings WHERE manga_id=?',
            (manga_id,)
        ).fetchone()
        if arow and arow['avg']:
            manga_rating_avg = round(arow['avg'], 2)
            manga_rating_count = arow['cnt']
        conn2.close()

    logger.info(
        f"📄 Рендер {manga_slug}: {len(chapters)} глав показано, "
        f"{total_in_db} в БД, {expected_chapters} ожидается"
    )

    return render_template('manga_detail.html',
                           manga=manga_data,
                           chapters=chapters,
                           first_chapter=first_chapter,
                           last_chapter=last_chapter,
                           subscribed=subscribed,
                           reading_history=reading_history,
                           is_loading_more=is_loading_more,
                           in_wishlist=in_wishlist,
                           user_manga_status=user_manga_status,
                           user_manga_rating=user_manga_rating,
                           manga_rating_avg=manga_rating_avg,
                           manga_rating_count=manga_rating_count,
                           user_id=user_id)

# ==================== ПРОФИЛИ / ТОП / МАГАЗИН ====================

@bp.route('/profile/me')
def profile_me():
    """Редирект на свой профиль"""
    user_id = session.get('user_id')
    if not user_id:
        return redirect(url_for('index'))
    return redirect(url_for('profile_page', user_id=user_id))


@bp.route('/profile/<int:user_id>')
def profile_page(user_id):
    """Публичная страница профиля"""
    profile_data = get_user_full_profile(user_id)
    if not profile_data:
        return "Пользователь не найден", 404

    viewer_id = session.get('user_id')
    is_own = (viewer_id == user_id)

    # Монеты зрителя для проверки в шаблоне
    viewer_coins = 0
    if viewer_id:
        stats = get_or_create_user_stats(viewer_id)
        viewer_coins = stats.get('coins', 0) if stats else 0

    return render_template('profile.html',
                           profile=profile_data,
                           is_own=is_own,
                           user_id=viewer_id,
                           viewer_coins=viewer_coins)


_TOP_ROW_SQL = '''SELECT u.id, u.telegram_first_name, u.telegram_username,
                  u.is_premium,
                  s.xp, s.level, s.total_chapters_read,
                  p.avatar_url,
                  p.background_url,
                  NULLIF(TRIM(COALESCE(p.custom_name, '')), '') as custom_name,
                  (SELECT si.css_value FROM shop_items si
                   JOIN user_items ui ON si.id = ui.item_id
                   WHERE ui.user_id = u.id AND ui.is_equipped = 1 AND si.type = 'frame'
                   LIMIT 1) as frame_css,
                  (SELECT si.preview_url FROM shop_items si
                   JOIN user_items ui ON si.id = ui.item_id
                   WHERE ui.user_id = u.id AND ui.is_equipped = 1 AND si.type = 'frame'
                   LIMIT 1) as frame_preview_url,
                  (SELECT COUNT(DISTINCT manga_id) FROM reading_history
                   WHERE user_id = u.id) as manga_count
           FROM users u
           JOIN user_stats s ON u.id = s.user_id
           LEFT JOIN user_profile p ON u.id = p.user_id'''


def _top_make_display(r):
    return (r.get('custom_name') or '').strip() or \
           r.get('telegram_first_name') or \
           r.get('telegram_username') or \
           f"#{r['id']}"


@cache.cached(timeout=120, key_prefix='top_leaders')
def _get_top_leaders():
    """Кешированный запрос топ-50 и числа пользователей (TTL 2 мин)."""
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT COUNT(*) as cnt FROM user_stats')
    _cnt_row = c.fetchone()
    total_users = (_cnt_row['cnt'] if _cnt_row else 1) or 1
    c.execute(_TOP_ROW_SQL + ' ORDER BY s.xp DESC LIMIT 50')
    rows = c.fetchall()
    leaders = []
    for row in rows:
        r = dict(row)
        r['display_name'] = _top_make_display(r)
        leaders.append(r)
    conn.close()
    return leaders, total_users


@bp.route('/top')
def top_page():
    """Таблица лидеров"""
    leaders, total_users = _get_top_leaders()
    top_ids = {r['id'] for r in leaders}

    user_id = session.get('user_id')
    my_rank_data = None
    if user_id and user_id not in top_ids:
        conn = get_db()
        c = conn.cursor()
        c.execute('''SELECT COUNT(*) + 1 AS rank FROM user_stats
                     WHERE xp > (SELECT xp FROM user_stats WHERE user_id = ?)''',
                  (user_id,))
        rank_row = c.fetchone()
        if rank_row:
            c.execute(_TOP_ROW_SQL + ' WHERE u.id = ?', (user_id,))
            ur = c.fetchone()
            if ur:
                my_data = dict(ur)
                my_data['display_name'] = _top_make_display(my_data)
                my_data['rank'] = rank_row['rank']
                my_rank_data = my_data
        conn.close()

    return render_template('top.html', leaders=leaders, user_id=user_id,
                           my_rank_data=my_rank_data, total_users=total_users)


@bp.route('/shop')
def shop_page():
    """Страница магазина"""
    user_id = session.get('user_id')

    conn = get_db()
    c = conn.cursor()

    # Все товары
    c.execute('SELECT * FROM shop_items ORDER BY type, price')
    items = [dict(row) for row in c.fetchall()]

    # Купленные товары текущего пользователя
    owned_ids = set()
    loaned_ids = set()
    equipped = {}
    coins = 0
    is_premium = 0
    premium_expires_at = None
    temp_expires = {}
    if user_id:
        c.execute('SELECT item_id, is_equipped, is_premium_loan, expires_at FROM user_items WHERE user_id = ?', (user_id,))
        for row in c.fetchall():
            owned_ids.add(row['item_id'])
            if row['is_premium_loan']:
                loaned_ids.add(row['item_id'])
            if row['is_equipped']:
                equipped[row['item_id']] = True
            if row['expires_at']:
                temp_expires[row['item_id']] = row['expires_at']
        c.execute('SELECT coins FROM user_stats WHERE user_id = ?', (user_id,))
        r = c.fetchone()
        coins = r['coins'] if r else 0
        c.execute('SELECT is_premium, premium_expires_at FROM users WHERE id = ?', (user_id,))
        ur = c.fetchone()
        if ur:
            is_premium = ur['is_premium']
            _exp = ur['premium_expires_at']
            premium_expires_at = _exp.isoformat() if hasattr(_exp, 'isoformat') else _exp

    conn.close()

    return render_template('shop.html',
                           items=items,
                           owned_ids=list(owned_ids),
                           loaned_ids=list(loaned_ids),
                           equipped=equipped,
                           coins=coins,
                           user_id=user_id,
                           is_premium=is_premium,
                           premium_expires_at=premium_expires_at,
                           temp_expires=temp_expires)


@bp.route('/api/shop/buy/<int:item_id>', methods=['POST'])
def shop_buy(item_id):
    """Купить товар из магазина"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401

    conn = get_db()
    c = conn.cursor()

    # Проверяем товар
    c.execute('SELECT * FROM shop_items WHERE id = ?', (item_id,))
    item = c.fetchone()
    if not item:
        conn.close()
        return jsonify({'error': 'Товар не найден'}), 404

    # Уже куплен?
    c.execute('SELECT id, is_premium_loan, expires_at FROM user_items WHERE user_id = ? AND item_id = ?', (user_id, item_id))
    existing = c.fetchone()
    now_iso = datetime.utcnow().isoformat()
    # Постоянный — блокируем
    if existing and not existing['is_premium_loan'] and not existing['expires_at']:
        conn.close()
        return jsonify({'error': 'Уже куплено навсегда'}), 400
    # Временный истёк — удалить старую запись
    if existing and existing['expires_at'] and existing['expires_at'] < now_iso:
        c.execute('DELETE FROM user_items WHERE user_id = ? AND item_id = ?', (user_id, item_id))
        existing = None

    # Проверяем монеты
    c.execute('INSERT OR IGNORE INTO user_stats (user_id) VALUES (?)', (user_id,))
    c.execute('SELECT coins FROM user_stats WHERE user_id = ?', (user_id,))
    row = c.fetchone()
    coins = row['coins'] if row else 0

    if coins < item['price']:
        conn.close()
        return jsonify({'error': 'Недостаточно монет'}), 400

    # Если был loan или temp — удалить (переход к постоянному)
    if existing:
        c.execute('DELETE FROM user_items WHERE user_id = ? AND item_id = ?', (user_id, item_id))

    # Списываем монеты и добавляем товар
    c.execute('UPDATE user_stats SET coins = coins - ? WHERE user_id = ?', (item['price'], user_id))
    if item['duration_days']:
        exp = (datetime.utcnow() + timedelta(days=item['duration_days'])).isoformat()
        c.execute('INSERT INTO user_items (user_id, item_id, expires_at) VALUES (?, ?, ?)', (user_id, item_id, exp))
    else:
        c.execute('INSERT INTO user_items (user_id, item_id) VALUES (?, ?)', (user_id, item_id))
    conn.commit()

    c.execute('SELECT coins FROM user_stats WHERE user_id = ?', (user_id,))
    new_coins = c.fetchone()['coins']
    conn.close()

    return jsonify({'success': True, 'coins': new_coins})


@bp.route('/api/shop/activate/<int:item_id>', methods=['POST'])
def shop_activate(item_id):
    """Premium: бесплатно активировать любой товар (loan)"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401

    conn = get_db()
    c = conn.cursor()

    # Проверяем Premium
    c.execute('SELECT is_premium, premium_expires_at FROM users WHERE id = ?', (user_id,))
    u = c.fetchone()
    now_dt = datetime.utcnow()
    expires_dt = _to_dt(u['premium_expires_at']) if u else None
    if not u or not u['is_premium'] or (expires_dt and expires_dt < now_dt):
        conn.close()
        return jsonify({'error': 'Требуется Premium подписка'}), 403

    # Проверяем товар
    c.execute('SELECT * FROM shop_items WHERE id = ?', (item_id,))
    item = c.fetchone()
    if not item:
        conn.close()
        return jsonify({'error': 'Товар не найден'}), 404

    # Проверяем, есть ли уже у пользователя
    c.execute('SELECT id, is_premium_loan FROM user_items WHERE user_id = ? AND item_id = ?', (user_id, item_id))
    existing = c.fetchone()
    if existing and not existing['is_premium_loan']:
        conn.close()
        return jsonify({'error': 'Уже куплено навсегда, предмет активен'}), 400
    if existing and existing['is_premium_loan']:
        conn.close()
        return jsonify({'error': 'Уже активировано'}), 400

    c.execute('INSERT INTO user_items (user_id, item_id, is_premium_loan) VALUES (?, ?, 1)', (user_id, item_id))
    conn.commit()
    conn.close()

    return jsonify({'success': True, 'activated': True})


@bp.route('/api/shop/packages')
def shop_packages():
    """Возвращает доступные пакеты монет за Stars"""
    return jsonify(COIN_PACKAGES)


@bp.route('/api/shop/create-invoice', methods=['POST'])
def shop_create_invoice():
    """Создаёт Telegram Stars invoice для покупки монет"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401

    data = request.get_json(silent=True) or {}
    package_id = data.get('package_id')
    pkg = next((p for p in COIN_PACKAGES if p['id'] == package_id), None)
    if not pkg:
        return jsonify({'error': 'Пакет не найден'}), 404

    if not _bot_loop or not _bot_loop.is_running() or not telegram_app:
        return jsonify({'error': 'Бот недоступен'}), 503

    payload = f"{pkg['id']}:{user_id}"

    async def _create_link():
        return await telegram_app.bot.create_invoice_link(
            title=pkg['label'],
            description=f"{pkg['coins']} монет для BubbleManga",
            payload=payload,
            currency="XTR",
            provider_token="",
            prices=[LabeledPrice(label=pkg['label'], amount=pkg['stars'])],
        )

    future = asyncio.run_coroutine_threadsafe(_create_link(), _bot_loop)
    try:
        url = future.result(timeout=10)
    except Exception as e:
        return jsonify({'error': f'Ошибка создания счёта: {e}'}), 500

    return jsonify({'url': url})


@bp.route('/api/user/balance')
def user_balance():
    """Возвращает текущий баланс монет пользователя"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT coins FROM user_stats WHERE user_id = ?', (user_id,))
    row = c.fetchone()
    conn.close()
    coins = row['coins'] if row else 0
    return jsonify({'coins': coins})


def _credit_coins(user_id, package_id, payment_id, payment_method='stars'):
    """Общая функция начисления монет после любой оплаты."""
    pkg = next((p for p in COIN_PACKAGES if p['id'] == package_id), None)
    if not pkg:
        return False
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute(
            '''INSERT OR IGNORE INTO coin_purchases
               (user_id, package_id, stars_paid, coins_received, payment_id, payment_method)
               VALUES (?, ?, 0, ?, ?, ?)''',
            (user_id, package_id, pkg['coins'], payment_id, payment_method)
        )
        credited = c.rowcount > 0
        if credited:
            c.execute('UPDATE user_stats SET coins = coins + ? WHERE user_id = ?',
                      (pkg['coins'], user_id))
        conn.commit()
        return credited
    finally:
        conn.close()


def _grant_premium(user_id, package_id, payment_id, payment_method='yookassa'):
    """Активирует/продлевает Premium после успешной оплаты."""
    pkg = next((p for p in PREMIUM_PACKAGES if p['id'] == package_id), None)
    if not pkg:
        return False
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('SELECT id FROM premium_purchases WHERE payment_id = ?', (payment_id,))
        if c.fetchone():
            return False  # уже обработано
        now = datetime.utcnow()
        c.execute('SELECT is_premium, premium_expires_at FROM users WHERE id = ?', (user_id,))
        row = c.fetchone()
        # Продлить если подписка ещё активна, иначе начать с сейчас
        if row and row['is_premium'] and row['premium_expires_at']:
            try:
                current_exp = _to_dt(row['premium_expires_at'])
                base = current_exp if current_exp > now else now
            except Exception:
                base = now
        else:
            base = now
        new_exp = base + timedelta(days=pkg['days'])
        c.execute(
            'UPDATE users SET is_premium=1, premium_expires_at=? WHERE id=?',
            (new_exp.isoformat(), user_id)
        )
        c.execute(
            'INSERT OR IGNORE INTO premium_purchases (user_id, package_id, payment_id, payment_method, expires_at) VALUES (?, ?, ?, ?, ?)',
            (user_id, package_id, payment_id, payment_method, new_exp.isoformat())
        )
        conn.commit()
        return True
    finally:
        conn.close()


@bp.route('/api/shop/create-payment', methods=['POST'])
def shop_create_payment():
    """Создаёт платёж через ЮКасса или CryptoBot."""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401

    data = request.get_json(silent=True) or {}
    package_id = data.get('package_id')
    method = data.get('method')

    pkg = next((p for p in COIN_PACKAGES if p['id'] == package_id), None)
    if not pkg:
        return jsonify({'error': 'Пакет не найден'}), 404

    if method == 'yookassa':
        if not YOOKASSA_SHOP_ID or not YOOKASSA_SECRET:
            return jsonify({'error': 'ЮКасса не настроена на сервере'}), 503
        try:
            from yookassa import Configuration, Payment as YKPayment
            import uuid as _uuid
            Configuration.account_id = YOOKASSA_SHOP_ID
            Configuration.secret_key = YOOKASSA_SECRET
            payment = YKPayment.create({
                'amount': {'value': str(pkg['rub']) + '.00', 'currency': 'RUB'},
                'confirmation': {'type': 'redirect',
                                 'return_url': f'{SITE_URL}/shop?tab=buy&paid=1'},
                'capture': True,
                'description': f"{pkg['label']} — BubbleManga",
                'metadata': {'package_id': pkg['id'], 'user_id': str(user_id)},
            }, str(_uuid.uuid4()))
            return jsonify({'url': payment.confirmation.confirmation_url})
        except ImportError:
            return jsonify({'error': 'Библиотека yookassa не установлена'}), 503
        except Exception as e:
            return jsonify({'error': f'Ошибка ЮКасса: {e}'}), 500

    elif method == 'crypto':
        if not CRYPTOCLOUD_API_KEY or not CRYPTOCLOUD_SHOP_ID:
            return jsonify({'error': 'Crypto Cloud не настроен на сервере'}), 503
        try:
            import requests as _req
            resp = _req.post(
                'https://api.cryptocloud.plus/v2/invoice/create',
                headers={
                    'Authorization': f'Token {CRYPTOCLOUD_API_KEY}',
                    'Content-Type': 'application/json',
                },
                json={
                    'shop_id': CRYPTOCLOUD_SHOP_ID,
                    'amount': float(pkg['usd']),
                    'currency': 'USD',
                    'order_id': f"{pkg['id']}:{user_id}",
                },
                timeout=10
            )
            result = resp.json()
            if resp.status_code != 200 or result.get('status') == 'error':
                return jsonify({'error': 'Ошибка Crypto Cloud: ' + str(result)}), 500
            return jsonify({'url': result['result']['link']})
        except ImportError:
            return jsonify({'error': 'Библиотека requests не установлена'}), 503
        except Exception as e:
            return jsonify({'error': f'Ошибка Crypto Cloud: {e}'}), 500

    return jsonify({'error': 'Неизвестный способ оплаты'}), 400


@bp.route('/api/shop/create-premium-payment', methods=['POST'])
def shop_create_premium_payment():
    """Создаёт платёж за Premium подписку через ЮКасса или Crypto Cloud."""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401

    data = request.get_json(silent=True) or {}
    package_id = data.get('package_id')
    method = data.get('method')

    pkg = next((p for p in PREMIUM_PACKAGES if p['id'] == package_id), None)
    if not pkg:
        return jsonify({'error': 'Пакет не найден'}), 404

    if method == 'yookassa':
        if not YOOKASSA_SHOP_ID or not YOOKASSA_SECRET:
            return jsonify({'error': 'ЮКасса не настроена на сервере'}), 503
        try:
            from yookassa import Configuration, Payment as YKPayment
            import uuid as _uuid
            Configuration.account_id = YOOKASSA_SHOP_ID
            Configuration.secret_key = YOOKASSA_SECRET
            payment = YKPayment.create({
                'amount': {'value': str(pkg['rub']) + '.00', 'currency': 'RUB'},
                'confirmation': {'type': 'redirect',
                                 'return_url': f'{SITE_URL}/shop?tab=premium&paid=1'},
                'capture': True,
                'description': f"{pkg['label']} — BubbleManga",
                'metadata': {'premium_package_id': pkg['id'], 'user_id': str(user_id)},
            }, str(_uuid.uuid4()))
            return jsonify({'url': payment.confirmation.confirmation_url})
        except ImportError:
            return jsonify({'error': 'Библиотека yookassa не установлена'}), 503
        except Exception as e:
            return jsonify({'error': f'Ошибка ЮКасса: {e}'}), 500

    elif method == 'crypto':
        if not CRYPTOCLOUD_API_KEY or not CRYPTOCLOUD_SHOP_ID:
            return jsonify({'error': 'Crypto Cloud не настроен на сервере'}), 503
        try:
            import requests as _req
            resp = _req.post(
                'https://api.cryptocloud.plus/v2/invoice/create',
                headers={
                    'Authorization': f'Token {CRYPTOCLOUD_API_KEY}',
                    'Content-Type': 'application/json',
                },
                json={
                    'shop_id': CRYPTOCLOUD_SHOP_ID,
                    'amount': float(pkg['usd']),
                    'currency': 'USD',
                    'order_id': f"premium:{pkg['id']}:{user_id}",
                },
                timeout=10
            )
            result = resp.json()
            if resp.status_code != 200 or result.get('status') == 'error':
                return jsonify({'error': 'Ошибка Crypto Cloud: ' + str(result)}), 500
            return jsonify({'url': result['result']['link']})
        except ImportError:
            return jsonify({'error': 'Библиотека requests не установлена'}), 503
        except Exception as e:
            return jsonify({'error': f'Ошибка Crypto Cloud: {e}'}), 500

    return jsonify({'error': 'Неизвестный способ оплаты'}), 400


@bp.route('/api/shop/gift-premium', methods=['POST'])
def shop_gift_premium():
    """Создаёт Stars invoice для подарочного Premium. {recipient_id, days} → {url}"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401

    data = request.get_json(silent=True) or {}
    recipient_id = data.get('recipient_id')
    days = data.get('days')

    if not recipient_id or not days:
        return jsonify({'error': 'recipient_id и days обязательны'}), 400
    try:
        recipient_id = int(recipient_id)
        days = int(days)
    except (ValueError, TypeError):
        return jsonify({'error': 'Неверные параметры'}), 400
    if days not in (30, 90, 365):
        return jsonify({'error': 'days должен быть 30, 90 или 365'}), 400
    if recipient_id == user_id:
        return jsonify({'error': 'Нельзя подарить самому себе'}), 400

    # Проверяем получателя
    conn = get_db()
    c = conn.cursor()
    rec = c.execute('SELECT id, telegram_first_name, telegram_username FROM users WHERE id=?', (recipient_id,)).fetchone()
    conn.close()
    if not rec:
        return jsonify({'error': 'Получатель не найден'}), 404

    label_map = {30: '1 месяц', 90: '3 месяца', 365: '1 год'}
    stars_map = {30: 50, 90: 130, 365: 450}
    label = label_map[days]
    stars = stars_map[days]
    recipient_name = rec['telegram_first_name'] or rec['telegram_username'] or f'ID {recipient_id}'
    payload = f'gift_premium:{recipient_id}:{days}:{user_id}'

    try:
        bot = telegram_app.bot if telegram_app else None
        if not bot:
            return jsonify({'error': 'Бот недоступен'}), 503
        if _bot_loop and _bot_loop.is_running():
            future = asyncio.run_coroutine_threadsafe(
                bot.create_invoice_link(
                    title=f'Premium на {label} для {recipient_name}',
                    description=f'Подарок Premium BubbleManga на {label}',
                    payload=payload,
                    currency='XTR',
                    provider_token='',
                    prices=[LabeledPrice(label=f'Premium {label}', amount=stars)],
                ),
                _bot_loop
            )
            url = future.result(timeout=10)
            return jsonify({'url': url})
        else:
            return jsonify({'error': 'Бот не запущен'}), 503
    except Exception as e:
        return jsonify({'error': f'Ошибка создания инвойса: {e}'}), 500


@bp.route('/webhook/yookassa', methods=['POST'])
def webhook_yookassa():
    """Вебхук от ЮКасса — зачисляет монеты или активирует Premium после успешной оплаты."""
    data = request.get_json(silent=True) or {}
    if data.get('event') != 'payment.succeeded':
        return '', 200
    obj = data.get('object', {})
    meta = obj.get('metadata', {})
    package_id = meta.get('package_id')
    premium_package_id = meta.get('premium_package_id')
    user_id_str = meta.get('user_id')
    payment_id = obj.get('id')
    if user_id_str and payment_id:
        if premium_package_id:
            _grant_premium(int(user_id_str), premium_package_id, f'yk_{payment_id}', 'yookassa')
        elif package_id:
            _credit_coins(int(user_id_str), package_id, f'yk_{payment_id}', 'yookassa')
    return '', 200


@bp.route('/webhook/cryptocloud', methods=['POST'])
def webhook_cryptocloud():
    """Вебхук от Crypto Cloud — зачисляет монеты после оплаты."""
    data = request.form.to_dict() if request.content_type and 'form' in request.content_type \
        else (request.get_json(silent=True) or {})

    if data.get('status') != 'success':
        return '', 200

    # Верификация JWT-токена (HS256, подписан SECRET KEY проекта)
    token = data.get('token', '')
    if CRYPTOCLOUD_SECRET_KEY and token:
        try:
            import jwt as _jwt
            _jwt.decode(token, CRYPTOCLOUD_SECRET_KEY, algorithms=['HS256'])
        except Exception:
            return '', 403

    order_id = data.get('order_id', '')
    invoice_id = str(data.get('invoice_id', ''))

    try:
        if order_id.startswith('premium:'):
            _, package_id, user_id_str = order_id.split(':', 2)
            _grant_premium(int(user_id_str), package_id, f'cc_{invoice_id}', 'crypto')
        else:
            package_id, user_id_str = order_id.rsplit(':', 1)
            _credit_coins(int(user_id_str), package_id, f'cc_{invoice_id}', 'crypto')
    except (ValueError, AttributeError):
        pass

    return '', 200


# ==================== WISHLIST ====================

@bp.route('/api/wishlist/<manga_id>', methods=['POST'])
def toggle_wishlist(manga_id):
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    conn = get_db()
    existing = conn.execute(
        'SELECT id FROM reading_wishlist WHERE user_id=? AND manga_id=?', (user_id, manga_id)
    ).fetchone()
    if existing:
        conn.execute('DELETE FROM reading_wishlist WHERE user_id=? AND manga_id=?', (user_id, manga_id))
        in_wishlist = False
    else:
        conn.execute('INSERT OR IGNORE INTO reading_wishlist (user_id, manga_id) VALUES (?,?)', (user_id, manga_id))
        in_wishlist = True
    conn.commit()
    conn.close()
    return jsonify({'in_wishlist': in_wishlist})


_MANGA_STATUS_LABELS = {
    'reading':   'Читаю',
    'completed': 'Прочитано',
    'planned':   'В планах',
    'dropped':   'Брошено',
    'paused':    'Отложено',
}

@bp.route('/api/manga-status/<manga_id>', methods=['POST'])
def set_manga_status(manga_id):
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    status = (request.json or {}).get('status')
    if status and status not in _MANGA_STATUS_LABELS:
        return jsonify({'error': 'Неверный статус'}), 400
    conn = get_db()
    if status:
        conn.execute(
            'INSERT INTO user_manga_status (user_id, manga_id, status) VALUES (?,?,?) '
            'ON CONFLICT(user_id, manga_id) DO UPDATE SET status=excluded.status, updated_at=CURRENT_TIMESTAMP',
            (user_id, manga_id, status)
        )
    else:
        conn.execute('DELETE FROM user_manga_status WHERE user_id=? AND manga_id=?', (user_id, manga_id))
    conn.commit()
    conn.close()
    return jsonify({'status': status})


@bp.route('/api/manga-statuses')
def get_manga_statuses():
    """Batch-получение статусов для списка манг текущего пользователя"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({})
    ids_raw = request.args.get('ids', '')
    manga_ids = [i.strip() for i in ids_raw.split(',') if i.strip()][:100]
    if not manga_ids:
        return jsonify({})
    conn = get_db()
    placeholders = ','.join(['?' for _ in manga_ids])
    rows = conn.execute(
        f'SELECT manga_id, status FROM user_manga_status WHERE user_id=? AND manga_id IN ({placeholders})',
        [user_id] + manga_ids
    ).fetchall()
    conn.close()
    return jsonify({r['manga_id']: r['status'] for r in rows})


@bp.route('/api/user/reading-list')
def api_user_reading_list():
    """Список манги пользователя по статусу чтения"""
    uid = request.args.get('uid', type=int)
    target_id = uid if uid else session.get('user_id')
    if not target_id:
        return jsonify([])
    status = request.args.get('status')
    conn = get_db()
    q = '''SELECT m.manga_id, m.manga_slug, m.manga_title, m.cover_url, m.manga_type,
                  ums.status, ums.updated_at
           FROM user_manga_status ums
           JOIN manga m ON ums.manga_id = m.manga_id
           WHERE ums.user_id=?'''
    params = [target_id]
    if status:
        q += ' AND ums.status=?'
        params.append(status)
    q += ' ORDER BY ums.updated_at DESC'
    rows = conn.execute(q, params).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@bp.route('/api/manga/<manga_id>/rate', methods=['POST'])
def api_manga_rate(manga_id):
    """Оценить мангу (1–10)"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    data = request.json or {}
    score = data.get('score')
    if not isinstance(score, int) or not (1 <= score <= 10):
        return jsonify({'error': 'Оценка должна быть от 1 до 10'}), 400
    conn = get_db()
    try:
        existing = conn.execute(
            'SELECT id FROM manga_user_ratings WHERE user_id=? AND manga_id=?', (user_id, manga_id)
        ).fetchone()
        if existing:
            conn.execute(
                'UPDATE manga_user_ratings SET score=?, updated_at=CURRENT_TIMESTAMP WHERE user_id=? AND manga_id=?',
                (score, user_id, manga_id)
            )
        else:
            conn.execute(
                'INSERT INTO manga_user_ratings (user_id, manga_id, score) VALUES (?,?,?)',
                (user_id, manga_id, score)
            )
        conn.commit()
        row = conn.execute(
            'SELECT AVG(score) as avg, COUNT(*) as cnt FROM manga_user_ratings WHERE manga_id=?',
            (manga_id,)
        ).fetchone()
        conn.close()
        return jsonify({'ok': True, 'avg': round(row['avg'], 2) if row['avg'] else None, 'count': row['cnt']})
    except Exception as e:
        conn.close()
        logger.error(f'api_manga_rate error: {e}')
        return jsonify({'error': str(e)}), 500


@bp.route('/api/manga/<manga_id>/recommend', methods=['POST'])
def api_manga_recommend(manga_id):
    """Рекомендовать мангу другому пользователю"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    data = request.json or {}
    target_user_id = data.get('target_user_id')
    message = (data.get('message') or '').strip()[:200]
    if not target_user_id:
        return jsonify({'error': 'Не указан получатель'}), 400
    conn = get_db()
    manga = conn.execute('SELECT manga_slug, manga_title FROM manga WHERE manga_id=?', (manga_id,)).fetchone()
    if not manga:
        conn.close()
        return jsonify({'error': 'Манга не найдена'}), 404
    sender = conn.execute(
        '''SELECT u.id, COALESCE(up.custom_name, u.telegram_first_name, u.telegram_username, 'Пользователь') as name
           FROM users u LEFT JOIN user_profile up ON up.user_id = u.id
           WHERE u.id=?''', (user_id,)).fetchone()
    title = f'{sender["name"]} рекомендует мангу'
    body = manga['manga_title']
    if message:
        body += f' — «{message}»'
    create_site_notification(
        target_user_id, 'manga_recommend', title, body,
        f'/profile/{target_user_id}?tab=library&lib=recommendations',
        ref_id=manga['manga_slug'], conn=conn
    )
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


@bp.route('/api/user/friend-recommendations')
def api_friend_recommendations():
    """Рекомендации манги от друзей текущего пользователя"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    conn = get_db()
    # ref_id хранит manga_slug (для новых), fallback — парсим URL (для старых)
    if _USE_PG:
        _slug_expr = "SPLIT_PART(REPLACE(sn.url, '/manga/', ''), '?', 1)"
    else:
        _slug_expr = "REPLACE(REPLACE(sn.url, '/manga/', ''), SUBSTR(sn.url, INSTR(sn.url, '?')), '')"
    rows = conn.execute(
        f'''SELECT sn.id, sn.title, sn.body, sn.url, sn.ref_id, sn.created_at, sn.is_read,
                  m.manga_id, m.manga_slug, m.manga_title, m.cover_url
           FROM site_notifications sn
           LEFT JOIN manga m ON m.manga_slug = COALESCE(
               NULLIF(sn.ref_id, ''),
               {_slug_expr}
           )
           WHERE sn.user_id=? AND sn.type='manga_recommend'
           ORDER BY sn.created_at DESC
           LIMIT 50''',
        (user_id,)
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        title = d.get('title', '') or ''
        body  = d.get('body',  '') or ''
        sender_name = title.replace(' рекомендует мангу', '') if ' рекомендует мангу' in title else title
        message = ''
        manga_title_parsed = body
        if ' — «' in body:
            manga_title_parsed, rest = body.split(' — «', 1)
            message = rest.rstrip('»')
        # slug: сначала ref_id, потом из url
        slug = d.get('manga_slug') or d.get('ref_id') or ''
        if not slug and d.get('url'):
            raw = d['url'].split('?')[0].replace('/manga/', '')
            if raw and not raw.startswith('/'):
                slug = raw
        result.append({
            'id': d['id'],
            'manga_id':    d.get('manga_id'),
            'manga_slug':  slug,
            'manga_title': d.get('manga_title') or manga_title_parsed,
            'cover_url':   d.get('cover_url') or '',
            'sender_name': sender_name,
            'message':     message,
            'created_at':  d['created_at'],
            'is_read':     d['is_read'],
        })
    return jsonify({'recommendations': result})


@bp.route('/api/user/wishlist')
def api_user_wishlist():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    conn = get_db()
    rows = conn.execute(
        '''SELECT m.manga_id, m.manga_slug, m.manga_title, m.cover_url, m.manga_type,
                  m.manga_status, rw.added_at
           FROM reading_wishlist rw
           JOIN manga m ON rw.manga_id = m.manga_id
           WHERE rw.user_id=?
           ORDER BY rw.added_at DESC''',
        (user_id,)
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


# ==================== COMMENT LIKES ====================

@bp.route('/api/comments/<int:comment_id>/like', methods=['POST'])
def toggle_comment_like(comment_id):
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    conn = get_db()
    existing = conn.execute(
        'SELECT id FROM comment_likes WHERE user_id=? AND comment_id=?', (user_id, comment_id)
    ).fetchone()
    if existing:
        conn.execute('DELETE FROM comment_likes WHERE user_id=? AND comment_id=?', (user_id, comment_id))
        liked = False
    else:
        conn.execute('INSERT OR IGNORE INTO comment_likes (user_id, comment_id) VALUES (?,?)', (user_id, comment_id))
        liked = True
    conn.commit()
    likes_count = conn.execute(
        'SELECT COUNT(*) FROM comment_likes WHERE comment_id=?', (comment_id,)
    ).fetchone()[0]
    conn.close()
    return jsonify({'liked': liked, 'likes_count': likes_count})


# ==================== REFERRAL ====================

@bp.route('/api/profile/referral')
def api_referral():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    conn = get_db()
    user = conn.execute('SELECT referral_code FROM users WHERE id=?', (user_id,)).fetchone()
    code = user['referral_code'] if user else None
    if not code:
        code = secrets.token_urlsafe(6).upper()
        conn.execute('UPDATE users SET referral_code=? WHERE id=?', (code, user_id))
        conn.commit()
    count = conn.execute('SELECT COUNT(*) FROM referrals WHERE referrer_id=?', (user_id,)).fetchone()[0]
    rewarded = conn.execute(
        'SELECT SUM(100) as total FROM referrals WHERE referrer_id=? AND rewarded=1', (user_id,)
    ).fetchone()
    coins_earned = rewarded[0] or 0
    conn.close()
    referral_url = f"https://t.me/bubblemanga_bot?start={code}"
    return jsonify({'code': code, 'referral_url': referral_url, 'count': count, 'coins_earned': coins_earned})


# ==================== DAILY QUESTS / SEASON API ====================

@bp.route('/api/daily-quests')
def api_daily_quests():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    today = _date.today().isoformat()
    conn = get_db()
    get_or_create_daily_quests(user_id, conn)
    rows = conn.execute(
        '''SELECT dq.id, dq.title, dq.description, dq.icon, dq.condition_value,
                  dq.xp_reward, dq.coins_reward,
                  udq.progress, udq.completed_at
           FROM daily_quests dq
           LEFT JOIN user_daily_quests udq ON dq.id = udq.quest_id
               AND udq.user_id=? AND udq.date=?
           WHERE dq.is_active=1''',
        (user_id, today)
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        d['progress'] = d['progress'] or 0
        d['completed'] = bool(d['completed_at'])
        result.append(d)
    return jsonify(result)


@bp.route('/api/season')
def api_season():
    user_id = session.get('user_id')
    conn = get_db()
    season = get_active_season(conn)
    if not season:
        conn.close()
        return jsonify(None)
    season_dict = dict(season)
    quests = conn.execute('SELECT * FROM season_quests WHERE season_id=?', (season['id'],)).fetchall()
    result_quests = []
    for q in quests:
        qd = dict(q)
        if user_id:
            row = conn.execute(
                'SELECT progress, completed_at FROM user_season_quests WHERE user_id=? AND season_quest_id=?',
                (user_id, q['id'])
            ).fetchone()
            qd['progress'] = row['progress'] if row else 0
            qd['completed'] = bool(row['completed_at']) if row else False
        else:
            qd['progress'] = 0
            qd['completed'] = False
        result_quests.append(qd)
    conn.close()
    season_dict['quests'] = result_quests
    return jsonify(season_dict)


# ==================== КОММЕНТАРИИ ====================

_COMMENT_QUERY = '''
    SELECT cm.id, cm.parent_id, cm.text, cm.created_at,
           u.id as user_id, u.telegram_first_name, u.telegram_username, u.is_premium,
           p.custom_name, p.avatar_url, p.custom_avatar_url, s.level,
           (SELECT si.css_value FROM shop_items si
            JOIN user_items ui ON si.id = ui.item_id
            WHERE ui.user_id = u.id AND ui.is_equipped = 1 AND si.type = 'frame'
            LIMIT 1) as frame_css,
           (SELECT COUNT(*) FROM comment_likes cl WHERE cl.comment_id = cm.id) as likes_count
    FROM comments cm
    JOIN users u ON cm.user_id = u.id
    LEFT JOIN user_profile p ON u.id = p.user_id
    LEFT JOIN user_stats s ON u.id = s.user_id
'''


@bp.route('/api/manga/<manga_slug>/comments')
def get_comments(manga_slug):
    offset = max(0, int(request.args.get('offset', 0)))
    limit  = min(50, max(1, int(request.args.get('limit', 20))))
    sort   = request.args.get('sort', 'new')  # 'new' | 'top'
    viewer_id = session.get('user_id')
    conn = get_db()
    c = conn.cursor()

    order_clause = 'ORDER BY likes_count DESC, cm.created_at DESC' if sort == 'top' else 'ORDER BY cm.created_at DESC'

    # Верхнеуровневые комментарии (без ответов)
    c.execute(
        _COMMENT_QUERY + f'WHERE cm.manga_slug = ? AND cm.parent_id IS NULL {order_clause} LIMIT ? OFFSET ?',
        (manga_slug, limit, offset)
    )
    top_comments = [dict(r) for r in c.fetchall()]

    # Количество верхнеуровневых (для пагинации)
    c.execute('SELECT COUNT(*) FROM comments WHERE manga_slug = ? AND parent_id IS NULL', (manga_slug,))
    top_total = c.fetchone()[0]

    # Общее количество (включая ответы — для счётчика)
    c.execute('SELECT COUNT(*) FROM comments WHERE manga_slug = ?', (manga_slug,))
    total_all = c.fetchone()[0]

    # liked_by_me для верхнеуровневых
    if viewer_id and top_comments:
        liked_set = {
            r[0] for r in c.execute(
                f'SELECT comment_id FROM comment_likes WHERE user_id=? AND comment_id IN ({",".join("?"*len(top_comments))})',
                (viewer_id, *[cmt['id'] for cmt in top_comments])
            ).fetchall()
        }
        for cmt in top_comments:
            cmt['liked_by_me'] = cmt['id'] in liked_set
    else:
        for cmt in top_comments:
            cmt['liked_by_me'] = False

    # Загрузить ответы для этих комментариев одним запросом
    if top_comments:
        parent_ids = [cmt['id'] for cmt in top_comments]
        placeholders = ','.join('?' * len(parent_ids))
        c.execute(
            _COMMENT_QUERY + f'WHERE cm.parent_id IN ({placeholders}) ORDER BY cm.created_at ASC',
            parent_ids
        )
        replies = [dict(r) for r in c.fetchall()]
        if viewer_id and replies:
            liked_replies = {
                r[0] for r in c.execute(
                    f'SELECT comment_id FROM comment_likes WHERE user_id=? AND comment_id IN ({",".join("?"*len(replies))})',
                    (viewer_id, *[r['id'] for r in replies])
                ).fetchall()
            }
            for r in replies:
                r['liked_by_me'] = r['id'] in liked_replies
        else:
            for r in replies:
                r['liked_by_me'] = False
        reply_map = {}
        for r in replies:
            reply_map.setdefault(r['parent_id'], []).append(r)
        for cmt in top_comments:
            cmt['replies'] = reply_map.get(cmt['id'], [])

    conn.close()
    return jsonify({
        'comments': top_comments,
        'total': total_all,
        'has_more': offset + limit < top_total
    })


@bp.route('/api/manga/<manga_slug>/comments', methods=['POST'])
@rate_limit(10, 60)
def post_comment(manga_slug):
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    body = request.json or {}
    text = body.get('text', '').strip()
    parent_id = body.get('parent_id')
    if not text:
        return jsonify({'error': 'Пустой комментарий'}), 400
    if len(text) > 1000:
        return jsonify({'error': 'Максимум 1000 символов'}), 400

    conn = get_db()
    c = conn.cursor()

    # Проверить parent_id и «выровнять» до верхнего уровня
    if parent_id:
        c.execute('SELECT id, parent_id, manga_slug FROM comments WHERE id = ?', (parent_id,))
        parent_row = c.fetchone()
        if not parent_row or parent_row['manga_slug'] != manga_slug:
            conn.close()
            return jsonify({'error': 'Родительский комментарий не найден'}), 404
        # Ответ на ответ → прикрепить к верхнему родителю
        if parent_row['parent_id'] is not None:
            parent_id = parent_row['parent_id']

    c.execute('INSERT INTO comments (manga_slug, user_id, text, parent_id) VALUES (?, ?, ?, ?)',
              (manga_slug, user_id, text, parent_id))
    comment_id = c.lastrowid
    conn.commit()
    check_quests(user_id, conn)
    update_daily_quest_progress(user_id, 'comments_today', conn)
    update_season_quest_progress(user_id, 'comments_posted', 1, conn)
    c.execute(_COMMENT_QUERY + 'WHERE cm.id = ?', (comment_id,))
    comment = dict(c.fetchone())
    conn.close()
    return jsonify({'success': True, 'comment': comment})


@bp.route('/api/comments/<int:comment_id>', methods=['DELETE'])
def delete_comment(comment_id):
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT user_id, parent_id FROM comments WHERE id = ?', (comment_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        return jsonify({'error': 'Не найдено'}), 404
    c.execute('SELECT telegram_id FROM users WHERE id = ?', (user_id,))
    u = c.fetchone()
    is_admin = u and u['telegram_id'] in ADMIN_TELEGRAM_IDS
    if row['user_id'] != user_id and not is_admin:
        conn.close()
        return jsonify({'error': 'Нет доступа'}), 403
    # Удалить сам комментарий и все ответы на него
    c.execute('DELETE FROM comments WHERE id = ? OR parent_id = ?', (comment_id, comment_id))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@bp.route('/api/profile/equip/<int:item_id>', methods=['POST'])
def profile_equip(item_id):
    """Надеть / снять украшение"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401

    conn = get_db()
    c = conn.cursor()

    c.execute('SELECT ui.*, si.type FROM user_items ui JOIN shop_items si ON ui.item_id = si.id'
              ' WHERE ui.user_id = ? AND ui.item_id = ?', (user_id, item_id))
    ui = c.fetchone()
    if not ui:
        conn.close()
        return jsonify({'error': 'Товар не куплен'}), 403

    item_type = ui['type']
    now_equipped = ui['is_equipped']

    c.execute('INSERT OR IGNORE INTO user_profile (user_id) VALUES (?)', (user_id,))

    if now_equipped:
        # ── Снять ──────────────────────────────────────────────────────────
        c.execute('UPDATE user_items SET is_equipped = 0 WHERE user_id = ? AND item_id = ?',
                  (user_id, item_id))
        col_map = {'frame': 'frame_item_id', 'badge': 'badge_item_id', 'title': 'title_item_id'}
        if item_type in col_map:
            c.execute(f'UPDATE user_profile SET {col_map[item_type]} = NULL WHERE user_id = ?',
                      (user_id,))
        elif item_type == 'avatar':
            # Восстановить кастомный аватар если был
            c.execute('SELECT custom_avatar_url FROM user_profile WHERE user_id = ?', (user_id,))
            row = c.fetchone()
            custom = row['custom_avatar_url'] if row else None
            c.execute('UPDATE user_profile SET avatar_url = ? WHERE user_id = ?', (custom, user_id))
        elif item_type == 'background':
            # Восстановить кастомный фон если был
            c.execute('SELECT custom_avatar_url FROM user_profile WHERE user_id = ?', (user_id,))
            # background_url просто обнуляем (кастомный фон тоже пропадёт — приемлемо)
            c.execute('UPDATE user_profile SET background_url = NULL WHERE user_id = ?', (user_id,))
    else:
        # ── Надеть ─────────────────────────────────────────────────────────
        # Снимаем другие того же типа
        c.execute(
            '''UPDATE user_items SET is_equipped = 0
               WHERE user_id = ? AND item_id IN (
                   SELECT ui2.item_id FROM user_items ui2
                   JOIN shop_items si2 ON ui2.item_id = si2.id
                   WHERE ui2.user_id = ? AND si2.type = ?
               )''',
            (user_id, user_id, item_type)
        )
        c.execute('UPDATE user_items SET is_equipped = 1 WHERE user_id = ? AND item_id = ?',
                  (user_id, item_id))
        col_map = {'frame': 'frame_item_id', 'badge': 'badge_item_id', 'title': 'title_item_id'}
        if item_type in col_map:
            c.execute(f'UPDATE user_profile SET {col_map[item_type]} = ? WHERE user_id = ?',
                      (item_id, user_id))
        elif item_type == 'avatar':
            # Получаем оригинальный URL товара и ставим как аватар
            c.execute('SELECT preview_url, full_url FROM shop_items WHERE id = ?', (item_id,))
            si = c.fetchone()
            avatar_url = (si['full_url'] or si['preview_url']) if si else None
            c.execute('UPDATE user_profile SET avatar_url = ? WHERE user_id = ?',
                      (avatar_url, user_id))
        elif item_type == 'background':
            # Получаем оригинальный URL товара и ставим как фон
            c.execute('SELECT preview_url, full_url, css_value FROM shop_items WHERE id = ?', (item_id,))
            si = c.fetchone()
            bg_url = (si['full_url'] or si['preview_url']) if si else None
            c.execute('UPDATE user_profile SET background_url = ? WHERE user_id = ?',
                      (bg_url, user_id))

    conn.commit()

    # Для аватара возвращаем новый URL чтобы обновить хедер без перезагрузки
    extra = {}
    if item_type == 'avatar':
        if not now_equipped:
            c2 = conn.cursor() if False else get_db().cursor()
            conn2 = get_db()
            c2 = conn2.cursor()
            c2.execute('SELECT avatar_url FROM user_profile WHERE user_id = ?', (user_id,))
            row2 = c2.fetchone()
            extra['avatar_url'] = row2['avatar_url'] if row2 else None
            conn2.close()
        else:
            extra['avatar_url'] = None  # сброс — нет аватара из магазина

    conn.close()
    return jsonify({'success': True, 'equipped': not now_equipped, **extra})


@bp.route('/api/profile/update', methods=['POST'])
@rate_limit(10, 60)
def profile_update():
    """Обновить bio и/или имя профиля"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401

    data = request.json or {}
    bio = data.get('bio', '')[:300]
    custom_name = data.get('custom_name', '').strip()[:50]
    conn = get_db()
    c = conn.cursor()
    c.execute('INSERT OR IGNORE INTO user_profile (user_id) VALUES (?)', (user_id,))

    # Проверяем ограничение смены ника (2 раза в месяц)
    current_month = datetime.utcnow().strftime('%Y-%m')
    c.execute('SELECT custom_name, name_change_count, name_change_month FROM user_profile WHERE user_id = ?', (user_id,))
    row = c.fetchone()
    old_name = (row['custom_name'] or '') if row else ''
    name_changing = custom_name != old_name

    if name_changing and custom_name:  # смена ника только если новое значение непустое
        count = row['name_change_count'] or 0 if row else 0
        month = row['name_change_month'] if row else None
        if month != current_month:
            count = 0  # новый месяц — сбрасываем счётчик
        if count >= 2:
            conn.close()
            return jsonify({'error': 'Ник можно менять не более 2 раз в месяц', 'name_limit': True}), 429
        c.execute(
            'UPDATE user_profile SET bio = ?, custom_name = ?, name_change_count = ?, name_change_month = ? WHERE user_id = ?',
            (bio, custom_name, count + 1, current_month, user_id)
        )
    else:
        c.execute('UPDATE user_profile SET bio = ? WHERE user_id = ?', (bio, user_id))

    conn.commit()
    conn.close()
    display_name = custom_name or None
    return jsonify({'success': True, 'display_name': display_name})


@bp.route('/upload/avatar', methods=['POST'])
@rate_limit(5, 60)
def upload_avatar():
    """Загрузить аватар (только Premium)"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401

    conn = get_db()
    c = conn.cursor()

    c.execute('SELECT is_premium FROM users WHERE id = ?', (user_id,))
    u = c.fetchone()
    if not u or not u['is_premium']:
        conn.close()
        return jsonify({'error': 'Требуется Premium подписка', 'premium_required': True}), 403

    if 'file' not in request.files:
        conn.close()
        return jsonify({'error': 'Файл не выбран'}), 400

    f = request.files['file']
    if not f.filename or not _allowed_file(f.filename):
        conn.close()
        return jsonify({'error': 'Недопустимый формат файла'}), 400

    user_dir = os.path.join(UPLOAD_FOLDER, str(user_id))
    os.makedirs(user_dir, exist_ok=True)
    ext = f.filename.rsplit('.', 1)[1].lower()
    filename = f'avatar_{int(time.time())}.{ext}'
    f.save(os.path.join(user_dir, filename))
    # удаляем старые файлы аватара
    for old in os.listdir(user_dir):
        if old.startswith('avatar_') and old != filename:
            try: os.remove(os.path.join(user_dir, old))
            except OSError: pass

    avatar_url = f'/static/uploads/{user_id}/{filename}'
    c.execute('INSERT OR IGNORE INTO user_profile (user_id) VALUES (?)', (user_id,))
    # Сохраняем как кастомный аватар и как текущий (снимаем shop-аватар)
    c.execute(
        'UPDATE user_profile SET avatar_url = ?, custom_avatar_url = ? WHERE user_id = ?',
        (avatar_url, avatar_url, user_id)
    )
    # Снять все shop-аватары
    c.execute(
        '''UPDATE user_items SET is_equipped = 0
           WHERE user_id = ? AND item_id IN (
               SELECT id FROM shop_items WHERE type = 'avatar'
           )''',
        (user_id,)
    )
    conn.commit()
    conn.close()

    schedule_webm_conversion(os.path.join(user_dir, filename))
    return jsonify({'success': True, 'avatar_url': avatar_url})


@bp.route('/upload/background', methods=['POST'])
@rate_limit(5, 60)
def upload_background():
    """Загрузить фон профиля (только Premium)"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401

    conn = get_db()
    c = conn.cursor()

    c.execute('SELECT is_premium FROM users WHERE id = ?', (user_id,))
    u = c.fetchone()
    if not u or not u['is_premium']:
        conn.close()
        return jsonify({'error': 'Требуется Premium подписка', 'premium_required': True}), 403

    if 'file' not in request.files:
        conn.close()
        return jsonify({'error': 'Файл не выбран'}), 400

    f = request.files['file']
    if not f.filename or not _allowed_file(f.filename):
        conn.close()
        return jsonify({'error': 'Недопустимый формат файла'}), 400

    user_dir = os.path.join(UPLOAD_FOLDER, str(user_id))
    os.makedirs(user_dir, exist_ok=True)
    ext = f.filename.rsplit('.', 1)[1].lower()
    filename = f'bg.{ext}'
    f.save(os.path.join(user_dir, filename))

    bg_url = f'/static/uploads/{user_id}/{filename}'
    c.execute('INSERT OR IGNORE INTO user_profile (user_id) VALUES (?)', (user_id,))
    c.execute('UPDATE user_profile SET background_url = ? WHERE user_id = ?', (bg_url, user_id))
    conn.commit()
    conn.close()

    schedule_webm_conversion(os.path.join(user_dir, filename))
    return jsonify({'success': True, 'background_url': bg_url})


@bp.route('/api/user/stats')
def api_user_stats():
    """Получить XP и уровень текущего пользователя (для хедера)"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'authenticated': False})

    # Проверяем in-memory кеш (30 сек)
    now = time.time()
    with _stats_cache_lock:
        cached = _stats_cache.get(user_id)
        if cached and cached['expires'] > now:
            return jsonify(cached['data'])

    stats = get_or_create_user_stats(user_id)
    if not stats:
        return jsonify({'authenticated': True, 'xp': 0, 'level': 1, 'coins': 0})

    data = {
        'authenticated': True,
        'xp': stats['xp'],
        'coins': stats['coins'],
        'level': stats['level'],
        'xp_progress_pct': min(100, int(
            (stats['xp'] - get_xp_for_level(stats['level'])) /
            max(1, get_xp_for_level(stats['level'] + 1) - get_xp_for_level(stats['level'])) * 100
        ))
    }

    with _stats_cache_lock:
        _stats_cache[user_id] = {'data': data, 'expires': now + 30}

    return jsonify(data)


# ==================== БИБЛИОТЕКА И КОЛЛЕКЦИИ ====================

@bp.route('/api/user/history')
def api_user_history():
    """История чтения пользователя"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify([])
    limit = min(int(request.args.get('limit', 50)), 200)
    offset = int(request.args.get('offset', 0))
    conn = get_db()
    c = conn.cursor()
    c.execute(
        '''SELECT m.manga_title, m.manga_slug, m.cover_url, m.manga_type,
                  c.chapter_number, c.chapter_slug, rh.last_read
           FROM reading_history rh
           JOIN manga m ON rh.manga_id = m.manga_id
           JOIN chapters c ON rh.chapter_id = c.chapter_id
           WHERE rh.user_id = ?
           ORDER BY rh.last_read DESC
           LIMIT ? OFFSET ?''',
        (user_id, limit, offset)
    )
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify(rows)


@bp.route('/api/user/subscriptions')
def api_user_subscriptions():
    """Подписки пользователя (uid= для просмотра чужих)"""
    uid = request.args.get('uid', type=int)
    target_id = uid if uid else session.get('user_id')
    if not target_id:
        return jsonify([])
    conn = get_db()
    c = conn.cursor()
    c.execute(
        '''SELECT m.manga_id, m.manga_title, m.manga_slug, m.cover_url,
                  m.manga_type, m.manga_status, s.subscribed_at
           FROM subscriptions s
           JOIN manga m ON s.manga_id = m.manga_id
           WHERE s.user_id = ?
           ORDER BY s.subscribed_at DESC''',
        (target_id,)
    )
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify(rows)


@bp.route('/api/user/collections')
def api_user_collections():
    """Коллекции пользователя (uid= для просмотра чужих публичных)"""
    uid = request.args.get('uid', type=int)
    viewer_id = session.get('user_id')
    if uid:
        target_id = uid
        only_public = True
    else:
        target_id = viewer_id
        only_public = False
    if not target_id:
        return jsonify([])
    conn = get_db()
    c = conn.cursor()
    if only_public:
        c.execute(
            '''SELECT c.id, c.name, c.description, c.cover_url, c.is_public, c.created_at,
                      COUNT(DISTINCT ci.manga_id) as items_count,
                      COUNT(DISTINCT lk.user_id) as likes_count
               FROM collections c
               LEFT JOIN collection_items ci ON c.id = ci.collection_id
               LEFT JOIN collection_likes lk ON c.id = lk.collection_id
               WHERE c.user_id = ? AND c.is_public = 1
               GROUP BY c.id
               ORDER BY c.created_at DESC''',
            (target_id,)
        )
    else:
        c.execute(
            '''SELECT c.id, c.name, c.description, c.cover_url, c.is_public, c.created_at,
                      COUNT(DISTINCT ci.manga_id) as items_count,
                      COUNT(DISTINCT lk.user_id) as likes_count
               FROM collections c
               LEFT JOIN collection_items ci ON c.id = ci.collection_id
               LEFT JOIN collection_likes lk ON c.id = lk.collection_id
               WHERE c.user_id = ?
               GROUP BY c.id
               ORDER BY c.created_at DESC''',
            (target_id,)
        )
    rows = [dict(r) for r in c.fetchall()]
    # Пометить лайкнутые текущим пользователем
    if viewer_id and rows:
        ids = [r['id'] for r in rows]
        placeholders = ','.join('?' * len(ids))
        c.execute(f'SELECT collection_id FROM collection_likes WHERE user_id = ? AND collection_id IN ({placeholders})',
                  [viewer_id] + ids)
        liked_ids = {r[0] for r in c.fetchall()}
        for r in rows:
            r['my_like'] = r['id'] in liked_ids
    conn.close()
    return jsonify(rows)


@bp.route('/api/users/<int:author_id>/follow-curator', methods=['POST'])
def api_follow_curator(author_id):
    """Toggle подписки на куратора"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    if user_id == author_id:
        return jsonify({'error': 'Нельзя подписаться на себя'}), 400
    conn = get_db()
    c = conn.cursor()
    existing = c.execute('SELECT id FROM curator_follows WHERE follower_id=? AND author_id=?',
                         (user_id, author_id)).fetchone()
    if existing:
        c.execute('DELETE FROM curator_follows WHERE follower_id=? AND author_id=?', (user_id, author_id))
        following = False
    else:
        c.execute('INSERT OR IGNORE INTO curator_follows (follower_id, author_id) VALUES (?,?)',
                  (user_id, author_id))
        following = True
    conn.commit()
    cnt = c.execute('SELECT COUNT(*) FROM curator_follows WHERE author_id=?', (author_id,)).fetchone()[0]
    conn.close()
    return jsonify({'following': following, 'followers_count': cnt})


@bp.route('/api/users/<int:author_id>/followers')
def api_curator_followers(author_id):
    """Число подписчиков куратора и статус текущего пользователя"""
    user_id = session.get('user_id')
    conn = get_db()
    c = conn.cursor()
    cnt = c.execute('SELECT COUNT(*) FROM curator_follows WHERE author_id=?', (author_id,)).fetchone()[0]
    following = False
    if user_id:
        following = bool(c.execute('SELECT id FROM curator_follows WHERE follower_id=? AND author_id=?',
                                   (user_id, author_id)).fetchone())
    conn.close()
    return jsonify({'followers_count': cnt, 'following': following})


@bp.route('/api/user/following-curators')
def api_following_curators():
    """Список кураторов, на которых подписан текущий пользователь"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'curators': []})
    conn = get_db()
    c = conn.cursor()
    rows = c.execute(
        '''SELECT u.id, u.telegram_first_name, u.telegram_username, u.is_premium,
                  up.custom_name, up.avatar_url
           FROM curator_follows cf
           JOIN users u ON cf.author_id = u.id
           LEFT JOIN user_profile up ON u.id = up.user_id
           WHERE cf.follower_id = ?
           ORDER BY cf.created_at DESC''',
        (user_id,)
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        display = (r['custom_name'] or '').strip() or r['telegram_first_name'] or r['telegram_username'] or f'Пользователь #{r["id"]}'
        result.append({'id': r['id'], 'display_name': display, 'avatar_url': r['avatar_url'], 'is_premium': r['is_premium']})
    return jsonify({'curators': result})


@bp.route('/api/collections', methods=['POST'])
def api_create_collection():
    """Создать коллекцию"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    data = request.get_json(silent=True) or {}
    name = (data.get('name') or '').strip()[:100]
    if not name:
        return jsonify({'error': 'Название обязательно'}), 400
    description = (data.get('description') or '').strip()[:500]
    is_public = 1 if data.get('is_public', True) else 0
    conn = get_db()
    c = conn.cursor()
    c.execute(
        'INSERT INTO collections (user_id, name, description, is_public) VALUES (?, ?, ?, ?)',
        (user_id, name, description, is_public)
    )
    conn.commit()
    new_id = c.lastrowid
    # Определяем имя автора
    c.execute('SELECT telegram_first_name, telegram_username FROM users WHERE id=?', (user_id,))
    _author = c.fetchone()
    author_name = (
        (_author['telegram_first_name'] or _author['telegram_username'] or f'Пользователь #{user_id}')
        if _author else f'Пользователь #{user_id}'
    )
    # Уведомляем подписчиков-кураторов
    followers = c.execute('SELECT follower_id FROM curator_follows WHERE author_id=?', (user_id,)).fetchall()
    conn.close()
    for f in followers:
        create_site_notification(
            f['follower_id'], 'new_collection',
            f'{author_name} создал(а) коллекцию',
            name, f'/collection/{new_id}'
        )
    return jsonify({'success': True, 'id': new_id, 'name': name, 'items_count': 0})


@bp.route('/api/collections/<int:coll_id>', methods=['PUT'])
def api_update_collection(coll_id):
    """Обновить коллекцию"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    data = request.get_json(silent=True) or {}
    name = (data.get('name') or '').strip()[:100]
    if not name:
        return jsonify({'error': 'Название обязательно'}), 400
    description = (data.get('description') or '').strip()[:500]
    is_public = 1 if data.get('is_public', True) else 0
    conn = get_db()
    c = conn.cursor()
    c.execute(
        'UPDATE collections SET name=?, description=?, is_public=? WHERE id=? AND user_id=?',
        (name, description, is_public, coll_id, user_id)
    )
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@bp.route('/api/collections/<int:coll_id>', methods=['DELETE'])
def api_delete_collection(coll_id):
    """Удалить коллекцию"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    conn = get_db()
    c = conn.cursor()
    c.execute('DELETE FROM collection_items WHERE collection_id = ?', (coll_id,))
    c.execute('DELETE FROM collections WHERE id = ? AND user_id = ?', (coll_id, user_id))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@bp.route('/api/collections/<int:coll_id>/items', methods=['GET'])
def api_collection_items(coll_id):
    """Манги в коллекции"""
    conn = get_db()
    c = conn.cursor()
    c.execute(
        '''SELECT m.manga_id, m.manga_title, m.manga_slug, m.cover_url, m.manga_type, ci.added_at
           FROM collection_items ci
           JOIN manga m ON ci.manga_id = m.manga_id
           WHERE ci.collection_id = ?
           ORDER BY ci.added_at DESC''',
        (coll_id,)
    )
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify(rows)


@bp.route('/api/collections/<int:coll_id>/manga', methods=['POST'])
def api_add_to_collection(coll_id):
    """Добавить мангу в коллекцию"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    data = request.get_json(silent=True) or {}
    manga_id = (data.get('manga_id') or '').strip()
    if not manga_id:
        return jsonify({'error': 'manga_id обязателен'}), 400
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT id FROM collections WHERE id = ? AND user_id = ?', (coll_id, user_id))
    if not c.fetchone():
        conn.close()
        return jsonify({'error': 'Коллекция не найдена'}), 404
    try:
        c.execute(
            'INSERT OR IGNORE INTO collection_items (collection_id, manga_id) VALUES (?, ?)',
            (coll_id, manga_id)
        )
        conn.commit()
    except Exception:
        pass
    conn.close()
    return jsonify({'success': True})


@bp.route('/api/collections/<int:coll_id>/manga/<manga_id>', methods=['DELETE'])
def api_remove_from_collection(coll_id, manga_id):
    """Удалить мангу из коллекции"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT id FROM collections WHERE id = ? AND user_id = ?', (coll_id, user_id))
    if not c.fetchone():
        conn.close()
        return jsonify({'error': 'Коллекция не найдена'}), 404
    c.execute(
        'DELETE FROM collection_items WHERE collection_id = ? AND manga_id = ?',
        (coll_id, manga_id)
    )
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@bp.route('/api/collections/<int:coll_id>/like', methods=['POST', 'DELETE'])
def api_collection_like(coll_id):
    """Лайк / анлайк коллекции"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT id, is_public, user_id FROM collections WHERE id = ?', (coll_id,))
    coll = c.fetchone()
    if not coll:
        conn.close()
        return jsonify({'error': 'Не найдено'}), 404
    if not coll['is_public'] and coll['user_id'] != user_id:
        conn.close()
        return jsonify({'error': 'Доступ запрещён'}), 403
    if request.method == 'POST':
        c.execute('INSERT OR IGNORE INTO collection_likes (user_id, collection_id) VALUES (?, ?)', (user_id, coll_id))
    else:
        c.execute('DELETE FROM collection_likes WHERE user_id = ? AND collection_id = ?', (user_id, coll_id))
    conn.commit()
    c.execute('SELECT COUNT(*) as cnt FROM collection_likes WHERE collection_id = ?', (coll_id,))
    likes_count = c.fetchone()['cnt']
    conn.close()
    return jsonify({'success': True, 'likes_count': likes_count, 'my_like': request.method == 'POST'})


@bp.route('/upload/collection-cover', methods=['POST'])
@rate_limit(5, 60)
def upload_collection_cover():
    """Загрузить обложку коллекции"""
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401
    coll_id = request.form.get('collection_id', type=int)
    if not coll_id:
        return jsonify({'error': 'collection_id обязателен'}), 400
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT id FROM collections WHERE id = ? AND user_id = ?', (coll_id, user_id))
    if not c.fetchone():
        conn.close()
        return jsonify({'error': 'Коллекция не найдена'}), 404
    if 'file' not in request.files:
        conn.close()
        return jsonify({'error': 'Файл не выбран'}), 400
    f = request.files['file']
    if not f.filename or not _allowed_file(f.filename):
        conn.close()
        return jsonify({'error': 'Недопустимый формат файла'}), 400
    coll_dir = os.path.join(UPLOAD_FOLDER, 'collections', str(coll_id))
    os.makedirs(coll_dir, exist_ok=True)
    ext = f.filename.rsplit('.', 1)[1].lower()
    filename = f'cover.{ext}'
    f.save(os.path.join(coll_dir, filename))
    cover_url = f'/static/uploads/collections/{coll_id}/{filename}'
    c.execute('UPDATE collections SET cover_url = ? WHERE id = ?', (cover_url, coll_id))
    conn.commit()
    conn.close()
    return jsonify({'success': True, 'cover_url': cover_url})


@bp.route('/collection/<int:coll_id>')
def collection_detail(coll_id):
    """Страница коллекции"""
    conn = get_db()
    c = conn.cursor()
    c.execute(
        '''SELECT c.id, c.name, c.description, c.cover_url, c.is_public, c.created_at, c.user_id,
                  COUNT(DISTINCT ci.manga_id) as items_count,
                  COUNT(DISTINCT lk.user_id) as likes_count,
                  COALESCE(p.custom_avatar_url, p.avatar_url) as owner_avatar,
                  NULLIF(TRIM(COALESCE(p.custom_name, '')), '') as owner_name,
                  u.telegram_first_name, u.telegram_username
           FROM collections c
           LEFT JOIN collection_items ci ON c.id = ci.collection_id
           LEFT JOIN collection_likes lk ON c.id = lk.collection_id
           LEFT JOIN user_profile p ON c.user_id = p.user_id
           LEFT JOIN users u ON c.user_id = u.id
           WHERE c.id = ?
           GROUP BY c.id, c.name, c.description, c.cover_url, c.is_public, c.created_at, c.user_id,
                    p.custom_avatar_url, p.avatar_url, p.custom_name,
                    u.telegram_first_name, u.telegram_username''',
        (coll_id,)
    )
    row = c.fetchone()
    if not row:
        conn.close()
        abort(404)
    coll = dict(row)
    user_id = session.get('user_id')
    if not coll['is_public'] and coll['user_id'] != user_id:
        conn.close()
        abort(403)
    my_like = False
    if user_id:
        c.execute('SELECT 1 FROM collection_likes WHERE user_id = ? AND collection_id = ?', (user_id, coll_id))
        my_like = c.fetchone() is not None
    c.execute(
        '''SELECT m.manga_id, m.manga_title, m.manga_slug, m.cover_url, m.manga_type
           FROM collection_items ci
           JOIN manga m ON ci.manga_id = m.manga_id
           WHERE ci.collection_id = ?
           ORDER BY ci.added_at DESC''',
        (coll_id,)
    )
    items = [dict(r) for r in c.fetchall()]
    conn.close()
    owner_name = (coll['owner_name'] or coll['telegram_first_name'] or
                  coll['telegram_username'] or f'#{coll["user_id"]}')
    is_owner = (user_id == coll['user_id'])
    return render_template('collection_detail.html',
                           coll=coll, items=items, my_like=my_like,
                           owner_name=owner_name, user_id=user_id, is_owner=is_owner)


@bp.route('/collections/top')
def collections_top_page():
    """Топ коллекций"""
    conn = get_db()
    c = conn.cursor()
    user_id = session.get('user_id')
    c.execute(
        '''SELECT c.id, c.name, c.description, c.cover_url, c.created_at, c.user_id,
                  COUNT(DISTINCT ci.manga_id) as items_count,
                  COUNT(DISTINCT lk.user_id) as likes_count,
                  COALESCE(p.custom_avatar_url, p.avatar_url) as owner_avatar,
                  NULLIF(TRIM(COALESCE(p.custom_name, '')), '') as owner_name,
                  u.telegram_first_name, u.telegram_username
           FROM collections c
           LEFT JOIN collection_items ci ON c.id = ci.collection_id
           LEFT JOIN collection_likes lk ON c.id = lk.collection_id
           LEFT JOIN user_profile p ON c.user_id = p.user_id
           LEFT JOIN users u ON c.user_id = u.id
           WHERE c.is_public = 1
           GROUP BY c.id, c.name, c.description, c.cover_url, c.created_at, c.user_id,
                    p.custom_avatar_url, p.avatar_url, p.custom_name,
                    u.telegram_first_name, u.telegram_username
           ORDER BY likes_count DESC, items_count DESC, c.created_at DESC
           LIMIT 50'''
    )
    collections = []
    for row in c.fetchall():
        d = dict(row)
        d['owner_name'] = (d['owner_name'] or d['telegram_first_name'] or
                           d['telegram_username'] or f'#{d["user_id"]}')
        collections.append(d)
    my_likes = set()
    if user_id and collections:
        ids = [d['id'] for d in collections]
        placeholders = ','.join('?' * len(ids))
        c.execute(f'SELECT collection_id FROM collection_likes WHERE user_id = ? AND collection_id IN ({placeholders})',
                  [user_id] + ids)
        my_likes = {r[0] for r in c.fetchall()}
    conn.close()
    return render_template('collections_top.html',
                           collections=collections, my_likes=my_likes, user_id=user_id)


# ==================== АДМИНКА ====================

def admin_required(f):
    """Декоратор: проверяем, что текущий пользователь — администратор"""
    @wraps(f)
    def decorated(*args, **kwargs):
        user_id = session.get('user_id')
        if not user_id:
            return redirect(url_for('index'))
        conn = get_db()
        c = conn.cursor()
        c.execute('SELECT telegram_id FROM users WHERE id = ?', (user_id,))
        row = c.fetchone()
        conn.close()
        if not row or row['telegram_id'] not in ADMIN_TELEGRAM_IDS:
            return "403 Forbidden", 403
        return f(*args, **kwargs)
    return decorated


@bp.route('/api/user/quests')
def api_user_quests():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Не авторизован'}), 401

    conn = get_db()
    c = conn.cursor()

    c.execute('SELECT level FROM user_stats WHERE user_id = ?', (user_id,))
    row = c.fetchone()
    current_level = row['level'] if row else 1

    c.execute(
        '''SELECT q.*,
                  COALESCE(uq.progress, 0) as progress,
                  uq.completed_at
           FROM quests q
           LEFT JOIN user_quests uq ON uq.quest_id = q.id AND uq.user_id = ?
           WHERE q.is_active = 1 AND q.required_level <= ?
           ORDER BY q.required_level ASC, q.id ASC''',
        (user_id, current_level)
    )
    quests = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify({'quests': quests, 'current_level': current_level})


@bp.route('/api/user/streak-calendar')
def api_streak_calendar():
    """Возвращает активность чтения за последние 365 дней для календаря."""
    uid = request.args.get('uid', type=int) or session.get('user_id')
    if not uid:
        return jsonify({'days': {}})
    conn = get_db()
    rows = conn.execute('''
        SELECT DATE(read_at) as day, COUNT(DISTINCT chapter_id) as count
        FROM chapters_read
        WHERE user_id = ?
          AND read_at >= DATE('now', '-364 days')
        GROUP BY DATE(read_at)
    ''', (uid,)).fetchall()
    conn.close()
    return jsonify({'days': {str(r['day'])[:10]: r['count'] for r in rows}})


@bp.route('/admin')
@admin_required
def admin_panel():
    return render_template('admin.html')


# ── Статистика ──────────────────────────────────────────────────────────────

_NO_BOT_USERS = "LOWER(COALESCE(telegram_username,'')) NOT LIKE 'bot\\_seed%'"

@bp.route('/api/admin/stats')
@admin_required
def api_admin_stats():
    conn = get_db()
    c = conn.cursor()

    c.execute(f'SELECT COUNT(*) FROM users WHERE {_NO_BOT_USERS}')
    total_users = c.fetchone()[0]

    c.execute(f'SELECT COUNT(*) FROM users WHERE is_active IS FALSE AND {_NO_BOT_USERS}')
    banned_users = c.fetchone()[0]

    c.execute(f'SELECT COUNT(*) FROM users WHERE is_premium = 1 AND {_NO_BOT_USERS}')
    premium_users = c.fetchone()[0]

    # Новые пользователи за последние 7 дней
    c.execute(f"SELECT COUNT(*) FROM users WHERE created_at >= datetime('now', '-7 days') AND {_NO_BOT_USERS}")
    new_users_week = c.fetchone()[0]

    c.execute('SELECT COUNT(*) FROM manga')
    total_manga = c.fetchone()[0]

    c.execute('SELECT COUNT(*) FROM chapters')
    total_chapters = c.fetchone()[0]

    c.execute(f'SELECT COUNT(*) FROM comments WHERE user_id IN (SELECT id FROM users WHERE {_NO_BOT_USERS})')
    total_comments = c.fetchone()[0]

    c.execute(f'SELECT COUNT(*) FROM subscriptions WHERE user_id IN (SELECT id FROM users WHERE {_NO_BOT_USERS})')
    total_subscriptions = c.fetchone()[0]

    c.execute(f'SELECT COUNT(*) FROM reading_history WHERE user_id IN (SELECT id FROM users WHERE {_NO_BOT_USERS})')
    total_history = c.fetchone()[0]

    c.execute(f'SELECT SUM(s.total_chapters_read) FROM user_stats s JOIN users u ON s.user_id = u.id WHERE {_NO_BOT_USERS}')
    row = c.fetchone()
    total_chapters_read = row[0] or 0

    # Топ-10 по XP
    c.execute(f'''SELECT u.id, u.telegram_username, u.telegram_first_name,
                        COALESCE(up.custom_name,'') as custom_name,
                        s.xp, s.level, s.coins
                 FROM user_stats s
                 JOIN users u ON s.user_id = u.id
                 LEFT JOIN user_profile up ON up.user_id = u.id
                 WHERE {_NO_BOT_USERS}
                 ORDER BY s.xp DESC LIMIT 10''')
    top_users = [dict(r) for r in c.fetchall()]

    # Топ манги по просмотрам
    c.execute('SELECT manga_id, manga_title, manga_slug, views FROM manga ORDER BY views DESC LIMIT 10')
    top_manga = [dict(r) for r in c.fetchall()]

    # Активность — регистрации по дням (последние 14 дней)
    c.execute(f"""SELECT date(created_at) as day, COUNT(*) as cnt
                 FROM users
                 WHERE created_at >= datetime('now', '-14 days') AND {_NO_BOT_USERS}
                 GROUP BY day ORDER BY day""")
    reg_activity = [dict(r) for r in c.fetchall()]

    conn.close()
    return jsonify({
        'users': {
            'total': total_users,
            'banned': banned_users,
            'premium': premium_users,
            'new_week': new_users_week,
        },
        'manga': {'total': total_manga},
        'chapters': {'total': total_chapters},
        'comments': {'total': total_comments},
        'subscriptions': {'total': total_subscriptions},
        'history': {'total': total_history, 'chapters_read': total_chapters_read},
        'top_users': top_users,
        'top_manga': top_manga,
        'reg_activity': reg_activity,
    })


# ── Пользователи ────────────────────────────────────────────────────────────

@bp.route('/api/admin/users')
@admin_required
def api_admin_users():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 30))
    search = request.args.get('q', '').strip()
    offset = (page - 1) * per_page

    conn = get_db()
    c = conn.cursor()

    where = ''
    params = []
    if search:
        where = "WHERE u.telegram_username LIKE ? OR u.telegram_first_name LIKE ? OR CAST(u.telegram_id AS TEXT) LIKE ? OR COALESCE(up.custom_name,'') LIKE ?"
        like = f'%{search}%'
        params = [like, like, like, like]

    c.execute(f'''SELECT COUNT(*) FROM users u LEFT JOIN user_profile up ON up.user_id = u.id {where}''', params)
    total = c.fetchone()[0]

    c.execute(f'''
        SELECT u.id, u.telegram_id, u.telegram_username, u.telegram_first_name,
               u.is_active, u.is_premium, u.premium_expires_at,
               u.created_at, u.last_login,
               COALESCE(up.custom_name,'') as custom_name,
               COALESCE(up.avatar_url,'') as avatar_url,
               COALESCE(s.xp,0) as xp, COALESCE(s.level,1) as level, COALESCE(s.coins,0) as coins,
               COALESCE(s.total_chapters_read,0) as chapters_read,
               (SELECT COUNT(*) FROM subscriptions WHERE user_id=u.id) as sub_count,
               (SELECT COUNT(*) FROM comments WHERE user_id=u.id) as comment_count
        FROM users u
        LEFT JOIN user_profile up ON up.user_id = u.id
        LEFT JOIN user_stats s ON s.user_id = u.id
        {where}
        ORDER BY u.id DESC
        LIMIT ? OFFSET ?
    ''', params + [per_page, offset])
    users = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify({'users': users, 'total': total, 'page': page, 'per_page': per_page})


@bp.route('/api/admin/users/<int:uid>/ban', methods=['POST'])
@admin_required
def api_admin_ban_user(uid):
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT is_active FROM users WHERE id = ?', (uid,))
    row = c.fetchone()
    if not row:
        conn.close()
        return jsonify({'error': 'Пользователь не найден'}), 404
    new_state = not row['is_active']
    c.execute('UPDATE users SET is_active = ? WHERE id = ?', (new_state, uid))
    conn.commit()
    conn.close()
    return jsonify({'success': True, 'is_active': bool(new_state)})


@bp.route('/api/admin/users/<int:uid>/premium', methods=['POST'])
@admin_required
def api_admin_set_premium(uid):
    data = request.get_json(silent=True) or {}
    enabled = bool(data.get('enabled', True))
    days = int(data.get('days', 30))
    conn = get_db()
    c = conn.cursor()
    if enabled:
        expires = (datetime.utcnow() + timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        c.execute('UPDATE users SET is_premium=1, premium_expires_at=? WHERE id=?', (expires, uid))
    else:
        c.execute('UPDATE users SET is_premium=0, premium_expires_at=NULL WHERE id=?', (uid,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@bp.route('/api/admin/users/<int:uid>/coins', methods=['POST'])
@admin_required
def api_admin_set_coins(uid):
    data = request.get_json(silent=True) or {}
    amount = int(data.get('amount', 0))
    mode = data.get('mode', 'set')  # 'set' | 'add'
    conn = get_db()
    c = conn.cursor()
    get_or_create_user_stats(uid, conn)
    if mode == 'add':
        c.execute('UPDATE user_stats SET coins = coins + ? WHERE user_id = ?', (amount, uid))
    else:
        c.execute('UPDATE user_stats SET coins = ? WHERE user_id = ?', (amount, uid))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@bp.route('/api/admin/users/<int:uid>/xp', methods=['POST'])
@admin_required
def api_admin_set_xp(uid):
    data = request.get_json(silent=True) or {}
    amount = int(data.get('amount', 0))
    mode = data.get('mode', 'set')
    conn = get_db()
    c = conn.cursor()
    get_or_create_user_stats(uid, conn)
    if mode == 'add':
        c.execute('UPDATE user_stats SET xp = xp + ? WHERE user_id = ?', (amount, uid))
    else:
        c.execute('UPDATE user_stats SET xp = ? WHERE user_id = ?', (amount, uid))
    # Пересчитываем уровень
    c.execute('SELECT xp FROM user_stats WHERE user_id = ?', (uid,))
    row = c.fetchone()
    if row:
        new_level = get_level_from_xp(row['xp'])
        c.execute('UPDATE user_stats SET level = ? WHERE user_id = ?', (new_level, uid))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@bp.route('/api/admin/users/<int:uid>/subscriptions', methods=['GET'])
@admin_required
def api_admin_user_subs(uid):
    conn = get_db()
    c = conn.cursor()
    c.execute('''SELECT s.id, s.manga_id, s.subscribed_at,
                        COALESCE(m.manga_title,'') as manga_title, COALESCE(m.manga_slug,'') as manga_slug
                 FROM subscriptions s
                 LEFT JOIN manga m ON m.manga_id = s.manga_id
                 WHERE s.user_id = ?
                 ORDER BY s.subscribed_at DESC''', (uid,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify(rows)


@bp.route('/api/admin/users/<int:uid>/subscriptions/<manga_id>', methods=['DELETE'])
@admin_required
def api_admin_remove_sub(uid, manga_id):
    conn = get_db()
    c = conn.cursor()
    c.execute('DELETE FROM subscriptions WHERE user_id=? AND manga_id=?', (uid, manga_id))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@bp.route('/api/admin/users/<int:uid>/subscriptions', methods=['POST'])
@admin_required
def api_admin_add_sub(uid):
    data = request.get_json(silent=True) or {}
    manga_id = (data.get('manga_id') or '').strip()
    if not manga_id:
        return jsonify({'error': 'manga_id обязателен'}), 400
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('INSERT OR IGNORE INTO subscriptions (user_id, manga_id) VALUES (?,?)', (uid, manga_id))
        conn.commit()
    except Exception:
        pass
    conn.close()
    return jsonify({'success': True})


# ── Комментарии ─────────────────────────────────────────────────────────────

@bp.route('/api/admin/comments')
@admin_required
def api_admin_comments():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))
    search = request.args.get('q', '').strip()
    offset = (page - 1) * per_page

    conn = get_db()
    c = conn.cursor()

    where = ''
    params: list = []
    if search:
        where = "WHERE c.text LIKE ? OR u.telegram_username LIKE ? OR c.manga_slug LIKE ?"
        like = f'%{search}%'
        params = [like, like, like]

    c.execute(f'SELECT COUNT(*) FROM comments c JOIN users u ON c.user_id=u.id {where}', params)
    total = c.fetchone()[0]

    c.execute(f'''
        SELECT c.id, c.manga_slug, c.text, c.created_at,
               u.id as user_id, u.telegram_username, u.telegram_first_name,
               COALESCE(up.custom_name,'') as custom_name,
               COALESCE(up.avatar_url,'') as avatar_url,
               COALESCE(m.manga_title,'') as manga_title
        FROM comments c
        JOIN users u ON c.user_id = u.id
        LEFT JOIN user_profile up ON up.user_id = u.id
        LEFT JOIN manga m ON m.manga_slug = c.manga_slug
        {where}
        ORDER BY c.created_at DESC
        LIMIT ? OFFSET ?
    ''', params + [per_page, offset])
    comments = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify({'comments': comments, 'total': total, 'page': page, 'per_page': per_page})


@bp.route('/api/admin/comments/<int:cid>', methods=['DELETE'])
@admin_required
def api_admin_delete_comment(cid):
    conn = get_db()
    c = conn.cursor()
    c.execute('DELETE FROM comments WHERE id = ?', (cid,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


# ── Магазин ─────────────────────────────────────────────────────────────────

SHOP_UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'static', 'uploads', 'shop')

@bp.route('/api/admin/shop/upload', methods=['POST'])
@admin_required
def api_admin_shop_upload():
    """Загрузить файл для товара магазина (аватар, фон, значок, рамка-картинка)"""
    if 'file' not in request.files:
        return jsonify({'error': 'Файл не выбран'}), 400
    f = request.files['file']
    if not f.filename or not _allowed_file(f.filename):
        return jsonify({'error': 'Недопустимый формат файла (png/jpg/jpeg/gif/webp)'}), 400

    os.makedirs(SHOP_UPLOAD_FOLDER, exist_ok=True)
    ext = f.filename.rsplit('.', 1)[1].lower()
    filename = f'{secrets.token_hex(12)}.{ext}'
    f.save(os.path.join(SHOP_UPLOAD_FOLDER, filename))
    url = f'/static/uploads/shop/{filename}'
    return jsonify({'success': True, 'url': url})


@bp.route('/api/admin/shop', methods=['GET'])
@admin_required
def api_admin_shop_items():
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT * FROM shop_items ORDER BY type, id')
    items = [dict(r) for r in c.fetchall()]
    # Добавляем кол-во покупок
    for it in items:
        c.execute('SELECT COUNT(*) FROM user_items WHERE item_id=?', (it['id'],))
        it['purchases'] = c.fetchone()[0]
    conn.close()
    return jsonify(items)


@bp.route('/api/admin/shop', methods=['POST'])
@admin_required
def api_admin_shop_create():
    data = request.get_json(silent=True) or {}
    name = (data.get('name') or '').strip()
    if not name:
        return jsonify({'error': 'name обязателен'}), 400
    conn = get_db()
    c = conn.cursor()
    c.execute(
        'INSERT INTO shop_items (name, description, type, preview_url, css_value, price, is_upload, is_animated, duration_days) VALUES (?,?,?,?,?,?,?,?,?)',
        (
            name,
            (data.get('description') or '').strip(),
            (data.get('type') or 'frame').strip(),
            (data.get('preview_url') or '').strip() or None,
            (data.get('css_value') or '').strip(),
            int(data.get('price', 0)),
            int(bool(data.get('is_upload', False))),
            int(bool(data.get('is_animated', False))),
            int(data['duration_days']) if data.get('duration_days') else None,
        )
    )
    conn.commit()
    new_id = c.lastrowid
    conn.close()
    return jsonify({'success': True, 'id': new_id})


@bp.route('/api/admin/shop/<int:item_id>', methods=['PUT'])
@admin_required
def api_admin_shop_update(item_id):
    data = request.get_json(silent=True) or {}
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT id FROM shop_items WHERE id=?', (item_id,))
    if not c.fetchone():
        conn.close()
        return jsonify({'error': 'Товар не найден'}), 404
    c.execute(
        '''UPDATE shop_items SET name=?, description=?, type=?, preview_url=?,
           css_value=?, price=?, is_upload=?, is_animated=?, duration_days=? WHERE id=?''',
        (
            (data.get('name') or '').strip(),
            (data.get('description') or '').strip(),
            (data.get('type') or 'frame').strip(),
            (data.get('preview_url') or '').strip() or None,
            (data.get('css_value') or '').strip(),
            int(data.get('price', 0)),
            int(bool(data.get('is_upload', False))),
            int(bool(data.get('is_animated', False))),
            int(data['duration_days']) if data.get('duration_days') else None,
            item_id,
        )
    )
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@bp.route('/api/admin/shop/<int:item_id>', methods=['DELETE'])
@admin_required
def api_admin_shop_delete(item_id):
    conn = get_db()
    c = conn.cursor()
    c.execute('DELETE FROM user_items WHERE item_id=?', (item_id,))
    c.execute('DELETE FROM shop_items WHERE id=?', (item_id,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


# ── Манга (в БД) ─────────────────────────────────────────────────────────────

@bp.route('/api/admin/manga')
@admin_required
def api_admin_manga():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 30))
    search = request.args.get('q', '').strip()
    offset = (page - 1) * per_page

    conn = get_db()
    c = conn.cursor()

    where = ''
    params: list = []
    if search:
        where = 'WHERE manga_title LIKE ? OR manga_slug LIKE ?'
        like = f'%{search}%'
        params = [like, like]

    c.execute(f'SELECT COUNT(*) FROM manga {where}', params)
    total = c.fetchone()[0]

    c.execute(f'''
        SELECT manga_id, manga_slug, manga_title, manga_type, manga_status,
               cover_url, views, chapters_count, last_updated,
               (SELECT COUNT(*) FROM subscriptions WHERE manga_id=m.manga_id) as sub_count,
               (SELECT COUNT(*) FROM comments WHERE manga_slug=m.manga_slug) as comment_count
        FROM manga m {where}
        ORDER BY views DESC
        LIMIT ? OFFSET ?
    ''', params + [per_page, offset])
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify({'manga': rows, 'total': total, 'page': page, 'per_page': per_page})


# ── XP Лог ──────────────────────────────────────────────────────────────────

@bp.route('/api/admin/xp_log')
@admin_required
def api_admin_xp_log():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))
    uid = request.args.get('uid')
    offset = (page - 1) * per_page

    conn = get_db()
    c = conn.cursor()

    where = 'WHERE x.user_id = ?' if uid else ''
    params: list = [int(uid)] if uid else []

    c.execute(f'SELECT COUNT(*) FROM xp_log x {where}', params)
    total = c.fetchone()[0]

    c.execute(f'''
        SELECT x.id, x.user_id, x.reason, x.ref_id, x.amount, x.created_at,
               u.telegram_username, u.telegram_first_name,
               COALESCE(up.custom_name,'') as custom_name
        FROM xp_log x
        JOIN users u ON u.id = x.user_id
        LEFT JOIN user_profile up ON up.user_id = x.user_id
        {where}
        ORDER BY x.created_at DESC
        LIMIT ? OFFSET ?
    ''', params + [per_page, offset])
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify({'logs': rows, 'total': total})


# ── Достижения (admin CRUD) ──────────────────────────────────────────────────

ACHIEVEMENT_UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'static', 'uploads', 'achievements')

@bp.route('/api/admin/achievements/upload', methods=['POST'])
@admin_required
def api_admin_achievements_upload():
    if 'file' not in request.files:
        return jsonify({'error': 'Файл не выбран'}), 400
    f = request.files['file']
    if not f.filename or not _allowed_file(f.filename):
        return jsonify({'error': 'Недопустимый формат файла'}), 400
    os.makedirs(ACHIEVEMENT_UPLOAD_FOLDER, exist_ok=True)
    ext = f.filename.rsplit('.', 1)[1].lower()
    filename = f'{secrets.token_hex(12)}.{ext}'
    f.save(os.path.join(ACHIEVEMENT_UPLOAD_FOLDER, filename))
    return jsonify({'success': True, 'url': f'/static/uploads/achievements/{filename}'})


@bp.route('/api/admin/achievements', methods=['GET'])
@admin_required
def api_admin_achievements_list():
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT * FROM achievements ORDER BY id')
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify({'achievements': rows})


@bp.route('/api/admin/achievements', methods=['POST'])
@admin_required
def api_admin_achievements_create():
    data = request.get_json(silent=True) or {}
    key = data.get('key', '').strip()
    name = data.get('name', '').strip()
    description = data.get('description', '').strip()
    icon = data.get('icon', '🏆').strip()
    icon_url = data.get('icon_url', '').strip() or None
    xp_reward = int(data.get('xp_reward', 0))
    condition_type = data.get('condition_type', '').strip()
    condition_value = int(data.get('condition_value', 1))
    if not key or not name or not condition_type:
        return jsonify({'error': 'Обязательные поля: key, name, condition_type'}), 400
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute(
            'INSERT INTO achievements (key,name,description,icon,icon_url,xp_reward,condition_type,condition_value) VALUES (?,?,?,?,?,?,?,?)',
            (key, name, description, icon, icon_url, xp_reward, condition_type, condition_value)
        )
        conn.commit()
        ach_id = c.lastrowid
    except sqlite3.IntegrityError:
        conn.close()
        return jsonify({'error': 'Ключ уже существует'}), 409
    conn.close()
    return jsonify({'success': True, 'id': ach_id})


@bp.route('/api/admin/achievements/<int:ach_id>', methods=['PUT'])
@admin_required
def api_admin_achievements_update(ach_id):
    data = request.get_json(silent=True) or {}
    conn = get_db()
    c = conn.cursor()
    fields, vals = [], []
    for col in ('key', 'name', 'description', 'icon', 'icon_url', 'xp_reward', 'condition_type', 'condition_value'):
        if col in data:
            fields.append(f'{col}=?')
            vals.append(data[col])
    if not fields:
        conn.close()
        return jsonify({'error': 'Нет данных'}), 400
    vals.append(ach_id)
    c.execute(f'UPDATE achievements SET {", ".join(fields)} WHERE id=?', vals)
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@bp.route('/api/admin/achievements/<int:ach_id>', methods=['DELETE'])
@admin_required
def api_admin_achievements_delete(ach_id):
    conn = get_db()
    c = conn.cursor()
    c.execute('DELETE FROM achievements WHERE id=?', (ach_id,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


# ── Задания (admin CRUD) ─────────────────────────────────────────────────────

QUEST_UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'static', 'uploads', 'quests')

@bp.route('/api/admin/quests/upload', methods=['POST'])
@admin_required
def api_admin_quests_upload():
    if 'file' not in request.files:
        return jsonify({'error': 'Файл не выбран'}), 400
    f = request.files['file']
    if not f.filename or not _allowed_file(f.filename):
        return jsonify({'error': 'Недопустимый формат файла'}), 400
    os.makedirs(QUEST_UPLOAD_FOLDER, exist_ok=True)
    ext = f.filename.rsplit('.', 1)[1].lower()
    filename = f'{secrets.token_hex(12)}.{ext}'
    f.save(os.path.join(QUEST_UPLOAD_FOLDER, filename))
    return jsonify({'success': True, 'url': f'/static/uploads/quests/{filename}'})


@bp.route('/api/admin/quests', methods=['GET'])
@admin_required
def api_admin_quests_list():
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT * FROM quests ORDER BY required_level, id')
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify({'quests': rows})


@bp.route('/api/admin/quests', methods=['POST'])
@admin_required
def api_admin_quests_create():
    data = request.get_json(silent=True) or {}
    title = data.get('title', '').strip()
    description = data.get('description', '').strip()
    icon = data.get('icon', '📋').strip()
    icon_url = data.get('icon_url', '').strip() or None
    required_level = int(data.get('required_level', 1))
    condition_type = data.get('condition_type', '').strip()
    condition_value = int(data.get('condition_value', 1))
    xp_reward = int(data.get('xp_reward', 0))
    coins_reward = int(data.get('coins_reward', 0))
    is_active = int(data.get('is_active', 1))
    if not title or not condition_type:
        return jsonify({'error': 'Обязательные поля: title, condition_type'}), 400
    conn = get_db()
    c = conn.cursor()
    c.execute(
        'INSERT INTO quests (title,description,icon,icon_url,required_level,condition_type,condition_value,xp_reward,coins_reward,is_active) VALUES (?,?,?,?,?,?,?,?,?,?)',
        (title, description, icon, icon_url, required_level, condition_type, condition_value, xp_reward, coins_reward, is_active)
    )
    conn.commit()
    qid = c.lastrowid
    conn.close()
    return jsonify({'success': True, 'id': qid})


@bp.route('/api/admin/quests/<int:qid>', methods=['PUT'])
@admin_required
def api_admin_quests_update(qid):
    data = request.get_json(silent=True) or {}
    conn = get_db()
    c = conn.cursor()
    fields, vals = [], []
    for col in ('title', 'description', 'icon', 'icon_url', 'required_level', 'condition_type', 'condition_value', 'xp_reward', 'coins_reward', 'is_active'):
        if col in data:
            fields.append(f'{col}=?')
            vals.append(data[col])
    if not fields:
        conn.close()
        return jsonify({'error': 'Нет данных'}), 400
    vals.append(qid)
    c.execute(f'UPDATE quests SET {", ".join(fields)} WHERE id=?', vals)
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@bp.route('/api/admin/quests/<int:qid>', methods=['DELETE'])
@admin_required
def api_admin_quests_delete(qid):
    conn = get_db()
    c = conn.cursor()
    c.execute('DELETE FROM user_quests WHERE quest_id=?', (qid,))
    c.execute('DELETE FROM quests WHERE id=?', (qid,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


# ==================== ЗАКЛАДКИ (СПИСОК ЧТЕНИЯ) ====================

@bp.route('/bookmarks')
def bookmarks_page():
    user_id = session.get('user_id')
    if not user_id:
        return redirect('/')
    return render_template('bookmarks.html', g_active='bookmarks')


# ==================== КАЛЕНДАРЬ МАНГИ ====================

@bp.route('/calendar')
def calendar_page():
    user_id = session.get('user_id')
    return render_template('calendar.html', user_id=user_id)


@bp.route('/api/calendar/days')
def api_calendar_days():
    """Дни месяца, в которые выходили главы. ?year=YYYY&month=M"""
    year  = request.args.get('year',  type=int)
    month = request.args.get('month', type=int)
    if not year or not month:
        return jsonify([])

    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT DATE(created_at) AS day, COUNT(*) AS count
               FROM chapters
               WHERE EXTRACT(YEAR FROM created_at) = ?
                 AND EXTRACT(MONTH FROM created_at) = ?
               GROUP BY DATE(created_at)
               ORDER BY day""",
            (year, month)
        ).fetchall()
        return jsonify([{'day': str(row['day'])[:10], 'count': row['count']} for row in rows])
    except Exception as e:
        logger.error(f"api_calendar_days error: {e}")
        return jsonify([])
    finally:
        conn.close()


@bp.route('/api/calendar/day')
def api_calendar_day():
    """Все главы, вышедшие в конкретный день. ?date=YYYY-MM-DD"""
    date = request.args.get('date', '').strip()
    if not date or len(date) != 10:
        return jsonify([])

    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT c.chapter_id, c.chapter_slug, c.chapter_number,
                      c.chapter_volume, c.chapter_name, c.chapter_url,
                      m.manga_title, m.cover_url, m.manga_slug, m.manga_type
               FROM chapters c
               JOIN manga m ON c.manga_id = m.manga_id
               WHERE DATE(c.created_at) = ?
               ORDER BY m.manga_title, CAST(c.chapter_number AS REAL)""",
            (date,)
        ).fetchall()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        logger.error(f"api_calendar_day error: {e}")
        return jsonify([])
    finally:
        conn.close()


# ==================== ADMIN: СЕЗОНЫ ====================

@bp.route('/admin/api/seasons', methods=['POST'])
@admin_required
def admin_create_season():
    data = request.json or {}
    name = data.get('name', '').strip()
    if not name:
        return jsonify({'error': 'name required'}), 400
    conn = get_db()
    conn.execute(
        'INSERT INTO seasons (name, description, icon, banner_url, starts_at, ends_at, is_active) VALUES (?,?,?,?,?,?,1)',
        (name, data.get('description',''), data.get('icon','🌸'), data.get('banner_url'), data.get('starts_at',''), data.get('ends_at',''))
    )
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


@bp.route('/admin/api/seasons/<int:season_id>/quests', methods=['POST'])
@admin_required
def admin_create_season_quest(season_id):
    data = request.json or {}
    conn = get_db()
    conn.execute(
        '''INSERT INTO season_quests (season_id, title, description, icon, condition_type, condition_value,
           xp_reward, coins_reward, item_reward_id) VALUES (?,?,?,?,?,?,?,?,?)''',
        (season_id, data.get('title',''), data.get('description',''), data.get('icon','✨'),
         data.get('condition_type','chapters_read'), int(data.get('condition_value',1)),
         int(data.get('xp_reward',0)), int(data.get('coins_reward',0)),
         data.get('item_reward_id'))
    )
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


@bp.route('/admin/api/seasons/<int:season_id>', methods=['DELETE'])
@admin_required
def admin_deactivate_season(season_id):
    conn = get_db()
    conn.execute('UPDATE seasons SET is_active=0 WHERE id=?', (season_id,))
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


@bp.route('/admin/api/seasons')
@admin_required
def admin_list_seasons():
    conn = get_db()
    rows = conn.execute('SELECT * FROM seasons ORDER BY id DESC').fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


# ==================== УВЕДОМЛЕНИЯ НА САЙТЕ ====================

@bp.route('/api/notifications')
def api_notifications():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'not_auth'}), 401
    conn = get_db()
    rows = conn.execute(
        '''SELECT id, type, title, body, url, ref_id, is_read, created_at
           FROM site_notifications
           WHERE user_id = ?
           ORDER BY created_at DESC LIMIT 50''',
        (user_id,)
    ).fetchall()
    unread = conn.execute(
        'SELECT COUNT(*) as cnt FROM site_notifications WHERE user_id = ? AND is_read = 0',
        (user_id,)
    ).fetchone()['cnt']
    conn.close()
    return jsonify({'notifications': [dict(r) for r in rows], 'unread': unread})


@bp.route('/api/notifications/read-all', methods=['POST'])
def api_notifications_read_all():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'not_auth'}), 401
    conn = get_db()
    conn.execute('UPDATE site_notifications SET is_read = 1 WHERE user_id = ?', (user_id,))
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


@bp.route('/api/notifications/<int:notif_id>/read', methods=['POST'])
def api_notification_read(notif_id):
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'not_auth'}), 401
    conn = get_db()
    conn.execute(
        'UPDATE site_notifications SET is_read = 1 WHERE id = ? AND user_id = ?',
        (notif_id, user_id)
    )
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


@bp.route('/api/settings/digest-hour', methods=['POST'])
def api_set_digest_hour():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'not_auth'}), 401
    data = request.get_json(silent=True) or {}
    hour = data.get('hour')
    if hour is None or not isinstance(hour, int) or not (0 <= hour <= 23):
        return jsonify({'error': 'invalid_hour'}), 400
    conn = get_db()
    conn.execute('UPDATE users SET digest_hour = ? WHERE id = ?', (hour, user_id))
    conn.commit()
    conn.close()
    return jsonify({'ok': True, 'hour': hour})


# ==================== ADMIN РАССЫЛКА ====================

def _build_notify_targets(data):
    """Формирует список пользователей и описание фильтров из параметров запроса."""
    target = data.get('target', 'all')
    conditions = []
    params = []
    filter_parts = []

    if target == 'specific':
        ids_raw = data.get('user_ids', '')
        ids = [int(x.strip()) for x in str(ids_raw).split(',') if x.strip().isdigit()]
        if not ids:
            return [], 'конкретные ID: не указаны'
        placeholders = ','.join('?' * len(ids))
        conditions.append(f'u.id IN ({placeholders})')
        params.extend(ids)
        filter_parts.append(f'ID: {ids_raw.strip()}')
    elif target == 'filtered':
        min_level   = data.get('min_level', '').strip()
        max_level   = data.get('max_level', '').strip()
        min_chapters = data.get('min_chapters', '').strip()
        premium_only = data.get('premium_only', False)
        manga_id     = data.get('manga_id', '').strip()
        if min_level.isdigit():
            conditions.append('COALESCE(us.level, 1) >= ?')
            params.append(int(min_level))
            filter_parts.append(f'уровень ≥ {min_level}')
        if max_level.isdigit():
            conditions.append('COALESCE(us.level, 1) <= ?')
            params.append(int(max_level))
            filter_parts.append(f'уровень ≤ {max_level}')
        if min_chapters.isdigit():
            conditions.append('COALESCE(us.total_chapters_read, 0) >= ?')
            params.append(int(min_chapters))
            filter_parts.append(f'глав прочитано ≥ {min_chapters}')
        if premium_only:
            conditions.append('u.is_premium = 1')
            filter_parts.append('только Premium')
        if manga_id:
            conditions.append('EXISTS (SELECT 1 FROM subscriptions sb WHERE sb.user_id = u.id AND sb.manga_id = ?)')
            params.append(manga_id)
            filter_parts.append(f'подписан на мангу {manga_id}')

    where = ('WHERE ' + ' AND '.join(conditions)) if conditions else ''
    conn = get_db()
    rows = conn.execute(
        f'''SELECT u.id, u.telegram_id FROM users u
            LEFT JOIN user_stats us ON us.user_id = u.id
            {where}''',
        params
    ).fetchall()
    conn.close()

    filter_desc = ', '.join(filter_parts) if filter_parts else ('все пользователи' if target == 'all' else target)
    return [dict(r) for r in rows], filter_desc


@bp.route('/api/admin/notify/preview', methods=['POST'])
@admin_required
def api_admin_notify_preview():
    """Подсчёт пользователей под фильтры (превью перед отправкой)."""
    users, filter_desc = _build_notify_targets(request.json or {})
    return jsonify({'count': len(users), 'filter_desc': filter_desc})


@bp.route('/api/admin/notify/send', methods=['POST'])
@admin_required
def api_admin_notify_send():
    """Отправить рассылку уведомлений."""
    data = request.json or {}
    title        = (data.get('title') or '').strip()
    body         = (data.get('body') or '').strip() or None
    url          = (data.get('url') or '').strip() or None
    notif_type   = (data.get('notif_type') or 'admin').strip()
    send_tg      = bool(data.get('send_telegram', False))

    if not title:
        return jsonify({'error': 'Укажи заголовок'}), 400

    users, filter_desc = _build_notify_targets(data)
    if not users:
        return jsonify({'error': 'Нет пользователей под эти фильтры'}), 400

    conn = get_db()
    try:
        for u in users:
            conn.execute(
                'INSERT INTO site_notifications (user_id, type, title, body, url) VALUES (?,?,?,?,?)',
                (u['id'], notif_type, title, body, url)
            )
        conn.execute(
            '''INSERT INTO admin_broadcasts
               (admin_id, title, body, url, notif_type, filter_desc, recipients_count, send_telegram)
               VALUES (?,?,?,?,?,?,?,?)''',
            (session.get('user_id'), title, body, url, notif_type, filter_desc, len(users), 1 if send_tg else 0)
        )
        conn.commit()
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500
    conn.close()

    # Telegram-рассылка (асинхронно, не блокирует ответ)
    if send_tg and _bot_loop and _bot_loop.is_running() and telegram_app:
        tg_targets = [u for u in users if u.get('telegram_id')]
        if tg_targets:
            tg_title = title
            tg_body  = body
            tg_url   = url
            async def _bulk_tg():
                msg = f"📢 <b>{tg_title}</b>"
                if tg_body:
                    msg += f"\n\n{tg_body}"
                if tg_url:
                    full = tg_url if tg_url.startswith('http') else f"{SITE_URL}{tg_url}"
                    msg += f"\n\n🔗 <a href='{full}'>Подробнее</a>"
                for u in tg_targets:
                    try:
                        await telegram_app.bot.send_message(
                            chat_id=u['telegram_id'], text=msg, parse_mode='HTML'
                        )
                    except Exception:
                        pass
            asyncio.run_coroutine_threadsafe(_bulk_tg(), _bot_loop)

    return jsonify({'ok': True, 'sent': len(users)})


@bp.route('/api/admin/notify/history')
@admin_required
def api_admin_notify_history():
    """История рассылок."""
    conn = get_db()
    rows = conn.execute(
        '''SELECT id, title, body, notif_type, filter_desc, recipients_count, send_telegram, created_at
           FROM admin_broadcasts ORDER BY created_at DESC LIMIT 50'''
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@bp.route('/api/admin/notify/manga-search')
@admin_required
def api_admin_notify_manga_search():
    """Поиск манги по названию для фильтра подписок."""
    q = request.args.get('q', '').strip()
    if len(q) < 2:
        return jsonify([])
    conn = get_db()
    rows = conn.execute(
        "SELECT manga_id, manga_title FROM manga WHERE manga_title LIKE ? LIMIT 10",
        (f'%{q}%',)
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

