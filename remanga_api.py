# -*- coding: utf-8 -*-
"""
remanga_api.py — Клиент для api.remanga.org

Написан по конфигурации MWX JSON v101 (rbgmanga).

Публичные методы:
  search(query, page=1, count=10)   → list[dict]
  get_manga(dir_slug)               → dict | None
  get_chapters(branch_id, ...)      → list[dict]
  get_chapter_pages(chapter_id)     → list[str]
"""

import requests
import logging
from typing import Optional

logger = logging.getLogger(__name__)

_TOKEN = "ET754qAHP4d4KMVjRrIl2jmem0cXQY"
_BASE  = "https://api.remanga.org/api"

_HEADERS = {
    "cookie":          f"token={_TOKEN}",
    "accept":          "*/*",
    "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "authorization":   f"Bearer {_TOKEN}",
    "content-type":    "application/json",
    "origin":          "https://remanga.org",
    "referer":         "https://remanga.org/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/130.0.0.0 Safari/537.36"
    ),
}


class RemangaAPI:
    """REST-клиент для api.remanga.org."""

    def __init__(self, timeout: int = 15):
        self._timeout = timeout
        self._session = requests.Session()
        self._session.headers.update(_HEADERS)

    # ── helpers ───────────────────────────────────────────────────────────────

    def _get(self, url: str, **params) -> Optional[dict]:
        try:
            r = self._session.get(url, params=params or None, timeout=self._timeout)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            logger.error(f"RemangaAPI GET {url}: {e}")
            return None

    # ── Поиск манги ───────────────────────────────────────────────────────────
    # manga_search_complete → search_link
    # GET /api/search/?count=10&page=N&query=...
    # Ответ: {"content": [{dir, main_name, secondary_name, en_name, cover_mid, ...}]}

    def search(self, query: str, page: int = 1, count: int = 10) -> list:
        """
        Поиск манги.

        Returns list[dict]:
            dir, en_name, title (main | secondary), cover_url
        """
        data = self._get(f"{_BASE}/search/", count=count, page=page, query=query)
        if not data:
            return []
        results = []
        for item in (data.get("content") or []):
            main      = item.get("main_name", "")
            secondary = item.get("secondary_name", "")
            title     = f"{main} | {secondary}" if secondary else main
            mid       = item.get("cover_mid", "") or ""
            cover_url = (f"https://remanga.org{mid}" if mid.startswith("/") else mid)
            results.append({
                "dir":       item.get("dir", ""),
                "en_name":   item.get("en_name", ""),
                "title":     title,
                "cover_url": cover_url,
            })
        return results

    # ── Детали манги ──────────────────────────────────────────────────────────
    # manga_complete → GET /api/titles/{dir}/
    # branch_id берётся из branches[0].id

    def get_manga(self, dir_slug: str) -> Optional[dict]:
        """
        Метаданные манги + branch_id для запроса глав.

        Returns dict:
            dir, title, secondary_name, en_name, description,
            cover_url, total_chapters, branch_id, genres, tags
        """
        data = self._get(f"{_BASE}/titles/{dir_slug}/")
        if not data:
            return None
        c = data.get("content") or {}
        if not c:
            return None

        branches  = c.get("branches") or []
        # branch_id: из первой ветки или из active_branch
        if branches:
            branch_id = branches[0].get("id")
        else:
            branch_id = c.get("active_branch")

        # Обложка: img.high, unicode-эскейпы u002F → /
        img  = c.get("img") or {}
        high = img.get("high", "").replace("u002F", "/")
        if high and not high.startswith("http"):
            cover_url = f"https://remanga.org/media/{high.lstrip('/')}"
        else:
            cover_url = high

        genres = [g["name"] for g in (c.get("genres")     or []) if g.get("name")]
        tags   = [t["name"] for t in (c.get("categories") or []) if t.get("name")]

        return {
            "dir":            c.get("dir", dir_slug),
            "title":          c.get("main_name", ""),
            "secondary_name": c.get("secondary_name", ""),
            "en_name":        c.get("en_name", ""),
            "description":    c.get("description", ""),
            "cover_url":      cover_url,
            "total_chapters": c.get("total_chapters", 0),
            "branch_id":      branch_id,
            "branches":       branches,
            "genres":         genres,
            "tags":           tags,
        }

    # ── Список глав ───────────────────────────────────────────────────────────
    # manga_complete → chapters_from_page
    # GET /api/titles/chapters/?branch_id=...&count=100&ordering=-index&user_data=0&page=N
    # Только главы с is_paid=false (JSON: add_chapter → next: "is_paid":false)

    def get_chapters(self, branch_id: int, max_pages: int = 100) -> list:
        """
        Все бесплатные главы ветки перевода.

        Returns list[dict]:
            id, tome, chapter, name, title
        """
        all_chapters: list = []
        for page in range(1, max_pages + 1):
            data = self._get(
                f"{_BASE}/titles/chapters/",
                branch_id=branch_id,
                count=100,
                ordering="-index",
                user_data=0,
                page=page,
            )
            if not data:
                break
            content = data.get("content") or []
            if not content:
                break

            for ch in content:
                if ch.get("is_paid"):
                    continue
                tome    = str(ch.get("tome")    or "").strip()
                chapter = str(ch.get("chapter") or "").strip()
                name    = ch.get("name") or ""

                parts: list = []
                if tome:    parts.append(f"Том {tome}")
                if chapter: parts.append(f"Глава {chapter}")
                if name:    parts.append(name)
                title = " ".join(parts) or f"id={ch.get('id')}"

                all_chapters.append({
                    "id":      ch.get("id"),
                    "tome":    tome,
                    "chapter": chapter,
                    "name":    name,
                    "title":   title,
                })

            # Проверяем, есть ли ещё страницы
            total = (data.get("props") or {}).get("total", 0)
            if len(all_chapters) >= total or len(content) < 100:
                break

        return all_chapters

    # ── Страницы главы ────────────────────────────────────────────────────────
    # chapter_complete → GET /api/v2/titles/chapters/{chapter_id}/
    #
    # Сервер (fallback_link):
    #   JSON: prefix.after="server", token1="fallback_link":"", token2='"', stop="pages"
    #   → из поля servers ищем первый fallback_link до начала pages
    #
    # Страницы:
    #   JSON: add_pages.start="pages", token1="/images/", token2='"'
    #         replace.prefix="images/" → итоговый URL = {server}images/{path}
    #   stop = "publishers"
    #
    # Тест: https://img.remanga.org/images/solo-leveling/hash/file.jpeg

    def get_chapter_pages(self, chapter_id) -> list:
        """
        Список URL изображений главы.

        API v2 возвращает данные напрямую (без обёртки content):
          {
            "server": {"fallback_link": "https://img3.reimg2.org/", ...},
            "pages":  [[{link: "https://...", ...}, ...], [...]]
          }
        Каждый элемент pages — группа (список страниц).
        link уже является полным URL.

        Returns list[str]
        """
        data = self._get(f"{_BASE}/v2/titles/chapters/{chapter_id}/")
        if not data:
            return []

        # v2 endpoint отдаёт данные прямо в корне ответа
        pages_raw = data.get("pages") or []

        urls: list = []
        for group in pages_raw:
            # Каждая группа — список страниц
            if isinstance(group, list):
                for page in group:
                    if not isinstance(page, dict):
                        continue
                    link = page.get("link") or page.get("url") or ""
                    if link:
                        urls.append(link)
            elif isinstance(group, dict):
                # На случай плоского списка
                link = group.get("link") or group.get("url") or ""
                if link:
                    urls.append(link)

        return urls
