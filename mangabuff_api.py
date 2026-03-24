"""
mangabuff_api.py — Скрапер для mangabuff.ru

Предоставляет:
  MangaBuffAPI        — базовый класс для работы с сайтом
  MangaBuffAPIExtended — расширенная версия с утилитами скачивания
"""

import re
import json
import time
import logging
import requests
from dataclasses import dataclass, field
from typing import List, Optional, Dict

logger = logging.getLogger(__name__)

BASE_URL  = "https://mangabuff.ru"
HEADERS   = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.5",
    "Referer": BASE_URL + "/",
}


@dataclass
class MangaCard:
    uniq:    str          # slug (e.g. "kak-nasytit-dyavola")
    title:   str
    url:     str
    cover:   str = ""


@dataclass
class ChapterInfo:
    uniq:   str           # chapter slug on mangabuff
    title:  str
    number: str = ""
    volume: str = ""
    link:   str = ""      # full URL


@dataclass
class ChapterPages:
    pages: List[str] = field(default_factory=list)


@dataclass
class MangaDetails:
    uniq:     str         # manga slug
    title:    str
    url:      str
    chapters: List[Dict]  # list of {"uniq", "title", "number", "volume", "link"}


class MangaBuffAPI:
    """Скрапер сайта mangabuff.ru."""

    def __init__(self, timeout: int = 20):
        self._timeout = timeout
        self._session = requests.Session()
        self._session.headers.update(HEADERS)

    # ──────────────────────────────────────────────────────────────────────
    # Вспомогательные методы
    # ──────────────────────────────────────────────────────────────────────

    def _get(self, url: str, **kwargs) -> Optional[requests.Response]:
        try:
            r = self._session.get(url, timeout=self._timeout, **kwargs)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            logger.error(f"MangaBuff GET {url}: {e}")
            return None

    @staticmethod
    def _extract_json_var(html: str, var_name: str) -> Optional[dict]:
        """Вытаскивает JSON из JS-переменной вида window.VAR = {...}; или var VAR = {...};"""
        patterns = [
            rf'window\.{var_name}\s*=\s*(\{{.*?\}});',
            rf'var\s+{var_name}\s*=\s*(\{{.*?\}});',
            rf'const\s+{var_name}\s*=\s*(\{{.*?\}});',
            rf'let\s+{var_name}\s*=\s*(\{{.*?\}});',
            rf'{var_name}\s*=\s*(\{{.*?\}});',
            # Тоже может быть массив
            rf'window\.{var_name}\s*=\s*(\[.*?\]);',
            rf'var\s+{var_name}\s*=\s*(\[.*?\]);',
        ]
        for pat in patterns:
            m = re.search(pat, html, re.DOTALL)
            if m:
                try:
                    return json.loads(m.group(1))
                except json.JSONDecodeError:
                    continue
        return None

    # ──────────────────────────────────────────────────────────────────────
    # Список манги
    # ──────────────────────────────────────────────────────────────────────

    def get_manga_list(self, page: int = 1) -> List[MangaCard]:
        """Получить список манги со страницы каталога."""
        url = f"{BASE_URL}/manga?page={page}"
        r = self._get(url)
        if not r:
            return []
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(r.text, "html.parser")
            results = []
            # Типовая разметка: карточки с классом manga-card, catalog__item, etc.
            selectors = [
                "div.manga-card",
                "div.catalog__item",
                "div.manga-item",
                "article.manga",
                "div.card",
            ]
            cards = []
            for sel in selectors:
                cards = soup.select(sel)
                if cards:
                    break

            for card in cards:
                a = card.find("a", href=True)
                if not a:
                    continue
                href = a["href"]
                # slug — последняя часть URL
                slug = href.rstrip("/").split("/")[-1]
                title_el = card.find(["h2", "h3", "span", "p"], class_=re.compile(r"title|name", re.I))
                title = title_el.get_text(strip=True) if title_el else slug
                img = card.find("img")
                cover = img.get("src", img.get("data-src", "")) if img else ""
                results.append(MangaCard(
                    uniq=slug,
                    title=title,
                    url=BASE_URL + href if href.startswith("/") else href,
                    cover=cover,
                ))
            return results
        except Exception as e:
            logger.error(f"get_manga_list page={page}: {e}")
            return []

    # ──────────────────────────────────────────────────────────────────────
    # Детали манги + список глав
    # ──────────────────────────────────────────────────────────────────────

    def get_manga_details(self, manga_url: str) -> Optional[MangaDetails]:
        """Получить информацию о манге и список всех глав."""
        r = self._get(manga_url)
        if not r:
            return None
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(r.text, "html.parser")
            slug = manga_url.rstrip("/").split("/")[-1]

            # Заголовок
            h1 = soup.find("h1")
            title = h1.get_text(strip=True) if h1 else slug

            chapters = self._parse_chapters_from_page(soup, manga_url, slug)
            return MangaDetails(uniq=slug, title=title, url=manga_url, chapters=chapters)
        except Exception as e:
            logger.error(f"get_manga_details {manga_url}: {e}")
            return None

    def get_manga_chapters(self, manga_slug: str) -> List[ChapterInfo]:
        """Сокращённый метод: получить главы манги по её slug."""
        url = f"{BASE_URL}/manga/{manga_slug}"
        details = self.get_manga_details(url)
        if not details:
            return []
        return [
            ChapterInfo(
                uniq=ch["uniq"],
                title=ch["title"],
                number=ch.get("number", ""),
                volume=ch.get("volume", ""),
                link=ch["link"],
            )
            for ch in details.chapters
        ]

    def _parse_chapters_from_page(self, soup, manga_url: str, manga_slug: str) -> List[Dict]:
        """Парсит список глав из страницы манги."""
        chapters = []

        # 1. Ищем JSON в script-тегах (Next.js / Nuxt / React)
        for script in soup.find_all("script"):
            text = script.string or ""
            # Типичные паттерны для встроенного JSON
            for var in ["__NUXT__", "__NEXT_DATA__", "chapters", "mangaChapters"]:
                data = self._extract_json_var(text, var)
                if data:
                    extracted = self._extract_chapters_from_json(data, manga_url, manga_slug)
                    if extracted:
                        return extracted

            # Ищем прямой массив chapters
            m = re.search(r'"chapters"\s*:\s*(\[.*?\])', text, re.DOTALL)
            if m:
                try:
                    raw = json.loads(m.group(1))
                    extracted = self._extract_chapters_from_json({"chapters": raw}, manga_url, manga_slug)
                    if extracted:
                        return extracted
                except Exception:
                    pass

        # 2. Парсинг HTML-списка глав
        list_selectors = [
            "div.chapters-list",
            "ul.chapters",
            "div.chapter-list",
            "div.chapters",
            ".manga__chapters",
        ]
        for sel in list_selectors:
            container = soup.select_one(sel)
            if container:
                for a in container.find_all("a", href=True):
                    href = a["href"]
                    ch_slug = href.rstrip("/").split("/")[-1]
                    text = a.get_text(" ", strip=True)
                    number, volume = self._parse_chapter_number(text, ch_slug)
                    chapters.append({
                        "uniq": ch_slug,
                        "title": text,
                        "number": number,
                        "volume": volume,
                        "link": BASE_URL + href if href.startswith("/") else href,
                    })
                if chapters:
                    return chapters

        # 3. Fallback: любые ссылки, содержащие /chapters/ или /chapter-
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if f"/manga/{manga_slug}/" in href and href != f"/manga/{manga_slug}":
                ch_slug = href.rstrip("/").split("/")[-1]
                if ch_slug == manga_slug:
                    continue
                text = a.get_text(" ", strip=True)
                number, volume = self._parse_chapter_number(text, ch_slug)
                # Дедупликация
                if not any(c["uniq"] == ch_slug for c in chapters):
                    chapters.append({
                        "uniq": ch_slug,
                        "title": text or ch_slug,
                        "number": number,
                        "volume": volume,
                        "link": BASE_URL + href if href.startswith("/") else href,
                    })

        return chapters

    def _extract_chapters_from_json(self, data: dict, manga_url: str, manga_slug: str) -> List[Dict]:
        """Рекурсивно ищет массив глав в JSON-структуре."""
        chapters = []

        def walk(obj, depth=0):
            if depth > 8:
                return
            if isinstance(obj, list):
                # Похоже на список глав?
                if obj and isinstance(obj[0], dict):
                    keys = set(obj[0].keys())
                    if keys & {"slug", "number", "chapter", "link", "url", "uniq"}:
                        for item in obj:
                            ch = self._normalize_chapter_dict(item, manga_url, manga_slug)
                            if ch:
                                chapters.append(ch)
                        return
                for item in obj:
                    walk(item, depth + 1)
            elif isinstance(obj, dict):
                for k, v in obj.items():
                    if k in ("chapters", "mangaChapters", "chapter_list", "items"):
                        walk(v, depth + 1)
                    else:
                        walk(v, depth + 1)

        walk(data)
        return chapters

    def _normalize_chapter_dict(self, item: dict, manga_url: str, manga_slug: str) -> Optional[Dict]:
        """Приводит произвольный dict главы к стандартному формату."""
        slug = (item.get("slug") or item.get("uniq") or item.get("id") or "")
        if not slug:
            return None
        slug = str(slug)
        number = str(item.get("number") or item.get("chapter_number") or item.get("num") or "")
        volume = str(item.get("volume") or item.get("chapter_volume") or item.get("vol") or "")
        title = (item.get("title") or item.get("name") or
                 item.get("chapter_name") or f"Глава {number}")
        link = item.get("link") or item.get("url") or f"{manga_url}/{slug}"
        return {
            "uniq": slug,
            "title": str(title),
            "number": number,
            "volume": volume,
            "link": link if link.startswith("http") else BASE_URL + link,
        }

    @staticmethod
    def _parse_chapter_number(text: str, slug: str) -> tuple:
        """Вытащить номер главы и тома из строки."""
        number = ""
        volume = ""
        # "Том 2 Глава 15 — Название"
        m = re.search(r'[Тт]ом\s*(\d+)', text)
        if m:
            volume = m.group(1)
        m = re.search(r'[Гг]лав[аы]\s*([\d.]+)', text)
        if m:
            number = m.group(1)
        # Из слага "glava-15" или "chapter-15"
        if not number:
            m = re.search(r'(?:glava|chapter|ch)-?([\d.]+)', slug, re.I)
            if m:
                number = m.group(1)
        return number, volume

    # ──────────────────────────────────────────────────────────────────────
    # Страницы главы
    # ──────────────────────────────────────────────────────────────────────

    def get_chapter_pages(self, chapter_url: str) -> ChapterPages:
        """Получить список URL изображений для главы."""
        r = self._get(chapter_url)
        if not r:
            return ChapterPages()
        try:
            pages = self._parse_pages_from_response(r, chapter_url)
            return ChapterPages(pages=pages)
        except Exception as e:
            logger.error(f"get_chapter_pages {chapter_url}: {e}")
            return ChapterPages()

    def _parse_pages_from_response(self, r: requests.Response, chapter_url: str) -> List[str]:
        """Извлекает URL изображений из HTML страницы главы."""
        html = r.text
        pages = []

        # 1. Ищем JSON с массивом страниц в JS-переменных
        for var in ["pages", "chapterPages", "__PAGES__", "images", "__CHAPTER_DATA__"]:
            m = re.search(
                rf'(?:window\.)?{var}\s*=\s*(\[.*?\])\s*;',
                html, re.DOTALL
            )
            if m:
                try:
                    raw = json.loads(m.group(1))
                    urls = self._urls_from_pages_json(raw)
                    if urls:
                        return urls
                except Exception:
                    pass

        # 2. Next.js: __NEXT_DATA__
        m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
        if m:
            try:
                nd = json.loads(m.group(1))
                urls = self._find_image_urls_in_json(nd)
                if urls:
                    return urls
            except Exception:
                pass

        # 3. Ищем JSON-строки с CDN URL
        patterns = [
            r'"(https?://[^"]+\.(?:jpg|jpeg|png|webp)(?:\?[^"]*)?)"',
            r"'(https?://[^']+\.(?:jpg|jpeg|png|webp)(?:\?[^']*)?)'",
        ]
        # Признаки НЕ-страниц манги
        _SKIP = ("/icon", "/logo", "/avatar", "/cover", "favicon",
                 "banner", "/posters/", "/img/manga/", "/assets/",
                 "placeholder", "noimage", "no-image")
        for pat in patterns:
            matches = re.findall(pat, html, re.IGNORECASE)
            filtered = [u for u in matches
                        if not any(x in u for x in _SKIP)]
            if len(filtered) >= 3:
                seen = set()
                for u in filtered:
                    if u not in seen:
                        seen.add(u)
                        pages.append(u)
                if pages:
                    return pages

        # 4. BeautifulSoup fallback — ищем img в reader-блоке
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")
            reader = (soup.find(class_=re.compile(r"reader|chapter-content|pages", re.I))
                      or soup)
            imgs = reader.find_all("img")
            for img in imgs:
                src = img.get("src") or img.get("data-src") or img.get("data-original", "")
                if src and re.search(r'\.(jpg|jpeg|png|webp)', src, re.I):
                    if not any(x in src for x in _SKIP):
                        pages.append(src)
        except Exception:
            pass

        return pages

    def _urls_from_pages_json(self, raw) -> List[str]:
        """Вытаскивает URL из JSON-структуры со страницами."""
        urls = []
        if isinstance(raw, list):
            for item in raw:
                if isinstance(item, str) and item.startswith("http"):
                    urls.append(item)
                elif isinstance(item, dict):
                    u = (item.get("url") or item.get("src") or item.get("image") or
                         item.get("path") or item.get("link") or "")
                    if not u and "image" in item and isinstance(item["image"], dict):
                        u = (item["image"].get("url") or item["image"].get("src") or
                             item["image"].get("compress", {}).get("url", ""))
                    if u:
                        urls.append(u)
        return urls

    def _find_image_urls_in_json(self, obj, depth=0) -> List[str]:
        """Рекурсивно ищет массив image-URL в JSON-дереве."""
        if depth > 10:
            return []
        if isinstance(obj, list):
            if obj and isinstance(obj[0], str) and obj[0].startswith("http"):
                if re.search(r'\.(jpg|jpeg|png|webp)', obj[0], re.I):
                    return obj
            for item in obj:
                r = self._find_image_urls_in_json(item, depth + 1)
                if r:
                    return r
        elif isinstance(obj, dict):
            for k, v in obj.items():
                if k in ("pages", "images", "imgs", "pageList"):
                    r = self._find_image_urls_in_json(v, depth + 1)
                    if r:
                        return r
            for v in obj.values():
                r = self._find_image_urls_in_json(v, depth + 1)
                if r:
                    return r
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Расширенная версия (была в оригинальном файле пользователя)
# ─────────────────────────────────────────────────────────────────────────────

import asyncio
import os
from typing import Callable

try:
    import aiohttp
    import aiofiles
    _ASYNC_AVAILABLE = True
except ImportError:
    _ASYNC_AVAILABLE = False


class MangaBuffAPIExtended(MangaBuffAPI):
    """Расширенная версия API с дополнительными возможностями."""

    def get_all_manga(self, start_page: int = 1, end_page: int = 228,
                      callback: Callable = None) -> List[MangaCard]:
        """Получает всю мангу со всех страниц."""
        all_manga = []
        for page in range(start_page, end_page + 1):
            if callback:
                callback(page, end_page)
            manga_list = self.get_manga_list(page)
            all_manga.extend(manga_list)
            time.sleep(0.5)
        return all_manga

    async def download_chapter_images_async(self, chapter_url: str, save_dir: str) -> List[str]:
        """Асинхронное скачивание изображений главы."""
        if not _ASYNC_AVAILABLE:
            raise RuntimeError("aiohttp/aiofiles не установлены")
        pages = self.get_chapter_pages(chapter_url)
        if not pages.pages:
            return []
        os.makedirs(save_dir, exist_ok=True)
        saved_files = []
        async with aiohttp.ClientSession() as sess:
            tasks = []
            for idx, img_url in enumerate(pages.pages, 1):
                ext = img_url.split(".")[-1].split("?")[0]
                if ext not in ["jpg", "jpeg", "png", "webp"]:
                    ext = "jpg"
                filepath = os.path.join(save_dir, f"{idx:03d}.{ext}")
                tasks.append(self._download_image(sess, img_url, filepath))
            saved_files = await asyncio.gather(*tasks)
        return [f for f in saved_files if f]

    async def _download_image(self, session, url: str, filepath: str) -> Optional[str]:
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    async with aiofiles.open(filepath, "wb") as f:
                        await f.write(await response.read())
                    return filepath
        except Exception as e:
            logger.warning(f"Ошибка скачивания {url}: {e}")
        return None

    def download_chapter_images(self, chapter_url: str, save_dir: str) -> List[str]:
        """Синхронное скачивание изображений главы."""
        pages = self.get_chapter_pages(chapter_url)
        if not pages.pages:
            return []
        os.makedirs(save_dir, exist_ok=True)
        saved = []
        for idx, img_url in enumerate(pages.pages, 1):
            ext = img_url.split(".")[-1].split("?")[0]
            if ext not in ["jpg", "jpeg", "png", "webp"]:
                ext = "jpg"
            filepath = os.path.join(save_dir, f"{idx:03d}.{ext}")
            try:
                r = self._session.get(img_url, timeout=30, stream=True)
                if r.status_code == 200:
                    with open(filepath, "wb") as f:
                        for chunk in r.iter_content(8192):
                            f.write(chunk)
                    saved.append(filepath)
            except Exception as e:
                logger.warning(f"Ошибка скачивания {img_url}: {e}")
        return saved

    def download_manga(self, manga_url: str, save_dir: str = "downloads",
                       async_download: bool = False) -> Dict[str, List[str]]:
        """Скачивает всю мангу (все главы)."""
        details = self.get_manga_details(manga_url)
        if not details:
            logger.error("Не удалось получить информацию о манге")
            return {}
        manga_dir = os.path.join(save_dir, details.uniq)
        os.makedirs(manga_dir, exist_ok=True)
        result = {}
        if async_download and _ASYNC_AVAILABLE:
            async def _download_all():
                tasks = []
                for ch in details.chapters:
                    ch_dir = os.path.join(manga_dir, ch["uniq"])
                    tasks.append(self.download_chapter_images_async(ch["link"], ch_dir))
                chapters_pages = await asyncio.gather(*tasks)
                return {ch["uniq"]: p for ch, p in zip(details.chapters, chapters_pages)}
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(_download_all())
            loop.close()
        else:
            for idx, ch in enumerate(details.chapters, 1):
                logger.info(f"Загрузка главы {idx}/{len(details.chapters)}: {ch['title']}")
                ch_dir = os.path.join(manga_dir, ch["uniq"])
                pages = self.download_chapter_images(ch["link"], ch_dir)
                result[ch["uniq"]] = pages
        return result
