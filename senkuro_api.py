# -*- coding: utf-8 -*-
"""
Клиент для работы с GraphQL API сервиса Senkuro.

Класс SenkuroAPI инкапсулирует все HTTP-запросы к API и базовый парсинг
ответов. Логика сохранения в БД и бизнес-логика остаются в main.py.
"""

import requests
import logging

logger = logging.getLogger(__name__)


class SenkuroAPI:
    """Клиент GraphQL API Senkuro"""

    GRAPHQL_URL = "https://api.senkuro.com/graphql"
    HEADERS = {"Content-Type": "application/json"}

    # Запрос данных главной страницы (последние главы)
    MAIN_PAGE_PAYLOAD = {
        "extensions": {
            "persistedQuery": {
                "sha256Hash": "c1a427930add310e7e68870182c3b17a84b3bac00a46bed07b72d0760f5fd09a",
                "version": 1
            }
        },
        "operationName": "fetchMainPage",
        "variables": {
            "label": {"exclude": ["hentai"]},
            "skipAnime": True,
            "skipLabelsSpotlight": False,
            "skipManga": False,
            "skipPosts": True
        }
    }

    # ── Вспомогательный метод ──────────────────────────────────────────────

    def _post(self, payload, timeout=10):
        """Выполнить POST-запрос к GraphQL API и вернуть распарсенный JSON."""
        import json
        
        # Сериализуем payload в JSON строку
        json_payload = json.dumps(payload)
        # print(f"Сериализованный JSON для отправки: {json_payload}")
        
        # # Проверим, есть ли в строке "after": null
        # if '"after": null' in json_payload:
        #     print("✅ after успешно преобразован в null")
        # else:
        #     print("❌ after НЕ преобразован в null")
        
        response = requests.post(
            self.GRAPHQL_URL,
            json=payload,  # или можно использовать data=json_payload с правильными headers
            headers=self.HEADERS,
            timeout=timeout
        )
        response.raise_for_status()
        return response.json()

    # ── Спотлайты (блоки главной страницы) ────────────────────────────────

    def fetch_spotlights(self, after=None, website_mode="SENKURO"):
        """
        Получить одну страницу экспериментальных спотлайтов.

        Args:
            after: курсор пагинации (None для первой страницы, "2", "4" и т.д.)
            website_mode: режим сайта (по умолчанию SENKURO)

        Returns:
            dict: {"edges": [...], "pageInfo": {"hasNextPage": bool, "endCursor": str|None}}
        """
        after_value = after
        payload = {
            "extensions": {
                "persistedQuery": {
                    "sha256Hash": "f5264f555ff8bfde7b5b985cd8eafc0720b159a4e5bf0e6874a1d3b51eb20a9e",
                    "version": 1
                }
            },
            "operationName": "fetchExperimentalSpotlights",
            "variables": {
                "after": after_value,
                "websiteMode": website_mode
            }
        }
        print(payload)
        try:
            data = self._post(payload)
            print(data)
            # Безопасное извлечение данных с проверкой на None
            if data is None:
                logger.error(f"❌ Получен None ответ от API (after={after})")
                return {"edges": [], "pageInfo": {"hasNextPage": False, "endCursor": None}}
            
            # Получаем data и проверяем на None
            data_obj = data.get("data")
            if data_obj is None:
                logger.error(f"❌ В ответе отсутствует поле 'data' (after={after})")
                logger.debug(f"Ответ API: {data}")
                return {"edges": [], "pageInfo": {"hasNextPage": False, "endCursor": None}}
            
            # Получаем experimentalSpotlights и проверяем на None
            result = data_obj.get("experimentalSpotlights")
            if result is None:
                logger.error(f"❌ В ответе отсутствует поле 'experimentalSpotlights' (after={after})")
                logger.debug(f"data_obj: {data_obj}")
                return {"edges": [], "pageInfo": {"hasNextPage": False, "endCursor": None}}
            
            # Безопасное получение edges и pageInfo
            edges = result.get("edges", [])
            if edges is None:  # На случай если edges = null в ответе
                edges = []
                
            page_info = result.get("pageInfo", {})
            if page_info is None:  # На случай если pageInfo = null в ответе
                page_info = {}
            
            logger.info(
                f"[fetchExperimentalSpotlights] after={after!r} → "
                f"{len(edges)} edges, hasNextPage={page_info.get('hasNextPage')}, "
                f"endCursor={page_info.get('endCursor')!r}"
            )
            
            # Безопасное логирование каждого спотлайта
            for i, edge in enumerate(edges):
                if edge is None:
                    logger.warning(f"  spotlight {i}: edge is None")
                    continue
                    
                node = edge.get("node")
                if node is None:
                    logger.warning(f"  spotlight {i}: node is None")
                    continue
                
                # Безопасное получение заголовков
                titles = node.get("titles") or []
                if titles is None:
                    titles = []
                    
                ru = next((t["content"] for t in titles if t and t.get("lang") == "RU"), None)
                en = next((t["content"] for t in titles if t and t.get("lang") == "EN"), None)
                
                # Безопасное получение количества манг
                nodes = node.get("nodes") or []
                if nodes is None:
                    nodes = []
                manga_count = len(nodes)
                
                logger.info(
                    f"  spotlight id={node.get('id')} ru={ru!r} en={en!r} mangas={manga_count}"
                )
                
            return result
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения спотлайтов (after={after}): {e}", exc_info=True)
            return {"edges": [], "pageInfo": {"hasNextPage": False, "endCursor": None}}
    # ── Поиск ─────────────────────────────────────────────────────────────

    def search(self, query, max_results=50):
        """
        Поиск манги по названию с пагинацией.

        Args:
            query: строка поиска
            max_results: максимальное количество результатов

        Returns:
            list[dict]: список манг (manga_id, manga_slug, manga_title, cover_url и т.д.)
        """
        results = []
        after = None

        while len(results) < max_results:
            payload = {
                "extensions": {
                    "persistedQuery": {
                        "sha256Hash": "e64937b4fc9c921c2141f2995473161bed921c75855c5de934752392175936bc",
                        "version": 1
                    }
                },
                "operationName": "search",
                "variables": {
                    "query": query,
                    "type": "MANGA",
                    "first": 20,
                    "after": after
                }
            }

            try:
                data = self._post(payload)
                if not data:
                    logger.warning(f"⚠️ search: API вернул пустой ответ для «{query}»")
                    break
                if data.get("errors"):
                    logger.error(f"❌ search GraphQL errors: {data['errors']}")
                    break

                search_data = (data.get("data") or {}).get("search", {})
                edges = search_data.get("edges") or []
                page_info = search_data.get("pageInfo") or {}

                logger.info(f"🔍 search: страница after={after!r} → {len(edges)} результатов для «{query}»")

                for edge in edges:
                    node = edge.get("node") or {}

                    titles = node.get("titles") or []
                    ru_title = next((t.get("content") for t in titles if t and t.get("lang") == "RU"), None)
                    en_title = titles[0].get("content", "") if titles else node.get("originalName", "")

                    cover = node.get("cover") or {}
                    cover_url = (
                        (cover.get("original") or {}).get("url", "") or
                        (cover.get("preview") or {}).get("url", "")
                    )

                    results.append({
                        'manga_id': node.get('id'),
                        'manga_slug': node.get('slug'),
                        'manga_title': ru_title or en_title,
                        'original_name': node.get('originalName'),
                        'manga_type': node.get('mangaType'),
                        'manga_status': node.get('mangaStatus'),
                        'rating': node.get('mangaRating'),
                        'cover_url': cover_url,
                        'translation_status': node.get('translitionStatus')
                    })

                if not page_info.get("hasNextPage") or not page_info.get("endCursor"):
                    break
                after = page_info["endCursor"]

            except Exception as e:
                logger.error(f"❌ Ошибка поиска по запросу «{query}»: {e}")
                break

        logger.info(f"🔍 search: итого {len(results)} результатов для «{query}»")
        return results[:max_results]

    # ── Детальная информация о манге ──────────────────────────────────────

    def fetch_manga(self, manga_slug):
        """
        Получить детальную информацию о манге.

        Args:
            manga_slug: уникальный слаг манги

        Returns:
            dict|None: данные манги (manga_id, manga_title, branch_id, tags и т.д.)
                       или None при ошибке / если манга не найдена
        """
        payload = {
            "extensions": {
                "persistedQuery": {
                    "sha256Hash": "6d8b28abb9a9ee3199f6553d8f0a61c005da8f5c56a88ebcf3778eff28d45bd5",
                    "version": 1
                }
            },
            "operationName": "fetchManga",
            "variables": {"slug": manga_slug}
        }

        try:
            data = self._post(payload)
            manga = data.get("data", {}).get("manga", {})

            if not manga:
                return None

            # Русское или оригинальное название
            titles = manga.get("titles", [])
            ru_title = next((t["content"] for t in titles if t["lang"] == "RU"), None)
            original_name = manga.get("originalName", {}).get("content", "")

            # Описание (берём русскую локализацию)
            description = ""
            for loc in manga.get("localizations", []):
                if loc.get("lang") != "RU":
                    continue
                for block in loc.get("description") or []:
                    if block.get("type") == "paragraph":
                        for item in block.get("content", []):
                            if item.get("type") == "text":
                                description += item.get("text", "")
                break

            # Обложка
            cover = manga.get("cover", {})
            cover_url = (
                cover.get("main", {}).get("url") or
                cover.get("original", {}).get("url") or
                cover.get("preview", {}).get("url", "")
            )

            # Теги/лейблы
            tags = []
            for label in manga.get("labels", []):
                label_titles = label.get("titles", [])
                ru_label = next((t["content"] for t in label_titles if t["lang"] == "RU"), None)
                en_label = next((t["content"] for t in label_titles if t["lang"] == "EN"), None)
                if ru_label or en_label:
                    tags.append(ru_label or en_label)

            # Выбор ветки перевода (primary или первая доступная)
            manga_id = manga.get('id')
            branch_id = manga_id  # Запасное значение

            branches = manga.get('branches', [])
            if branches:
                primary = next((b for b in branches if b.get('primaryBranch')), None)
                selected = primary or branches[0]
                branch_id = selected.get('id', manga_id)
                logger.info(
                    f"🌿 Ветка для {manga_slug}: {branch_id} "
                    f"(primary={selected.get('primaryBranch', False)}, "
                    f"chapters={selected.get('chapters', 0)})"
                )
            else:
                logger.warning(f"⚠️ Ветки не найдены для {manga_slug}, используем manga_id")

            return {
                'manga_id': manga_id,
                'manga_slug': manga.get('slug'),
                'manga_title': ru_title or original_name,
                'original_name': original_name,
                'manga_type': manga.get('type'),
                'manga_status': manga.get('status'),
                'rating': manga.get('rating'),
                'views': manga.get('views', 0),
                'score': manga.get('score', 0),
                'chapters_count': manga.get('chapters', 0),
                'description': description,
                'cover_url': cover_url,
                'tags': tags[:10],
                'formats': manga.get('formats', []),
                'is_licensed': manga.get('isLicensed', False),
                'translation_status': manga.get('translitionStatus'),
                'branch_id': branch_id
            }
        except Exception as e:
            logger.error(f"❌ Ошибка получения деталей манги «{manga_slug}»: {e}")
            import traceback
            traceback.print_exc()
            return None

    # ── Главы манги ───────────────────────────────────────────────────────

    def fetch_manga_chapters_page(self, branch_id, after=None):
        """
        Получить одну страницу глав манги (с пагинацией).

        Args:
            branch_id: идентификатор ветки перевода
            after: курсор пагинации (None для первой страницы)

        Returns:
            dict: {"edges": [...], "pageInfo": {"hasNextPage": bool, "endCursor": str|None}}
                  Пустой dict при ошибке или тайм-ауте.
        """
        payload = {
            "extensions": {
                "persistedQuery": {
                    "sha256Hash": "8c854e121f05aa93b0c37889e732410df9ea207b4186c965c845a8d970bdcc12",
                    "version": 1
                }
            },
            "operationName": "fetchMangaChapters",
            "variables": {
                "after": after,
                "branchId": branch_id,
                "number": None,
                "orderBy": {
                    "direction": "DESC",
                    "field": "NUMBER"
                }
            }
        }

        try:
            data = self._post(payload, timeout=15)
            return data.get("data", {}).get("mangaChapters", {})
        except requests.exceptions.Timeout:
            logger.error(f"⏰ Тайм-аут при получении глав (branch_id={branch_id}, after={after})")
            return {}
        except Exception as e:
            logger.error(f"❌ Ошибка получения страницы глав (branch_id={branch_id}): {e}")
            return {}

    # ── Популярные манги ──────────────────────────────────────────────────

    def fetch_popular_manga(self, period="MONTH", limit=12):
        """
        Получить список популярных манг за указанный период.

        Args:
            period: период (DAY, WEEK, MONTH)
            limit: максимальное количество возвращаемых записей

        Returns:
            list[dict]: список манг (manga_id, manga_slug, manga_title, cover_url, score)
        """
        payload = {
            "extensions": {
                "persistedQuery": {
                    "sha256Hash": "896d217de6cea8cedadd67abbfeed5e17589e77708d1e38b4f6a726ae409ca67",
                    "version": 1
                }
            },
            "operationName": "fetchPopularMangaByPeriod",
            "variables": {"period": period}
        }

        try:
            data = self._post(payload)
            popular = data.get("data", {}).get("mangaPopularByPeriod", [])[:limit]

            result = []
            for manga in popular:
                titles = manga.get("titles", [])
                ru_title = next((t["content"] for t in titles if t["lang"] == "RU"), None)
                en_title = next((t["content"] for t in titles if t["lang"] == "EN"), None)

                cover = manga.get("cover", {})
                cover_url = (
                    cover.get("original", {}).get("url", "") or
                    cover.get("preview", {}).get("url", "")
                )

                result.append({
                    'manga_id': manga.get('id'),
                    'manga_slug': manga.get('slug'),
                    'manga_title': ru_title or en_title or manga.get('slug'),
                    'cover_url': cover_url,
                    'score': manga.get('score', 0)
                })

            return result
        except Exception as e:
            logger.error(f"❌ Ошибка получения популярных манг: {e}")
            return []

    # ── Главная страница / последние главы ────────────────────────────────

    def fetch_main_page(self):
        """
        Получить данные главной страницы (последние обновлённые главы).

        Returns:
            list: список edges из lastMangaChapters, или [] при ошибке
        """
        try:
            data = self._post(self.MAIN_PAGE_PAYLOAD)
            return data.get("data", {}).get("lastMangaChapters", {}).get("edges", [])
        except Exception as e:
            logger.error(f"❌ Ошибка получения данных главной страницы: {e}")
            return []

    # ── Страницы главы ────────────────────────────────────────────────────

    def fetch_chapter_pages(self, chapter_slug):
        """
        Получить список страниц (изображений) главы.

        Args:
            chapter_slug: уникальный слаг главы

        Returns:
            list: список объектов страниц (каждый содержит поле image.compress.url)
        """
        payload = {
            "extensions": {
                "persistedQuery": {
                    "sha256Hash": "8e166106650d3659d21e7aadc15e7e59e5def36f1793a9b15287c73a1e27aa50",
                    "version": 1
                }
            },
            "operationName": "fetchMangaChapter",
            "variables": {"slug": chapter_slug}
        }

        try:
            data = self._post(payload)
            return data.get("data", {}).get("mangaChapter", {}).get("pages", [])
        except Exception as e:
            logger.error(f"❌ Ошибка получения страниц главы «{chapter_slug}»: {e}")
            return []
