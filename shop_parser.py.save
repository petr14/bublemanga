# shop_parser.py - исправленная версия с обработкой ошибок

import requests
import json
import os
import time
from pathlib import Path

class MiraiCollectiblesParser:
    def __init__(self, base_url="https://api.senkuro.com/graphql", delay=1):
        self.base_url = base_url
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'Origin': 'https://api.senkuro.com',
            'Referer': 'https://api.senkuro.com/shop'
        })
        
    def create_query_payload(self, after_cursor=None):
        variables = {
            "after": after_cursor,
            "excludePurchased": False,
            "first": 20,
            "onlyLimited": False,
            "onlyWithSubscription": False,
            "orderBy": {
                "direction": "DESC",
                "field": "CREATED_AT"
            },
            "priceGroup": None,
            "rating": {
                "exclude": [],
                "include": []
            },
            "type": "BANNER",
            "visible": True
        }
        
        # Удаляем None значения
        variables = {k: v for k, v in variables.items() if v is not None}
        
        payload = {
            "operationName": "fetchCollectibles",
            "variables": variables,
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": "e0035214ce75614fa374dc898b1f73df98868783f02ffb9803972d2b6e2a8b72"
                }
            }
        }
        
        return payload
    
    def fetch_page(self, after_cursor=None):
        """Получает одну страницу с коллекциями"""
        payload = self.create_query_payload(after_cursor)
        
        try:
            print(f"Отправка запроса с after={after_cursor}")
            response = self.session.post(self.base_url, json=payload, timeout=30)
            
            print(f"Статус ответа: {response.status_code}")
            
            if response.status_code != 200:
                print(f"Ошибка HTTP {response.status_code}")
                print(f"Текст ответа: {response.text[:500]}")
                return None
            
            # Проверяем, что ответ - JSON
            content_type = response.headers.get('content-type', '')
            if 'application/json' not in content_type:
                print(f"Неожиданный Content-Type: {content_type}")
                print(f"Первые 500 символов ответа: {response.text[:500]}")
                return None
            
            return response.json()
            
        except requests.exceptions.Timeout:
            print("Таймаут при запросе")
            return None
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при запросе: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"Ошибка парсинга JSON: {e}")
            print(f"Первые 500 символов ответа: {response.text[:500]}")
            return None
    
    def extract_images_from_collectible(self, collectible):
        """Извлекает все варианты изображений из одного collectible"""
        images = []
        
        node = collectible.get('node', {})
        image_data = node.get('image', {})
        
        if not image_data:
            return images
        
        collectible_info = {
            'id': node.get('id'),
            'slug': node.get('slug'),
            'title_en': next((t['content'] for t in node.get('titles', []) if t['lang'] == 'EN'), ''),
            'title_ru': next((t['content'] for t in node.get('titles', []) if t['lang'] == 'RU'), ''),
            'type': node.get('type'),
            'price': node.get('price'),
            'created_at': node.get('createdAt')
        }
        
        # Оригинальное изображение
        original = image_data.get('original')
        if original and original.get('url'):
            images.append({
                **collectible_info,
                'url': original['url'],
                'width': None,
                'height': None,
                'format': original['url'].split('.')[-1] if '.' in original['url'] else 'unknown',
                'is_animated': image_data.get('animation', False),
                'variant_type': 'original'
            })
        
        # Варианты изображений (для анимированных)
        variants = image_data.get('variants', [])
        for variant in variants:
            if variant.get('url'):
                images.append({
                    **collectible_info,
                    'url': variant['url'],
                    'width': variant.get('width'),
                    'height': variant.get('height'),
                    'format': variant.get('format', 'unknown'),
                    'codec': variant.get('codec'),
                    'is_animated': True,
                    'variant_type': f"{variant.get('width')}x{variant.get('height')}"
                })
        
        return images
    
    def parse_all_pages(self):
        """Парсит все страницы и возвращает список всех изображений"""
        all_images = []
        page_number = 1
        after_cursor = None
        has_next = True
        max_pages = 10  # Ограничение на количество страниц для теста
        consecutive_errors = 0
        max_consecutive_errors = 3
        
        print("Начинаем парсинг...")
        
        while has_next and page_number <= max_pages:
            print(f"\nЗагрузка страницы {page_number}...")
            
            data = self.fetch_page(after_cursor)
            
            if not data:
                consecutive_errors += 1
                print(f"Ошибка получения данных ({consecutive_errors}/{max_consecutive_errors})")
                
                if consecutive_errors >= max_consecutive_errors:
                    print("Слишком много ошибок подряд. Прерываем.")
                    break
                    
                time.sleep(self.delay * 2)  # Удвоенная задержка при ошибке
                continue
            
            # Сбрасываем счетчик ошибок при успехе
            consecutive_errors = 0
            
            if 'data' not in data:
                print("Нет поля 'data' в ответе")
                print("Полный ответ:", json.dumps(data, indent=2)[:500])
                break
            
            collectibles_data = data.get('data', {}).get('collectibles', {})
            if not collectibles_data:
                print("Нет поля 'collectibles' в data")
                break
            
            edges = collectibles_data.get('edges', [])
            page_info = collectibles_data.get('pageInfo', {})
            
            if not edges:
                print("Нет коллекций на странице")
                break
            
            print(f"Найдено {len(edges)} коллекций на странице {page_number}")
            
            # Извлекаем изображения из каждой коллекции
            for collectible in edges:
                images = self.extract_images_from_collectible(collectible)
                all_images.extend(images)
                if images:
                    print(f"  - {images[0]['title_en']}: {len(images)} вариантов изображений")
            
            # Проверяем есть ли следующая страница
            has_next = page_info.get('hasNextPage', False)
            after_cursor = page_info.get('endCursor')
            
            if has_next and after_cursor:
                print(f"Следующая страница: {after_cursor}")
                page_number += 1
                time.sleep(self.delay)  # Задержка между запросами
            else:
                print("\nВсе страницы обработаны!")
                break
        
        if page_number >= max_pages:
            print(f"\nДостигнуто максимальное количество страниц ({max_pages})")
        
        return all_images
    
    def download_images(self, images, download_dir="static/banner"):
        """Скачивает все изображения в указанную директорию"""
        # Создаем директорию если её нет
        Path(download_dir).mkdir(parents=True, exist_ok=True)
        
        if not images:
            print("Нет изображений для скачивания")
            return 0
        
        print(f"\nНачинаем скачивание {len(images)} файлов в {download_dir}...")
        
        # Для отслеживания уже скачанных файлов (избегаем дубликатов)
        downloaded_files = set()
        downloaded_count = 0
        skipped_count = 0
        error_count = 0
        
        for idx, img in enumerate(images, 1):
            try:
                # Создаем имя файла
                slug = img['slug']
                variant = img['variant_type']
                file_ext = img['format'].lower()
                
                # Нормализуем расширения
                if file_ext == 'jpeg':
                    file_ext = 'jpg'
                
                # Очищаем имя файла от недопустимых символов
                safe_slug = "".join(c for c in slug if c.isalnum() or c in '-_').rstrip()
                if not safe_slug:
                    safe_slug = f"collectible_{img['id']}"
                
                filename = f"{safe_slug}_{variant}.{file_ext}"
                filepath = os.path.join(download_dir, filename)
                
                # Пропускаем если файл уже скачан
                if filename in downloaded_files or os.path.exists(filepath):
                    print(f"[{idx}/{len(images)}] Пропущено (уже есть): {filename}")
                    skipped_count += 1
                    continue
                
                # Скачиваем файл
                response = self.session.get(img['url'], stream=True, timeout=30)
                response.raise_for_status()
                
                # Проверяем размер контента
                content_length = response.headers.get('content-length')
                if content_length:
                    size_mb = int(content_length) / (1024 * 1024)
                    if size_mb > 0:
                        print(f"  Размер: {size_mb:.2f} MB")
                
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                downloaded_files.add(filename)
                downloaded_count += 1
                print(f"[{idx}/{len(images)}] Скачано: {filename}")
                
                # Небольшая задержка между скачиваниями
                time.sleep(0.5)
                
            except requests.exceptions.Timeout:
                print(f"[{idx}/{len(images)}] Таймаут при скачивании {img.get('url')}")
                error_count += 1
            except requests.exceptions.RequestException as e:
                print(f"[{idx}/{len(images)}] Ошибка сети: {e}")
                error_count += 1
            except Exception as e:
                print(f"[{idx}/{len(images)}] Ошибка: {e}")
                error_count += 1
        
        # Сохраняем информацию о файлах
        self.save_download_info(images, download_dir)
        
        print(f"\nИтоги скачивания:")
        print(f"  Скачано: {downloaded_count}")
        print(f"  Пропущено: {skipped_count}")
        print(f"  Ошибок: {error_count}")
        
        return downloaded_count
    
    def save_download_info(self, images, download_dir):
        """Сохраняет информацию о скачанных файлах"""
        info = []
        seen = set()
        
        for img in images:
            slug = img['slug']
            variant = img['variant_type']
            file_ext = img['format'].lower()
            
            if file_ext == 'jpeg':
                file_ext = 'jpg'
            
            safe_slug = "".join(c for c in slug if c.isalnum() or c in '-_').rstrip()
            if not safe_slug:
                safe_slug = f"collectible_{img['id']}"
            
            filename = f"{safe_slug}_{variant}.{file_ext}"
            
            if filename not in seen:
                seen.add(filename)
                info.append({
                    'filename': filename,
                    'slug': img['slug'],
                    'title_en': img['title_en'],
                    'title_ru': img['title_ru'],
                    'url': img['url'],
                    'width': img['width'],
                    'height': img['height'],
                    'format': img['format'],
                    'is_animated': img['is_animated'],
                    'variant': variant,
                    'created_at': img['created_at']
                })
        
        json_path = os.path.join(download_dir, 'images_info.json')
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(info, f, ensure_ascii=False, indent=2)
        
        print(f"\nИнформация о файлах сохранена в {json_path}")
    
    def save_to_json(self, images, filename="collectibles_images.json"):
        """Сохраняет информацию об изображениях в JSON файл"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(images, f, ensure_ascii=False, indent=2)
        print(f"\nИнформация сохранена в {filename}")
    
    def print_summary(self, images):
        """Выводит сводку по собранным данным"""
        print("\n" + "="*50)
        print("СВОДКА ПО СОБРАННЫМ ДАННЫМ")
        print("="*50)
        
        if not images:
            print("Нет данных для отображения")
            return
        
        total_collectibles = len(set(img['id'] for img in images))
        print(f"Всего коллекций: {total_collectibles}")
        print(f"Всего файлов изображений: {len(images)}")
        
        # Статистика по типам
        formats = {}
        resolutions = {}
        for img in images:
            fmt = img['format']
            formats[fmt] = formats.get(fmt, 0) + 1
            
            if img.get('width') and img.get('height'):
                res = f"{img['width']}x{img['height']}"
                resolutions[res] = resolutions.get(res, 0) + 1
        
        print("\nФорматы файлов:")
        for fmt, count in sorted(formats.items()):
            print(f"  - {fmt}: {count}")
        
        if resolutions:
            print("\nРазрешения:")
            for res, count in sorted(resolutions.items()):
                print(f"  - {res}: {count}")
        
        animated = sum(1 for img in images if img.get('is_animated', False))
        static = len(images) - animated
        print(f"\nАнимированных: {animated}")
        print(f"Статических: {static}")

def test_connection():
    """Тестирует соединение с API"""
    print("Тестирование соединения с API...")
    
    test_url = "https://mirai.senkuro.net/graphql"
    test_payload = {
        "operationName": "fetchCollectibles",
        "variables": {"first": 1, "type": "BANNER"},
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "e0035214ce75614fa374dc898b1f73df98868783f02ffb9803972d2b6e2a8b72"
            }
        }
    }
    
    try:
        session = requests.Session()
        response = session.post(test_url, json=test_payload, timeout=10)
        print(f"Статус: {response.status_code}")
        print(f"Content-Type: {response.headers.get('content-type')}")
        
        if response.status_code == 200:
            try:
                data = response.json()
                print("✓ API доступен и отвечает JSON")
                return True
            except:
                print("✗ Ответ не является JSON")
                print("Первые 200 символов:", response.text[:200])
                return False
        else:
            print(f"✗ Ошибка HTTP {response.status_code}")
            print("Ответ:", response.text[:200])
            return False
    except Exception as e:
        print(f"✗ Ошибка соединения: {e}")
        return False

def main():
    print("Парсер баннеров Mirai")
    print("=" * 50)
    
    # Тестируем соединение
    if not test_connection():
        print("\nНе удалось подключиться к API. Проверьте:")
        print("1. Доступность сайта https://mirai.senkuro.net")
        print("2. Настройки прокси (если используются)")
        print("3. Брандмауэр/сетевые ограничения")
        return
    
    # Создаем директории
    Path("static/banner").mkdir(parents=True, exist_ok=True)
    print("✓ Директория static/banner создана/проверена")
    
    # Создаем парсер
    parser = MiraiCollectiblesParser(delay=2)  # Увеличили задержку
    
    # Парсим все страницы
    all_images = parser.parse_all_pages()
    
    if not all_images:
        print("\nНе удалось получить изображения. Попробуйте:")
        print("1. Запустить скрипт позже")
        print("2. Проверить работу VPN (если сайт заблокирован)")
        print("3. Связаться с поддержкой сайта")
        return
    
    # Сохраняем информацию в JSON
    parser.save_to_json(all_images)
    
    # Выводим сводку
    parser.print_summary(all_images)
    
    # Спрашиваем хочет ли пользователь скачать изображения
    download = input("\nХотите скачать все изображения в static/banner? (y/n): ").lower()
    if download == 'y':
        downloaded = parser.download_images(all_images)
        print(f"\nУспешно скачано {downloaded} файлов в папку static/banner/")
    
    print("\nГотово!")

if __name__ == "__main__":
    main()
