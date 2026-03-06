#!/bin/bash

# ==============================================
# Скрипт получения SSL-сертификата для bubblemanga.ru
# С проверкой занятости портов 80 и 443
# ==============================================

DOMAIN=“bubblemanga.ru”
EMAIL=“admin@bubblemanga.ru”  # Замените на ваш реальный email

# Цвета для вывода
RED=’\033[0;31m‘
GREEN=’\033[0;32m‘
YELLOW=’\033[1;33m‘
NC=’\033[0m‘ # No Color

# Функция проверки порта
check_port() {
    if ss -tuln | grep ":$1 " > /dev/null; then
        return 0 # Порт занят
    else
        return 1 # Порт свободен
    fi
}

echo -e ”${YELLOW}Начинаем подготовку к получению SSL-сертификата для $DOMAIN${NC}“

# Шаг 1: Проверка порта 80
echo -n ”Проверка порта 80... “
if check_port 80; then
    echo -e ”${RED}ЗАНЯТ${NC}“
    echo ”Необходимо освободить порт 80 для верификации домена.“
    echo ”Попытка найти процесс, занимающий порт 80:“
    sudo lsof -i :80 || sudo ss -lptn ”sport = :80“

    echo
    echo ”Вам нужно остановить службу, которая использует порт 80.“
    echo ”Например:“
    echo ”  sudo systemctl stop nginx“
    echo ”  sudo systemctl stop apache2“
    echo ”  docker stop \$(docker ps -q --filter publish=80)“
    echo
    read -p ”После освобождения порта 80 нажмите Enter для продолжения... “
else
    echo -e ”${GREEN}СВОБОДЕН${NC}“
fi

# Повторная проверка после ожидания
if check_port 80; then
    echo -e ”${RED}Ошибка: Порт 80 все еще занят. Скрипт прерван.${NC}“
    exit 1
fi

# Шаг 2: Установка Certbot (если не установлен)
echo
echo -e ”${YELLOW}Установка Certbot...${NC}“
if ! command -v certbot &> /dev/null; then
    sudo apt update
    sudo apt install -y certbot
    if [ $? -eq 0 ]; then
        echo -e ”${GREEN}Certbot успешно установлен.${NC}“
    else
        echo -e ”${RED}Ошибка установки Certbot. Попробуйте установить вручную.${NC}“
        exit 1
    fi
else
    echo -e ”${GREEN}Certbot уже установлен.${NC}“
fi

# Шаг 3: Получение сертификата (standalone режим, так как 443 занят)
echo
echo -e ”${YELLOW}Получаем сертификат для $DOMAIN...${NC}“
echo ”Используется временный веб-сервер на порту 80 (режим standalone).“

sudo certbot certonly --standalone \
    -d $DOMAIN \
    --non-interactive \
    --agree-tos \
    --email $EMAIL \
    --force-renewal \
    --http-01-port 80

# Шаг 4: Проверка результата
if [ $? -eq 0 ]; then
    echo
    echo -e ”${GREEN}✅ Сертификат успешно получен!${NC}“
    echo ”--------------------------------------------------“
    echo ”📍 Путь к сертификатам:“
    echo ”  Ключ: /etc/letsencrypt/live/$DOMAIN/privkey.pem“
    echo ”  Сертификат: /etc/letsencrypt/live/$DOMAIN/fullchain.pem“
    echo ”--------------------------------------------------“

    # Шаг 5: Информация о порте 443
    echo
    echo -e ”${YELLOW}Проверка порта 443...${NC}“
    if check_port 443; then
        echo -e ”${RED}Порт 443 занят.${NC}“
        echo ”Для работы HTTPS вам нужно настроить Nginx/Apache так, чтобы они слушали 443 порт,“
        echo ”используя полученные выше сертификаты.“
        echo
        echo ”Пример конфигурации для Nginx:“
        echo ”  server {“
        echo ”      listen 443 ssl;“
        echo ”      server_name $DOMAIN;“
        echo ”      ssl_certificate /etc/letsencrypt/live/$DOMAIN/fullchain.pem;“
        echo ”      ssl_certificate_key /etc/letsencrypt/live/$DOMAIN/privkey.pem;“
        echo ”      # ... остальные настройки ...“
        echo ”  }“
        echo
        echo ”Чтобы освободить порт 443, найдите и остановите процесс:“
        echo ”  sudo lsof -i :443“
    else
        echo -e ”${GREEN}Порт 443 свободен.${NC} Вы можете сразу настроить HTTPS на 443 порту.“
    fi

    # Шаг 6: Информация о продлении
    echo
    echo -e ”${YELLOW}Настройка автоматического продления:${NC}“
    echo ”Сертификат действителен 90 дней. Для проверки продления выполните:“
    echo ”  sudo certbot renew --dry-run“

else
    echo
    echo -e ”${RED}❌ Ошибка при получении сертификата.${NC}“
    echo ”Возможные причины:“
    echo ”  1. Порт 80 был занят во время выполнения (проверьте еще раз).“
    echo ”  2. Домен $DOMAIN не смотрит на IP этого сервера.“
    echo ”  3. Проблемы с DNS или файерволом (межсетевым экраном).“
fi