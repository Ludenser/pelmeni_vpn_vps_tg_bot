# VPN Telegram Bot

Telegram-бот для управления пользователями VLESS/Reality в `x-ui`.

## Что умеет

- роли: владелец, админ, ограниченный пользователь;
- управление профилями через Telegram;
- квоты на количество активных профилей для ограниченных пользователей;
- интерактивные пошаговые команды для операций с именами;
- хранение ролей в SQLite (`/etc/vpn-bot.sqlite`).

## Конфигурация

Переменные окружения читаются из `/etc/vpn-bot.env` (через systemd):

- `VPN_BOT_TOKEN`
- `VPN_OWNER_ID`
- `VPN_SERVER_IP`
- `VPN_PUBLIC_KEY`
- `VPN_SHORT_ID`
- `VPN_SNI`
- `VPN_ALERT_COOLDOWN_SEC`

## Сервис

- unit: `/etc/systemd/system/vpn-bot.service`
- запуск: `systemctl restart vpn-bot.service`
- статус: `systemctl status vpn-bot.service`

## Разработка

- файл приложения: `vpn-bot.py`
- проверка синтаксиса: `python3 -m py_compile vpn-bot.py`
