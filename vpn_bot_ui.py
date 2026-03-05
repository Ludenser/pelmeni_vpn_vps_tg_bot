from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from vpn_bot_config import PAGE_SIZE
from vpn_bot_xray import _coerce_limited_user_rec, get_inbound


def _format_identity(rec):
    full_name = ''
    username = ''
    if isinstance(rec, dict):
        full_name = str(rec.get('full_name', '') or '').strip()
        username = str(rec.get('username', '') or '').strip().lstrip('@')
    if full_name and username:
        return f'{full_name} (@{username})'
    if full_name:
        return full_name
    if username:
        return f'@{username}'
    return 'без имени'


def _limited_users_overview_payload(users: dict, page: int = 0, page_size: int = PAGE_SIZE):
    if not users:
        return 'Список лимитных пользователей пуст', None
    sorted_users = sorted(users.items(), key=lambda x: (not str(x[0]).isdigit(), str(x[0])))
    total = len(sorted_users)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    start = page * page_size
    end = start + page_size
    chunk = sorted_users[start:end]

    lines = [
        f'Лимитные пользователи (страница {page + 1}/{total_pages}):\n'
        'Нажми на ID, чтобы открыть действия.\n'
    ]
    keyboard = []
    for tg_id, rec in chunk:
        names, limit = _coerce_limited_user_rec(rec)
        used = len(names)
        profile = ', '.join(names[:2]) if names else ''
        if len(names) > 2:
            profile += f' ...(+{len(names) - 2})'
        p = profile if profile else 'профиль ещё не создан'
        identity = _format_identity(rec)
        lines.append(f'{tg_id} ({identity}) → {p} | создано: {used}/{limit}')
        username = ''
        if isinstance(rec, dict):
            username = str(rec.get('username', '') or '').strip().lstrip('@')
        btn_label = f'ID {tg_id} (@{username})' if username else f'ID {tg_id}'
        keyboard.append([InlineKeyboardButton(btn_label, callback_data=f'lu:open:{tg_id}')])
    if total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton('<<', callback_data=f'lu:list:{page - 1}'))
        nav_row.append(InlineKeyboardButton(f'{page + 1}/{total_pages}', callback_data='lu:list'))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton('>>', callback_data=f'lu:list:{page + 1}'))
        keyboard.append(nav_row)
    return '\n'.join(lines), InlineKeyboardMarkup(keyboard)


def _limited_user_card_payload(target_uid: str, users: dict, viewer_is_owner: bool):
    rec = users.get(str(target_uid))
    if rec is None:
        return f'Пользователь {target_uid} не найден', InlineKeyboardMarkup(
            [[InlineKeyboardButton('<< К списку', callback_data='lu:list')]]
        )

    tracked_names, limit = _coerce_limited_user_rec(rec)
    key_port = 443
    if isinstance(rec, dict):
        key_port = int(rec.get('key_port', 443) or 443)
    if key_port not in {443, 2087}:
        key_port = 443
    _, settings = get_inbound()
    active_names = []
    if settings:
        clients = settings.get('clients', [])
        active_lookup = {x.get('email', '').lower() for x in clients if x.get('email')}
        active_names = [n for n in tracked_names if n.lower() in active_lookup]

    lines = [
        f'Лимитный пользователь: {target_uid}',
        f'Идентификатор: {_format_identity(rec)}',
        f'Активные профили: {len(active_names)}/{limit}',
        f'Порт выдачи ключей: {key_port}',
    ]
    if active_names:
        lines.append('')
        lines.append('Профили:')
        for i, name in enumerate(active_names, 1):
            lines.append(f'{i}. {name}')
    else:
        lines.append('Профилей пока нет.')

    keyboard = [
        [InlineKeyboardButton('Изменить лимит', callback_data=f'lu:limit:{target_uid}')],
        [InlineKeyboardButton('Порт ключей', callback_data=f'lu:port:{target_uid}')],
        [InlineKeyboardButton('Переименовать профиль', callback_data=f'lu:sel:{target_uid}:ren')],
        [InlineKeyboardButton('Удалить профиль', callback_data=f'lu:sel:{target_uid}:del')],
    ]
    if viewer_is_owner:
        keyboard.append([InlineKeyboardButton('Удалить лимитного пользователя', callback_data=f'lu:rmask:{target_uid}')])
    keyboard.append([InlineKeyboardButton('<< К списку', callback_data='lu:list')])
    return '\n'.join(lines), InlineKeyboardMarkup(keyboard)
