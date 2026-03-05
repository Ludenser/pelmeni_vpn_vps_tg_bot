from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, MessageHandler, filters

from vpn_bot_access import (
    clear_pending,
    get_access_attempt_identity,
    get_pending,
    init_access_db,
    is_admin,
    is_allowed,
    is_owner,
    load_admins,
    load_users,
    migrate_access_from_json,
    notify_unauthorized_user,
    save_admins,
    save_users,
    seed_access_defaults,
    set_pending,
)
from vpn_bot_config import (
    BOT_TOKEN,
    DEFAULT_LIMITED_MAX_ACTIVE,
    OWNER_ID,
    PAGE_SIZE,
    PUBLIC_KEY,
    SERVER_IP,
    SHORT_ID,
    SNI,
    log,
)
from vpn_bot_ui import _limited_user_card_payload, _limited_users_overview_payload
from vpn_bot_xray import (
    _coerce_limited_user_rec,
    admin_add_profile,
    admin_delete_profile,
    admin_rename_profile,
    format_limited_profiles_text,
    get_inbound,
    get_limited_user_active_profiles,
    limited_user_delete_profile,
    limited_user_rename_profile,
    provision_limited_user_profile,
    send_user_links,
    validate_profile_name,
)


def _vpn_list_payload(clients: list, page: int = 0, page_size: int = PAGE_SIZE):
    total = len(clients)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    start = page * page_size
    end = start + page_size
    chunk = clients[start:end]

    keyboard = []
    for i, client in enumerate(chunk, start + 1):
        name = str(client.get('email', '—'))
        keyboard.append([InlineKeyboardButton(f'{i}. {name}', callback_data=f'link:{name}')])

    if total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton('<<', callback_data=f'listpg:{page - 1}'))
        nav_row.append(InlineKeyboardButton(f'{page + 1}/{total_pages}', callback_data='listpg:0'))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton('>>', callback_data=f'listpg:{page + 1}'))
        keyboard.append(nav_row)

    text = (
        f'Пользователи VPN ({total} шт), страница {page + 1}/{total_pages}.\n'
        'Нажми на имя чтобы получить ссылки:'
    )
    return text, InlineKeyboardMarkup(keyboard)


async def cmd_start(u: Update, c):
    uid = u.effective_user.id
    user = u.effective_user
    clear_pending(uid)
    if not is_allowed(uid):
        await notify_unauthorized_user(c.bot, user)
        await u.message.reply_text('У тебя нет доступа к этому боту.')
        return
    if is_owner(uid):
        await u.message.reply_text(
            'VPN Manager (владелец)\n\n'
            '/add Имя — добавить пользователя\n'
            '/list — список всех пользователей\n'
            '/link Имя — ссылки + QR-код\n'
            '/delete Имя — удалить пользователя\n'
            '/rename Имя НовоеИмя — переименовать пользователя\n'
            '/myvpn — создать/получить свой профиль\n'
            '/admins — список администраторов\n'
            '/addadmin ID — добавить администратора (только владелец)\n'
            '/removeadmin ID — удалить администратора (только владелец)\n'
            f'/adduser ID [лимит, по умолчанию {DEFAULT_LIMITED_MAX_ACTIVE}] — добавить лимитного пользователя (только владелец)\n'
            '/setuserlimit ID лимит — изменить лимит активных профилей (только владелец)\n'
            '/removeuser ID — удалить лимитного пользователя (только владелец)\n'
            '/users — список лимитных пользователей'
        )
        return
    if is_admin(uid):
        await u.message.reply_text(
            'VPN Manager (админ)\n\n'
            '/add Имя — добавить пользователя\n'
            '/list — список всех пользователей\n'
            '/link Имя — ссылки + QR-код\n'
            '/delete Имя — удалить пользователя\n'
            '/rename Имя НовоеИмя — переименовать пользователя\n'
            '/myvpn — создать/получить свой профиль\n'
            '/admins — список администраторов\n'
            '/users — список лимитных пользователей'
        )
        return
    await u.message.reply_text(
        'VPN Manager (лимитный доступ)\n\n'
        '/myvpn — создать профиль и получить ссылки\n'
        '/myvpn list — мои активные профили\n'
        '/myvpn rename СТАРОЕ НОВОЕ — переименовать свой профиль\n'
        '/myvpn delete ИМЯ — удалить свой профиль'
    )


async def cmd_add(u: Update, c):
    if not is_admin(u.effective_user.id):
        return
    if not c.args:
        set_pending(u.effective_user.id, 'admin_add_name')
        await u.message.reply_text(
            'Введи имя нового пользователя одним сообщением.\n'
            'Для отмены отправь: отмена'
        )
        return
    name = validate_profile_name(' '.join(c.args).strip())
    if not name:
        await u.message.reply_text('Некорректное имя. Разрешены буквы/цифры/пробел/._-@, до 64 символов.')
        return
    uuid, err = admin_add_profile(name)
    if err:
        await u.message.reply_text(err)
        return
    await send_user_links(u.message, c, name, uuid)


async def cmd_list(u: Update, c):
    if not is_admin(u.effective_user.id):
        return
    _, settings = get_inbound()
    if not settings:
        await u.message.reply_text('Ошибка: inbound не найден')
        return
    cl = settings['clients']
    if not cl:
        await u.message.reply_text('Список пуст')
        return
    text, markup = _vpn_list_payload(cl, page=0)
    await u.message.reply_text(text, reply_markup=markup)


async def cmd_link(u: Update, c):
    if not is_admin(u.effective_user.id):
        return
    if not c.args:
        await u.message.reply_text('Укажи имя: /link Антон')
        return
    name = ' '.join(c.args).strip()
    _, settings = get_inbound()
    if not settings:
        await u.message.reply_text('Ошибка: inbound не найден')
        return
    cl = next((x for x in settings['clients'] if x.get('email', '').lower() == name.lower()), None)
    if not cl:
        await u.message.reply_text(f'Пользователь {name} не найден')
        return
    await send_user_links(u.message, c, cl['email'], cl['id'])


async def cb_link(u: Update, c):
    query = u.callback_query
    await query.answer()
    if not is_admin(u.effective_user.id):
        return
    if not query.data.startswith('link:'):
        return
    name = query.data[5:]
    _, settings = get_inbound()
    if not settings:
        return
    cl = next((x for x in settings['clients'] if x.get('email', '') == name), None)
    if not cl:
        await query.message.reply_text(f'Пользователь {name} не найден')
        return
    await send_user_links(query.message, c, cl['email'], cl['id'])


async def cb_list_page(u: Update, c):
    query = u.callback_query
    await query.answer()
    if not is_admin(u.effective_user.id):
        return
    data = query.data or ''
    if not data.startswith('listpg:'):
        return
    page_raw = data.split(':', 1)[1] if ':' in data else '0'
    page = int(page_raw) if page_raw.isdigit() else 0
    _, settings = get_inbound()
    if not settings:
        await query.edit_message_text('Ошибка: inbound не найден')
        return
    cl = settings['clients']
    if not cl:
        await query.edit_message_text('Список пуст')
        return
    text, markup = _vpn_list_payload(cl, page=page)
    await query.edit_message_text(text, reply_markup=markup)


async def cmd_delete(u: Update, c):
    if not is_admin(u.effective_user.id):
        return
    if not c.args:
        set_pending(u.effective_user.id, 'admin_delete_name')
        await u.message.reply_text(
            'Введи имя пользователя для удаления.\n'
            'Для отмены отправь: отмена'
        )
        return
    name = validate_profile_name(' '.join(c.args).strip())
    if not name:
        await u.message.reply_text('Некорректное имя для удаления.')
        return
    err = admin_delete_profile(name)
    if err:
        await u.message.reply_text(err)
        return
    await u.message.reply_text(f'Пользователь {name} удалён')


async def cmd_rename(u: Update, c):
    if not is_admin(u.effective_user.id):
        return
    if not c.args:
        set_pending(u.effective_user.id, 'admin_rename_old')
        await u.message.reply_text(
            'Введи текущее имя пользователя для переименования.\n'
            'Для отмены отправь: отмена'
        )
        return
    if len(c.args) == 1:
        old_name = validate_profile_name(c.args[0].strip())
        if not old_name:
            await u.message.reply_text('Некорректное старое имя.')
            return
        set_pending(u.effective_user.id, 'admin_rename_new', {'old_name': old_name})
        await u.message.reply_text(f'Ок. Теперь введи новое имя для {old_name}.')
        return
    import shlex
    try:
        parts = shlex.split(' '.join(c.args))
    except Exception:
        parts = c.args
    if len(parts) < 2:
        await u.message.reply_text('Нужно два имени: /rename СтароеИмя НовоеИмя')
        return
    old_name = validate_profile_name(parts[0].strip())
    new_name = validate_profile_name(' '.join(parts[1:]).strip())
    if not old_name or not new_name:
        await u.message.reply_text('Некорректные имена. Разрешены буквы/цифры/пробел/._-@, до 64 символов.')
        return
    err = admin_rename_profile(old_name, new_name)
    if err:
        await u.message.reply_text(err)
        return
    await u.message.reply_text(f'Переименовано: {old_name} → {new_name}')


async def cmd_admins(u: Update, c):
    if not is_admin(u.effective_user.id):
        return
    admins = load_admins()
    lines = ['Администраторы бота:\n']
    for aid in sorted(admins):
        lines.append(f'{aid}' + (' (владелец)' if aid == OWNER_ID else ''))
    await u.message.reply_text('\n'.join(lines))


async def cmd_addadmin(u: Update, c):
    if not is_owner(u.effective_user.id):
        await u.message.reply_text('Только владелец может добавлять администраторов.')
        return
    if not c.args or not c.args[0].isdigit():
        await u.message.reply_text('Укажи Telegram ID: /addadmin 123456789')
        return
    new_id = int(c.args[0])
    admins = load_admins()
    if new_id in admins:
        await u.message.reply_text(f'{new_id} уже в списке')
        return
    admins.add(new_id)
    save_admins(admins)
    await u.message.reply_text(f'Добавлен: {new_id}\nСписок: {", ".join(str(i) for i in sorted(admins))}')


async def cmd_removeadmin(u: Update, c):
    if not is_owner(u.effective_user.id):
        await u.message.reply_text('Только владелец может удалять администраторов.')
        return
    if not c.args or not c.args[0].isdigit():
        await u.message.reply_text('Укажи Telegram ID: /removeadmin 123456789')
        return
    rm_id = int(c.args[0])
    if rm_id == OWNER_ID:
        await u.message.reply_text('Нельзя удалить владельца.')
        return
    admins = load_admins()
    if rm_id not in admins:
        await u.message.reply_text(f'{rm_id} не найден')
        return
    admins.discard(rm_id)
    save_admins(admins)
    await u.message.reply_text(f'Удалён: {rm_id}\nСписок: {", ".join(str(i) for i in sorted(admins))}')


async def cmd_adduser(u: Update, c):
    if not is_owner(u.effective_user.id):
        await u.message.reply_text('Только владелец может добавлять лимитных пользователей.')
        return
    if not c.args or not c.args[0].isdigit():
        await u.message.reply_text('Укажи Telegram ID: /adduser 123456789 [лимит]')
        return
    new_id = c.args[0]
    limit = DEFAULT_LIMITED_MAX_ACTIVE
    if len(c.args) > 1:
        if not c.args[1].isdigit():
            await u.message.reply_text('Лимит должен быть числом: /adduser 123456789 2')
            return
        limit = max(0, int(c.args[1]))
    users = load_users()
    if new_id in users:
        await u.message.reply_text(f'{new_id} уже в списке лимитных пользователей')
        return
    full_name, username = get_access_attempt_identity(new_id)
    users[new_id] = {
        'profile_names': [],
        'full_name': full_name,
        'username': username,
        'max_active': limit,
    }
    save_users(users)
    identity = f'\nИмя: {full_name or "—"}\nUsername: @{username}' if username else f'\nИмя: {full_name or "—"}'
    await u.message.reply_text(
        f'Добавлен лимитный пользователь: {new_id}\n'
        f'Лимит активных профилей: {limit}{identity}'
    )


async def cmd_setuserlimit(u: Update, c):
    if not is_owner(u.effective_user.id):
        await u.message.reply_text('Только владелец может менять лимит лимитного пользователя.')
        return
    if len(c.args) < 2 or not c.args[0].isdigit() or not c.args[1].isdigit():
        await u.message.reply_text('Использование: /setuserlimit 123456789 3')
        return
    uid = c.args[0]
    new_limit = max(0, int(c.args[1]))
    users = load_users()
    rec = users.get(uid)
    if rec is None:
        await u.message.reply_text(f'{uid} не найден')
        return
    if not isinstance(rec, dict):
        rec = {
            'profile_names': [str(rec)] if rec else [],
            'full_name': '',
            'username': '',
            'max_active': DEFAULT_LIMITED_MAX_ACTIVE,
        }
    rec['max_active'] = new_limit
    users[uid] = rec
    save_users(users)
    active_now = len(rec.get('profile_names', []))
    await u.message.reply_text(f'Обновлён лимит для {uid}: активных сейчас {active_now}/{new_limit}')


async def cmd_removeuser(u: Update, c):
    if not is_owner(u.effective_user.id):
        await u.message.reply_text('Только владелец может удалять лимитных пользователей.')
        return
    if not c.args or not c.args[0].isdigit():
        await u.message.reply_text('Укажи Telegram ID: /removeuser 123456789')
        return
    rm_id = c.args[0]
    users = load_users()
    if rm_id not in users:
        await u.message.reply_text(f'{rm_id} не найден')
        return
    users.pop(rm_id, None)
    save_users(users)
    await u.message.reply_text(f'Лимитный пользователь удалён: {rm_id}')


async def cmd_users(u: Update, c):
    if not is_admin(u.effective_user.id):
        return
    users = load_users()
    text, markup = _limited_users_overview_payload(users, page=0)
    if markup is None:
        await u.message.reply_text(text)
        return
    await u.message.reply_text(text, reply_markup=markup)


async def cb_limited_users(u: Update, c):
    query = u.callback_query
    await query.answer()
    uid = u.effective_user.id
    if not is_admin(uid):
        return
    data = query.data or ''
    if not data.startswith('lu:'):
        return

    parts = data.split(':')
    action = parts[1] if len(parts) > 1 else ''
    users = load_users()

    if action == 'list':
        page = 0
        if len(parts) >= 3 and parts[2].isdigit():
            page = int(parts[2])
        text, markup = _limited_users_overview_payload(users, page=page)
        if markup is None:
            await query.edit_message_text(text)
            return
        await query.edit_message_text(text, reply_markup=markup)
        return

    if action == 'open' and len(parts) >= 3:
        target_uid = parts[2]
        text, markup = _limited_user_card_payload(target_uid, users, is_owner(uid))
        await query.edit_message_text(text, reply_markup=markup)
        return

    if action == 'limit' and len(parts) >= 3:
        target_uid = parts[2]
        rec = users.get(str(target_uid))
        if rec is None:
            await query.answer('Пользователь не найден', show_alert=True)
            return
        _, current_limit = _coerce_limited_user_rec(rec)
        text = (
            f'Изменение лимита для {target_uid}\n'
            f'Текущий лимит: {current_limit}\n'
            'Выбери значение или введи вручную.'
        )
        keyboard = [
            [
                InlineKeyboardButton('1', callback_data=f'lu:limset:{target_uid}:1'),
                InlineKeyboardButton('2', callback_data=f'lu:limset:{target_uid}:2'),
                InlineKeyboardButton('3', callback_data=f'lu:limset:{target_uid}:3'),
            ],
            [
                InlineKeyboardButton('4', callback_data=f'lu:limset:{target_uid}:4'),
                InlineKeyboardButton('5', callback_data=f'lu:limset:{target_uid}:5'),
                InlineKeyboardButton('10', callback_data=f'lu:limset:{target_uid}:10'),
            ],
            [InlineKeyboardButton('Ввести вручную', callback_data=f'lu:limman:{target_uid}')],
            [InlineKeyboardButton('<< Назад', callback_data=f'lu:open:{target_uid}')],
        ]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if action == 'limset' and len(parts) >= 4:
        if not is_owner(uid):
            await query.answer('Только владелец может менять лимит', show_alert=True)
            return
        target_uid, raw_limit = parts[2], parts[3]
        if not raw_limit.isdigit():
            await query.answer('Некорректный лимит', show_alert=True)
            return
        rec = users.get(str(target_uid))
        if rec is None:
            await query.answer('Пользователь не найден', show_alert=True)
            return
        names, _ = _coerce_limited_user_rec(rec)
        rec_obj = {
            'profile_names': names,
            'full_name': str(rec.get('full_name', '') or '').strip() if isinstance(rec, dict) else '',
            'username': str(rec.get('username', '') or '').strip().lstrip('@') if isinstance(rec, dict) else '',
            'max_active': max(0, int(raw_limit))
        }
        users[str(target_uid)] = rec_obj
        save_users(users)
        text, markup = _limited_user_card_payload(target_uid, load_users(), is_owner(uid))
        await query.edit_message_text(text, reply_markup=markup)
        return

    if action == 'limman' and len(parts) >= 3:
        if not is_owner(uid):
            await query.answer('Только владелец может менять лимит', show_alert=True)
            return
        target_uid = parts[2]
        if target_uid not in users:
            await query.answer('Пользователь не найден', show_alert=True)
            return
        set_pending(uid, 'limited_admin_set_limit', {'target_uid': target_uid})
        await query.message.reply_text(
            f'Введи новый лимит для {target_uid} (целое число >= 0).\n'
            'Для отмены отправь: отмена'
        )
        return

    if action == 'rmask' and len(parts) >= 3:
        target_uid = parts[2]
        if target_uid not in users:
            await query.answer('Пользователь не найден', show_alert=True)
            return
        if not is_owner(uid):
            await query.answer('Только владелец может удалять лимитных пользователей', show_alert=True)
            return
        text = (
            f'Удалить лимитного пользователя {target_uid}?\n'
            'Это удалит только доступ в таблице ограничений, VPN-профили останутся.'
        )
        keyboard = [
            [InlineKeyboardButton('Да, удалить', callback_data=f'lu:rmdo:{target_uid}')],
            [InlineKeyboardButton('Отмена', callback_data=f'lu:open:{target_uid}')],
        ]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if action == 'rmdo' and len(parts) >= 3:
        if not is_owner(uid):
            await query.answer('Только владелец может удалять лимитных пользователей', show_alert=True)
            return
        target_uid = parts[2]
        if target_uid not in users:
            await query.answer('Пользователь уже удалён', show_alert=True)
            text, markup = _limited_users_overview_payload(users, page=0)
            if markup is None:
                await query.edit_message_text(text)
            else:
                await query.edit_message_text(text, reply_markup=markup)
            return
        users.pop(target_uid, None)
        save_users(users)
        text, markup = _limited_users_overview_payload(load_users(), page=0)
        if markup is None:
            await query.edit_message_text(text)
            return
        await query.edit_message_text(text, reply_markup=markup)
        return

    if action == 'sel' and len(parts) >= 4:
        target_uid = parts[2]
        mode = parts[3]
        rec = users.get(str(target_uid))
        if rec is None:
            await query.answer('Пользователь не найден', show_alert=True)
            return
        names, _ = _coerce_limited_user_rec(rec)
        _, settings = get_inbound()
        if not settings:
            await query.answer('Ошибка: inbound не найден', show_alert=True)
            return
        active_lookup = {x.get('email', '').lower() for x in settings.get('clients', []) if x.get('email')}
        active_names = [n for n in names if n.lower() in active_lookup]
        if not active_names:
            await query.answer('У пользователя нет активных профилей', show_alert=True)
            return
        title = 'Выбери профиль для переименования:' if mode == 'ren' else 'Выбери профиль для удаления:'
        keyboard = []
        for idx, name in enumerate(active_names):
            cb = f'lu:{mode}:{target_uid}:{idx}'
            keyboard.append([InlineKeyboardButton(name, callback_data=cb)])
        keyboard.append([InlineKeyboardButton('<< Назад', callback_data=f'lu:open:{target_uid}')])
        await query.edit_message_text(title, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if action in {'ren', 'del'} and len(parts) >= 4:
        target_uid, idx_raw = parts[2], parts[3]
        rec = users.get(str(target_uid))
        if rec is None:
            await query.answer('Пользователь не найден', show_alert=True)
            return
        if not idx_raw.isdigit():
            await query.answer('Некорректный профиль', show_alert=True)
            return
        names, _ = _coerce_limited_user_rec(rec)
        _, settings = get_inbound()
        if not settings:
            await query.answer('Ошибка: inbound не найден', show_alert=True)
            return
        active_lookup = {x.get('email', '').lower() for x in settings.get('clients', []) if x.get('email')}
        active_names = [n for n in names if n.lower() in active_lookup]
        idx = int(idx_raw)
        if idx < 0 or idx >= len(active_names):
            await query.answer('Профиль не найден', show_alert=True)
            return
        old_name = active_names[idx]
        if action == 'del':
            err = limited_user_delete_profile(int(target_uid), old_name)
            if err:
                await query.answer(err, show_alert=True)
                return
            text, markup = _limited_user_card_payload(target_uid, load_users(), is_owner(uid))
            await query.edit_message_text(text, reply_markup=markup)
            return
        set_pending(uid, 'limited_admin_rename_new', {'target_uid': target_uid, 'old_name': old_name})
        await query.message.reply_text(
            f'Профиль {old_name}\nВведи новое имя.\n'
            'Для отмены отправь: отмена'
        )
        return


async def cmd_myvpn(u: Update, c):
    uid = u.effective_user.id
    if not is_allowed(uid):
        return
    if is_admin(uid):
        await u.message.reply_text(
            'Для админов команда /myvpn отключена.\n'
            'Используй /add Имя или /link Имя.'
        )
        return
    if c.args:
        sub = (c.args[0] or '').lower()
        if sub == 'list':
            info, err = get_limited_user_active_profiles(uid)
            if err:
                await u.message.reply_text(err)
                return
            await u.message.reply_text(format_limited_profiles_text(info))
            return
        if sub == 'delete':
            if len(c.args) < 2:
                set_pending(uid, 'limited_delete_name')
                await u.message.reply_text(
                    'Введи имя своего профиля для удаления.\n'
                    'Для отмены отправь: отмена'
                )
                return
            name = validate_profile_name(' '.join(c.args[1:]).strip())
            if not name:
                await u.message.reply_text('Некорректное имя профиля.')
                return
            err = limited_user_delete_profile(uid, name)
            if err:
                await u.message.reply_text(err)
                return
            await u.message.reply_text(f'Твой профиль удалён: {name}')
            return
        if sub == 'rename':
            import shlex
            try:
                parts = shlex.split(' '.join(c.args[1:]))
            except Exception:
                parts = c.args[1:]
            if len(parts) < 2:
                if len(parts) == 1:
                    old_name = validate_profile_name(parts[0].strip())
                    if not old_name:
                        await u.message.reply_text('Некорректное старое имя профиля.')
                        return
                    set_pending(uid, 'limited_rename_new', {'old_name': old_name})
                    await u.message.reply_text(f'Ок. Теперь введи новое имя для профиля {old_name}.')
                    return
                set_pending(uid, 'limited_rename_old')
                await u.message.reply_text(
                    'Введи текущее имя своего профиля.\n'
                    'Для отмены отправь: отмена'
                )
                return
            old_name = validate_profile_name(parts[0].strip())
            new_name = validate_profile_name(' '.join(parts[1:]).strip())
            if not old_name or not new_name:
                await u.message.reply_text('Некорректные имена профилей.')
                return
            err = limited_user_rename_profile(uid, old_name, new_name)
            if err:
                await u.message.reply_text(err)
                return
            await u.message.reply_text(f'Твой профиль переименован: {old_name} → {new_name}')
            return
        await u.message.reply_text(
            'Неизвестная подкоманда.\n'
            'Доступно:\n'
            '/myvpn\n'
            '/myvpn list\n'
            '/myvpn rename СТАРОЕ НОВОЕ\n'
            '/myvpn delete ИМЯ'
        )
        return
    name, uuid, err = provision_limited_user_profile(u.effective_user)
    if err:
        await u.message.reply_text(err)
        return
    await send_user_links(u.message, c, name, uuid)


async def cmd_myprofiles(u: Update, c):
    uid = u.effective_user.id
    if not is_allowed(uid):
        return
    if is_admin(uid):
        await u.message.reply_text(
            'Для админов команда /myprofiles отключена.\n'
            'Используй /list для просмотра всех пользователей.'
        )
        return
    info, err = get_limited_user_active_profiles(uid)
    if err:
        await u.message.reply_text(err)
        return
    await u.message.reply_text(format_limited_profiles_text(info))


async def handle_unknown(u: Update, c):
    uid = u.effective_user.id
    msg = u.message
    if not msg or not msg.text:
        return
    text = msg.text.strip()
    if not is_allowed(uid):
        await notify_unauthorized_user(c.bot, u.effective_user)
        return

    pending = get_pending(uid)
    if not pending:
        return
    if text.lower() in {'отмена', 'cancel'}:
        clear_pending(uid)
        await msg.reply_text('Операция отменена.')
        return

    action = pending.get('action')
    data = pending.get('data', {})

    if action == 'admin_add_name':
        name = validate_profile_name(text)
        if not name:
            await msg.reply_text('Некорректное имя. Попробуй ещё раз или отправь "отмена".')
            return
        uuid, err = admin_add_profile(name)
        if err:
            clear_pending(uid)
            await msg.reply_text(err)
            return
        clear_pending(uid)
        await send_user_links(msg, c, name, uuid)
        return

    if action == 'admin_delete_name':
        name = validate_profile_name(text)
        if not name:
            await msg.reply_text('Некорректное имя. Попробуй ещё раз или отправь "отмена".')
            return
        err = admin_delete_profile(name)
        clear_pending(uid)
        if err:
            await msg.reply_text(err)
            return
        await msg.reply_text(f'Пользователь {name} удалён')
        return

    if action == 'admin_rename_old':
        old_name = validate_profile_name(text)
        if not old_name:
            await msg.reply_text('Некорректное старое имя. Попробуй ещё раз или отправь "отмена".')
            return
        set_pending(uid, 'admin_rename_new', {'old_name': old_name})
        await msg.reply_text(f'Теперь введи новое имя для {old_name}.')
        return

    if action == 'admin_rename_new':
        old_name = data.get('old_name', '')
        new_name = validate_profile_name(text)
        if not new_name:
            await msg.reply_text('Некорректное новое имя. Попробуй ещё раз или отправь "отмена".')
            return
        err = admin_rename_profile(old_name, new_name)
        clear_pending(uid)
        if err:
            await msg.reply_text(err)
            return
        await msg.reply_text(f'Переименовано: {old_name} → {new_name}')
        return

    if action == 'limited_delete_name':
        name = validate_profile_name(text)
        if not name:
            await msg.reply_text('Некорректное имя профиля. Попробуй ещё раз или отправь "отмена".')
            return
        err = limited_user_delete_profile(uid, name)
        clear_pending(uid)
        if err:
            await msg.reply_text(err)
            return
        await msg.reply_text(f'Твой профиль удалён: {name}')
        return

    if action == 'limited_rename_old':
        old_name = validate_profile_name(text)
        if not old_name:
            await msg.reply_text('Некорректное старое имя профиля. Попробуй ещё раз или отправь "отмена".')
            return
        set_pending(uid, 'limited_rename_new', {'old_name': old_name})
        await msg.reply_text(f'Теперь введи новое имя для профиля {old_name}.')
        return

    if action == 'limited_rename_new':
        old_name = data.get('old_name', '')
        new_name = validate_profile_name(text)
        if not new_name:
            await msg.reply_text('Некорректное новое имя профиля. Попробуй ещё раз или отправь "отмена".')
            return
        err = limited_user_rename_profile(uid, old_name, new_name)
        clear_pending(uid)
        if err:
            await msg.reply_text(err)
            return
        await msg.reply_text(f'Твой профиль переименован: {old_name} → {new_name}')
        return

    if action == 'limited_admin_set_limit':
        target_uid = str(data.get('target_uid', '')).strip()
        if not is_owner(uid):
            clear_pending(uid)
            await msg.reply_text('Только владелец может менять лимиты.')
            return
        if not target_uid:
            clear_pending(uid)
            await msg.reply_text('Ошибка состояния. Повтори через /users.')
            return
        if not text.isdigit():
            await msg.reply_text('Нужен целый лимит >= 0. Попробуй ещё раз или отправь "отмена".')
            return
        new_limit = max(0, int(text))
        users = load_users()
        rec = users.get(target_uid)
        if rec is None:
            clear_pending(uid)
            await msg.reply_text(f'{target_uid} не найден')
            return
        names, _ = _coerce_limited_user_rec(rec)
        users[target_uid] = {
            'profile_names': names,
            'full_name': str(rec.get('full_name', '') or '').strip() if isinstance(rec, dict) else '',
            'username': str(rec.get('username', '') or '').strip().lstrip('@') if isinstance(rec, dict) else '',
            'max_active': new_limit
        }
        save_users(users)
        clear_pending(uid)
        await msg.reply_text(f'Лимит обновлён для {target_uid}: {len(names)}/{new_limit}')
        return

    if action == 'limited_admin_rename_new':
        if not is_admin(uid):
            clear_pending(uid)
            return
        target_uid = str(data.get('target_uid', '')).strip()
        old_name = str(data.get('old_name', '')).strip()
        new_name = validate_profile_name(text)
        if not new_name:
            await msg.reply_text('Некорректное новое имя. Попробуй ещё раз или отправь "отмена".')
            return
        if not target_uid.isdigit():
            clear_pending(uid)
            await msg.reply_text('Ошибка состояния. Повтори через /users.')
            return
        err = limited_user_rename_profile(int(target_uid), old_name, new_name)
        clear_pending(uid)
        if err:
            await msg.reply_text(err)
            return
        await msg.reply_text(f'Переименовано для {target_uid}: {old_name} → {new_name}')
        return


def main():
    required_env = {
        'VPN_BOT_TOKEN': BOT_TOKEN,
        'VPN_SERVER_IP': SERVER_IP,
        'VPN_PUBLIC_KEY': PUBLIC_KEY,
        'VPN_SHORT_ID': SHORT_ID,
        'VPN_SNI': SNI,
    }
    missing = [k for k, v in required_env.items() if not v]
    if missing:
        raise RuntimeError(f'Не заданы обязательные переменные окружения: {", ".join(missing)}')
    init_access_db()
    migrate_access_from_json()
    seed_access_defaults()

    app = Application.builder().token(BOT_TOKEN).build()
    for cmd, fn in [
        ('start', cmd_start),
        ('help', cmd_start),
        ('add', cmd_add),
        ('list', cmd_list),
        ('link', cmd_link),
        ('delete', cmd_delete),
        ('rename', cmd_rename),
        ('admins', cmd_admins),
        ('addadmin', cmd_addadmin),
        ('removeadmin', cmd_removeadmin),
        ('adduser', cmd_adduser),
        ('setuserlimit', cmd_setuserlimit),
        ('removeuser', cmd_removeuser),
        ('users', cmd_users),
        ('myvpn', cmd_myvpn),
        ('myprofiles', cmd_myprofiles),
    ]:
        app.add_handler(CommandHandler(cmd, fn))
    app.add_handler(CallbackQueryHandler(cb_link, pattern='^link:'))
    app.add_handler(CallbackQueryHandler(cb_list_page, pattern='^listpg:'))
    app.add_handler(CallbackQueryHandler(cb_limited_users, pattern='^lu:'))
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_unknown))
    log.info('VPN Bot started')
    app.run_polling(allowed_updates=Update.ALL_TYPES)
