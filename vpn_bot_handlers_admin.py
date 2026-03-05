from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update

from vpn_bot_access import (
    delete_limited_user,
    get_access_attempt_identity,
    get_limited_user,
    is_admin,
    is_owner,
    load_admins,
    load_users,
    save_admins,
    set_pending,
    upsert_limited_user,
)
from vpn_bot_config import DEFAULT_LIMITED_MAX_ACTIVE, OWNER_ID, PAGE_SIZE
from vpn_bot_ui import _limited_user_card_payload, _limited_users_overview_payload
from vpn_bot_xray import (
    _coerce_limited_user_rec,
    admin_add_profile,
    admin_delete_profile,
    admin_rename_profile,
    get_inbound,
    limited_user_delete_profile,
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
        name = str(client.get("email", "—"))
        client_id = str(client.get("id", "") or "").strip()
        if not client_id:
            continue
        keyboard.append(
            [InlineKeyboardButton(f"{i}. {name}", callback_data=f"linku:{client_id}")]
        )

    if total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append(
                InlineKeyboardButton("<<", callback_data=f"listpg:{page - 1}")
            )
        nav_row.append(
            InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="listpg:0")
        )
        if page < total_pages - 1:
            nav_row.append(
                InlineKeyboardButton(">>", callback_data=f"listpg:{page + 1}")
            )
        keyboard.append(nav_row)

    text = (
        f"Пользователи VPN ({total} шт), страница {page + 1}/{total_pages}.\n"
        "Нажми на имя чтобы получить ссылки:"
    )
    return text, InlineKeyboardMarkup(keyboard)


def _record_with_updates(
    rec,
    *,
    profile_names=None,
    max_active=None,
    key_port=None,
    full_name=None,
    username=None,
):
    base = rec if isinstance(rec, dict) else {}
    return {
        "profile_names": profile_names
        if profile_names is not None
        else list(base.get("profile_names", []) or []),
        "full_name": str(
            full_name if full_name is not None else base.get("full_name", "") or ""
        ).strip(),
        "username": str(
            username if username is not None else base.get("username", "") or ""
        )
        .strip()
        .lstrip("@"),
        "key_port": int(
            key_port if key_port is not None else base.get("key_port", 443) or 443
        ),
        "max_active": int(
            max_active
            if max_active is not None
            else base.get("max_active", DEFAULT_LIMITED_MAX_ACTIVE)
            or DEFAULT_LIMITED_MAX_ACTIVE
        ),
    }


async def cmd_add(u: Update, c):
    if not is_admin(u.effective_user.id):
        return
    if not c.args:
        set_pending(u.effective_user.id, "admin_add_name")
        await u.message.reply_text(
            "Введи имя нового пользователя одним сообщением.\nДля отмены отправь: отмена"
        )
        return
    name = validate_profile_name(" ".join(c.args).strip())
    if not name:
        await u.message.reply_text(
            "Некорректное имя. Разрешены буквы/цифры/пробел/._-@, до 64 символов."
        )
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
        await u.message.reply_text("Ошибка: inbound не найден")
        return
    cl = settings["clients"]
    if not cl:
        await u.message.reply_text("Список пуст")
        return
    text, markup = _vpn_list_payload(cl, page=0)
    await u.message.reply_text(text, reply_markup=markup)


async def cmd_link(u: Update, c):
    if not is_admin(u.effective_user.id):
        return
    if not c.args:
        await u.message.reply_text("Укажи имя: /link Антон")
        return
    name = " ".join(c.args).strip()
    _, settings = get_inbound()
    if not settings:
        await u.message.reply_text("Ошибка: inbound не найден")
        return
    cl = next(
        (x for x in settings["clients"] if x.get("email", "").lower() == name.lower()),
        None,
    )
    if not cl:
        await u.message.reply_text(f"Пользователь {name} не найден")
        return
    await send_user_links(u.message, c, cl["email"], cl["id"])


async def cb_link(u: Update, c):
    query = u.callback_query
    await query.answer()
    if not is_admin(u.effective_user.id):
        return
    data = query.data or ""
    if not data.startswith("linku:"):
        return
    client_id = data[6:]
    _, settings = get_inbound()
    if not settings:
        return
    cl = next(
        (x for x in settings["clients"] if str(x.get("id", "")) == client_id), None
    )
    if not cl:
        await query.message.reply_text("Пользователь не найден")
        return
    await send_user_links(query.message, c, cl.get("email", "—"), cl["id"])


async def cb_list_page(u: Update, c):
    query = u.callback_query
    await query.answer()
    if not is_admin(u.effective_user.id):
        return
    data = query.data or ""
    if not data.startswith("listpg:"):
        return
    page_raw = data.split(":", 1)[1] if ":" in data else "0"
    page = int(page_raw) if page_raw.isdigit() else 0
    _, settings = get_inbound()
    if not settings:
        await query.edit_message_text("Ошибка: inbound не найден")
        return
    cl = settings["clients"]
    if not cl:
        await query.edit_message_text("Список пуст")
        return
    text, markup = _vpn_list_payload(cl, page=page)
    await query.edit_message_text(text, reply_markup=markup)


async def cmd_delete(u: Update, c):
    if not is_admin(u.effective_user.id):
        return
    if not c.args:
        set_pending(u.effective_user.id, "admin_delete_name")
        await u.message.reply_text(
            "Введи имя пользователя для удаления.\nДля отмены отправь: отмена"
        )
        return
    name = validate_profile_name(" ".join(c.args).strip())
    if not name:
        await u.message.reply_text("Некорректное имя для удаления.")
        return
    err = admin_delete_profile(name)
    if err:
        await u.message.reply_text(err)
        return
    await u.message.reply_text(f"Пользователь {name} удалён")


async def cmd_rename(u: Update, c):
    if not is_admin(u.effective_user.id):
        return
    if not c.args:
        set_pending(u.effective_user.id, "admin_rename_old")
        await u.message.reply_text(
            "Введи текущее имя пользователя для переименования.\nДля отмены отправь: отмена"
        )
        return
    if len(c.args) == 1:
        old_name = validate_profile_name(c.args[0].strip())
        if not old_name:
            await u.message.reply_text("Некорректное старое имя.")
            return
        set_pending(u.effective_user.id, "admin_rename_new", {"old_name": old_name})
        await u.message.reply_text(f"Ок. Теперь введи новое имя для {old_name}.")
        return
    import shlex

    try:
        parts = shlex.split(" ".join(c.args))
    except Exception:
        parts = c.args
    if len(parts) < 2:
        await u.message.reply_text("Нужно два имени: /rename СтароеИмя НовоеИмя")
        return
    old_name = validate_profile_name(parts[0].strip())
    new_name = validate_profile_name(" ".join(parts[1:]).strip())
    if not old_name or not new_name:
        await u.message.reply_text(
            "Некорректные имена. Разрешены буквы/цифры/пробел/._-@, до 64 символов."
        )
        return
    err = admin_rename_profile(old_name, new_name)
    if err:
        await u.message.reply_text(err)
        return
    await u.message.reply_text(f"Переименовано: {old_name} → {new_name}")


async def cmd_admins(u: Update, c):
    if not is_admin(u.effective_user.id):
        return
    admins = load_admins()
    lines = ["Администраторы бота:\n"]
    for aid in sorted(admins):
        lines.append(f"{aid}" + (" (владелец)" if aid == OWNER_ID else ""))
    await u.message.reply_text("\n".join(lines))


async def cmd_addadmin(u: Update, c):
    if not is_owner(u.effective_user.id):
        await u.message.reply_text("Только владелец может добавлять администраторов.")
        return
    if not c.args or not c.args[0].isdigit():
        await u.message.reply_text("Укажи Telegram ID: /addadmin 123456789")
        return
    new_id = int(c.args[0])
    admins = load_admins()
    if new_id in admins:
        await u.message.reply_text(f"{new_id} уже в списке")
        return
    admins.add(new_id)
    save_admins(admins)
    await u.message.reply_text(
        f"Добавлен: {new_id}\nСписок: {', '.join(str(i) for i in sorted(admins))}"
    )


async def cmd_removeadmin(u: Update, c):
    if not is_owner(u.effective_user.id):
        await u.message.reply_text("Только владелец может удалять администраторов.")
        return
    if not c.args or not c.args[0].isdigit():
        await u.message.reply_text("Укажи Telegram ID: /removeadmin 123456789")
        return
    rm_id = int(c.args[0])
    if rm_id == OWNER_ID:
        await u.message.reply_text("Нельзя удалить владельца.")
        return
    admins = load_admins()
    if rm_id not in admins:
        await u.message.reply_text(f"{rm_id} не найден")
        return
    admins.discard(rm_id)
    save_admins(admins)
    await u.message.reply_text(
        f"Удалён: {rm_id}\nСписок: {', '.join(str(i) for i in sorted(admins))}"
    )


async def cmd_adduser(u: Update, c):
    if not is_owner(u.effective_user.id):
        await u.message.reply_text(
            "Только владелец может добавлять лимитных пользователей."
        )
        return
    if not c.args or not c.args[0].isdigit():
        await u.message.reply_text("Укажи Telegram ID: /adduser 123456789 [лимит]")
        return
    new_id = c.args[0]
    limit = DEFAULT_LIMITED_MAX_ACTIVE
    if len(c.args) > 1:
        if not c.args[1].isdigit():
            await u.message.reply_text("Лимит должен быть числом: /adduser 123456789 2")
            return
        limit = max(0, int(c.args[1]))
    if get_limited_user(new_id) is not None:
        await u.message.reply_text(f"{new_id} уже в списке лимитных пользователей")
        return
    full_name, username = get_access_attempt_identity(new_id)
    upsert_limited_user(
        new_id,
        {
            "profile_names": [],
            "full_name": full_name,
            "username": username,
            "key_port": 443,
            "max_active": limit,
        },
    )
    identity = (
        f"\nИмя: {full_name or '—'}\nUsername: @{username}"
        if username
        else f"\nИмя: {full_name or '—'}"
    )
    await u.message.reply_text(
        f"Добавлен лимитный пользователь: {new_id}\n"
        f"Лимит активных профилей: {limit}\n"
        f"Порт выдачи ключей: 443{identity}"
    )
    try:
        await c.bot.send_message(
            chat_id=int(new_id),
            text=(
                "Тебе выдан доступ к VPN-боту.\n\n"
                f"Лимит активных профилей: {limit}\n"
                "Порт выдачи ключей: 443\n"
                "Отправь /start, чтобы увидеть доступные команды."
            ),
        )
    except Exception:
        pass


async def cmd_setuserlimit(u: Update, c):
    if not is_owner(u.effective_user.id):
        await u.message.reply_text(
            "Только владелец может менять лимит лимитного пользователя."
        )
        return
    if len(c.args) < 2 or not c.args[0].isdigit() or not c.args[1].isdigit():
        await u.message.reply_text("Использование: /setuserlimit 123456789 3")
        return
    uid = c.args[0]
    new_limit = max(0, int(c.args[1]))
    rec = get_limited_user(uid)
    if rec is None:
        await u.message.reply_text(f"{uid} не найден")
        return
    rec = _record_with_updates(rec, max_active=new_limit)
    upsert_limited_user(uid, rec)
    active_now = len(rec.get("profile_names", []))
    await u.message.reply_text(
        f"Обновлён лимит для {uid}: активных сейчас {active_now}/{new_limit}"
    )


async def cmd_removeuser(u: Update, c):
    if not is_owner(u.effective_user.id):
        await u.message.reply_text(
            "Только владелец может удалять лимитных пользователей."
        )
        return
    if not c.args or not c.args[0].isdigit():
        await u.message.reply_text("Укажи Telegram ID: /removeuser 123456789")
        return
    rm_id = c.args[0]
    if not delete_limited_user(rm_id):
        await u.message.reply_text(f"{rm_id} не найден")
        return
    await u.message.reply_text(f"Лимитный пользователь удалён: {rm_id}")


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
    data = query.data or ""
    if not data.startswith("lu:"):
        return
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    users = load_users()

    if action == "list":
        page = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else 0
        text, markup = _limited_users_overview_payload(users, page=page)
        if markup is None:
            await query.edit_message_text(text)
            return
        await query.edit_message_text(text, reply_markup=markup)
        return

    if action == "open" and len(parts) >= 3:
        target_uid = parts[2]
        text, markup = _limited_user_card_payload(target_uid, users, is_owner(uid))
        await query.edit_message_text(text, reply_markup=markup)
        return

    if action == "limit" and len(parts) >= 3:
        target_uid = parts[2]
        rec = users.get(str(target_uid))
        if rec is None:
            await query.answer("Пользователь не найден", show_alert=True)
            return
        _, current_limit = _coerce_limited_user_rec(rec)
        keyboard = [
            [
                InlineKeyboardButton("1", callback_data=f"lu:limset:{target_uid}:1"),
                InlineKeyboardButton("2", callback_data=f"lu:limset:{target_uid}:2"),
                InlineKeyboardButton("3", callback_data=f"lu:limset:{target_uid}:3"),
            ],
            [
                InlineKeyboardButton("4", callback_data=f"lu:limset:{target_uid}:4"),
                InlineKeyboardButton("5", callback_data=f"lu:limset:{target_uid}:5"),
                InlineKeyboardButton("10", callback_data=f"lu:limset:{target_uid}:10"),
            ],
            [
                InlineKeyboardButton(
                    "Ввести вручную", callback_data=f"lu:limman:{target_uid}"
                )
            ],
            [InlineKeyboardButton("<< Назад", callback_data=f"lu:open:{target_uid}")],
        ]
        await query.edit_message_text(
            f"Изменение лимита для {target_uid}\nТекущий лимит: {current_limit}\nВыбери значение или введи вручную.",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    if action == "port" and len(parts) >= 3:
        target_uid = parts[2]
        rec = users.get(str(target_uid))
        if rec is None:
            await query.answer("Пользователь не найден", show_alert=True)
            return
        current_port = (
            int(rec.get("key_port", 443) or 443) if isinstance(rec, dict) else 443
        )
        if current_port not in {443, 2087}:
            current_port = 443
        keyboard = [
            [
                InlineKeyboardButton(
                    "443" + (" ✓" if current_port == 443 else ""),
                    callback_data=f"lu:portset:{target_uid}:443",
                ),
                InlineKeyboardButton(
                    "2087" + (" ✓" if current_port == 2087 else ""),
                    callback_data=f"lu:portset:{target_uid}:2087",
                ),
            ],
            [InlineKeyboardButton("<< Назад", callback_data=f"lu:open:{target_uid}")],
        ]
        await query.edit_message_text(
            f"Порт выдачи ключей для {target_uid}\nТекущий порт: {current_port}\nВыбери порт.",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    if action == "portset" and len(parts) >= 4:
        target_uid, raw_port = parts[2], parts[3]
        if raw_port not in {"443", "2087"}:
            await query.answer("Некорректный порт", show_alert=True)
            return
        rec = get_limited_user(target_uid)
        if rec is None:
            await query.answer("Пользователь не найден", show_alert=True)
            return
        names, _ = _coerce_limited_user_rec(rec)
        upsert_limited_user(
            target_uid,
            _record_with_updates(rec, profile_names=names, key_port=int(raw_port)),
        )
        text, markup = _limited_user_card_payload(
            target_uid, load_users(), is_owner(uid)
        )
        await query.edit_message_text(text, reply_markup=markup)
        return

    if action == "limset" and len(parts) >= 4:
        if not is_owner(uid):
            await query.answer("Только владелец может менять лимит", show_alert=True)
            return
        target_uid, raw_limit = parts[2], parts[3]
        if not raw_limit.isdigit():
            await query.answer("Некорректный лимит", show_alert=True)
            return
        rec = get_limited_user(target_uid)
        if rec is None:
            await query.answer("Пользователь не найден", show_alert=True)
            return
        names, _ = _coerce_limited_user_rec(rec)
        upsert_limited_user(
            target_uid,
            _record_with_updates(
                rec, profile_names=names, max_active=max(0, int(raw_limit))
            ),
        )
        text, markup = _limited_user_card_payload(
            target_uid, load_users(), is_owner(uid)
        )
        await query.edit_message_text(text, reply_markup=markup)
        return

    if action == "limman" and len(parts) >= 3:
        if not is_owner(uid):
            await query.answer("Только владелец может менять лимит", show_alert=True)
            return
        target_uid = parts[2]
        if target_uid not in users:
            await query.answer("Пользователь не найден", show_alert=True)
            return
        set_pending(uid, "limited_admin_set_limit", {"target_uid": target_uid})
        await query.message.reply_text(
            f"Введи новый лимит для {target_uid} (целое число >= 0).\nДля отмены отправь: отмена"
        )
        return

    if action == "rmask" and len(parts) >= 3:
        target_uid = parts[2]
        if target_uid not in users:
            await query.answer("Пользователь не найден", show_alert=True)
            return
        if not is_owner(uid):
            await query.answer(
                "Только владелец может удалять лимитных пользователей", show_alert=True
            )
            return
        keyboard = [
            [
                InlineKeyboardButton(
                    "Да, удалить", callback_data=f"lu:rmdo:{target_uid}"
                )
            ],
            [InlineKeyboardButton("Отмена", callback_data=f"lu:open:{target_uid}")],
        ]
        await query.edit_message_text(
            f"Удалить лимитного пользователя {target_uid}?\nЭто удалит только доступ в таблице ограничений, VPN-профили останутся.",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    if action == "rmdo" and len(parts) >= 3:
        if not is_owner(uid):
            await query.answer(
                "Только владелец может удалять лимитных пользователей", show_alert=True
            )
            return
        target_uid = parts[2]
        if not delete_limited_user(target_uid):
            await query.answer("Пользователь уже удалён", show_alert=True)
            text, markup = _limited_users_overview_payload(users, page=0)
            if markup is None:
                await query.edit_message_text(text)
            else:
                await query.edit_message_text(text, reply_markup=markup)
            return
        text, markup = _limited_users_overview_payload(load_users(), page=0)
        if markup is None:
            await query.edit_message_text(text)
            return
        await query.edit_message_text(text, reply_markup=markup)
        return

    if action == "sel" and len(parts) >= 4:
        target_uid = parts[2]
        mode = parts[3]
        rec = users.get(str(target_uid))
        if rec is None:
            await query.answer("Пользователь не найден", show_alert=True)
            return
        names, _ = _coerce_limited_user_rec(rec)
        _, settings = get_inbound()
        if not settings:
            await query.answer("Ошибка: inbound не найден", show_alert=True)
            return
        active_lookup = {
            x.get("email", "").lower()
            for x in settings.get("clients", [])
            if x.get("email")
        }
        active_names = [n for n in names if n.lower() in active_lookup]
        if not active_names:
            await query.answer("У пользователя нет активных профилей", show_alert=True)
            return
        title = (
            "Выбери профиль для переименования:"
            if mode == "ren"
            else "Выбери профиль для удаления:"
        )
        keyboard = [
            [InlineKeyboardButton(name, callback_data=f"lu:{mode}:{target_uid}:{idx}")]
            for idx, name in enumerate(active_names)
        ]
        keyboard.append(
            [InlineKeyboardButton("<< Назад", callback_data=f"lu:open:{target_uid}")]
        )
        await query.edit_message_text(
            title, reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    if action in {"ren", "del"} and len(parts) >= 4:
        target_uid, idx_raw = parts[2], parts[3]
        rec = users.get(str(target_uid))
        if rec is None:
            await query.answer("Пользователь не найден", show_alert=True)
            return
        if not idx_raw.isdigit():
            await query.answer("Некорректный профиль", show_alert=True)
            return
        names, _ = _coerce_limited_user_rec(rec)
        _, settings = get_inbound()
        if not settings:
            await query.answer("Ошибка: inbound не найден", show_alert=True)
            return
        active_lookup = {
            x.get("email", "").lower()
            for x in settings.get("clients", [])
            if x.get("email")
        }
        active_names = [n for n in names if n.lower() in active_lookup]
        idx = int(idx_raw)
        if idx < 0 or idx >= len(active_names):
            await query.answer("Профиль не найден", show_alert=True)
            return
        old_name = active_names[idx]
        if action == "del":
            err = limited_user_delete_profile(int(target_uid), old_name)
            if err:
                await query.answer(err, show_alert=True)
                return
            text, markup = _limited_user_card_payload(
                target_uid, load_users(), is_owner(uid)
            )
            await query.edit_message_text(text, reply_markup=markup)
            return
        set_pending(
            uid,
            "limited_admin_rename_new",
            {"target_uid": target_uid, "old_name": old_name},
        )
        await query.message.reply_text(
            f"Профиль {old_name}\nВведи новое имя.\nДля отмены отправь: отмена"
        )
        return


async def cb_owner_approve(u: Update, c):
    query = u.callback_query
    await query.answer()
    if not is_owner(u.effective_user.id):
        return
    parts = (query.data or "").split(":")
    if len(parts) < 3 or parts[0] != "apv":
        return
    action, target_uid = parts[1], parts[2]
    if not target_uid.isdigit():
        await query.answer("Некорректный ID", show_alert=True)
        return
    if action == "addadmin":
        new_id = int(target_uid)
        admins = load_admins()
        if new_id in admins:
            await query.answer("Уже админ", show_alert=True)
            return
        admins.add(new_id)
        save_admins(admins)
        await query.message.reply_text(f"Аппрув выполнен: {new_id} добавлен в админы.")
        try:
            await c.bot.send_message(
                chat_id=new_id,
                text="Тебе выдан доступ администратора к VPN-боту.\nОтправь /start.",
            )
        except Exception:
            pass
        return
    if action == "adduser":
        if len(parts) < 4 or not parts[3].isdigit():
            await query.answer("Некорректный лимит", show_alert=True)
            return
        limit = max(0, int(parts[3]))
        full_name, username = get_access_attempt_identity(target_uid)
        rec = get_limited_user(target_uid) or {}
        rec = _record_with_updates(
            rec,
            max_active=limit,
            full_name=full_name or rec.get("full_name", ""),
            username=username or rec.get("username", ""),
            key_port=rec.get("key_port", 443),
        )
        upsert_limited_user(target_uid, rec)
        await query.message.reply_text(
            f"Аппрув выполнен: {target_uid} добавлен как лимитный пользователь.\nЛимит: {limit}, порт ключей: {rec['key_port']}"
        )
        try:
            await c.bot.send_message(
                chat_id=int(target_uid),
                text=(
                    "Тебе выдан доступ к VPN-боту.\n\n"
                    f"Лимит активных профилей: {limit}\n"
                    f"Порт выдачи ключей: {rec['key_port']}\n"
                    "Отправь /start, чтобы увидеть команды."
                ),
            )
        except Exception:
            pass
