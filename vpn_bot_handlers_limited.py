from telegram import Update

from vpn_bot_access import (
    clear_pending,
    get_pending,
    get_limited_user,
    is_admin,
    is_allowed,
    is_owner,
    notify_unauthorized_user,
    set_pending,
    upsert_limited_user,
)
from vpn_bot_config import DEFAULT_LIMITED_MAX_ACTIVE
from vpn_bot_xray import (
    _coerce_limited_user_rec,
    admin_add_profile,
    admin_delete_profile,
    admin_rename_profile,
    format_limited_profiles_text,
    get_limited_user_active_profiles,
    limited_user_delete_profile,
    limited_user_rename_profile,
    provision_limited_user_profile,
    send_user_links,
    validate_profile_name,
)


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


async def cmd_myvpn(u: Update, c):
    uid = u.effective_user.id
    if not is_allowed(uid):
        return
    if is_admin(uid):
        await u.message.reply_text(
            "Для админов команда /myvpn отключена.\nИспользуй /add Имя или /link Имя."
        )
        return
    if c.args:
        sub = (c.args[0] or "").lower()
        if sub == "list":
            info, err = get_limited_user_active_profiles(uid)
            if err:
                await u.message.reply_text(err)
                return
            await u.message.reply_text(format_limited_profiles_text(info))
            return
        if sub == "delete":
            if len(c.args) < 2:
                set_pending(uid, "limited_delete_name")
                await u.message.reply_text(
                    "Введи имя своего профиля для удаления.\nДля отмены отправь: отмена"
                )
                return
            name = validate_profile_name(" ".join(c.args[1:]).strip())
            if not name:
                await u.message.reply_text("Некорректное имя профиля.")
                return
            err = limited_user_delete_profile(uid, name)
            if err:
                await u.message.reply_text(err)
                return
            await u.message.reply_text(f"Твой профиль удалён: {name}")
            return
        if sub == "rename":
            import shlex

            try:
                parts = shlex.split(" ".join(c.args[1:]))
            except Exception:
                parts = c.args[1:]
            if len(parts) < 2:
                if len(parts) == 1:
                    old_name = validate_profile_name(parts[0].strip())
                    if not old_name:
                        await u.message.reply_text("Некорректное старое имя профиля.")
                        return
                    set_pending(uid, "limited_rename_new", {"old_name": old_name})
                    await u.message.reply_text(
                        f"Ок. Теперь введи новое имя для профиля {old_name}."
                    )
                    return
                set_pending(uid, "limited_rename_old")
                await u.message.reply_text(
                    "Введи текущее имя своего профиля.\nДля отмены отправь: отмена"
                )
                return
            old_name = validate_profile_name(parts[0].strip())
            new_name = validate_profile_name(" ".join(parts[1:]).strip())
            if not old_name or not new_name:
                await u.message.reply_text("Некорректные имена профилей.")
                return
            err = limited_user_rename_profile(uid, old_name, new_name)
            if err:
                await u.message.reply_text(err)
                return
            await u.message.reply_text(
                f"Твой профиль переименован: {old_name} → {new_name}"
            )
            return
        await u.message.reply_text(
            "Неизвестная подкоманда.\n"
            "Доступно:\n"
            "/myvpn\n"
            "/myvpn list\n"
            "/myvpn rename СТАРОЕ НОВОЕ\n"
            "/myvpn delete ИМЯ"
        )
        return

    name, uuid, err = provision_limited_user_profile(u.effective_user)
    if err:
        await u.message.reply_text(err)
        return
    rec = get_limited_user(uid)
    key_port = 443
    if isinstance(rec, dict):
        key_port = int(rec.get("key_port", 443) or 443)
    if key_port not in {443, 2087}:
        key_port = 443
    await send_user_links(u.message, c, name, uuid, ports=[key_port])


async def cmd_myprofiles(u: Update, c):
    uid = u.effective_user.id
    if not is_allowed(uid):
        return
    if is_admin(uid):
        await u.message.reply_text(
            "Для админов команда /myprofiles отключена.\nИспользуй /list для просмотра всех пользователей."
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
    if text.lower() in {"отмена", "cancel"}:
        clear_pending(uid)
        await msg.reply_text("Операция отменена.")
        return

    action = pending.get("action")
    data = pending.get("data", {})

    if action == "admin_add_name":
        name = validate_profile_name(text)
        if not name:
            await msg.reply_text(
                'Некорректное имя. Попробуй ещё раз или отправь "отмена".'
            )
            return
        uuid, err = admin_add_profile(name)
        if err:
            clear_pending(uid)
            await msg.reply_text(err)
            return
        clear_pending(uid)
        await send_user_links(msg, c, name, uuid)
        return

    if action == "admin_delete_name":
        name = validate_profile_name(text)
        if not name:
            await msg.reply_text(
                'Некорректное имя. Попробуй ещё раз или отправь "отмена".'
            )
            return
        err = admin_delete_profile(name)
        clear_pending(uid)
        if err:
            await msg.reply_text(err)
            return
        await msg.reply_text(f"Пользователь {name} удалён")
        return

    if action == "admin_rename_old":
        old_name = validate_profile_name(text)
        if not old_name:
            await msg.reply_text(
                'Некорректное старое имя. Попробуй ещё раз или отправь "отмена".'
            )
            return
        set_pending(uid, "admin_rename_new", {"old_name": old_name})
        await msg.reply_text(f"Теперь введи новое имя для {old_name}.")
        return

    if action == "admin_rename_new":
        old_name = data.get("old_name", "")
        new_name = validate_profile_name(text)
        if not new_name:
            await msg.reply_text(
                'Некорректное новое имя. Попробуй ещё раз или отправь "отмена".'
            )
            return
        err = admin_rename_profile(old_name, new_name)
        clear_pending(uid)
        if err:
            await msg.reply_text(err)
            return
        await msg.reply_text(f"Переименовано: {old_name} → {new_name}")
        return

    if action == "limited_delete_name":
        name = validate_profile_name(text)
        if not name:
            await msg.reply_text(
                'Некорректное имя профиля. Попробуй ещё раз или отправь "отмена".'
            )
            return
        err = limited_user_delete_profile(uid, name)
        clear_pending(uid)
        if err:
            await msg.reply_text(err)
            return
        await msg.reply_text(f"Твой профиль удалён: {name}")
        return

    if action == "limited_rename_old":
        old_name = validate_profile_name(text)
        if not old_name:
            await msg.reply_text(
                'Некорректное старое имя профиля. Попробуй ещё раз или отправь "отмена".'
            )
            return
        set_pending(uid, "limited_rename_new", {"old_name": old_name})
        await msg.reply_text(f"Теперь введи новое имя для профиля {old_name}.")
        return

    if action == "limited_rename_new":
        old_name = data.get("old_name", "")
        new_name = validate_profile_name(text)
        if not new_name:
            await msg.reply_text(
                'Некорректное новое имя профиля. Попробуй ещё раз или отправь "отмена".'
            )
            return
        err = limited_user_rename_profile(uid, old_name, new_name)
        clear_pending(uid)
        if err:
            await msg.reply_text(err)
            return
        await msg.reply_text(f"Твой профиль переименован: {old_name} → {new_name}")
        return

    if action == "limited_admin_set_limit":
        target_uid = str(data.get("target_uid", "")).strip()
        if not is_owner(uid):
            clear_pending(uid)
            await msg.reply_text("Только владелец может менять лимиты.")
            return
        if not target_uid:
            clear_pending(uid)
            await msg.reply_text("Ошибка состояния. Повтори через /users.")
            return
        if not text.isdigit():
            await msg.reply_text(
                'Нужен целый лимит >= 0. Попробуй ещё раз или отправь "отмена".'
            )
            return
        new_limit = max(0, int(text))
        rec = get_limited_user(target_uid)
        if rec is None:
            clear_pending(uid)
            await msg.reply_text(f"{target_uid} не найден")
            return
        names, _ = _coerce_limited_user_rec(rec)
        upsert_limited_user(
            target_uid,
            _record_with_updates(rec, profile_names=names, max_active=new_limit),
        )
        clear_pending(uid)
        await msg.reply_text(
            f"Лимит обновлён для {target_uid}: {len(names)}/{new_limit}"
        )
        return

    if action == "limited_admin_rename_new":
        if not is_admin(uid):
            clear_pending(uid)
            return
        target_uid = str(data.get("target_uid", "")).strip()
        old_name = str(data.get("old_name", "")).strip()
        new_name = validate_profile_name(text)
        if not new_name:
            await msg.reply_text(
                'Некорректное новое имя. Попробуй ещё раз или отправь "отмена".'
            )
            return
        if not target_uid.isdigit():
            clear_pending(uid)
            await msg.reply_text("Ошибка состояния. Повтори через /users.")
            return
        err = limited_user_rename_profile(int(target_uid), old_name, new_name)
        clear_pending(uid)
        if err:
            await msg.reply_text(err)
            return
        await msg.reply_text(f"Переименовано для {target_uid}: {old_name} → {new_name}")
        return
