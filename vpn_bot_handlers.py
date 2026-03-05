from telegram import Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    filters,
)

from vpn_bot_access import (
    clear_pending,
    init_access_db,
    is_admin,
    is_allowed,
    is_owner,
    migrate_access_from_json,
    notify_unauthorized_user,
    seed_access_defaults,
)
from vpn_bot_config import (
    BOT_TOKEN,
    DEFAULT_LIMITED_MAX_ACTIVE,
    PUBLIC_KEY,
    SERVER_IP,
    SHORT_ID,
    SNI,
    log,
)
from vpn_bot_handlers_admin import (
    cb_link,
    cb_limited_users,
    cb_list_page,
    cb_owner_approve,
    cmd_add,
    cmd_addadmin,
    cmd_adduser,
    cmd_admins,
    cmd_delete,
    cmd_link,
    cmd_list,
    cmd_removeadmin,
    cmd_removeuser,
    cmd_rename,
    cmd_setuserlimit,
    cmd_users,
)
from vpn_bot_handlers_limited import cmd_myprofiles, cmd_myvpn, handle_unknown


async def cmd_start(u: Update, c):
    uid = u.effective_user.id
    user = u.effective_user
    clear_pending(uid)
    if not is_allowed(uid):
        sent_now = await notify_unauthorized_user(c.bot, user)
        if sent_now:
            status_line = "Я отправил владельцу запрос на доступ с твоим ID."
        else:
            status_line = "Запрос владельцу уже отправлялся недавно. Повторно отправлю чуть позже."
        await u.message.reply_text(
            "Доступ к боту пока не выдан.\n\n"
            f"{status_line}\n"
            "После одобрения ты получишь отдельное сообщение от бота.\n\n"
            f"Твой Telegram ID: {uid}"
        )
        return
    if is_owner(uid):
        await u.message.reply_text(
            "VPN Manager (владелец)\n\n"
            "/add Имя — добавить пользователя\n"
            "/list — список всех пользователей\n"
            "/link Имя — ссылки + QR-код\n"
            "/delete Имя — удалить пользователя\n"
            "/rename Имя НовоеИмя — переименовать пользователя\n"
            "/myvpn — создать/получить свой профиль\n"
            "/admins — список администраторов\n"
            "/addadmin ID — добавить администратора (только владелец)\n"
            "/removeadmin ID — удалить администратора (только владелец)\n"
            f"/adduser ID [лимит, по умолчанию {DEFAULT_LIMITED_MAX_ACTIVE}] — добавить лимитного пользователя (только владелец)\n"
            "/setuserlimit ID лимит — изменить лимит активных профилей (только владелец)\n"
            "/removeuser ID — удалить лимитного пользователя (только владелец)\n"
            "/users — список лимитных пользователей"
        )
        return
    if is_admin(uid):
        await u.message.reply_text(
            "VPN Manager (админ)\n\n"
            "/add Имя — добавить пользователя\n"
            "/list — список всех пользователей\n"
            "/link Имя — ссылки + QR-код\n"
            "/delete Имя — удалить пользователя\n"
            "/rename Имя НовоеИмя — переименовать пользователя\n"
            "/myvpn — создать/получить свой профиль\n"
            "/admins — список администраторов\n"
            "/users — список лимитных пользователей"
        )
        return
    await u.message.reply_text(
        "VPN Manager (лимитный доступ)\n\n"
        "/myvpn — создать профиль и получить ссылки\n"
        "/myvpn list — мои активные профили\n"
        "/myvpn rename СТАРОЕ НОВОЕ — переименовать свой профиль\n"
        "/myvpn delete ИМЯ — удалить свой профиль"
    )


def main():
    required_env = {
        "VPN_BOT_TOKEN": BOT_TOKEN,
        "VPN_SERVER_IP": SERVER_IP,
        "VPN_PUBLIC_KEY": PUBLIC_KEY,
        "VPN_SHORT_ID": SHORT_ID,
        "VPN_SNI": SNI,
    }
    missing = [k for k, v in required_env.items() if not v]
    if missing:
        raise RuntimeError(
            f"Не заданы обязательные переменные окружения: {', '.join(missing)}"
        )
    init_access_db()
    migrate_access_from_json()
    seed_access_defaults()

    app = Application.builder().token(BOT_TOKEN).build()
    for cmd, fn in [
        ("start", cmd_start),
        ("help", cmd_start),
        ("add", cmd_add),
        ("list", cmd_list),
        ("link", cmd_link),
        ("delete", cmd_delete),
        ("rename", cmd_rename),
        ("admins", cmd_admins),
        ("addadmin", cmd_addadmin),
        ("removeadmin", cmd_removeadmin),
        ("adduser", cmd_adduser),
        ("setuserlimit", cmd_setuserlimit),
        ("removeuser", cmd_removeuser),
        ("users", cmd_users),
        ("myvpn", cmd_myvpn),
        ("myprofiles", cmd_myprofiles),
    ]:
        app.add_handler(CommandHandler(cmd, fn))
    app.add_handler(CallbackQueryHandler(cb_link, pattern="^linku:"))
    app.add_handler(CallbackQueryHandler(cb_list_page, pattern="^listpg:"))
    app.add_handler(CallbackQueryHandler(cb_limited_users, pattern="^lu:"))
    app.add_handler(CallbackQueryHandler(cb_owner_approve, pattern="^apv:"))
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_unknown))
    log.info("VPN Bot started")
    app.run_polling(allowed_updates=Update.ALL_TYPES)
