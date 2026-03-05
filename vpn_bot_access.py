import json
import os
import sqlite3
import time

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from vpn_bot_config import (
    ACCESS_DB_PATH,
    DEFAULT_LIMITED_MAX_ACTIVE,
    LEGACY_ADMINS_FILE,
    LEGACY_USERS_FILE,
    OWNER_ID,
    UNAUTHORIZED_ALERT_COOLDOWN_SEC,
    log,
)

_UNAUTHORIZED_ALERT_STATE = {}
_PENDING_INPUT_STATE = {}


def access_conn():
    return sqlite3.connect(ACCESS_DB_PATH)


def init_access_db():
    conn = access_conn()
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("CREATE TABLE IF NOT EXISTS admins (user_id INTEGER PRIMARY KEY)")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS limited_users ("
        "user_id TEXT PRIMARY KEY,"
        'profile_name TEXT NOT NULL DEFAULT "",'
        'profile_names TEXT NOT NULL DEFAULT "[]",'
        'full_name TEXT NOT NULL DEFAULT "",'
        'username TEXT NOT NULL DEFAULT "",'
        "key_port INTEGER NOT NULL DEFAULT 443,"
        "max_active INTEGER NOT NULL DEFAULT 3,"
        "max_creates INTEGER NOT NULL DEFAULT 1,"
        "used_creates INTEGER NOT NULL DEFAULT 0"
        ")"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS access_attempts ("
        "user_id TEXT PRIMARY KEY,"
        'full_name TEXT NOT NULL DEFAULT "",'
        'username TEXT NOT NULL DEFAULT "",'
        "updated_at INTEGER NOT NULL DEFAULT 0"
        ")"
    )
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(limited_users)")
    cols = {row[1] for row in cur.fetchall()}
    if "max_creates" not in cols:
        conn.execute(
            "ALTER TABLE limited_users ADD COLUMN max_creates INTEGER NOT NULL DEFAULT 1"
        )
    if "used_creates" not in cols:
        conn.execute(
            "ALTER TABLE limited_users ADD COLUMN used_creates INTEGER NOT NULL DEFAULT 0"
        )
    if "profile_names" not in cols:
        conn.execute(
            'ALTER TABLE limited_users ADD COLUMN profile_names TEXT NOT NULL DEFAULT "[]"'
        )
    if "max_active" not in cols:
        conn.execute(
            "ALTER TABLE limited_users ADD COLUMN max_active INTEGER NOT NULL DEFAULT 3"
        )
        conn.execute(
            f"UPDATE limited_users SET max_active = COALESCE(max_creates, {DEFAULT_LIMITED_MAX_ACTIVE})"
        )
    if "full_name" not in cols:
        conn.execute(
            'ALTER TABLE limited_users ADD COLUMN full_name TEXT NOT NULL DEFAULT ""'
        )
    if "username" not in cols:
        conn.execute(
            'ALTER TABLE limited_users ADD COLUMN username TEXT NOT NULL DEFAULT ""'
        )
    if "key_port" not in cols:
        conn.execute(
            "ALTER TABLE limited_users ADD COLUMN key_port INTEGER NOT NULL DEFAULT 443"
        )
    conn.execute(
        "UPDATE limited_users SET profile_names=json_array(profile_name) "
        'WHERE COALESCE(profile_name, "") != "" AND COALESCE(profile_names, "[]")="[]"'
    )
    conn.execute(
        "UPDATE limited_users SET used_creates=1 "
        'WHERE COALESCE(profile_name, "") != "" AND COALESCE(used_creates, 0)=0'
    )
    conn.commit()
    conn.close()


def migrate_access_from_json():
    migrated_any = False
    if os.path.exists(LEGACY_ADMINS_FILE):
        try:
            with open(LEGACY_ADMINS_FILE) as f:
                data = json.load(f)
            if isinstance(data, list):
                save_admins({int(x) for x in data if str(x).isdigit()})
                migrated_any = True
                os.rename(LEGACY_ADMINS_FILE, LEGACY_ADMINS_FILE + ".migrated")
        except Exception as e:
            log.warning("Не удалось мигрировать admins JSON: %s", e)
    if os.path.exists(LEGACY_USERS_FILE):
        try:
            with open(LEGACY_USERS_FILE) as f:
                data = json.load(f)
            if isinstance(data, dict):
                clean = {
                    str(k): str(v or "") for k, v in data.items() if str(k).isdigit()
                }
                save_users(clean)
                migrated_any = True
                os.rename(LEGACY_USERS_FILE, LEGACY_USERS_FILE + ".migrated")
        except Exception as e:
            log.warning("Не удалось мигрировать users JSON: %s", e)
    return migrated_any


def seed_access_defaults():
    admins = load_admins()
    if not admins:
        save_admins({OWNER_ID, 32431493})
        return
    if OWNER_ID not in admins:
        admins.add(OWNER_ID)
        save_admins(admins)


def load_admins() -> set:
    conn = access_conn()
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM admins")
    rows = cur.fetchall()
    conn.close()
    return {int(row[0]) for row in rows}


def save_admins(admins: set):
    conn = access_conn()
    admin_ids = sorted({int(a) for a in admins})
    conn.execute("BEGIN IMMEDIATE")
    conn.executemany(
        "INSERT OR IGNORE INTO admins (user_id) VALUES (?)", [(x,) for x in admin_ids]
    )
    if admin_ids:
        placeholders = ",".join(["?"] * len(admin_ids))
        conn.execute(
            f"DELETE FROM admins WHERE user_id NOT IN ({placeholders})", admin_ids
        )
    else:
        conn.execute("DELETE FROM admins")
    conn.commit()
    conn.close()


def load_users() -> dict:
    conn = access_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT user_id, profile_name, profile_names, full_name, username, key_port, max_active FROM limited_users"
    )
    rows = cur.fetchall()
    conn.close()
    out = {}
    for (
        uid,
        profile,
        profile_names_raw,
        full_name,
        username,
        key_port,
        max_active,
    ) in rows:
        profile_names = []
        try:
            parsed = json.loads(profile_names_raw or "[]")
            if isinstance(parsed, list):
                profile_names = [str(x) for x in parsed if str(x).strip()]
        except Exception:
            profile_names = []
        if not profile_names and profile:
            profile_names = [str(profile)]
        out[str(uid)] = {
            "profile_names": profile_names,
            "full_name": str(full_name or "").strip(),
            "username": str(username or "").strip().lstrip("@"),
            "key_port": int(key_port or 443),
            "max_active": int(max_active or DEFAULT_LIMITED_MAX_ACTIVE),
        }
    return out


def _normalize_limited_user_record(data) -> dict:
    if isinstance(data, dict):
        profile_names = data.get("profile_names", [])
        if not isinstance(profile_names, list):
            profile_names = []
        clean_profiles = [str(x) for x in profile_names if str(x).strip()]
        full_name = str(data.get("full_name", "") or "").strip()
        username = str(data.get("username", "") or "").strip().lstrip("@")
        key_port = int(data.get("key_port", 443) or 443)
        if key_port not in {443, 2087}:
            key_port = 443
        max_active = int(
            data.get("max_active", DEFAULT_LIMITED_MAX_ACTIVE)
            or DEFAULT_LIMITED_MAX_ACTIVE
        )
    else:
        profile_name = str(data or "")
        clean_profiles = [profile_name] if profile_name else []
        full_name = ""
        username = ""
        key_port = 443
        max_active = DEFAULT_LIMITED_MAX_ACTIVE
    max_active = max(0, max_active)
    return {
        "profile_name": clean_profiles[0] if clean_profiles else "",
        "profile_names": clean_profiles,
        "full_name": full_name,
        "username": username,
        "key_port": key_port,
        "max_active": max_active,
    }


def get_limited_user(user_id: int | str):
    uid = str(user_id).strip()
    if not uid:
        return None
    conn = access_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT user_id, profile_name, profile_names, full_name, username, key_port, max_active "
        "FROM limited_users WHERE user_id=?",
        (uid,),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    _, profile_name, profile_names_raw, full_name, username, key_port, max_active = row
    try:
        parsed = json.loads(profile_names_raw or "[]")
        names = (
            [str(x) for x in parsed if str(x).strip()]
            if isinstance(parsed, list)
            else []
        )
    except Exception:
        names = []
    if not names and profile_name:
        names = [str(profile_name)]
    return {
        "profile_names": names,
        "full_name": str(full_name or "").strip(),
        "username": str(username or "").strip().lstrip("@"),
        "key_port": int(key_port or 443),
        "max_active": int(max_active or DEFAULT_LIMITED_MAX_ACTIVE),
    }


def upsert_limited_user(user_id: int | str, data):
    uid = str(user_id).strip()
    if not uid:
        return
    rec = _normalize_limited_user_record(data)
    conn = access_conn()
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "INSERT INTO limited_users (user_id, profile_name, profile_names, full_name, username, key_port, max_active) "
        "VALUES (?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET "
        "profile_name=excluded.profile_name, "
        "profile_names=excluded.profile_names, "
        "full_name=excluded.full_name, "
        "username=excluded.username, "
        "key_port=excluded.key_port, "
        "max_active=excluded.max_active",
        (
            uid,
            rec["profile_name"],
            json.dumps(rec["profile_names"], ensure_ascii=False),
            rec["full_name"],
            rec["username"],
            rec["key_port"],
            rec["max_active"],
        ),
    )
    conn.commit()
    conn.close()


def delete_limited_user(user_id: int | str) -> bool:
    uid = str(user_id).strip()
    if not uid:
        return False
    conn = access_conn()
    conn.execute("BEGIN IMMEDIATE")
    cur = conn.cursor()
    cur.execute("DELETE FROM limited_users WHERE user_id=?", (uid,))
    deleted = cur.rowcount > 0
    conn.commit()
    conn.close()
    return deleted


def save_users(users: dict):
    # Backward-compatible helper. Performs per-user UPSERT without deleting others.
    for uid, data in users.items():
        upsert_limited_user(uid, data)


def is_admin(uid):
    return uid in load_admins()


def is_limited(uid):
    return str(uid) in load_users()


def is_allowed(uid):
    return is_admin(uid) or is_limited(uid)


def is_owner(uid):
    return uid == OWNER_ID


def upsert_access_attempt(user_id: int | str, full_name: str, username: str):
    uid = str(user_id).strip()
    if not uid:
        return
    f_name = str(full_name or "").strip()
    u_name = str(username or "").strip().lstrip("@")
    now = int(time.time())
    conn = access_conn()
    conn.execute(
        "INSERT INTO access_attempts (user_id, full_name, username, updated_at) VALUES (?, ?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET "
        "full_name=excluded.full_name, username=excluded.username, updated_at=excluded.updated_at",
        (uid, f_name, u_name, now),
    )
    conn.commit()
    conn.close()


def get_access_attempt_identity(user_id: int | str):
    uid = str(user_id).strip()
    if not uid:
        return "", ""
    conn = access_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT full_name, username FROM access_attempts WHERE user_id=?", (uid,)
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return "", ""
    return str(row[0] or "").strip(), str(row[1] or "").strip().lstrip("@")


def should_send_unauthorized_alert(uid: int) -> bool:
    now = time.time()
    last = _UNAUTHORIZED_ALERT_STATE.get(uid, 0.0)
    if now - last < UNAUTHORIZED_ALERT_COOLDOWN_SEC:
        return False
    _UNAUTHORIZED_ALERT_STATE[uid] = now
    return True


async def notify_unauthorized_user(bot, user):
    uid = user.id
    upsert_access_attempt(uid, user.full_name or "", user.username or "")
    if not should_send_unauthorized_alert(uid):
        return False
    name = f"@{user.username}" if user.username else user.full_name or "—"
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Лимит 1", callback_data=f"apv:adduser:{uid}:1"),
                InlineKeyboardButton(
                    f"Лимит {DEFAULT_LIMITED_MAX_ACTIVE}",
                    callback_data=f"apv:adduser:{uid}:{DEFAULT_LIMITED_MAX_ACTIVE}",
                ),
                InlineKeyboardButton("Лимит 5", callback_data=f"apv:adduser:{uid}:5"),
            ],
            [
                InlineKeyboardButton(
                    "Сделать админом", callback_data=f"apv:addadmin:{uid}"
                )
            ],
        ]
    )
    await bot.send_message(
        OWNER_ID,
        f"Попытка доступа к боту:\n"
        f"ID: {uid}\n"
        f"Имя: {user.full_name}\n"
        f"Username: {name}\n\n"
        "Выбери действие кнопками ниже.\n"
        f"Команды на всякий случай:\n"
        f"/addadmin {uid}\n"
        f"/adduser {uid} {DEFAULT_LIMITED_MAX_ACTIVE}",
        reply_markup=keyboard,
    )
    return True


def set_pending(uid: int, action: str, data: dict | None = None):
    _PENDING_INPUT_STATE[uid] = {"action": action, "data": data or {}}


def get_pending(uid: int):
    return _PENDING_INPUT_STATE.get(uid)


def clear_pending(uid: int):
    _PENDING_INPUT_STATE.pop(uid, None)
