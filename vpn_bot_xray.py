import io
import json
import sqlite3
import subprocess
import urllib.parse

import qrcode

from vpn_bot_access import get_limited_user, upsert_limited_user
from vpn_bot_config import (
    DB_PATH,
    DEFAULT_LIMITED_MAX_ACTIVE,
    PROFILE_NAME_RE,
    PUBLIC_KEY,
    SERVER_IP,
    SHORT_ID,
    SNI,
)


def get_inbound():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT id, settings FROM inbounds WHERE port=443")
    row = cur.fetchone()
    conn.close()
    return (None, None) if not row else (row[0], json.loads(row[1]))


def save_settings(iid, s):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE inbounds SET settings=? WHERE id=?", (json.dumps(s), iid))
    conn.commit()
    conn.close()


def make_link(uuid, name, port):
    tag = urllib.parse.quote(name) + ("_WiFi" if port != 443 else "")
    return (
        f"vless://{uuid}@{SERVER_IP}:{port}"
        f"?type=tcp&security=reality"
        f"&pbk={PUBLIC_KEY}&fp=chrome"
        f"&sni={SNI}&sid={SHORT_ID}"
        f"&flow=xtls-rprx-vision#{tag}"
    )


def make_qr_bytes(text: str) -> bytes:
    img = qrcode.make(text)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf


def restart_xray():
    subprocess.run(["x-ui", "restart-xray"], capture_output=True)


def normalize_profile_name(raw: str) -> str:
    name = (raw or "").strip().replace(" ", "_")
    return name[:64] if name else ""


def validate_profile_name(name: str) -> str | None:
    cleaned = " ".join((name or "").split())
    if not cleaned:
        return None
    if len(cleaned) > 64:
        return None
    if not PROFILE_NAME_RE.fullmatch(cleaned):
        return None
    return cleaned


def choose_unique_name(base: str, used_names: set) -> str:
    candidate = base
    i = 2
    while any(x.lower() == candidate.lower() for x in used_names):
        candidate = f"{base}_{i}"
        i += 1
    return candidate


def admin_add_profile(name: str):
    iid, settings = get_inbound()
    if not settings:
        return None, "Ошибка: inbound не найден"
    if any(x.get("email", "").lower() == name.lower() for x in settings["clients"]):
        return None, f"Пользователь {name} уже существует"
    uuid = open("/proc/sys/kernel/random/uuid").read().strip()
    settings["clients"].append(
        {"email": name, "flow": "xtls-rprx-vision", "id": uuid, "password": ""}
    )
    save_settings(iid, settings)
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "INSERT INTO client_traffics (inbound_id,enable,email,up,down,expiry_time,total) VALUES (?,1,?,0,0,0,0)",
            (iid, name),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass
    restart_xray()
    return uuid, None


def admin_delete_profile(name: str):
    iid, settings = get_inbound()
    if not settings:
        return "Ошибка: inbound не найден"
    before = len(settings["clients"])
    settings["clients"] = [
        x for x in settings["clients"] if x.get("email", "").lower() != name.lower()
    ]
    if len(settings["clients"]) == before:
        return f"Пользователь {name} не найден"
    save_settings(iid, settings)
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("DELETE FROM client_traffics WHERE email=?", (name,))
        conn.commit()
        conn.close()
    except Exception:
        pass
    restart_xray()
    return None


def admin_rename_profile(old_name: str, new_name: str):
    iid, settings = get_inbound()
    if not settings:
        return "Ошибка: inbound не найден"
    client = next(
        (
            x
            for x in settings["clients"]
            if x.get("email", "").lower() == old_name.lower()
        ),
        None,
    )
    if not client:
        return f"Пользователь {old_name} не найден"
    if any(
        x.get("email", "").lower() == new_name.lower()
        for x in settings["clients"]
        if x is not client
    ):
        return f"Имя {new_name} уже занято"
    old_email = client["email"]
    client["email"] = new_name
    save_settings(iid, settings)
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "UPDATE client_traffics SET email=? WHERE email=?", (new_name, old_email)
        )
        conn.commit()
        conn.close()
    except Exception:
        pass
    restart_xray()
    return None


def provision_limited_user_profile(tg_user):
    uid = str(tg_user.id)
    user_rec = get_limited_user(uid)
    if user_rec is None:
        return (
            None,
            None,
            "Тебе не выдан доступ на создание профиля. Обратись к владельцу.",
        )

    if not isinstance(user_rec, dict):
        user_rec = {
            "profile_names": [str(user_rec)] if user_rec else [],
            "max_active": DEFAULT_LIMITED_MAX_ACTIVE,
        }
    iid, settings = get_inbound()
    if not settings:
        return None, None, "Ошибка: inbound не найден"

    clients = settings.get("clients", [])
    client_by_email = {x.get("email", "").lower(): x for x in clients if x.get("email")}
    tracked_names = user_rec.get("profile_names", [])
    if not isinstance(tracked_names, list):
        tracked_names = []
    active_names = [name for name in tracked_names if name.lower() in client_by_email]
    user_rec["profile_names"] = active_names

    max_active = int(
        user_rec.get("max_active", DEFAULT_LIMITED_MAX_ACTIVE)
        or DEFAULT_LIMITED_MAX_ACTIVE
    )
    if len(active_names) >= max_active:
        return (
            None,
            None,
            f"Достигнут лимит активных профилей: {len(active_names)}/{max_active}.\nУдали один профиль и попробуй снова.",
        )

    used_names = {x.get("email", "") for x in clients if x.get("email")}
    base = normalize_profile_name(
        tg_user.username or tg_user.full_name or f"user_{tg_user.id}"
    )
    if not base:
        base = f"user_{tg_user.id}"
    name = choose_unique_name(base, used_names)
    uuid = open("/proc/sys/kernel/random/uuid").read().strip()

    clients.append(
        {"email": name, "flow": "xtls-rprx-vision", "id": uuid, "password": ""}
    )
    settings["clients"] = clients
    save_settings(iid, settings)
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "INSERT INTO client_traffics (inbound_id,enable,email,up,down,expiry_time,total) VALUES (?,1,?,0,0,0,0)",
            (iid, name),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass

    active_names.append(name)
    user_rec["profile_names"] = active_names
    upsert_limited_user(uid, user_rec)
    restart_xray()
    return name, uuid, None


def get_limited_user_active_profiles(uid: int):
    rec = get_limited_user(uid)
    if rec is None:
        return None, "Тебе не выдан доступ на создание профиля. Обратись к владельцу."
    if not isinstance(rec, dict):
        names = [str(rec)] if rec else []
        limit = DEFAULT_LIMITED_MAX_ACTIVE
    else:
        names = rec.get("profile_names", [])
        if not isinstance(names, list):
            names = []
        limit = int(
            rec.get("max_active", DEFAULT_LIMITED_MAX_ACTIVE)
            or DEFAULT_LIMITED_MAX_ACTIVE
        )

    _, settings = get_inbound()
    if not settings:
        return None, "Ошибка: inbound не найден"
    clients = settings.get("clients", [])
    client_by_email = {x.get("email", "").lower(): x for x in clients if x.get("email")}
    active = [name for name in names if name.lower() in client_by_email]
    return {"active_names": active, "limit": limit}, None


def format_limited_profiles_text(info: dict) -> str:
    names = info["active_names"]
    limit = info["limit"]
    if not names:
        return f"У тебя пока нет активных профилей.\nЛимит: 0/{limit}\nИспользуй /myvpn, чтобы создать профиль."
    lines = [f"Твои активные профили ({len(names)}/{limit}):\n"]
    for i, name in enumerate(names, 1):
        lines.append(f"{i}. {name}")
    return "\n".join(lines)


def limited_user_delete_profile(uid: int, name: str):
    rec = get_limited_user(uid)
    if rec is None:
        return "Тебе не выдан доступ на создание профиля. Обратись к владельцу."
    names = (
        rec.get("profile_names", [])
        if isinstance(rec, dict)
        else ([str(rec)] if rec else [])
    )
    if not isinstance(names, list):
        names = []

    iid, settings = get_inbound()
    if not settings:
        return "Ошибка: inbound не найден"
    clients = settings.get("clients", [])
    client_by_email = {x.get("email", "").lower(): x for x in clients if x.get("email")}
    active_owned = [n for n in names if n.lower() in client_by_email]
    if name.lower() not in {n.lower() for n in active_owned}:
        return "Ты можешь удалять только свои активные профили (/myvpn list)."

    before = len(clients)
    settings["clients"] = [
        x for x in clients if x.get("email", "").lower() != name.lower()
    ]
    if len(settings["clients"]) == before:
        return f"Профиль {name} не найден"
    save_settings(iid, settings)
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("DELETE FROM client_traffics WHERE email=?", (name,))
        conn.commit()
        conn.close()
    except Exception:
        pass

    rec["profile_names"] = (
        [n for n in names if n.lower() != name.lower()] if isinstance(rec, dict) else []
    )
    upsert_limited_user(uid, rec)
    restart_xray()
    return None


def limited_user_rename_profile(uid: int, old_name: str, new_name: str):
    rec = get_limited_user(uid)
    if rec is None:
        return "Тебе не выдан доступ на создание профиля. Обратись к владельцу."
    names = (
        rec.get("profile_names", [])
        if isinstance(rec, dict)
        else ([str(rec)] if rec else [])
    )
    if not isinstance(names, list):
        names = []

    iid, settings = get_inbound()
    if not settings:
        return "Ошибка: inbound не найден"
    clients = settings.get("clients", [])
    client_by_email = {x.get("email", "").lower(): x for x in clients if x.get("email")}
    active_owned = [n for n in names if n.lower() in client_by_email]
    if old_name.lower() not in {n.lower() for n in active_owned}:
        return "Ты можешь переименовывать только свои активные профили (/myvpn list)."

    client = next(
        (x for x in clients if x.get("email", "").lower() == old_name.lower()), None
    )
    if not client:
        return f"Профиль {old_name} не найден"
    if any(
        x.get("email", "").lower() == new_name.lower()
        for x in clients
        if x is not client
    ):
        return f"Имя {new_name} уже занято"

    old_email = client["email"]
    client["email"] = new_name
    save_settings(iid, settings)
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "UPDATE client_traffics SET email=? WHERE email=?", (new_name, old_email)
        )
        conn.commit()
        conn.close()
    except Exception:
        pass

    rec["profile_names"] = (
        [new_name if n.lower() == old_name.lower() else n for n in names]
        if isinstance(rec, dict)
        else [new_name]
    )
    upsert_limited_user(uid, rec)
    restart_xray()
    return None


async def send_user_links(
    update_or_message, context, name: str, uuid: str, ports: list[int] | None = None
):
    msg = update_or_message
    selected_ports = ports if ports else [443, 2087]
    selected_ports = [int(p) for p in selected_ports if int(p) in {443, 2087}]
    if not selected_ports:
        selected_ports = [443]

    links = {port: make_link(uuid, name, port) for port in selected_ports}
    lines = [f"Данные подключения для {name}:\n"]
    if 443 in links:
        lines.append(f"Порт 443 (основная):\n{links[443]}")
    if 2087 in links:
        lines.append(f"Порт 2087 (WiFi резерв):\n{links[2087]}")
    await msg.reply_text("\n\n".join(lines))

    qr_port = 443 if 443 in links else selected_ports[0]
    await msg.reply_photo(
        photo=make_qr_bytes(links[qr_port]),
        caption=f"QR-код для {name} (порт {qr_port})",
    )


def _coerce_limited_user_rec(rec):
    if isinstance(rec, dict):
        names = rec.get("profile_names", [])
        if not isinstance(names, list):
            names = []
        limit = int(
            rec.get("max_active", DEFAULT_LIMITED_MAX_ACTIVE)
            or DEFAULT_LIMITED_MAX_ACTIVE
        )
    else:
        base = str(rec or "")
        names = [base] if base else []
        limit = DEFAULT_LIMITED_MAX_ACTIVE
    return [str(x) for x in names if str(x).strip()], max(0, int(limit))
