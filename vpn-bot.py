import logging, sqlite3, json, subprocess, urllib.parse, os, io, re, time
import qrcode
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters

def get_env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    return int(raw) if raw.isdigit() else default

BOT_TOKEN   = os.getenv('VPN_BOT_TOKEN', '').strip()
OWNER_ID    = get_env_int('VPN_OWNER_ID', 294083624)
ACCESS_DB_PATH     = '/etc/vpn-bot.sqlite'
LEGACY_ADMINS_FILE = '/etc/vpn-bot-admins.json'
LEGACY_USERS_FILE  = '/etc/vpn-bot-users.json'
DB_PATH     = '/etc/x-ui/x-ui.db'
SERVER_IP   = os.getenv('VPN_SERVER_IP', '').strip()
PUBLIC_KEY  = os.getenv('VPN_PUBLIC_KEY', '').strip()
SHORT_ID    = os.getenv('VPN_SHORT_ID', '').strip()
SNI         = os.getenv('VPN_SNI', '').strip()
UNAUTHORIZED_ALERT_COOLDOWN_SEC = int(os.getenv('VPN_ALERT_COOLDOWN_SEC', '300'))
PROFILE_NAME_RE = re.compile(r'^[\w .@-]{1,64}$', re.UNICODE)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger(__name__)
_UNAUTHORIZED_ALERT_STATE = {}
_PENDING_INPUT_STATE = {}


# ── Доступ ─────────────────────────────────────────────────────

def access_conn():
    return sqlite3.connect(ACCESS_DB_PATH)

def init_access_db():
    conn = access_conn()
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute(
        'CREATE TABLE IF NOT EXISTS admins ('
        'user_id INTEGER PRIMARY KEY'
        ')'
    )
    conn.execute(
        'CREATE TABLE IF NOT EXISTS limited_users ('
        'user_id TEXT PRIMARY KEY,'
        'profile_name TEXT NOT NULL DEFAULT "",'
        'profile_names TEXT NOT NULL DEFAULT "[]",'
        'max_active INTEGER NOT NULL DEFAULT 1,'
        'max_creates INTEGER NOT NULL DEFAULT 1,'
        'used_creates INTEGER NOT NULL DEFAULT 0'
        ')'
    )
    cur = conn.cursor()
    cur.execute('PRAGMA table_info(limited_users)')
    cols = {row[1] for row in cur.fetchall()}
    if 'max_creates' not in cols:
        conn.execute('ALTER TABLE limited_users ADD COLUMN max_creates INTEGER NOT NULL DEFAULT 1')
    if 'used_creates' not in cols:
        conn.execute('ALTER TABLE limited_users ADD COLUMN used_creates INTEGER NOT NULL DEFAULT 0')
    if 'profile_names' not in cols:
        conn.execute('ALTER TABLE limited_users ADD COLUMN profile_names TEXT NOT NULL DEFAULT "[]"')
    if 'max_active' not in cols:
        conn.execute('ALTER TABLE limited_users ADD COLUMN max_active INTEGER NOT NULL DEFAULT 1')
        conn.execute('UPDATE limited_users SET max_active = COALESCE(max_creates, 1)')
    conn.execute(
        'UPDATE limited_users SET profile_names=json_array(profile_name) '
        'WHERE COALESCE(profile_name, "") != "" AND COALESCE(profile_names, "[]")="[]"'
    )
    # Для старых записей: если профиль уже был, считаем это одним использованным созданием.
    conn.execute(
        'UPDATE limited_users SET used_creates=1 '
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
                os.rename(LEGACY_ADMINS_FILE, LEGACY_ADMINS_FILE + '.migrated')
        except Exception as e:
            log.warning('Не удалось мигрировать admins JSON: %s', e)
    if os.path.exists(LEGACY_USERS_FILE):
        try:
            with open(LEGACY_USERS_FILE) as f:
                data = json.load(f)
            if isinstance(data, dict):
                clean = {str(k): str(v or '') for k, v in data.items() if str(k).isdigit()}
                save_users(clean)
                migrated_any = True
                os.rename(LEGACY_USERS_FILE, LEGACY_USERS_FILE + '.migrated')
        except Exception as e:
            log.warning('Не удалось мигрировать users JSON: %s', e)
    return migrated_any

def seed_access_defaults():
    admins = load_admins()
    if not admins:
        # Сохраняем прежнее поведение первого запуска.
        save_admins({OWNER_ID, 32431493})
        return
    if OWNER_ID not in admins:
        admins.add(OWNER_ID)
        save_admins(admins)

def load_admins() -> set:
    conn = access_conn()
    cur = conn.cursor()
    cur.execute('SELECT user_id FROM admins')
    rows = cur.fetchall()
    conn.close()
    return {int(row[0]) for row in rows}

def save_admins(admins: set):
    conn = access_conn()
    admin_ids = sorted({int(a) for a in admins})
    conn.execute('BEGIN IMMEDIATE')
    conn.executemany(
        'INSERT OR IGNORE INTO admins (user_id) VALUES (?)',
        [(x,) for x in admin_ids]
    )
    if admin_ids:
        placeholders = ','.join(['?'] * len(admin_ids))
        conn.execute(f'DELETE FROM admins WHERE user_id NOT IN ({placeholders})', admin_ids)
    else:
        conn.execute('DELETE FROM admins')
    conn.commit()
    conn.close()

def load_users() -> dict:
    conn = access_conn()
    cur = conn.cursor()
    cur.execute('SELECT user_id, profile_name, profile_names, max_active FROM limited_users')
    rows = cur.fetchall()
    conn.close()
    out = {}
    for uid, profile, profile_names_raw, max_active in rows:
        profile_names = []
        try:
            parsed = json.loads(profile_names_raw or '[]')
            if isinstance(parsed, list):
                profile_names = [str(x) for x in parsed if str(x).strip()]
        except Exception:
            profile_names = []
        if not profile_names and profile:
            profile_names = [str(profile)]
        out[str(uid)] = {
            'profile_names': profile_names,
            'max_active': int(max_active or 1),
        }
    return out

def save_users(users: dict):
    conn = access_conn()
    conn.execute('BEGIN IMMEDIATE')
    payload = []
    user_ids = []
    for uid, data in users.items():
        uid_str = str(uid)
        if isinstance(data, dict):
            profile_names = data.get('profile_names', [])
            if not isinstance(profile_names, list):
                profile_names = []
            clean_profiles = [str(x) for x in profile_names if str(x).strip()]
            profile_name = clean_profiles[0] if clean_profiles else ''
            max_active = int(data.get('max_active', 1) or 1)
        else:
            # legacy format: uid -> "profile_name"
            profile_name = str(data or '')
            clean_profiles = [profile_name] if profile_name else []
            max_active = 1
        max_active = max(0, max_active)
        payload.append((
            uid_str,
            profile_name,
            json.dumps(clean_profiles, ensure_ascii=False),
            max_active
        ))
        user_ids.append(uid_str)
    if payload:
        conn.executemany(
            'INSERT INTO limited_users (user_id, profile_name, profile_names, max_active) VALUES (?, ?, ?, ?) '
            'ON CONFLICT(user_id) DO UPDATE SET '
            'profile_name=excluded.profile_name, '
            'profile_names=excluded.profile_names, '
            'max_active=excluded.max_active',
            payload
        )
        placeholders = ','.join(['?'] * len(user_ids))
        conn.execute(f'DELETE FROM limited_users WHERE user_id NOT IN ({placeholders})', user_ids)
    else:
        conn.execute('DELETE FROM limited_users')
    conn.commit()
    conn.close()

def is_admin(uid): return uid in load_admins()
def is_limited(uid): return str(uid) in load_users()
def is_allowed(uid): return is_admin(uid) or is_limited(uid)
def is_owner(uid):   return uid == OWNER_ID


# ── VPN helpers ────────────────────────────────────────────────

def get_inbound():
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.execute('SELECT id, settings FROM inbounds WHERE port=443')
    row = cur.fetchone()
    conn.close()
    return (None, None) if not row else (row[0], json.loads(row[1]))

def save_settings(iid, s):
    conn = sqlite3.connect(DB_PATH)
    conn.execute('UPDATE inbounds SET settings=? WHERE id=?', (json.dumps(s), iid))
    conn.commit()
    conn.close()

def make_link(uuid, name, port):
    tag = urllib.parse.quote(name) + ('_WiFi' if port != 443 else '')
    return (f'vless://{uuid}@{SERVER_IP}:{port}'
            f'?type=tcp&security=reality'
            f'&pbk={PUBLIC_KEY}&fp=chrome'
            f'&sni={SNI}&sid={SHORT_ID}'
            f'&flow=xtls-rprx-vision#{tag}')

def make_qr_bytes(text: str) -> bytes:
    img = qrcode.make(text)
    buf = io.BytesIO()
    img.save(buf, format='PNG')
    buf.seek(0)
    return buf

def restart_xray():
    subprocess.run(['x-ui', 'restart-xray'], capture_output=True)

def normalize_profile_name(raw: str) -> str:
    name = (raw or '').strip().replace(' ', '_')
    return name[:64] if name else ''

def validate_profile_name(name: str) -> str | None:
    cleaned = ' '.join((name or '').split())
    if not cleaned:
        return None
    if len(cleaned) > 64:
        return None
    if not PROFILE_NAME_RE.fullmatch(cleaned):
        return None
    return cleaned

def should_send_unauthorized_alert(uid: int) -> bool:
    now = time.time()
    last = _UNAUTHORIZED_ALERT_STATE.get(uid, 0.0)
    if now - last < UNAUTHORIZED_ALERT_COOLDOWN_SEC:
        return False
    _UNAUTHORIZED_ALERT_STATE[uid] = now
    return True

async def notify_unauthorized_user(bot, user):
    uid = user.id
    if not should_send_unauthorized_alert(uid):
        return
    name = f'@{user.username}' if user.username else user.full_name or '—'
    await bot.send_message(
        OWNER_ID,
        f'Попытка доступа к боту:\n'
        f'ID: {uid}\n'
        f'Имя: {user.full_name}\n'
        f'Username: {name}\n\n'
        f'Чтобы добавить админом: /addadmin {uid}\n'
        f'Чтобы добавить ограниченно: /adduser {uid} 1'
    )

def choose_unique_name(base: str, used_names: set) -> str:
    candidate = base
    i = 2
    while any(x.lower() == candidate.lower() for x in used_names):
        candidate = f'{base}_{i}'
        i += 1
    return candidate

def set_pending(uid: int, action: str, data: dict | None = None):
    _PENDING_INPUT_STATE[uid] = {'action': action, 'data': data or {}}

def get_pending(uid: int):
    return _PENDING_INPUT_STATE.get(uid)

def clear_pending(uid: int):
    _PENDING_INPUT_STATE.pop(uid, None)

def admin_add_profile(name: str):
    iid, settings = get_inbound()
    if not settings:
        return None, 'Ошибка: inbound не найден'
    if any(x.get('email', '').lower() == name.lower() for x in settings['clients']):
        return None, f'Пользователь {name} уже существует'
    uuid = open('/proc/sys/kernel/random/uuid').read().strip()
    settings['clients'].append({'email': name, 'flow': 'xtls-rprx-vision', 'id': uuid, 'password': ''})
    save_settings(iid, settings)
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            'INSERT INTO client_traffics (inbound_id,enable,email,up,down,expiry_time,total) VALUES (?,1,?,0,0,0,0)',
            (iid, name)
        )
        conn.commit(); conn.close()
    except Exception:
        pass
    restart_xray()
    return uuid, None

def admin_delete_profile(name: str):
    iid, settings = get_inbound()
    if not settings:
        return 'Ошибка: inbound не найден'
    before = len(settings['clients'])
    settings['clients'] = [x for x in settings['clients'] if x.get('email', '').lower() != name.lower()]
    if len(settings['clients']) == before:
        return f'Пользователь {name} не найден'
    save_settings(iid, settings)
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute('DELETE FROM client_traffics WHERE email=?', (name,))
        conn.commit(); conn.close()
    except Exception:
        pass
    restart_xray()
    return None

def admin_rename_profile(old_name: str, new_name: str):
    iid, settings = get_inbound()
    if not settings:
        return 'Ошибка: inbound не найден'
    client = next((x for x in settings['clients'] if x.get('email', '').lower() == old_name.lower()), None)
    if not client:
        return f'Пользователь {old_name} не найден'
    if any(x.get('email', '').lower() == new_name.lower() for x in settings['clients'] if x is not client):
        return f'Имя {new_name} уже занято'
    old_email = client['email']
    client['email'] = new_name
    save_settings(iid, settings)
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute('UPDATE client_traffics SET email=? WHERE email=?', (new_name, old_email))
        conn.commit(); conn.close()
    except Exception:
        pass
    restart_xray()
    return None

def provision_limited_user_profile(tg_user):
    uid = str(tg_user.id)
    users = load_users()
    user_rec = users.get(uid)
    if user_rec is None:
        return None, None, 'Тебе не выдан доступ на создание профиля. Обратись к владельцу.'

    if not isinstance(user_rec, dict):
        user_rec = {
            'profile_names': [str(user_rec)] if user_rec else [],
            'max_active': 1,
        }
    iid, settings = get_inbound()
    if not settings:
        return None, None, 'Ошибка: inbound не найден'

    clients = settings.get('clients', [])
    client_by_email = {x.get('email', '').lower(): x for x in clients if x.get('email')}
    tracked_names = user_rec.get('profile_names', [])
    if not isinstance(tracked_names, list):
        tracked_names = []
    active_names = [name for name in tracked_names if name.lower() in client_by_email]
    user_rec['profile_names'] = active_names

    max_active = int(user_rec.get('max_active', 1) or 1)
    if len(active_names) >= max_active:
        return None, None, (
            f'Достигнут лимит активных профилей: {len(active_names)}/{max_active}.\n'
            'Удали один профиль и попробуй снова.'
        )

    used_names = {x.get('email', '') for x in clients if x.get('email')}
    base = normalize_profile_name(tg_user.username or tg_user.full_name or f'user_{tg_user.id}')
    if not base:
        base = f'user_{tg_user.id}'
    name = choose_unique_name(base, used_names)
    uuid = open('/proc/sys/kernel/random/uuid').read().strip()

    clients.append({'email': name, 'flow': 'xtls-rprx-vision', 'id': uuid, 'password': ''})
    settings['clients'] = clients
    save_settings(iid, settings)
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            'INSERT INTO client_traffics (inbound_id,enable,email,up,down,expiry_time,total) VALUES (?,1,?,0,0,0,0)',
            (iid, name)
        )
        conn.commit(); conn.close()
    except Exception:
        pass

    active_names.append(name)
    user_rec['profile_names'] = active_names
    users[uid] = user_rec
    save_users(users)
    restart_xray()
    return name, uuid, None

def get_limited_user_active_profiles(uid: int):
    users = load_users()
    rec = users.get(str(uid))
    if rec is None:
        return None, 'Тебе не выдан доступ на создание профиля. Обратись к владельцу.'
    if not isinstance(rec, dict):
        names = [str(rec)] if rec else []
        limit = 1
    else:
        names = rec.get('profile_names', [])
        if not isinstance(names, list):
            names = []
        limit = int(rec.get('max_active', 1) or 1)

    _, settings = get_inbound()
    if not settings:
        return None, 'Ошибка: inbound не найден'
    clients = settings.get('clients', [])
    client_by_email = {x.get('email', '').lower(): x for x in clients if x.get('email')}
    active = [name for name in names if name.lower() in client_by_email]
    return {'active_names': active, 'limit': limit}, None

def format_limited_profiles_text(info: dict) -> str:
    names = info['active_names']
    limit = info['limit']
    if not names:
        return (
            f'У тебя пока нет активных профилей.\nЛимит: 0/{limit}\n'
            'Используй /myvpn, чтобы создать профиль.'
        )
    lines = [f'Твои активные профили ({len(names)}/{limit}):\n']
    for i, name in enumerate(names, 1):
        lines.append(f'{i}. {name}')
    return '\n'.join(lines)

def limited_user_delete_profile(uid: int, name: str):
    users = load_users()
    rec = users.get(str(uid))
    if rec is None:
        return 'Тебе не выдан доступ на создание профиля. Обратись к владельцу.'
    names = rec.get('profile_names', []) if isinstance(rec, dict) else ([str(rec)] if rec else [])
    if not isinstance(names, list):
        names = []

    iid, settings = get_inbound()
    if not settings:
        return 'Ошибка: inbound не найден'
    clients = settings.get('clients', [])
    client_by_email = {x.get('email', '').lower(): x for x in clients if x.get('email')}
    active_owned = [n for n in names if n.lower() in client_by_email]
    if name.lower() not in {n.lower() for n in active_owned}:
        return 'Ты можешь удалять только свои активные профили (/myvpn list).'

    before = len(clients)
    settings['clients'] = [x for x in clients if x.get('email', '').lower() != name.lower()]
    if len(settings['clients']) == before:
        return f'Профиль {name} не найден'
    save_settings(iid, settings)
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute('DELETE FROM client_traffics WHERE email=?', (name,))
        conn.commit(); conn.close()
    except Exception:
        pass

    rec['profile_names'] = [n for n in names if n.lower() != name.lower()] if isinstance(rec, dict) else []
    users[str(uid)] = rec
    save_users(users)
    restart_xray()
    return None

def limited_user_rename_profile(uid: int, old_name: str, new_name: str):
    users = load_users()
    rec = users.get(str(uid))
    if rec is None:
        return 'Тебе не выдан доступ на создание профиля. Обратись к владельцу.'
    names = rec.get('profile_names', []) if isinstance(rec, dict) else ([str(rec)] if rec else [])
    if not isinstance(names, list):
        names = []

    iid, settings = get_inbound()
    if not settings:
        return 'Ошибка: inbound не найден'
    clients = settings.get('clients', [])
    client_by_email = {x.get('email', '').lower(): x for x in clients if x.get('email')}
    active_owned = [n for n in names if n.lower() in client_by_email]
    if old_name.lower() not in {n.lower() for n in active_owned}:
        return 'Ты можешь переименовывать только свои активные профили (/myvpn list).'

    client = next((x for x in clients if x.get('email', '').lower() == old_name.lower()), None)
    if not client:
        return f'Профиль {old_name} не найден'
    if any(x.get('email', '').lower() == new_name.lower() for x in clients if x is not client):
        return f'Имя {new_name} уже занято'

    old_email = client['email']
    client['email'] = new_name
    save_settings(iid, settings)
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute('UPDATE client_traffics SET email=? WHERE email=?', (new_name, old_email))
        conn.commit(); conn.close()
    except Exception:
        pass

    rec['profile_names'] = [new_name if n.lower() == old_name.lower() else n for n in names] if isinstance(rec, dict) else [new_name]
    users[str(uid)] = rec
    save_users(users)
    restart_xray()
    return None


# ── Отправка ссылок + QR ───────────────────────────────────────

async def send_user_links(update_or_message, context, name: str, uuid: str):
    """Отправляет текст со ссылками и QR-код картинкой."""
    msg    = update_or_message
    l443   = make_link(uuid, name, 443)
    l2087  = make_link(uuid, name, 2087)

    await msg.reply_text(
        f'Данные подключения для {name}:\n\n'
        f'Порт 443 (основная):\n{l443}\n\n'
        f'Порт 2087 (WiFi резерв):\n{l2087}'
    )
    # QR для основной ссылки
    await msg.reply_photo(
        photo=make_qr_bytes(l443),
        caption=f'QR-код для {name} (порт 443)'
    )


# ── Команды ────────────────────────────────────────────────────

async def cmd_start(u: Update, c):
    uid  = u.effective_user.id
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
            '/adduser ID [лимит] — добавить ограниченного пользователя (только владелец)\n'
            '/setuserlimit ID лимит — изменить лимит активных профилей (только владелец)\n'
            '/removeuser ID — удалить ограниченного пользователя (только владелец)\n'
            '/users — список ограниченных пользователей'
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
            '/users — список ограниченных пользователей'
        )
        return
    await u.message.reply_text(
        'VPN Manager (ограниченный доступ)\n\n'
        '/myvpn — создать профиль и получить ссылки\n'
        '/myvpn list — мои активные профили\n'
        '/myvpn rename СТАРОЕ НОВОЕ — переименовать свой профиль\n'
        '/myvpn delete ИМЯ — удалить свой профиль'
    )

async def cmd_add(u: Update, c):
    if not is_admin(u.effective_user.id): return
    if not c.args:
        set_pending(u.effective_user.id, 'admin_add_name')
        await u.message.reply_text(
            'Введи имя нового пользователя одним сообщением.\n'
            'Для отмены отправь: отмена'
        ); return
    name = validate_profile_name(' '.join(c.args).strip())
    if not name:
        await u.message.reply_text('Некорректное имя. Разрешены буквы/цифры/пробел/._-@, до 64 символов.'); return
    uuid, err = admin_add_profile(name)
    if err:
        await u.message.reply_text(err); return
    await send_user_links(u.message, c, name, uuid)

async def cmd_list(u: Update, c):
    if not is_admin(u.effective_user.id): return
    _, settings = get_inbound()
    if not settings:
        await u.message.reply_text('Ошибка: inbound не найден'); return
    cl = settings['clients']
    if not cl:
        await u.message.reply_text('Список пуст'); return

    # Кнопки для каждого пользователя — нажмёшь, получишь ссылки+QR
    keyboard = [
        [InlineKeyboardButton(f'{i}. {x.get("email","—")}', callback_data=f'link:{x.get("email")}')]
        for i, x in enumerate(cl, 1)
    ]
    await u.message.reply_text(
        f'Пользователи VPN ({len(cl)} шт).\nНажми на имя чтобы получить ссылки:',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def cmd_link(u: Update, c):
    if not is_admin(u.effective_user.id): return
    if not c.args:
        await u.message.reply_text('Укажи имя: /link Антон'); return
    name = ' '.join(c.args).strip()
    _, settings = get_inbound()
    if not settings:
        await u.message.reply_text('Ошибка: inbound не найден'); return
    cl = next((x for x in settings['clients'] if x.get('email', '').lower() == name.lower()), None)
    if not cl:
        await u.message.reply_text(f'Пользователь {name} не найден'); return
    await send_user_links(u.message, c, cl['email'], cl['id'])

async def cb_link(u: Update, c):
    """Обработчик нажатия кнопки в /list."""
    query = u.callback_query
    await query.answer()
    if not is_admin(u.effective_user.id): return
    if not query.data.startswith('link:'): return
    name = query.data[5:]
    _, settings = get_inbound()
    if not settings: return
    cl = next((x for x in settings['clients'] if x.get('email', '') == name), None)
    if not cl:
        await query.message.reply_text(f'Пользователь {name} не найден'); return
    await send_user_links(query.message, c, cl['email'], cl['id'])

async def cmd_delete(u: Update, c):
    if not is_admin(u.effective_user.id): return
    if not c.args:
        set_pending(u.effective_user.id, 'admin_delete_name')
        await u.message.reply_text(
            'Введи имя пользователя для удаления.\n'
            'Для отмены отправь: отмена'
        ); return
    name = validate_profile_name(' '.join(c.args).strip())
    if not name:
        await u.message.reply_text('Некорректное имя для удаления.'); return
    err = admin_delete_profile(name)
    if err:
        await u.message.reply_text(err); return
    await u.message.reply_text(f'Пользователь {name} удалён')

async def cmd_rename(u: Update, c):
    if not is_admin(u.effective_user.id): return
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
            await u.message.reply_text('Некорректное старое имя.'); return
        set_pending(u.effective_user.id, 'admin_rename_new', {'old_name': old_name})
        await u.message.reply_text(f'Ок. Теперь введи новое имя для {old_name}.')
        return
    import shlex
    try:
        parts = shlex.split(' '.join(c.args))
    except Exception:
        parts = c.args
    if len(parts) < 2:
        await u.message.reply_text('Нужно два имени: /rename СтароеИмя НовоеИмя'); return
    old_name = validate_profile_name(parts[0].strip())
    new_name = validate_profile_name(' '.join(parts[1:]).strip())
    if not old_name or not new_name:
        await u.message.reply_text('Некорректные имена. Разрешены буквы/цифры/пробел/._-@, до 64 символов.'); return
    err = admin_rename_profile(old_name, new_name)
    if err:
        await u.message.reply_text(err); return
    await u.message.reply_text(f'Переименовано: {old_name} → {new_name}')


async def cmd_admins(u: Update, c):
    if not is_admin(u.effective_user.id): return
    admins = load_admins()
    lines  = ['Администраторы бота:\n']
    for aid in sorted(admins):
        lines.append(f'{aid}' + (' (владелец)' if aid == OWNER_ID else ''))
    await u.message.reply_text('\n'.join(lines))

async def cmd_addadmin(u: Update, c):
    if not is_owner(u.effective_user.id):
        await u.message.reply_text('Только владелец может добавлять администраторов.'); return
    if not c.args or not c.args[0].isdigit():
        await u.message.reply_text('Укажи Telegram ID: /addadmin 123456789'); return
    new_id = int(c.args[0])
    admins = load_admins()
    if new_id in admins:
        await u.message.reply_text(f'{new_id} уже в списке'); return
    admins.add(new_id)
    save_admins(admins)
    await u.message.reply_text(f'Добавлен: {new_id}\nСписок: {", ".join(str(i) for i in sorted(admins))}')

async def cmd_removeadmin(u: Update, c):
    if not is_owner(u.effective_user.id):
        await u.message.reply_text('Только владелец может удалять администраторов.'); return
    if not c.args or not c.args[0].isdigit():
        await u.message.reply_text('Укажи Telegram ID: /removeadmin 123456789'); return
    rm_id = int(c.args[0])
    if rm_id == OWNER_ID:
        await u.message.reply_text('Нельзя удалить владельца.'); return
    admins = load_admins()
    if rm_id not in admins:
        await u.message.reply_text(f'{rm_id} не найден'); return
    admins.discard(rm_id)
    save_admins(admins)
    await u.message.reply_text(f'Удалён: {rm_id}\nСписок: {", ".join(str(i) for i in sorted(admins))}')

async def cmd_adduser(u: Update, c):
    if not is_owner(u.effective_user.id):
        await u.message.reply_text('Только владелец может добавлять ограниченных пользователей.'); return
    if not c.args or not c.args[0].isdigit():
        await u.message.reply_text('Укажи Telegram ID: /adduser 123456789 [лимит]'); return
    new_id = c.args[0]
    limit = 1
    if len(c.args) > 1:
        if not c.args[1].isdigit():
            await u.message.reply_text('Лимит должен быть числом: /adduser 123456789 2'); return
        limit = max(0, int(c.args[1]))
    users = load_users()
    if new_id in users:
        await u.message.reply_text(f'{new_id} уже в списке ограниченных пользователей'); return
    users[new_id] = {
        'profile_names': [],
        'max_active': limit,
    }
    save_users(users)
    await u.message.reply_text(
        f'Добавлен ограниченный пользователь: {new_id}\n'
        f'Лимит активных профилей: {limit}'
    )

async def cmd_setuserlimit(u: Update, c):
    if not is_owner(u.effective_user.id):
        await u.message.reply_text('Только владелец может менять лимит ограниченного пользователя.'); return
    if len(c.args) < 2 or not c.args[0].isdigit() or not c.args[1].isdigit():
        await u.message.reply_text('Использование: /setuserlimit 123456789 3'); return
    uid = c.args[0]
    new_limit = max(0, int(c.args[1]))
    users = load_users()
    rec = users.get(uid)
    if rec is None:
        await u.message.reply_text(f'{uid} не найден'); return
    if not isinstance(rec, dict):
        rec = {
            'profile_names': [str(rec)] if rec else [],
            'max_active': 1,
        }
    rec['max_active'] = new_limit
    users[uid] = rec
    save_users(users)
    active_now = len(rec.get('profile_names', []))
    await u.message.reply_text(
        f'Обновлён лимит для {uid}: активных сейчас {active_now}/{new_limit}'
    )

async def cmd_removeuser(u: Update, c):
    if not is_owner(u.effective_user.id):
        await u.message.reply_text('Только владелец может удалять ограниченных пользователей.'); return
    if not c.args or not c.args[0].isdigit():
        await u.message.reply_text('Укажи Telegram ID: /removeuser 123456789'); return
    rm_id = c.args[0]
    users = load_users()
    if rm_id not in users:
        await u.message.reply_text(f'{rm_id} не найден'); return
    users.pop(rm_id, None)
    save_users(users)
    await u.message.reply_text(f'Ограниченный пользователь удалён: {rm_id}')

async def cmd_users(u: Update, c):
    if not is_admin(u.effective_user.id): return
    users = load_users()
    if not users:
        await u.message.reply_text('Список ограниченных пользователей пуст'); return
    lines = ['Ограниченные пользователи:\n']
    for tg_id, rec in sorted(users.items(), key=lambda x: (not x[0].isdigit(), x[0])):
        if isinstance(rec, dict):
            names = rec.get('profile_names', [])
            if not isinstance(names, list):
                names = []
            used = len(names)
            limit = int(rec.get('max_active', 1) or 1)
            profile = ', '.join(names[:3]) if names else ''
            if len(names) > 3:
                profile += f' ...(+{len(names) - 3})'
        else:
            profile = str(rec or '')
            used = 1 if profile else 0
            limit = 1
        p = profile if profile else 'профиль ещё не создан'
        lines.append(f'{tg_id} → {p} | создано: {used}/{limit}')
    await u.message.reply_text('\n'.join(lines))

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
                await u.message.reply_text(err); return
            await u.message.reply_text(format_limited_profiles_text(info))
            return
        if sub == 'delete':
            if len(c.args) < 2:
                set_pending(uid, 'limited_delete_name')
                await u.message.reply_text(
                    'Введи имя своего профиля для удаления.\n'
                    'Для отмены отправь: отмена'
                ); return
            name = validate_profile_name(' '.join(c.args[1:]).strip())
            if not name:
                await u.message.reply_text('Некорректное имя профиля.'); return
            err = limited_user_delete_profile(uid, name)
            if err:
                await u.message.reply_text(err); return
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
                        await u.message.reply_text('Некорректное старое имя профиля.'); return
                    set_pending(uid, 'limited_rename_new', {'old_name': old_name})
                    await u.message.reply_text(f'Ок. Теперь введи новое имя для профиля {old_name}.')
                    return
                set_pending(uid, 'limited_rename_old')
                await u.message.reply_text(
                    'Введи текущее имя своего профиля.\n'
                    'Для отмены отправь: отмена'
                ); return
            old_name = validate_profile_name(parts[0].strip())
            new_name = validate_profile_name(' '.join(parts[1:]).strip())
            if not old_name or not new_name:
                await u.message.reply_text('Некорректные имена профилей.'); return
            err = limited_user_rename_profile(uid, old_name, new_name)
            if err:
                await u.message.reply_text(err); return
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
        await u.message.reply_text(err); return
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
        await u.message.reply_text(err); return
    await u.message.reply_text(format_limited_profiles_text(info))

async def handle_unknown(u: Update, c):
    uid  = u.effective_user.id
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
            await msg.reply_text('Некорректное имя. Попробуй ещё раз или отправь "отмена".'); return
        uuid, err = admin_add_profile(name)
        if err:
            clear_pending(uid)
            await msg.reply_text(err); return
        clear_pending(uid)
        await send_user_links(msg, c, name, uuid); return

    if action == 'admin_delete_name':
        name = validate_profile_name(text)
        if not name:
            await msg.reply_text('Некорректное имя. Попробуй ещё раз или отправь "отмена".'); return
        err = admin_delete_profile(name)
        clear_pending(uid)
        if err:
            await msg.reply_text(err); return
        await msg.reply_text(f'Пользователь {name} удалён'); return

    if action == 'admin_rename_old':
        old_name = validate_profile_name(text)
        if not old_name:
            await msg.reply_text('Некорректное старое имя. Попробуй ещё раз или отправь "отмена".'); return
        set_pending(uid, 'admin_rename_new', {'old_name': old_name})
        await msg.reply_text(f'Теперь введи новое имя для {old_name}.'); return

    if action == 'admin_rename_new':
        old_name = data.get('old_name', '')
        new_name = validate_profile_name(text)
        if not new_name:
            await msg.reply_text('Некорректное новое имя. Попробуй ещё раз или отправь "отмена".'); return
        err = admin_rename_profile(old_name, new_name)
        clear_pending(uid)
        if err:
            await msg.reply_text(err); return
        await msg.reply_text(f'Переименовано: {old_name} → {new_name}'); return

    if action == 'limited_delete_name':
        name = validate_profile_name(text)
        if not name:
            await msg.reply_text('Некорректное имя профиля. Попробуй ещё раз или отправь "отмена".'); return
        err = limited_user_delete_profile(uid, name)
        clear_pending(uid)
        if err:
            await msg.reply_text(err); return
        await msg.reply_text(f'Твой профиль удалён: {name}'); return

    if action == 'limited_rename_old':
        old_name = validate_profile_name(text)
        if not old_name:
            await msg.reply_text('Некорректное старое имя профиля. Попробуй ещё раз или отправь "отмена".'); return
        set_pending(uid, 'limited_rename_new', {'old_name': old_name})
        await msg.reply_text(f'Теперь введи новое имя для профиля {old_name}.'); return

    if action == 'limited_rename_new':
        old_name = data.get('old_name', '')
        new_name = validate_profile_name(text)
        if not new_name:
            await msg.reply_text('Некорректное новое имя профиля. Попробуй ещё раз или отправь "отмена".'); return
        err = limited_user_rename_profile(uid, old_name, new_name)
        clear_pending(uid)
        if err:
            await msg.reply_text(err); return
        await msg.reply_text(f'Твой профиль переименован: {old_name} → {new_name}'); return


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
        ('start',       cmd_start),
        ('help',        cmd_start),
        ('add',         cmd_add),
        ('list',        cmd_list),
        ('link',        cmd_link),
        ('delete',      cmd_delete),
        ('rename',      cmd_rename),
        ('admins',      cmd_admins),
        ('addadmin',    cmd_addadmin),
        ('removeadmin', cmd_removeadmin),
        ('adduser',     cmd_adduser),
        ('setuserlimit', cmd_setuserlimit),
        ('removeuser',  cmd_removeuser),
        ('users',       cmd_users),
        ('myvpn',       cmd_myvpn),
        ('myprofiles',  cmd_myprofiles),
    ]:
        app.add_handler(CommandHandler(cmd, fn))
    app.add_handler(CallbackQueryHandler(cb_link, pattern='^link:'))
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_unknown))
    log.info('VPN Bot started')
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
