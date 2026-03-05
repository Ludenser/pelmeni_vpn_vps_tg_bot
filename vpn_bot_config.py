import logging
import os
import re


def get_env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    return int(raw) if raw.isdigit() else default


BOT_TOKEN = os.getenv("VPN_BOT_TOKEN", "").strip()
OWNER_ID = get_env_int("VPN_OWNER_ID", 294083624)
ACCESS_DB_PATH = "/etc/vpn-bot.sqlite"
LEGACY_ADMINS_FILE = "/etc/vpn-bot-admins.json"
LEGACY_USERS_FILE = "/etc/vpn-bot-users.json"
DB_PATH = "/etc/x-ui/x-ui.db"
SERVER_IP = os.getenv("VPN_SERVER_IP", "").strip()
PUBLIC_KEY = os.getenv("VPN_PUBLIC_KEY", "").strip()
SHORT_ID = os.getenv("VPN_SHORT_ID", "").strip()
SNI = os.getenv("VPN_SNI", "").strip()
UNAUTHORIZED_ALERT_COOLDOWN_SEC = int(os.getenv("VPN_ALERT_COOLDOWN_SEC", "300"))
PROFILE_NAME_RE = re.compile(r"^[\w .@-]{1,64}$", re.UNICODE)
DEFAULT_LIMITED_MAX_ACTIVE = 3
PAGE_SIZE = 15

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)
