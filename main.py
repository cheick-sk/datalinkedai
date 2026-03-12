"""
DataLinkedAI v5.0 — Plateforme Freelance Data | 100% PRODUCTION READY
Sekouna KABA | Data Engineer Senior | TJM 500-900€

Corrections v3.0 :
  FIX-A  Context manager DB       → plus aucune fuite de connexion PostgreSQL
  FIX-B  asyncio.get_running_loop → compatible Python 3.10/3.11/3.12
  FIX-C  generate_cv_pdf async    → ne bloque plus l'event loop
  FIX-D  sent_at / applied_at     → timestamps correctement enregistrés
  FIX-E  import random top-level  → plus de ré-import à chaque copilot run
  FIX-F  Rollback auto            → intégrité DB garantie en cas d'erreur

Variables OBLIGATOIRES :
  OPENAI_API_KEY ou ANTHROPIC_API_KEY
  API_KEY  (cle secrete, ex: datalinked-2025)

Variables RECOMMANDEES :
  DATABASE_URL   (Supabase postgresql://...)
  RESEND_API_KEY ou GMAIL_USER + GMAIL_APP_PASS
  APIFY_TOKEN    (scraping offres reelles)
  COPILOT_EMAIL  (email reception digest matinal)
"""

import os, json, sqlite3, httpx, logging, smtplib, ssl, asyncio, io, re, base64, random, pathlib
from email.mime.text      import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base      import MIMEBase
from email                import encoders
from datetime             import datetime, timedelta
from contextlib           import asynccontextmanager, contextmanager
from fastapi              import FastAPI, HTTPException, BackgroundTasks, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses    import HTMLResponse, FileResponse
from fastapi.staticfiles  import StaticFiles
from fastapi.security     import APIKeyHeader
from pydantic             import BaseModel
from typing               import Optional, List
from apscheduler.schedulers.asyncio import AsyncIOScheduler
try:
    from slowapi import Limiter, _rate_limit_exceeded_handler
    from slowapi.util import get_remote_address
    from slowapi.errors import RateLimitExceeded
    SLOWAPI_OK = True
except ImportError:
    SLOWAPI_OK = False
from apscheduler.triggers.cron      import CronTrigger

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger(__name__)

# ── Rate limiting en mémoire (sans dépendance externe) ─────────────────────
import time
from collections import defaultdict

_rate_store: dict = defaultdict(list)   # ip → [timestamps]

def _rate_limit(request: Request, max_calls: int, window_seconds: int):
    """Lève HTTP 429 si l'IP dépasse max_calls dans window_seconds."""
    ip  = request.client.host if request.client else "unknown"
    now = time.time()
    _rate_store[ip] = [t for t in _rate_store[ip] if now - t < window_seconds]
    if len(_rate_store[ip]) >= max_calls:
        raise HTTPException(
            status_code=429,
            detail=f"Trop de requêtes — max {max_calls} appels / {window_seconds}s. Réessaie plus tard.",
            headers={"Retry-After": str(window_seconds)},
        )
    _rate_store[ip].append(now)

# ══════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════

OPENAI_API_KEY    = os.getenv("OPENAI_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
GROQ_API_KEY      = os.getenv("GROQ_API_KEY", "")

# Priorite : Groq (gratuit) > OpenAI > Anthropic
if GROQ_API_KEY:
    AI_PROVIDER = "groq";      AI_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
elif OPENAI_API_KEY:
    AI_PROVIDER = "openai";    AI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")
elif ANTHROPIC_API_KEY:
    AI_PROVIDER = "anthropic"; AI_MODEL = "claude-sonnet-4-20250514"
else:
    AI_PROVIDER = "none";      AI_MODEL = ""

DATABASE_URL = os.getenv("DATABASE_URL", "")
DB_PATH      = "/tmp/datalinkedai.db"
USE_POSTGRES = bool(DATABASE_URL)

API_KEY        = os.getenv("API_KEY", "")
SKIP_AUTH      = {"/", "/health", "/docs", "/openapi.json", "/redoc", "/dashboard"}
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

RESEND_API_KEY  = os.getenv("RESEND_API_KEY", "")
GMAIL_USER      = os.getenv("GMAIL_USER", "")
GMAIL_APP_PASS  = os.getenv("GMAIL_APP_PASS", "")
GMAIL_IMAP_HOST = "imap.gmail.com"
GMAIL_IMAP_PORT = 993
GMAIL_CHECK_REPLIES = os.getenv("GMAIL_CHECK_REPLIES", "true").lower() == "true"
EMAIL_FROM_NAME = os.getenv("EMAIL_FROM_NAME", "Sekouna KABA — Data Architect")
EMAIL_FROM_ADDR = os.getenv("EMAIL_FROM_ADDR", GMAIL_USER or "sekouna@datalinkedai.com")
EMAIL_PROVIDER  = "resend" if RESEND_API_KEY else ("gmail" if GMAIL_USER and GMAIL_APP_PASS else "none")

COPILOT_EMAIL        = os.getenv("COPILOT_EMAIL", GMAIL_USER or "kaba.sekouna@gmail.com")
COPILOT_HOUR         = int(os.getenv("COPILOT_HOUR", "8"))
AUTO_APPLY_ENABLED   = os.getenv("AUTO_APPLY", "false").lower() == "true"
AUTO_APPLY_MIN_SCORE = int(os.getenv("AUTO_APPLY_MIN_SCORE", "85"))
FOLLOWUP_DAYS        = int(os.getenv("FOLLOWUP_DAYS", "5"))
GMAIL_PARSE_ENABLED  = os.getenv("GMAIL_PARSE_ENABLED", "true").lower() == "true"  # Parsing réponses
APIFY_TOKEN          = os.getenv("APIFY_TOKEN", "")
PUSHOVER_TOKEN       = os.getenv("PUSHOVER_TOKEN", "")
PUSHOVER_USER        = os.getenv("PUSHOVER_USER", "")
HUNTER_API_KEY       = os.getenv("HUNTER_API_KEY", "")   # hunter.io — 100 recherches/mois gratuit
PUSHOVER_USER        = os.getenv("PUSHOVER_USER", "")
DASHBOARD_URL        = os.getenv("DASHBOARD_URL", "https://datalinkedai.onrender.com/dashboard")

PH        = "%s" if USE_POSTGRES else "?"
scheduler = AsyncIOScheduler(timezone="Europe/Paris")

# ── Rate limiter (slowapi) ──────────────────────────────────────────
if SLOWAPI_OK:
    limiter = Limiter(key_func=get_remote_address, default_limits=["200/hour"])
else:
    limiter = None  # Fallback : rate limiting désactivé si slowapi absent

# ── Compteur mémoire pour endpoints critiques (backup si pas slowapi)
_rate_counters: dict = {}  # {ip: {endpoint: [timestamps]}}

def _check_rate(request: Request, endpoint: str, max_calls: int = 10, window: int = 3600) -> None:
    """Rate limiter maison — bloque si > max_calls dans `window` secondes."""
    import time
    ip  = request.client.host if request.client else "unknown"
    key = f"{ip}:{endpoint}"
    now = time.time()
    ts  = [t for t in _rate_counters.get(key, []) if now - t < window]
    ts.append(now)
    _rate_counters[key] = ts
    if len(ts) > max_calls:
        raise HTTPException(429, f"Trop de requetes — max {max_calls} par heure pour {endpoint}")

# ── Profil Sekouna ─────────────────────────────────────────────────
PROFILE = {
    "name": "KABA Sekouna", "title": "Data Engineer Senior — Lead Tech",
    "email": "kaba.sekouna@gmail.com", "phone": "+33 06 59 02 21 57",
    "location": "Montreuil, Ile-de-France",
    "tjm_min": 500, "tjm_max": 900, "tjm_current": 650, "tjm_target": 760,
    "experiences": [
        {"company": "SACEM",         "period": "Juin 2024 → Present",     "role": "Data Architect/Engineer",
         "stack": "Snowflake, PySpark, Python, Airflow, DBT Core, S3, Kafka, Lambda AWS, SnowPipe, CI/CD GitLab"},
        {"company": "MSO-SOFT",      "period": "Dec 2023 - Juil 2024",    "role": "Data Architect/Engineer",
         "stack": "Docker, DBT-Core, Airflow, Apache Superset, Minio, PostgreSQL, GitLab CI/CD"},
        {"company": "Thales Group",  "period": "Oct 2022 - Dec 2023",     "role": "Data Architect Senior",
         "stack": "GCP BigQuery, PySpark, Talend, Dataiku, Azure ADLS Gen2, AWS RDS"},
        {"company": "Accor",         "period": "Nov 2021 - Oct 2022",     "role": "Consultant Data Engineer/BI",
         "stack": "Snowflake, DBT Core, Terraform, Talend Cloud, AWS Glue, Azure"},
        {"company": "ARCADE (Keyrus)","period": "Fev 2021 - Oct 2021",    "role": "Tech Lead BI/Data Engineer",
         "stack": "Azure DevOps, Snowflake, Azure Storage, Power BI, Talend 7"},
    ],
    "skills": ["Snowflake", "DBT Core", "Apache Airflow", "PySpark", "Python", "Kafka",
               "AWS", "Azure", "GCP", "Terraform", "GitLab CI/CD", "Docker",
               "PostgreSQL", "Power BI", "Tableau", "Dataiku"],
}

# ══════════════════════════════════════════════════════
#  SECURITE
# ══════════════════════════════════════════════════════

async def verify_api_key(request: Request, key: str = Depends(api_key_header)):
    path = request.url.path
    if path in SKIP_AUTH or path.startswith(("/docs", "/redoc", "/openapi", "/static")):
        return True
    if not API_KEY:
        # Pas de clé configurée → accès libre (dev mode) + avertissement
        logger.warning(f"API_KEY non configurée — endpoint {path} accessible sans auth")
        return True
    if key != API_KEY:
        raise HTTPException(401, "Cle API invalide — Header requis: X-API-Key: <ta_cle>",
                            headers={"WWW-Authenticate": "ApiKey"})
    return True

# ══════════════════════════════════════════════════════
#  FIX-A : BASE DE DONNEES — context manager anti-fuite
# ══════════════════════════════════════════════════════

def _raw_conn():
    """Ouvre une connexion brute (PostgreSQL ou SQLite)."""
    if USE_POSTGRES:
        import psycopg2
        return psycopg2.connect(DATABASE_URL)
    return sqlite3.connect(DB_PATH)

@contextmanager
def db_conn():
    """
    Context manager DB :
      - commit automatique si pas d'exception
      - rollback automatique en cas d'erreur
      - close TOUJOURS appelé (plus de fuites de connexion)
    """
    conn = _raw_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass

def db_exec(conn, sql: str, params=()):
    """Execute une requête SQL avec les bons placeholders."""
    sql2 = sql.replace("?", PH)
    cur  = conn.cursor()
    cur.execute(sql2, params)
    return cur

def db_insert(conn, sql: str, params=()):
    """
    Execute un INSERT et retourne le curseur avec .lastrowid renseigné.
    Compatible SQLite (lastrowid natif) et PostgreSQL (RETURNING id).
    """
    if USE_POSTGRES:
        sql2 = sql.replace("?", PH)
        if "RETURNING" not in sql2.upper():
            sql2 = sql2 + " RETURNING id"
        cur = conn.cursor()
        cur.execute(sql2, params)
        row = cur.fetchone()
        cur.lastrowid = row[0] if row else None
        return cur
    return db_exec(conn, sql, params)

def init_db():
    """Crée les tables si elles n'existent pas encore."""
    tables = [
        "CREATE TABLE IF NOT EXISTS offers(id SERIAL PRIMARY KEY,title TEXT,source TEXT,description TEXT,match_score INTEGER,tjm_negotiate TEXT,urgency TEXT,status TEXT DEFAULT 'new',url TEXT,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS cv_adaptations(id SERIAL PRIMARY KEY,offer_title TEXT,score INTEGER,title_adapted TEXT,accroche TEXT,cover_letter TEXT,tjm_suggest TEXT,cv_pdf_b64 TEXT,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS linkedin_posts(id SERIAL PRIMARY KEY,topic TEXT,format TEXT,tone TEXT,content TEXT,status TEXT DEFAULT 'draft',created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS activities(id SERIAL PRIMARY KEY,icon TEXT,text TEXT,module TEXT,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS emails(id SERIAL PRIMARY KEY,to_name TEXT,to_email TEXT,company TEXT,role TEXT,subject TEXT,body_text TEXT,body_html TEXT,status TEXT DEFAULT 'draft',sent_at TIMESTAMP,replied_at TIMESTAMP,provider TEXT,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS email_contacts(id SERIAL PRIMARY KEY,name TEXT,email TEXT,company TEXT,role TEXT,source TEXT,notes TEXT,status TEXT DEFAULT 'new',created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS copilot_runs(id SERIAL PRIMARY KEY,offers_found INTEGER DEFAULT 0,top_offer TEXT,top_score INTEGER,post_topic TEXT,digest_sent INTEGER DEFAULT 0,auto_applied INTEGER DEFAULT 0,run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS auto_applications(id SERIAL PRIMARY KEY,offer_id INTEGER,offer_title TEXT,company_email TEXT,match_score INTEGER,tjm_negotiate TEXT,email_subject TEXT,email_body TEXT,cv_pdf_b64 TEXT,status TEXT DEFAULT 'pending',applied_at TIMESTAMP,followup_at TIMESTAMP,followup_sent INTEGER DEFAULT 0,reply_received INTEGER DEFAULT 0,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS negotiations(id SERIAL PRIMARY KEY,context TEXT,current_offer TEXT,target_tjm TEXT,script TEXT,counter_offer TEXT,arguments TEXT,outcome TEXT,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS pending_approvals(id SERIAL PRIMARY KEY,type TEXT,title TEXT,preview TEXT,payload TEXT,status TEXT DEFAULT 'pending',approved_at TIMESTAMP,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    ]
    if not USE_POSTGRES:
        tables = [
            t.replace("SERIAL PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT")
             .replace("TIMESTAMP DEFAULT CURRENT_TIMESTAMP", "TEXT DEFAULT(datetime('now'))")
             .replace("TIMESTAMP,", "TEXT,")
             .replace("TIMESTAMP)", "TEXT)")
            for t in tables
        ]
    try:
        with db_conn() as conn:
            cur = conn.cursor()
            for t in tables:
                cur.execute(t)
        logger.info("DB tables OK")
    except Exception as e:
        logger.error(f"init_db ERREUR: {e}")
        raise

def log_act(icon: str, text: str, mod: str):
    """Enregistre une activité (non-bloquant, erreurs ignorées)."""
    try:
        with db_conn() as conn:
            db_exec(conn, "INSERT INTO activities(icon,text,module) VALUES(?,?,?)", (icon, text, mod))
    except Exception as e:
        logger.warning(f"log_act: {e}")

# ══════════════════════════════════════════════════════
#  IA
# ══════════════════════════════════════════════════════

async def ask(prompt: str, tokens: int = 1400, _retry: int = 0) -> str:
    """Appel IA avec retry automatique sur 429 (rate limit Groq) et fallback sur timeout."""
    if AI_PROVIDER == "none":
        raise HTTPException(503, "Aucune cle IA configuree. Ajoute GROQ_API_KEY (gratuit sur console.groq.com).")
    try:
        async with httpx.AsyncClient(timeout=45.0) as cl:
            if AI_PROVIDER in ("groq", "openai"):
                base_url = "https://api.groq.com/openai/v1" if AI_PROVIDER == "groq" else "https://api.openai.com/v1"
                api_key  = GROQ_API_KEY if AI_PROVIDER == "groq" else OPENAI_API_KEY
                r = await cl.post(
                    f"{base_url}/chat/completions",
                    headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                    json={"model": AI_MODEL, "max_tokens": tokens, "temperature": 0.7,
                          "messages": [{"role": "user", "content": prompt}]},
                )
                # 429 = rate limit Groq → retry avec backoff
                if r.status_code == 429 and _retry < 3:
                    wait = int(r.headers.get("retry-after", 15 * (2 ** _retry)))
                    logger.warning(f"Groq rate limit 429 — attente {wait}s (retry {_retry+1}/3)")
                    await asyncio.sleep(min(wait, 45))
                    return await ask(prompt, tokens, _retry + 1)
                if r.status_code == 429:
                    raise HTTPException(429, "Quota Groq épuisé. Attends quelques minutes ou configure OPENAI_API_KEY.")
                r.raise_for_status()
                return r.json()["choices"][0]["message"]["content"]
            else:  # anthropic
                r = await cl.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01",
                             "content-type": "application/json"},
                    json={"model": AI_MODEL, "max_tokens": tokens,
                          "messages": [{"role": "user", "content": prompt}]},
                )
                if r.status_code == 429 and _retry < 3:
                    await asyncio.sleep(15 * (2 ** _retry))
                    return await ask(prompt, tokens, _retry + 1)
                r.raise_for_status()
                return "".join(b.get("text", "") for b in r.json().get("content", []))
    except HTTPException:
        raise
    except httpx.TimeoutException:
        raise HTTPException(504, "Timeout IA — le modele met trop de temps a repondre. Relance plus tard.")
    except httpx.HTTPStatusError as e:
        raise HTTPException(502, f"Erreur IA {e.response.status_code}: {e.response.text[:200]}")

async def ask_json(prompt: str, tokens: int = 1600, fallback: dict = None) -> dict:
    """Appel IA attendant du JSON. Ne lève plus d'exception sur parse error — retourne fallback."""
    try:
        raw = await ask(prompt, tokens)
    except HTTPException as e:
        if e.status_code in (429, 503, 504):
            logger.warning(f"ask_json: IA indisponible ({e.status_code}) — fallback")
            return fallback or {}
        raise
    cleaned = re.sub(r"```json\s*|```\s*", "", raw).strip()
    match = re.search(r"\{[\s\S]*\}", cleaned)
    if match:
        cleaned = match.group(0)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        logger.error(f"ask_json parse error: {e}\nRaw (300c): {raw[:300]}")
        if fallback is not None:
            return fallback
        raise HTTPException(500, f"L'IA n'a pas renvoyé du JSON valide. Réessaie. (extrait: {raw[:100]})")

# ══════════════════════════════════════════════════════
#  EMAIL
# ══════════════════════════════════════════════════════

async def send_via_resend(to_email, to_name, subject, body_html, body_text, att_bytes=None, att_name=None):
    payload = {
        "from": f"{EMAIL_FROM_NAME} <{EMAIL_FROM_ADDR}>",
        "to": [f"{to_name} <{to_email}>"],
        "subject": subject, "html": body_html, "text": body_text,
    }
    if att_bytes and att_name:
        payload["attachments"] = [{"filename": att_name, "content": base64.b64encode(att_bytes).decode()}]
    async with httpx.AsyncClient(timeout=30.0) as cl:
        r = await cl.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"},
            json=payload,
        )
        r.raise_for_status()
        return r.json()

def _gmail_sync(to_email, to_name, subject, body_html, body_text, att_bytes=None, att_name=None):
    """Envoi Gmail synchrone — appelé via run_in_executor pour ne pas bloquer l'event loop."""
    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"]    = f"{EMAIL_FROM_NAME} <{GMAIL_USER}>"
    msg["To"]      = f"{to_name} <{to_email}>"
    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(body_text, "plain", "utf-8"))
    alt.attach(MIMEText(body_html, "html", "utf-8"))
    msg.attach(alt)
    if att_bytes and att_name:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(att_bytes)
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f'attachment; filename="{att_name}"')
        msg.attach(part)
    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as s:
        s.login(GMAIL_USER, GMAIL_APP_PASS)
        s.sendmail(GMAIL_USER, to_email, msg.as_string())
    return {"status": "sent", "provider": "gmail"}

async def send_via_gmail(to_email, to_name, subject, body_html, body_text, att_bytes=None, att_name=None):
    # FIX-B : get_running_loop() au lieu de get_event_loop() (deprecated Python 3.10+)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, _gmail_sync, to_email, to_name, subject, body_html, body_text, att_bytes, att_name
    )

async def send_email(to_email, to_name, subject, body_html, body_text, att_bytes=None, att_name=None):
    if EMAIL_PROVIDER == "none":
        raise HTTPException(400, "Aucun provider email. Configure RESEND_API_KEY ou GMAIL_USER+GMAIL_APP_PASS.")
    if EMAIL_PROVIDER == "resend":
        return await send_via_resend(to_email, to_name, subject, body_html, body_text, att_bytes, att_name)
    return await send_via_gmail(to_email, to_name, subject, body_html, body_text, att_bytes, att_name)

async def enrich_company_email(company: str, domain: str = "") -> str:
    """Cherche l email recruteur via Hunter.io (100/mois gratuit).
    Retourne l email trouvé ou chaîne vide si rien."""
    if not HUNTER_API_KEY or not company:
        return ""
    try:
        # Étape 1 : trouver le domaine si pas fourni
        if not domain:
            async with httpx.AsyncClient(timeout=8.0) as cl:
                r = await cl.get(
                    "https://api.hunter.io/v2/domain-search",
                    params={"company": company, "api_key": HUNTER_API_KEY, "limit": 1}
                )
                if r.status_code == 200:
                    data = r.json().get("data", {})
                    domain = data.get("domain", "")
                    # Prendre le premier email trouvé si disponible
                    emails = data.get("emails", [])
                    if emails:
                        best = sorted(emails, key=lambda e: e.get("confidence", 0), reverse=True)[0]
                        email = best.get("value", "")
                        if email and best.get("confidence", 0) >= 50:
                            logger.info(f"Hunter.io: {email} pour {company} (confiance {best.get('confidence')}%)")
                            return email

        # Étape 2 : email finder si on a le domaine mais pas l email
        if domain:
            async with httpx.AsyncClient(timeout=8.0) as cl:
                r = await cl.get(
                    "https://api.hunter.io/v2/email-finder",
                    params={"domain": domain, "api_key": HUNTER_API_KEY}
                )
                if r.status_code == 200:
                    data = r.json().get("data", {})
                    email = data.get("email", "")
                    score = data.get("score", 0)
                    if email and score >= 50:
                        logger.info(f"Hunter.io finder: {email} pour {domain} (score {score})")
                        return email
    except Exception as e:
        logger.warning(f"Hunter.io error pour {company}: {e}")
    return ""


async def notify(title: str, message: str, url: str = "", priority: int = 0) -> bool:
    """Envoie une notification push via Pushover (gratuit, app mobile)."""
    if not PUSHOVER_TOKEN or not PUSHOVER_USER:
        return False
    try:
        async with httpx.AsyncClient(timeout=10.0) as cl:
            payload = {"token": PUSHOVER_TOKEN, "user": PUSHOVER_USER,
                       "title": title[:100], "message": message[:512], "priority": priority}
            if url:
                payload["url"] = url
                payload["url_title"] = "Ouvrir le Dashboard"
            r = await cl.post("https://api.pushover.net/1/messages.json", data=payload)
            return r.status_code == 200
    except Exception as e:
        logger.warning(f"Pushover notification failed: {e}")
        return False

def make_html_email(body_text: str) -> str:
    paras = "".join(
        f"<p style='margin:0 0 16px;line-height:1.7'>{p.strip()}</p>"
        for p in body_text.split("\n\n") if p.strip()
    )
    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head>
<body style="font-family:Helvetica Neue,Arial,sans-serif;max-width:620px;margin:0 auto;padding:32px 20px;color:#1a1a2e">
  <div style="border-left:3px solid #1A56FF;padding-left:20px;margin-bottom:32px">{paras}</div>
  <hr style="border:none;border-top:1px solid #eee;margin:32px 0">
  <div style="font-size:13px;color:#666;line-height:1.6">
    <strong>Sekouna KABA</strong><br>Data Engineer Senior — Data Architect | Freelance<br>
    TJM 500-900€/j · Full Remote · Ile-de-France<br>
    kaba.sekouna@gmail.com | +33 06 59 02 21 57
  </div>
</body></html>"""

# ══════════════════════════════════════════════════════
#  FIX-C : CV PDF — async (ne bloque plus l'event loop)
# ══════════════════════════════════════════════════════

def _generate_cv_pdf_sync(cv_data: dict, offer_title: str = "") -> bytes:
    """CV PDF moderne 2 colonnes — sidebar gauche colorée + contenu principal."""
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles    import ParagraphStyle
        from reportlab.lib.units     import cm, mm
        from reportlab.lib           import colors
        from reportlab.platypus      import (SimpleDocTemplate, Paragraph, Spacer,
                                             HRFlowable, Table, TableStyle, KeepTogether)
        from reportlab.lib.enums     import TA_LEFT, TA_CENTER, TA_JUSTIFY
        from reportlab.pdfgen        import canvas as pdfcanvas
        from reportlab.platypus      import BaseDocTemplate, PageTemplate, Frame
    except ImportError:
        logger.warning("reportlab non installe — PDF desactive")
        return b""

    # ── Palette ────────────────────────────────────────────────────────
    C_BLUE    = colors.HexColor("#1A3A6B")   # sidebar fond
    C_ACCENT  = colors.HexColor("#2563EB")   # titres sections droite, liens
    C_LIGHT   = colors.HexColor("#EFF4FF")   # fond kpis
    C_DARK    = colors.HexColor("#0F172A")   # texte principal
    C_MUTED   = colors.HexColor("#64748B")   # texte secondaire
    C_WHITE   = colors.white
    C_SIDEBAR = colors.HexColor("#F0F4FF")   # compétences tag fond
    C_GOLD    = colors.HexColor("#F59E0B")   # accent TJM
    C_DIVIDER = colors.HexColor("#CBD5E1")

    W, H = A4  # 595.27 x 841.89 pts

    # ── Données ────────────────────────────────────────────────────────
    title_adapted  = cv_data.get("title_adapted", PROFILE["title"])
    accroche       = cv_data.get("accroche", "")
    kpis           = cv_data.get("kpis", ["8 ans d'xp Data", "3 clouds maitrisés", "TJM 760€/j"])
    comp_core      = cv_data.get("competences_core", cv_data.get("competences", PROFILE["skills"]))[:8]
    comp_sec       = cv_data.get("competences_secondaires", PROFILE["skills"])[:8]
    experiences    = cv_data.get("experiences", [])
    formations     = cv_data.get("formations", [{"diplome": "Master Informatique / Data Engineering", "ecole": "", "annee": ""}])
    certifications = cv_data.get("certifications", ["AWS Certified", "GCP Pro Data Engineer", "Snowflake SnowPro"])
    langues        = cv_data.get("langues", [{"langue": "Français", "niveau": "Natif"}, {"langue": "Anglais", "niveau": "Courant"}])
    soft_skills    = cv_data.get("soft_skills", ["Leadership technique", "Communication", "Autonomie", "Agilité"])
    tjm            = cv_data.get("tjm_suggest", "760€/j")

    if not experiences:
        experiences = [{"company": e["company"], "period": e["period"], "role": e["role"],
                        "context": e.get("pitch", ""),
                        "missions": [e.get("stack", ""), e.get("pitch", "")],
                        "stack": e.get("stack", "")} for e in PROFILE["experiences"]]

    # ── Canvas callback : dessine le fond sidebar ────────────────────
    SIDEBAR_W = 175   # largeur sidebar en pts

    def draw_background(canv, doc):
        canv.saveState()
        # Fond sidebar gauche
        canv.setFillColor(C_BLUE)
        canv.rect(0, 0, SIDEBAR_W, H, fill=1, stroke=0)
        # Bande top header
        canv.setFillColor(C_ACCENT)
        canv.rect(0, H - 100, W, 100, fill=1, stroke=0)
        # Ligne de séparation verticale subtile
        canv.setStrokeColor(colors.HexColor("#1E40AF"))
        canv.setLineWidth(0.5)
        canv.line(SIDEBAR_W, 0, SIDEBAR_W, H - 100)
        canv.restoreState()

    # ── Styles ─────────────────────────────────────────────────────────
    def PS(name, **kw):
        return ParagraphStyle(name, **kw)

    # Header (fond bleu foncé)
    sName    = PS("name",    fontSize=24, fontName="Helvetica-Bold", textColor=C_WHITE, leading=28, spaceAfter=2)
    sTitle   = PS("title",   fontSize=11, fontName="Helvetica",      textColor=colors.HexColor("#93C5FD"), leading=16, spaceAfter=0)
    sTjm     = PS("tjm",     fontSize=10, fontName="Helvetica-Bold", textColor=C_GOLD,  leading=14)

    # Sidebar (fond bleu sombre)
    sSidH    = PS("sidH",    fontSize=8,  fontName="Helvetica-Bold", textColor=colors.HexColor("#93C5FD"),
                  spaceBefore=14, spaceAfter=5, leading=12, letterSpacing=1.5)
    sSidTxt  = PS("sidTxt",  fontSize=8,  fontName="Helvetica",      textColor=C_WHITE, leading=12, spaceAfter=3)
    sSidMut  = PS("sidMut",  fontSize=7,  fontName="Helvetica",      textColor=colors.HexColor("#94A3B8"), leading=11, spaceAfter=2)
    sSidTag  = PS("sidTag",  fontSize=7.5,fontName="Helvetica-Bold", textColor=C_WHITE, leading=11, spaceAfter=3)

    # Contenu principal (fond blanc)
    sSecH    = PS("secH",    fontSize=9,  fontName="Helvetica-Bold", textColor=C_ACCENT,
                  spaceBefore=14, spaceAfter=4, leading=12, letterSpacing=1.5)
    sAccr    = PS("accr",    fontSize=9,  fontName="Helvetica",      textColor=C_DARK,  leading=14, spaceAfter=6,
                  alignment=TA_JUSTIFY)
    sExpCo   = PS("expCo",   fontSize=10, fontName="Helvetica-Bold", textColor=C_DARK,  leading=13, spaceAfter=1)
    sExpRole = PS("expRole", fontSize=8.5,fontName="Helvetica",      textColor=C_ACCENT,leading=12, spaceAfter=1)
    sExpDt   = PS("expDt",   fontSize=7.5,fontName="Helvetica",      textColor=C_MUTED, leading=11, spaceAfter=2)
    sExpCtx  = PS("expCtx",  fontSize=8,  fontName="Helvetica",      textColor=C_MUTED, leading=12, spaceAfter=3,
                  alignment=TA_JUSTIFY)
    sMission = PS("miss",    fontSize=8,  fontName="Helvetica",      textColor=C_DARK,  leading=13, leftIndent=10,
                  spaceAfter=2)
    sStack   = PS("stack",   fontSize=7.5,fontName="Helvetica-Bold", textColor=C_ACCENT,leading=11, spaceAfter=4)
    sFormD   = PS("formD",   fontSize=9,  fontName="Helvetica-Bold", textColor=C_DARK,  leading=13, spaceAfter=1)
    sFormS   = PS("formS",   fontSize=8,  fontName="Helvetica",      textColor=C_MUTED, leading=11, spaceAfter=4)

    HR_main  = lambda: HRFlowable(width="100%", thickness=0.5, color=C_DIVIDER, spaceAfter=6, spaceBefore=0)
    HR_sid   = lambda: HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#2D4A7A"), spaceAfter=6, spaceBefore=0)

    # ── Frames layout ──────────────────────────────────────────────────
    MARGIN   = 12
    HDR_H    = 100   # hauteur header
    SIDE_X   = MARGIN
    SIDE_Y   = MARGIN
    SIDE_W   = SIDEBAR_W - 2 * MARGIN
    SIDE_H   = H - HDR_H - 2 * MARGIN

    MAIN_X   = SIDEBAR_W + MARGIN
    MAIN_Y   = MARGIN
    MAIN_W   = W - SIDEBAR_W - 2 * MARGIN
    MAIN_H   = H - HDR_H - 2 * MARGIN

    HDR_X    = MARGIN
    HDR_Y    = H - HDR_H + MARGIN
    HDR_W    = W - 2 * MARGIN
    HDR_HH   = HDR_H - 2 * MARGIN

    frame_header  = Frame(HDR_X,  HDR_Y,  HDR_W,  HDR_HH, id="header",  leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0)
    frame_sidebar = Frame(SIDE_X, SIDE_Y, SIDE_W, SIDE_H, id="sidebar", leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0)
    frame_main    = Frame(MAIN_X, MAIN_Y, MAIN_W, MAIN_H, id="main",    leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0)

    buf = io.BytesIO()

    class MultiFrameDoc(BaseDocTemplate):
        def build(self, flowables):
            self._calc()
            self.canv = pdfcanvas.Canvas(self.filename, pagesize=A4)
            self.handle_documentBegin()
            self.handle_pageBegin()
            # Distribuer les flowables dans les frames manuellement
            for f in flowables:
                self.handle_flowable(f)
            self.handle_documentEnd()

    # Approche plus simple : 3 colonnes via Table
    # Header row (pleine largeur) + 2 colonnes (sidebar | main)

    # ── CONTENU HEADER ─────────────────────────────────────────────────
    contact_line = "kaba.sekouna@gmail.com  |  +33 06 59 02 21 57  |  Montreuil, Ile-de-France"
    github_line  = "github.com/cheick-sk/datalinkedai  |  LinkedIn: /in/sekouna-kaba"

    hdr_left = [
        Paragraph("KABA Sekouna", sName),
        Paragraph(title_adapted, sTitle),
    ]
    hdr_right = [
        Paragraph(f"TJM : {tjm}", sTjm),
        Spacer(1, 4),
        Paragraph(contact_line, PS("ci", fontSize=8, fontName="Helvetica", textColor=colors.HexColor("#BFDBFE"), leading=12)),
        Paragraph(github_line,  PS("gi", fontSize=7.5, fontName="Helvetica", textColor=colors.HexColor("#93C5FD"), leading=11)),
    ]

    # ── CONTENU SIDEBAR ────────────────────────────────────────────────
    def sid_section(title):
        return [Paragraph(title.upper(), sSidH), HR_sid()]

    sidebar_content = []

    # KPIs
    sidebar_content += sid_section("Chiffres clés")
    for kpi in kpis[:3]:
        sidebar_content.append(Paragraph(f"• {kpi}", sSidTag))

    # Compétences core
    sidebar_content += sid_section("Compétences clés")
    for c in comp_core:
        sidebar_content.append(Paragraph(f"▸ {c}", sSidTxt))

    # Compétences secondaires
    sidebar_content += sid_section("Autres technologies")
    sidebar_content.append(Paragraph("  ·  ".join(comp_sec), sSidMut))

    # Soft skills
    sidebar_content += sid_section("Soft Skills")
    for s in soft_skills:
        sidebar_content.append(Paragraph(f"◦ {s}", sSidMut))

    # Langues
    sidebar_content += sid_section("Langues")
    for lang in langues:
        sidebar_content.append(
            Paragraph(f"{lang.get('langue','?')}  —  {lang.get('niveau','?')}", sSidTxt)
        )

    # Certifications
    sidebar_content += sid_section("Certifications")
    for cert in certifications[:4]:
        sidebar_content.append(Paragraph(f"✓ {cert}", sSidMut))

    # ── CONTENU PRINCIPAL ──────────────────────────────────────────────
    def main_section(title):
        return [Paragraph(title.upper(), sSecH), HR_main()]

    main_content = []

    # Profil
    if accroche:
        main_content += main_section("Profil")
        main_content.append(Paragraph(accroche, sAccr))

    # Expériences
    main_content += main_section("Expériences Professionnelles")
    for exp in experiences[:5]:
        block = []
        block.append(Paragraph(exp.get("company", ""), sExpCo))
        block.append(Paragraph(exp.get("role", ""), sExpRole))
        block.append(Paragraph(exp.get("period", ""), sExpDt))
        ctx = exp.get("context", "")
        if ctx:
            block.append(Paragraph(ctx, sExpCtx))
        missions = exp.get("missions", [])
        if isinstance(missions, str):
            missions = [missions]
        for m in missions[:5]:
            if m and m.strip():
                block.append(Paragraph(f"→  {m}", sMission))
        stack = exp.get("stack", "")
        if stack:
            block.append(Paragraph(f"Stack : {stack}", sStack))
        block.append(Spacer(1, 5))
        main_content.append(KeepTogether(block))

    # Formation
    main_content += main_section("Formation")
    for f in (formations if isinstance(formations, list) else []):
        diplome = f.get("diplome", "")
        ecole   = f.get("ecole", "")
        annee   = f.get("annee", "")
        main_content.append(Paragraph(diplome, sFormD))
        if ecole or annee:
            main_content.append(Paragraph(f"{ecole}  {annee}".strip(), sFormS))

    # ── ASSEMBLY via Table 2 colonnes ──────────────────────────────────
    # Convertir les listes en un seul "stack" par cellule

    from reportlab.platypus import KeepInFrame

    sid_frame  = KeepInFrame(SIDE_W,  SIDE_H,  sidebar_content, mode="shrink")
    main_frame = KeepInFrame(MAIN_W,  MAIN_H,  main_content,    mode="shrink")

    # Table 1 ligne x 2 cols
    col_widths = [SIDEBAR_W - 2*MARGIN, W - SIDEBAR_W - 2*MARGIN]
    body_table = Table([[sid_frame, main_frame]], colWidths=col_widths)
    body_table.setStyle(TableStyle([
        ("VALIGN",      (0,0), (-1,-1), "TOP"),
        ("LEFTPADDING", (0,0), (0,0),   0),
        ("RIGHTPADDING",(0,0), (0,0),   8),
        ("LEFTPADDING", (1,0), (1,0),   10),
        ("RIGHTPADDING",(1,0), (1,0),   0),
        ("TOPPADDING",  (0,0), (-1,-1), 0),
        ("BOTTOMPADDING",(0,0),(-1,-1), 0),
    ]))

    # Table header (pleine largeur)
    hdr_left_frame  = KeepInFrame(HDR_W * 0.62, HDR_HH, hdr_left,  mode="shrink")
    hdr_right_frame = KeepInFrame(HDR_W * 0.35, HDR_HH, hdr_right, mode="shrink")
    hdr_table = Table([[hdr_left_frame, hdr_right_frame]],
                      colWidths=[HDR_W * 0.62, HDR_W * 0.38])
    hdr_table.setStyle(TableStyle([
        ("VALIGN",       (0,0), (-1,-1), "MIDDLE"),
        ("LEFTPADDING",  (0,0), (-1,-1), 0),
        ("RIGHTPADDING", (0,0), (-1,-1), 0),
        ("TOPPADDING",   (0,0), (-1,-1), 0),
        ("BOTTOMPADDING",(0,0), (-1,-1), 0),
        ("ALIGN",        (1,0), (1,0),   "RIGHT"),
    ]))

    # Document final
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=MARGIN, rightMargin=MARGIN,
        topMargin=MARGIN,  bottomMargin=MARGIN,
    )

    story = [hdr_table, Spacer(1, 8), body_table]
    doc.build(story, onFirstPage=draw_background, onLaterPages=draw_background)
    return buf.getvalue()

async def generate_cv_pdf(cv_data: dict, offer_title: str = "") -> bytes:
    """Wrapper async — délègue à un thread pour ne pas bloquer l'event loop."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _generate_cv_pdf_sync, cv_data, offer_title)

# ══════════════════════════════════════════════════════
#  SCRAPING APIFY
# ══════════════════════════════════════════════════════

MOCK_OFFERS = [
    {"title": "Lead Data Architect Snowflake + dbt", "company": "FinTech Scale-up",
     "source": "LinkedIn", "url": "https://linkedin.com/jobs/1",
     "description": "Data Architect freelance 12 mois full remote. Snowflake, dbt Core, Airflow, Python, AWS. Architecture medallion, migration legacy."},
    {"title": "Data Architect Azure Lakehouse", "company": "Cabinet IT International",
     "source": "Malt", "url": "https://malt.fr/mission/2",
     "description": "12 mois remote. Azure Data Factory, ADLS Gen2, Databricks, dbt, Power BI. Architecture lakehouse enterprise."},
    {"title": "Senior Data Engineer GCP BigQuery", "company": "Groupe Media",
     "source": "Comet", "url": "https://comet.co/3",
     "description": "6 mois full remote. GCP BigQuery, Dataflow, dbt, Airflow, Python. Pipelines streaming."},
    {"title": "Data Platform Lead Databricks", "company": "Licorne SaaS",
     "source": "WTTJ", "url": "https://wttj.co/4",
     "description": "12 mois remote. Databricks Unity Catalog, dbt, Terraform, GitLab CI/CD, AWS. Data platform from scratch."},
    {"title": "Lead Data Engineer Kafka + Spark", "company": "E-commerce Scale-up",
     "source": "RemoteOK", "url": "https://remoteok.com/5",
     "description": "9 mois full remote. Kafka, PySpark, Airflow, Snowflake. Plateforme data temps reel 50M evenements/jour."},
]

async def scrape_offers_apify() -> list:
    """Scraping via Apify avec fallback mock automatique."""
    logger.info("Scraping via Apify...")
    all_offers = []
    queries = [
        "Data Architect freelance France remote",
        "Data Engineer Senior freelance remote France",
        "Lead Data Engineer Snowflake DBT freelance",
    ]
    async with httpx.AsyncClient(timeout=120.0) as cl:
        for q in queries:
            try:
                run_r = await cl.post(
                    "https://api.apify.com/v2/acts/hMvNSpz3JnHgl5jkh/runs",
                    headers={"Authorization": f"Bearer {APIFY_TOKEN}"},
                    params={"token": APIFY_TOKEN},
                    json={"queries": q, "location": "France", "maxResults": 5,
                          "contractType": "CONTRACT", "datePosted": "past-week"},
                )
                if run_r.status_code not in (200, 201):
                    logger.warning(f"Apify run failed {run_r.status_code}: {run_r.text[:200]}")
                    continue
                run_id = run_r.json().get("data", {}).get("id")
                if not run_id:
                    continue
                for attempt in range(12):
                    await asyncio.sleep(8)
                    st = await cl.get(f"https://api.apify.com/v2/actor-runs/{run_id}",
                                      headers={"Authorization": f"Bearer {APIFY_TOKEN}"})
                    status = st.json().get("data", {}).get("status", "")
                    if status in ("SUCCEEDED", "FAILED", "ABORTED"):
                        break
                    logger.info(f"Apify {run_id}: {status} ({attempt+1}/12)")
                items_r = await cl.get(
                    f"https://api.apify.com/v2/actor-runs/{run_id}/dataset/items",
                    headers={"Authorization": f"Bearer {APIFY_TOKEN}"},
                    params={"limit": 5, "clean": "true"},
                )
                items_r.raise_for_status()
                for item in items_r.json():
                    title   = item.get("title") or item.get("positionName") or ""
                    company = item.get("companyName") or item.get("company") or ""
                    desc    = (item.get("descriptionText") or item.get("description")
                               or item.get("jobDescription") or "")
                    url     = item.get("jobUrl") or item.get("url") or ""
                    if title:
                        all_offers.append({"title": title, "company": company,
                                           "source": "LinkedIn", "url": url,
                                           "description": f"{title} chez {company}. {desc[:600]}"})
            except Exception as e:
                logger.warning(f"Apify '{q[:30]}': {e}")

    if all_offers:
        seen, deduped = set(), []
        for o in all_offers:
            key = o["title"].lower()[:40]
            if key not in seen:
                seen.add(key); deduped.append(o)
        logger.info(f"Apify: {len(deduped)} offres uniques")
        return deduped[:8]

    logger.warning("Apify: aucun résultat — fallback mock")
    return MOCK_OFFERS

async def get_offers_today() -> list:
    return await scrape_offers_apify() if APIFY_TOKEN else MOCK_OFFERS

# ══════════════════════════════════════════════════════
#  COPILOT ENGINE
# ══════════════════════════════════════════════════════

async def run_copilot():
    logger.info("Copilot démarré...")
    now_str    = datetime.now().strftime("%A %d %B")
    raw_offers = await get_offers_today()
    analyzed, auto_applied = [], 0

    for raw in raw_offers:
        try:
            result = await ask_json(f"""Sekouna KABA Data Architect/Engineer Senior freelance, TJM 500-900euro.
Profil: {json.dumps(PROFILE)}
Offre: {raw['description']}
JSON: {{"match_score":<0-100>,"title":"{raw['title']}","tjm_negotiate":"<TJM 500-900euro>",
"urgency":"<Postuler maintenant/Peut attendre>","key_techs":["<t1>","<t2>","<t3>"],
"match_reason":"<raison principale en 1 phrase>"}}
JSON seul.""", 600,
                    fallback={"match_score": 0, "title": raw.get("title",""), "tjm_negotiate": "760euro/j",
                              "urgency": "Peut attendre", "key_techs": [], "match_reason": "IA indisponible"})
            with db_conn() as conn:
                cur = db_insert(
                    conn,
                    "INSERT INTO offers(title,source,description,match_score,tjm_negotiate,urgency,status,url) VALUES(?,?,?,?,?,?,?,?)",
                    (raw["title"], raw["source"], raw["description"], result.get("match_score"),
                     result.get("tjm_negotiate"), result.get("urgency", ""), "analyzed", raw.get("url", "")),
                )
                offer_id = cur.lastrowid
            # Enrichissement email si pas fourni par le scraping
            _raw_email = raw.get("company_email", "")
            if not _raw_email and HUNTER_API_KEY:
                _raw_email = await enrich_company_email(raw.get("company",""), raw.get("domain",""))

            entry = {**result, "offer_id": offer_id,
                     "url": raw.get("url"), "company": raw.get("company"),
                     "source": raw.get("source"), "description": raw.get("description",""),
                     "company_email": _raw_email}
            analyzed.append(entry)
            if result.get("match_score", 0) >= AUTO_APPLY_MIN_SCORE and AUTO_APPLY_ENABLED:
                await run_auto_apply(offer_id, raw["title"], raw.get("company", ""), raw["description"], result, raw.get("company_email",""))
                auto_applied += 1
        except Exception as e:
            logger.error(f"Copilot offer error: {e}")

    analyzed.sort(key=lambda x: x.get("match_score", 0), reverse=True)
    top3  = analyzed[:3]
    topic = random.choice([
        "Snowflake vs Databricks", "dbt Core en production", "Architecture Medallion en pratique",
        "PySpark optimisation avancée", "Kafka streaming temps réel", "TJM Freelance Data — comment négocier",
        "Erreur que j ai faite en mission", "Ce que les ESN ne te disent pas", "Data Mesh vs Data Warehouse",
        "Airflow en 2025 — toujours pertinent ?", "Comment j ai migré un legacy en 3 mois"
    ])
    fmt = random.choice([
        "Hook choc + histoire personnelle + leçon", "Liste numerotee contre-intuitive",
        "Confession professionnelle + twist", "Storytelling mission avec chiffres"
    ])

    try:
        post = await ask(f"""Tu es Sekouna KABA, Data Architect/Engineer Senior freelance (SACEM, Thales, Accor).
{json.dumps(PROFILE)}

Ecris un post LinkedIn en FRANÇAIS qui va exploser en engagement.

SUJET: {topic}
FORMAT: {fmt}

RÈGLES ABSOLUES pour un post viral Data/Tech:
1. LIGNE 1 = hook IRRÉSISTIBLE (question provocatrice, stat choc, ou phrase courte qui force le "voir plus")
   Exemples: "J'ai refusé une mission à 900€/j. Voici pourquoi." / "95% des architectures data ont ce défaut." / "3 ans de Snowflake. Ce que personne ne dit."
2. Ligne 2 = ligne vide (pause dramatique)
3. Corps = histoire CONCRÈTE avec tes missions réelles (SACEM/Thales/Accor/Dataiku) + chiffres précis
4. Structure aérée : 1-2 phrases max par bloc, BEAUCOUP de sauts de ligne
5. Fin = question engageante qui provoque les commentaires OU CTA fort
6. 3-5 emojis bien placés (pas en début de chaque ligne)
7. 5-7 hashtags thématiques en toute fin
8. Longueur : 180-250 mots
9. TON : expert humble, direct, pas de jargon inutile, authentique

UNIQUEMENT le texte du post, rien d'autre.""", 900)
        with db_conn() as conn:
            db_exec(conn,
                "INSERT INTO linkedin_posts(topic,format,tone,content,status) VALUES(?,?,?,?,?)",
                (topic, fmt, "viral", post.strip(), "ready"))
    except Exception as e:
        post = f"[Erreur post: {e}]"

    # ── Créer les approbations en attente ──────────────────────────────
    approvals_created = []

    # 1. Post LinkedIn → approval
    if post and not post.startswith("[Erreur"):
        with db_conn() as conn:
            cur = db_insert(conn,
                "INSERT INTO pending_approvals(type,title,preview,payload,status) VALUES(?,?,?,?,?)",
                ("linkedin_post", f"Post LinkedIn: {topic}", post[:200],
                 json.dumps({"content": post, "topic": topic, "format": fmt}), "pending"))
            approvals_created.append({"type": "linkedin_post", "id": cur.lastrowid, "title": f"Post LinkedIn: {topic}"})

    # 2. Candidatures pour offres score ≥ 60 → approvals (sans envoyer, PDF différé)
    # CAP : max 3 candidatures par run pour éviter timeout Render (30s)
    candidatures_this_run = 0
    for entry in analyzed:
        if candidatures_this_run >= 3:
            break
        if entry.get("match_score", 0) >= 60:
            try:
                apply_result = await run_auto_apply(
                    entry["offer_id"], entry.get("title",""),
                    entry.get("company",""), entry.get("description",""),
                    entry, entry.get("company_email",""),
                    defer_pdf=True   # PDF généré à l'approbation, pas maintenant
                )
                with db_conn() as conn:
                    cur = db_insert(conn,
                        "INSERT INTO pending_approvals(type,title,preview,payload,status) VALUES(?,?,?,?,?)",
                        ("candidature",
                         f"Candidature: {entry.get('title','')} ({entry.get('match_score',0)}%)",
                         apply_result.get("email_body","")[:200],
                         json.dumps({"app_id": apply_result["app_id"],
                                     "subject": apply_result["subject"],
                                     "email_body": apply_result["email_body"],
                                     "offer_title": entry.get("title",""),
                                     "company": entry.get("company",""),
                                     "company_email": entry.get("company_email",""),
                                     "score": entry.get("match_score",0)}),
                         "pending"))
                    approvals_created.append({"type": "candidature", "id": cur.lastrowid,
                                              "title": f"Candidature: {entry.get('title','')} ({entry.get('match_score',0)}%)"})
                candidatures_this_run += 1
            except Exception as e:
                logger.error(f"Approval candidature error: {e}")

    # ── Email récap avec lien vers dashboard validation ─────────────────
    digest_sent = False
    if COPILOT_EMAIL and EMAIL_PROVIDER != "none":
        try:
            offers_html = "".join(
                f"<div style='border-left:4px solid "
                f"{'#00B96B' if o.get('match_score',0)>=85 else '#F59E0B'};"
                f"padding:10px 14px;margin-bottom:8px;background:#F9FAFB'>"
                f"<strong>{o.get('title','')}</strong> "
                f"<span style='color:{'#00B96B' if o.get('match_score',0)>=85 else '#F59E0B'}'>"
                f"{o.get('match_score',0)}%</span><br>"
                f"<small>{o.get('company','')} | TJM: {o.get('tjm_negotiate','?')}</small><br>"
                f"<small>{o.get('match_reason','')}</small></div>"
                for o in top3
            )
            approvals_html = "".join(
                f"<div style='padding:8px 12px;margin-bottom:6px;background:#EEF2FF;border-radius:6px;font-size:13px'>"
                f"{'📝' if a['type']=='linkedin_post' else '📨'} {a['title']}</div>"
                for a in approvals_created
            )
            render_host = os.getenv("RENDER_EXTERNAL_URL", os.getenv("FLY_APP_NAME", ""))
            _default_url = f"https://{render_host}/dashboard" if render_host and not render_host.startswith("http") else (render_host + "/dashboard" if render_host else "https://datalinkedai.onrender.com/dashboard")
            dashboard_url = os.getenv("DASHBOARD_URL", _default_url)
            body_html = f"""<html><body style='font-family:sans-serif;max-width:600px;margin:0 auto;padding:24px'>
<div style='background:linear-gradient(135deg,#1A56FF,#7C3AED);padding:20px;border-radius:12px;margin-bottom:20px'>
<p style='color:rgba(255,255,255,.7);font-size:11px;margin:0'>COPILOT QUOTIDIEN</p>
<h2 style='color:white;margin:4px 0 0'>Brief data du {now_str}</h2>
<p style='color:rgba(255,255,255,.8);font-size:13px;margin:8px 0 0'>{len(approvals_created)} element(s) en attente de validation</p>
</div>
<h3 style='color:#1A56FF'>En attente de ta validation</h3>
{approvals_html}
<div style='text-align:center;margin:24px 0'>
<a href='{dashboard_url}' style='background:linear-gradient(135deg,#1A56FF,#7C3AED);color:white;padding:14px 32px;border-radius:8px;text-decoration:none;font-weight:700;font-size:15px'>
Valider depuis le Dashboard
</a>
</div>
<h3>Top offres analysees</h3>{offers_html}
<hr style='border:none;border-top:1px solid #eee;margin:20px 0'>
<h3>Post LinkedIn (en attente validation)</h3>
<div style='background:#F4F6FB;padding:14px;border-radius:8px;font-size:13px;white-space:pre-wrap'>{post[:300]}...</div>
<p style='color:#aaa;font-size:11px;text-align:center;margin-top:20px'>DataLinkedAI v3.0 | Sekouna KABA</p>
</body></html>"""
            body_text = f"Brief {now_str} - {len(approvals_created)} element(s) a valider\n\n"
            body_text += "\n".join(f"- {a['title']}" for a in approvals_created)
            body_text += f"\n\nValider: {dashboard_url}"
            await send_email(COPILOT_EMAIL, "Sekouna",
                             f"[Validation] {len(approvals_created)} element(s) en attente - {now_str}",
                             body_html, body_text)
            digest_sent = True
        except Exception as e:
            logger.error(f"Digest error: {e}")

    top = top3[0] if top3 else {}  # FIX: défini AVANT notify() qui l'utilise

    # Notification push — résumé copilot
    if approvals_created:
        await notify(
            f"DataLinkedAI — {len(approvals_created)} element(s) a valider",
            f"{len([a for a in approvals_created if a['type']=='candidature'])} candidature(s) + "
            f"{len([a for a in approvals_created if a['type']=='linkedin_post'])} post LinkedIn "
            f"en attente. Top score: {top.get('match_score', 0)}%",
            url=DASHBOARD_URL,
            priority=0
        )
    with db_conn() as conn:
        db_exec(conn,
            "INSERT INTO copilot_runs(offers_found,top_offer,top_score,post_topic,digest_sent,auto_applied) VALUES(?,?,?,?,?,?)",
            (len(analyzed), top.get("title",""), top.get("match_score",0), topic, int(digest_sent), auto_applied))
    log_act("🌅", f"{len(analyzed)} offres analysees, top {top.get('match_score',0)}%", "copilot")
    return {"offers_analyzed": len(analyzed), "top_offers": top3, "post_topic": topic,
            "auto_applied": auto_applied, "digest_sent": digest_sent,
            "scraping": "apify" if APIFY_TOKEN else "mock"}

# ══════════════════════════════════════════════════════
#  AGENT ENGINE
# ══════════════════════════════════════════════════════

async def run_auto_apply(offer_id, title, company, description, analysis, company_email: str = "", defer_pdf: bool = False):
    """Génère CV + email de candidature.
    defer_pdf=True : saute la génération PDF (copilot rapide) — le PDF sera généré à l'approbation.
    defer_pdf=False : génère le PDF immédiatement (candidature manuelle).
    """
    # Si pas d email fourni → tentative Hunter.io en dernier recours
    if not company_email and company and HUNTER_API_KEY:
        company_email = await enrich_company_email(company)
        if company_email:
            log_act("🔍", f"Hunter.io: {company_email} trouvé pour {company}", "agent")

    cv = await ask_json(f"""Tu es un expert RH spécialisé Data Engineering. Génère un CV JSON complet, moderne et percutant pour Sekouna KABA.

PROFIL COMPLET:
{json.dumps(PROFILE)}

OFFRE CIBLÉE: {description[:1000]}

INSTRUCTIONS IMPORTANTES:
- title_adapted: titre exact qui CORRESPOND à l offre (ex: "Lead Data Architect Snowflake & dbt | Freelance")
- accroche: 3-4 phrases IMPACTANTES à la 1ère personne. Commence par un chiffre ou un résultat concret. Mentionne les missions SACEM/Thales/Accor. Donne envie de lire la suite.
- competences_core: 6-8 compétences PRINCIPALES directement liées à l offre (ex: "Snowflake SnowPro Certified", "dbt Core / dbt Cloud", "Apache Airflow")
- competences_secondaires: 6-8 compétences complémentaires (Cloud, DevOps, BI...)
- kpis: 3 chiffres clés percutants (ex: "50M+ événements/jour", "12 pipelines migrés", "3 clouds maîtrisés")
- experiences: TOUTES les expériences avec 4-5 missions détaillées chacune, stack technique, résultats mesurables
- formations: liste des diplômes et certifications
- langues: langues avec niveau
- tjm_suggest: TJM précis entre 500-900euro adapté à l offre
- soft_skills: 4 soft skills pertinents pour ce poste

JSON EXACT:
{{"title_adapted":"<titre adapté>","accroche":"<3-4 phrases percutantes>","kpis":["<kpi1>","<kpi2>","<kpi3>"],"competences_core":["<c1>","<c2>","<c3>","<c4>","<c5>","<c6>"],"competences_secondaires":["<c1>","<c2>","<c3>","<c4>","<c5>"],"experiences":[{{"company":"<nom>","period":"<période>","role":"<titre exact>","context":"<contexte mission en 1 phrase>","missions":["<mission détaillée avec impact>","<mission détaillée>","<mission détaillée>","<mission>"],"stack":"<tech1, tech2, tech3>"}}],"formations":[{{"diplome":"<titre>","ecole":"<école>","annee":"<année>"}}],"certifications":["<cert1>","<cert2>"],"langues":[{{"langue":"<langue>","niveau":"<niveau>"}}],"soft_skills":["<skill1>","<skill2>","<skill3>","<skill4>"],"tjm_suggest":"<TJM>€/j"}}
JSON seul, aucun texte autour.""", 1800)

    # FIX-C : génération PDF async — différée si mode copilot (évite timeout Render 30s)
    if defer_pdf:
        pdf_bytes = b""
        pdf_b64   = ""   # sera généré à l'approbation dans approve_item()
    else:
        pdf_bytes = await generate_cv_pdf(cv, title)
        pdf_b64   = base64.b64encode(pdf_bytes).decode() if pdf_bytes else ""

    email_body = await ask(f"""Tu es Sekouna KABA, Data Engineer Senior freelance.
Email candidature pour: {title} chez {company or "l entreprise"}.
TJM: {analysis.get('tjm_negotiate','760euro/j')}
100-130 mots, direct, 1ere personne, mentionne SACEM ou Thales, mentionne CV en PJ.
UNIQUEMENT le texte.""", 500)

    subject      = f"Candidature freelance — {cv.get('title_adapted', title)}"
    followup_at  = (datetime.now() + timedelta(days=FOLLOWUP_DAYS)).isoformat()
    status       = "pending"   # toujours pending — envoi déclenché par validation humaine
    applied_at   = None

    with db_conn() as conn:
        cur = db_insert(
            conn,
            "INSERT INTO auto_applications(offer_id,offer_title,company_email,match_score,tjm_negotiate,email_subject,email_body,cv_pdf_b64,status,applied_at,followup_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (offer_id, title, company_email,
             analysis.get("match_score"), analysis.get("tjm_negotiate",""),
             subject, email_body, pdf_b64, status, applied_at, followup_at),
        )
        app_id = cur.lastrowid
    log_act("draft", f"Candidature préparée: {title} ({analysis.get('match_score')}%)", "agent")
    # pdf_bytes non retourné (bytes non sérialisables en JSON) — stocké en DB via pdf_b64
    return {"app_id": app_id, "status": status, "subject": subject, "followup_at": followup_at,
            "cv": cv, "cv_pdf_generated": bool(pdf_bytes), "email_body": email_body}


# ══════════════════════════════════════════════════════
#  BACKUP AUTOMATIQUE
# ══════════════════════════════════════════════════════

async def backup_database() -> dict:
    """Backup automatique de la DB SQLite vers fichier horodaté.
    En PostgreSQL, génère un dump SQL texte.
    Stocke les 7 derniers backups, supprime les anciens."""
    import shutil as _shutil
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = "/tmp/datalinkedai_backups"
    os.makedirs(backup_dir, exist_ok=True)
    try:
        if not USE_POSTGRES:
            src_path = DB_PATH
            dst_path = f"{backup_dir}/backup_{ts}.db"
            _shutil.copy2(src_path, dst_path)
            size_kb = os.path.getsize(dst_path) // 1024
            backups = sorted([f for f in os.listdir(backup_dir) if f.startswith("backup_") and f.endswith(".db")])
            for old_b in backups[:-7]:
                os.remove(f"{backup_dir}/{old_b}")
            logger.info(f"Backup SQLite: {dst_path} ({size_kb}Ko)")
            log_act("💾", f"Backup DB: {ts} ({size_kb}Ko)", "system")
            return {"status": "ok", "path": dst_path, "size_kb": size_kb,
                    "type": "sqlite", "timestamp": ts, "backups_kept": min(len(backups), 7)}
        else:
            # PostgreSQL — export JSON des tables critiques (pg_dump non dispo sur Render free)
            json_path = f"{backup_dir}/backup_{ts}.json"
            with db_conn() as conn:
                export = {}
                for table in ["offers","auto_applications","emails","pending_approvals","negotiations","linkedin_posts"]:
                    try:
                        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
                        desc = conn.execute(f"SELECT * FROM {table} LIMIT 0").description
                        cols = [d[0] for d in desc] if desc else []
                        export[table] = [dict(zip(cols, r)) for r in rows]
                    except Exception:
                        export[table] = []
            with open(json_path, 'w') as f:
                json.dump(export, f, default=str, indent=2)
            size_kb = os.path.getsize(json_path) // 1024
            backups = sorted([f for f in os.listdir(backup_dir) if f.startswith("backup_")])
            for old_b in backups[:-7]:
                os.remove(f"{backup_dir}/{old_b}")
            log_act("💾", f"Backup PostgreSQL JSON: {ts} ({size_kb}Ko)", "system")
            return {"status": "ok", "path": json_path, "size_kb": size_kb,
                    "type": "json_export", "timestamp": ts}
    except Exception as e:
        logger.error(f"Backup error: {e}")
        return {"status": "error", "error": str(e)}

async def list_backups() -> list:
    """Liste les backups disponibles."""
    backup_dir = "/tmp/datalinkedai_backups"
    if not os.path.exists(backup_dir):
        return []
    files = []
    for f in sorted(os.listdir(backup_dir), reverse=True):
        if f.startswith("backup_"):
            path = f"{backup_dir}/{f}"
            ts_raw = f.replace("backup_","").split(".")[0]
            files.append({
                "filename": f, "size_kb": os.path.getsize(path) // 1024,
                "timestamp": ts_raw,
                "type": "sqlite" if f.endswith(".db") else ("postgresql" if f.endswith(".sql") else "json"),
            })
    return files[:7]


async def parse_gmail_replies() -> dict:
    """Scanne la boîte Gmail pour détecter les réponses aux candidatures.
    Met à jour reply_received=1 et notifie via Pushover.
    Nécessite GMAIL_USER + GMAIL_APP_PASS."""
    if not GMAIL_USER or not GMAIL_APP_PASS or not GMAIL_PARSE_ENABLED:
        return {"checked": 0, "replies_found": 0, "reason": "Gmail non configuré"}

    import imaplib, email as _email_lib
    from email.header import decode_header as _dh

    replies_found = 0
    checked = 0

    try:
        # Récupérer les sujets des candidatures envoyées pour matching
        with db_conn() as conn:
            pending = db_exec(conn,
                "SELECT id, email_subject, offer_title FROM auto_applications WHERE status='sent' AND reply_received=0",
            ).fetchall()

        if not pending:
            return {"checked": 0, "replies_found": 0, "reason": "Pas de candidatures en attente de réponse"}

        # Connexion IMAP Gmail
        def _imap_scan():
            nonlocal replies_found, checked
            mail = imaplib.IMAP4_SSL("imap.gmail.com", 993)
            mail.login(GMAIL_USER, GMAIL_APP_PASS)
            mail.select("INBOX")

            # Chercher les emails des 30 derniers jours
            from datetime import timedelta
            since = (datetime.now() - timedelta(days=30)).strftime("%d-%b-%Y")
            _, msg_ids = mail.search(None, f'(SINCE "{since}")')

            ids = msg_ids[0].split()[-50:] if msg_ids[0] else []  # max 50 derniers
            checked = len(ids)

            # Indexer les sujets attendus pour matching rapide
            subjects_map = {}  # sujet_normalisé → (app_id, offer_title)
            for app_id, subject, offer_title in pending:
                if subject:
                    # Normaliser: minuscule, sans Re:/Fwd:
                    import re as _re
                    norm = _re.sub(r'^(re|fwd?|tr|aw):\s*', '', subject.lower().strip(), flags=_re.I)
                    subjects_map[norm] = (app_id, offer_title)

            matched_ids = []
            for uid in ids:
                try:
                    _, msg_data = mail.fetch(uid, "(RFC822)")
                    msg = _email_lib.message_from_bytes(msg_data[0][1])
                    raw_subject = msg.get("Subject", "")
                    # Décoder le sujet
                    parts = _dh(raw_subject)
                    subj = "".join(
                        p.decode(enc or "utf-8") if isinstance(p, bytes) else p
                        for p, enc in parts
                    )
                    import re as _re
                    norm_subj = _re.sub(r'^(re|fwd?|tr|aw):\s*', '', subj.lower().strip(), flags=_re.I)

                    # Vérifier si ça matche une candidature
                    for expected_subj, (app_id, offer_title) in subjects_map.items():
                        if expected_subj in norm_subj or norm_subj in expected_subj:
                            matched_ids.append((app_id, offer_title, subj))
                            break
                except Exception:
                    pass

            mail.logout()
            return matched_ids

        loop = asyncio.get_event_loop()
        matched = await loop.run_in_executor(None, _imap_scan)

        for app_id, offer_title, reply_subject in matched:
            with db_conn() as conn:
                db_exec(conn,
                    "UPDATE auto_applications SET reply_received=1, status='replied' WHERE id=?",
                    (app_id,))
            log_act("💬", f"Réponse reçue: {offer_title}", "agent")
            await notify(
                "💬 Réponse reçue !",
                f"{offer_title} — Objet: {reply_subject[:60]}",
                url=DASHBOARD_URL, priority=1  # priority=1 = son + vibration
            )
            replies_found += 1

        return {"checked": checked, "replies_found": replies_found,
                "updated": [r[1] for r in matched]}

    except imaplib.IMAP4.error as e:
        logger.error(f"Gmail IMAP error: {e}")
        return {"checked": 0, "replies_found": 0, "error": str(e)}
    except Exception as e:
        logger.error(f"Gmail parse error: {e}")
        return {"checked": 0, "replies_found": 0, "error": str(e)}

async def run_followups():
    """Envoie les emails de relance pour les candidatures sans réponse."""
    now = datetime.now().isoformat()
    with db_conn() as conn:
        due = db_exec(
            conn,
            "SELECT id,offer_title,company_email,email_body,tjm_negotiate FROM auto_applications WHERE status='sent' AND followup_sent=0 AND followup_at<=? AND reply_received=0",
            (now,),
        ).fetchall()
    sent_count = 0
    for row in due:
        app_id, title, company_email, orig_body, tjm = row
        try:
            # Générer le texte de relance via IA
            followup_text = await ask(
                f"Tu es Sekouna KABA, Data Architect freelance. "
                f"Écris un email de relance court (50-70 mots) pour la candidature: {title}. "
                f"TJM proposé: {tjm or '760€/j'}. "
                f"Ton naturel, direct, rappelle la candidature initiale, propose un échange rapide. "
                f"UNIQUEMENT le texte de l'email.", 400)

            subject  = f"Re: Candidature freelance — {title}"
            body_html = make_html_email(followup_text)

            # Envoyer seulement si on a l'adresse email
            if company_email and EMAIL_PROVIDER != "none":
                try:
                    await send_email(company_email, "", subject, body_html, followup_text)
                    with db_conn() as conn:
                        db_exec(conn,
                            "UPDATE auto_applications SET followup_sent=1 WHERE id=?",
                            (app_id,))
                    log_act("relance", f"Relance envoyée: {title} → {company_email}", "agent")
                    sent_count += 1
                except Exception as e:
                    logger.error(f"Followup send {app_id}: {e}")
            else:
                # Pas d'email — on marque quand même pour ne pas re-tenter
                with db_conn() as conn:
                    db_exec(conn,
                        "UPDATE auto_applications SET followup_sent=1 WHERE id=?",
                        (app_id,))
                log_act("relance", f"Relance générée (pas d'email configuré): {title}", "agent")
                sent_count += 1
        except Exception as e:
            logger.error(f"Followup {app_id}: {e}")
    return {"followups_checked": len(due), "followups_sent": sent_count}


# ══════════════════════════════════════════════════════
#  NEGOCIATEUR ENGINE
# ══════════════════════════════════════════════════════

async def run_negotiation(context, current_offer, target):
    return await ask_json(f"""Sekouna KABA, Data Architect/Engineer Senior freelance. TJM 500-900euro.
{json.dumps(PROFILE)}
Negociation — Offre: {current_offer} | Cible: {target} | Contexte: {context}
JSON:{{"counter_offer":"<TJM exact 500-900euro>","opening_line":"<1ere phrase exacte>",
"arguments":["<arg1>","<arg2>","<arg3>"],"full_script":"<script complet 150-200 mots>",
"email_version":"<version email 120 mots, objet inclus>",
"if_they_refuse":"<que repondre si refus>","anchoring_tip":"<technique ancrage>",
"walk_away_point":"<TJM minimum>","confidence_score":<0-100>}}
JSON seul.""", 1200)

# ══════════════════════════════════════════════════════
#  APP
# ══════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("=" * 60)
    logger.info("DataLinkedAI v5.0 — Démarrage")
    logger.info(f"  IA        : {AI_PROVIDER} / {AI_MODEL or 'non configuré'}")
    logger.info(f"  DB        : {'PostgreSQL Supabase' if USE_POSTGRES else 'SQLite /tmp (non persistant)'}")
    logger.info(f"  Email     : {EMAIL_PROVIDER}")
    logger.info(f"  Scraping  : {'Apify (réel)' if APIFY_TOKEN else 'Mock'}")
    logger.info(f"  Sécurité  : {'API Key ON' if API_KEY else '⚠️  DESACTIVEE — configure API_KEY'}")
    logger.info(f"  Auto-apply: {'ACTIF (>=' + str(AUTO_APPLY_MIN_SCORE) + '%)' if AUTO_APPLY_ENABLED else 'Manuel'}")
    if AI_PROVIDER == "none":
        logger.warning("⚠️  Aucune clé IA — les endpoints IA renverront une erreur 500")
    logger.info("=" * 60)
    init_db()
    scheduler.add_job(run_copilot,   CronTrigger(day_of_week="mon-fri", hour=COPILOT_HOUR, minute=0),
                      id="copilot",  replace_existing=True)
    scheduler.add_job(backup_database, CronTrigger(hour=2, minute=0),
                      id="daily_backup", misfire_grace_time=600)
    scheduler.add_job(parse_gmail_replies, "interval", hours=2, id="gmail_parse",
                      misfire_grace_time=300)
    scheduler.add_job(run_followups, CronTrigger(hour=10, minute=0),
                      id="followups", replace_existing=True)
    scheduler.start()
    yield
    # Shutdown
    scheduler.shutdown()
    logger.info("DataLinkedAI arrêté proprement.")

app = FastAPI(
    title="DataLinkedAI API v5.0",
    description="Plateforme freelance data automatisée | Sekouna KABA | TJM 500-900€",
    version="5.0.0",
    lifespan=lifespan,
    dependencies=[Depends(verify_api_key)],
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

_static = pathlib.Path(__file__).parent / "static"
if _static.exists():
    app.mount("/static", StaticFiles(directory=str(_static)), name="static")

@app.get("/dashboard", include_in_schema=False)
async def dashboard_ui():
    f = _static / "dashboard.html"
    if f.exists():
        return FileResponse(str(f), media_type="text/html")
    return HTMLResponse("<h2>dashboard.html introuvable dans /static/</h2>", status_code=404)

# ── Health ───────────────────────────────────────────
@app.get("/health", tags=["Système"])
async def health():
    return {
        "status": "ok", "version": "5.0.0",
        "ai": AI_PROVIDER, "model": AI_MODEL,
        "db": "postgresql" if USE_POSTGRES else "sqlite",
        "email": EMAIL_PROVIDER,
        "scraping": "apify" if APIFY_TOKEN else "mock",
        "security": "api_key" if API_KEY else "none",
        "auto_apply": AUTO_APPLY_ENABLED,
        "jobs": [{"id": j.id, "next": str(j.next_run_time)} for j in scheduler.get_jobs()],
    }

@app.get("/api/profile", tags=["Système"])
async def profile(_=Depends(verify_api_key)):
    return PROFILE

@app.get("/api/dashboard", tags=["Système"])
async def dashboard(_=Depends(verify_api_key)):
    with db_conn() as conn:
        def cnt(q, p=()):
            return db_exec(conn, q, p).fetchone()[0]
        s = {
            "offers":       cnt("SELECT COUNT(*) FROM offers"),
            "cv":           cnt("SELECT COUNT(*) FROM cv_adaptations"),
            "posts":        cnt("SELECT COUNT(*) FROM linkedin_posts"),
            "emails_sent":  cnt("SELECT COUNT(*) FROM emails WHERE status='sent'"),
            "applications": cnt("SELECT COUNT(*) FROM auto_applications"),
            "replies":      cnt("SELECT COUNT(*) FROM auto_applications WHERE reply_received=1"),
            "tjm_target":   PROFILE["tjm_target"],
        }
        apps_sent = cnt("SELECT COUNT(*) FROM auto_applications WHERE status='sent'")
        s["reply_rate"] = f"{round(s['replies'] / apps_sent * 100)}%" if apps_sent > 0 else "0%"
        acts = db_exec(
            conn,
            "SELECT icon,text,module,created_at FROM activities ORDER BY created_at DESC LIMIT 10",
        ).fetchall()
    return {
        "stats": s,
        "activities": [{"icon": a[0], "text": a[1], "module": a[2], "ts": str(a[3])} for a in acts],
        "system": {"db": "postgresql" if USE_POSTGRES else "sqlite",
                   "ai": AI_PROVIDER, "email": EMAIL_PROVIDER},
    }

# ── Offres ───────────────────────────────────────────
class OfferReq(BaseModel):
    text: str
    
    class Config:
        json_schema_extra = {"example": {"text": "Description de l'offre..."}}
    
    def __init__(self, **data):
        if 'text' in data and len(data['text']) > 8000:
            data['text'] = data['text'][:8000]
        super().__init__(**data)

@app.post("/api/offers/analyze", tags=["Offres Freelance"])
async def analyze_offer(request: Request, req: OfferReq, _=Depends(verify_api_key)):
    _check_rate(request, "analyze_offer", 20, 3600)
    r = await ask_json(f"""Sekouna KABA Data Engineer Senior/Architect, TJM 500-900euro.
Profil:{json.dumps(PROFILE)}\nOffre:---{req.text}---
JSON:{{"match_score":<0-100>,"title":"<titre>","tjm_range":"<fourchette>","tjm_negotiate":"<TJM 500-900euro>",
"remote":<bool>,"duration":"<duree>","key_techs":["<t1>","<t2>","<t3>","<t4>","<t5>"],
"match_reasons":["<r1>","<r2>","<r3>"],"gaps":["<g1>"],
"urgency":"<Postuler maintenant/Peut attendre/Moins prioritaire>",
"urgency_reason":"<pourquoi>","negotiation_tip":"<conseil TJM 2 phrases>"}}
JSON seul.""", 1200)
    with db_conn() as conn:
        db_exec(conn,
            "INSERT INTO offers(title,source,description,match_score,tjm_negotiate,urgency) VALUES(?,?,?,?,?,?)",
            (r.get("title","Offre"), "manual", req.text[:1000],
             r.get("match_score"), r.get("tjm_negotiate"), r.get("urgency","")))
    log_act("📋", f"Analysee: {r.get('title','')} ({r.get('match_score')}%)", "offers")
    return r

@app.get("/api/offers", tags=["Offres Freelance"])
async def list_offers(limit: int = 50, offset: int = 0, _=Depends(verify_api_key)):
    with db_conn() as conn:
        total = db_exec(conn, "SELECT COUNT(*) FROM offers").fetchone()[0]
        rows  = db_exec(
            conn,
            f"SELECT id,title,source,match_score,tjm_negotiate,urgency,status,url,created_at FROM offers ORDER BY created_at DESC LIMIT {min(limit,200)} OFFSET {offset}",
        ).fetchall()
    return {"total": total, "offset": offset, "limit": limit,
            "items": [{"id": r[0], "title": r[1], "source": r[2], "match_score": r[3],
             "tjm_negotiate": r[4], "urgency": r[5], "status": r[6],
             "url": r[7], "created_at": str(r[8])} for r in rows]}

# ── CV ───────────────────────────────────────────────
class CVReq(BaseModel):
    offer_text: str
    generate_pdf: bool = True

@app.post("/api/cv/adapt", tags=["CV Adaptatif"])
async def adapt_cv(req: CVReq, request: Request, _=Depends(verify_api_key)):
    _check_rate(request, "adapt_cv", 20, 3600)
    _rate_limit(request, max_calls=10, window_seconds=3600)  # 10 fois/heure
    r = await ask_json(f"""Sekouna KABA Data Engineer Senior/Architect, TJM 500-900euro.
CV:{json.dumps(PROFILE)}\nOffre:---{req.offer_text}---
JSON:{{"score":<0-100>,"title_adapted":"<titre>","accroche":"<3 phrases 1ere personne>",
"competences":["<s1>","<s2>","<s3>","<s4>","<s5>","<s6>"],
"experiences":[{{"company":"<n>","period":"<p>","role":"<r>","pertinence":<0-100>,"pitch":"<1 phrase>","missions":["<m1>","<m2>","<m3>"]}}],
"cover_letter":"<3 paragraphes directs>","strengths":["<f1>","<f2>","<f3>"],
"advice":"<conseil 2 phrases>","tjm_suggest":"<TJM 500-900euro et pourquoi>"}}
Max 5 experiences triees pertinence. JSON seul.""", 1800)
    pdf_bytes = await generate_cv_pdf(r, r.get("title_adapted","")) if req.generate_pdf else b""
    pdf_b64   = base64.b64encode(pdf_bytes).decode() if pdf_bytes else ""
    with db_conn() as conn:
        db_exec(conn,
            "INSERT INTO cv_adaptations(offer_title,score,title_adapted,accroche,cover_letter,tjm_suggest,cv_pdf_b64) VALUES(?,?,?,?,?,?,?)",
            (r.get("title_adapted",""), r.get("score",0), r.get("title_adapted",""),
             r.get("accroche",""), r.get("cover_letter",""), r.get("tjm_suggest",""), pdf_b64))
    log_act("📄", f"Adapte: {r.get('title_adapted','')} ({r.get('score')}%)", "cv")
    return {**r, "cv_pdf_generated": bool(pdf_bytes), "pdf_size_kb": round(len(pdf_bytes)/1024,1) if pdf_bytes else 0}

@app.get("/api/cv/history", tags=["CV Adaptatif"])
async def cv_history(_=Depends(verify_api_key)):
    with db_conn() as conn:
        rows = db_exec(
            conn,
            "SELECT id,offer_title,score,title_adapted,tjm_suggest,created_at FROM cv_adaptations ORDER BY created_at DESC LIMIT 20",
        ).fetchall()
    return [{"id": r[0], "offer_title": r[1], "score": r[2],
             "title_adapted": r[3], "tjm_suggest": r[4], "created_at": str(r[5])} for r in rows]

# ── LinkedIn ─────────────────────────────────────────
class PostReq(BaseModel):
    topic: str
    format: str
    tone: str = "expert"
    angle: str = ""

@app.post("/api/linkedin/generate", tags=["LinkedIn Studio"])
async def gen_post(req: PostReq, request: Request, _=Depends(verify_api_key)):
    _check_rate(request, "gen_post", 20, 3600)
    _rate_limit(request, max_calls=20, window_seconds=3600)  # 20 fois/heure
    content = await ask(f"""Tu es Sekouna KABA, Data Architect/Engineer Senior freelance.
Missions récentes : SACEM (Data Architect), Thales Group (Senior Data Engineer), Accor (Data Platform).
{json.dumps(PROFILE)}

Ecris un post LinkedIn VIRAL en français.
Sujet: {req.topic}
Format demandé: {req.format}
Ton: {req.tone}
{f'Angle spécifique: {req.angle}' if req.angle else ''}

STRUCTURE VIRALE OBLIGATOIRE:
— Ligne 1 : hook IRRÉSISTIBLE (max 12 mots) qui force le clic "voir plus"
  Types de hooks qui marchent: chiffre choc, question provocatrice, confession, prise de position tranchée
  Ex: "J'ai failli rater une mission à 850€/j à cause de ça."
— Ligne 2 : vide
— Corps : histoire concrète avec tes missions réelles + métriques précises (volumétrie, durée, impact)
  Sauts de ligne fréquents — 1-2 phrases par paragraphe max
— Fin : question qui APPELLE les commentaires ("Et vous, vous faites comment ?") OU CTA fort
— 4-6 emojis positionnés stratégiquement (pas en début de chaque phrase)
— 6-8 hashtags pertinents en toute fin sur une ligne
— 200-260 mots au total

TON: expert accessible, direct, authentique — pas corporate, pas lisse
UNIQUEMENT le texte du post, rien d'autre.""", 1000)
    with db_conn() as conn:
        db_exec(conn,
            "INSERT INTO linkedin_posts(topic,format,tone,content) VALUES(?,?,?,?)",
            (req.topic, req.format, req.tone, content.strip()))
    log_act("💼", f"Post: {req.topic} ({req.format})", "linkedin")
    return {"content": content.strip()}

@app.get("/api/linkedin/posts", tags=["LinkedIn Studio"])
async def list_posts(_=Depends(verify_api_key)):
    with db_conn() as conn:
        rows = db_exec(
            conn,
            "SELECT id,topic,format,tone,content,status,created_at FROM linkedin_posts ORDER BY created_at DESC LIMIT 20",
        ).fetchall()
    return [{"id": r[0], "topic": r[1], "format": r[2], "tone": r[3],
             "content": r[4], "status": r[5], "created_at": str(r[6])} for r in rows]

# ── Prospection ──────────────────────────────────────
class ProspectReq(BaseModel):
    target_context: str

@app.post("/api/prospecting/generate", tags=["Prospection"])
async def gen_prospect(request: Request, req: ProspectReq, _=Depends(verify_api_key)):
    _check_rate(request, "gen_prospect", 15, 3600)
    r = await ask_json(f"""Sekouna KABA Data Engineer Senior/Architect freelance.{json.dumps(PROFILE)}
Contexte:{req.target_context}
JSON:{{"targets":[{{"role":"<role>","why":"<raison>","msg":"<message connexion 280 chars>"}}],
"connection_msg":"<message LinkedIn 300 chars>","inmail_subject":"<objet>",
"inmail":"<InMail 180 mots>","weekly":["<action 1>","<action 2>","<action 3>","<action 4>"]}}
JSON seul.""", 1400)
    log_act("🎯", "Strategie prospection generee", "prospecting")
    return r

# ── TJM ──────────────────────────────────────────────
class TJMReq(BaseModel):
    context: str = ""

@app.post("/api/tjm/analyze", tags=["TJM Optimizer"])
async def analyze_tjm(request: Request, req: TJMReq, _=Depends(verify_api_key)):
    _check_rate(request, "analyze_tjm", 15, 3600)
    r = await ask_json(f"""Sekouna KABA Data Architect/Engineer Senior freelance. TJM 500-900euro.{json.dumps(PROFILE)}
Contexte:{req.context or "Mission generale Data Engineer Senior/Architect full remote"}
JSON:{{"tjm_market":<median>,"tjm_top":<top>,"tjm_recommend":<500-900>,"summary":"<2 phrases>",
"value_drivers":["<v1>","<v2>","<v3>"],"negotiation_script":"<script 150 mots>",
"skills_premium":["<s1>","<s2>","<s3>"],
"benchmark":[{{"role":"Data Engineer","tjm":640}},{{"role":"Data Architect","tjm":730}},{{"role":"Lead Data Engineer","tjm":770}}]}}
JSON seul.""", 1400)
    log_act("💰", f"TJM: recommande {r.get('tjm_recommend','?')}euro/j", "tjm")
    return r

# ── Proposition ──────────────────────────────────────
class ProposalReq(BaseModel):
    brief: str

@app.post("/api/proposal/generate", tags=["Proposition Client"])
async def gen_proposal(req: ProposalReq, request: Request, _=Depends(verify_api_key)):
    _check_rate(request, "gen_proposal", 15, 3600)
    _rate_limit(request, max_calls=10, window_seconds=3600)  # 10 fois/heure
    r = await ask_json(f"""Sekouna KABA Data Architect/Engineer freelance. TJM 500-900euro.{json.dumps(PROFILE)}
Brief:{req.brief}
JSON:{{"title":"<titre>","exec_summary":"<3 phrases>","problem":"<2 phrases>","solution":"<3 para>",
"deliverables":["<l1>","<l2>","<l3>","<l4>"],"methodology":["<e1>","<e2>","<e3>","<e4>"],
"why_me":["<r1>","<r2>","<r3>"],"timeline":"<duree>","pricing":"<TJM et total>","next_steps":"<CTA>"}}
JSON seul.""", 1600)
    log_act("📑", "Proposition generee", "proposal")
    return r

# ── Audit ────────────────────────────────────────────
@app.post("/api/audit/profile", tags=["Audit Profil"])
async def audit_profile(request: Request, _=Depends(verify_api_key)):
    _check_rate(request, "audit_profile", 10, 3600)
    r = await ask_json(f"""Audit profil LinkedIn Sekouna KABA freelance.{json.dumps(PROFILE)}
JSON:{{"score":<0-100>,"seo":<0-100>,
"sections":[{{"name":"<section>","score":<0-100>,"status":"<OK/A ameliorer/Critique>","advice":"<conseil>"}}],
"headline":"<titre LinkedIn optimise>","about":"<A propos 200 mots SEO>",
"keywords_missing":["<kw1>","<kw2>","<kw3>"],"quick_wins":["<w1>","<w2>","<w3>"]}}
JSON seul.""", 1600)
    log_act("🔍", f"Audit score {r.get('score',0)}/100", "audit")
    return r

# ══════════════════════════════════════════════════════
#  EMAIL PROSPECTION
# ══════════════════════════════════════════════════════

class EmailComposeReq(BaseModel):
    to_name: str; to_email: str; company: str; role: str
    context: str = ""; tone: str = "professionnel"

class EmailSendReq(BaseModel):
    email_id: int; confirm: bool = False

class EmailBatchReq(BaseModel):
    contacts: List[dict]; context: str = ""; tone: str = "professionnel"; auto_send: bool = False

class ContactReq(BaseModel):
    name: str; email: str; company: str; role: str; source: str = "manuel"; notes: str = ""

@app.post("/api/email/compose", tags=["Email Prospection"])
async def compose_email(request: Request, req: EmailComposeReq, _=Depends(verify_api_key)):
    _check_rate(request, "compose_email", 30, 3600)
    result = await ask_json(f"""Tu es Sekouna KABA, Data Engineer Senior/Architect freelance.{json.dumps(PROFILE)}
Email prospection B2B pour: {req.to_name}, {req.role} chez {req.company}.
Contexte: {req.context or "entreprise tech/data"}. Ton: {req.tone}.
120-160 mots MAX. Objet accrocheur. 1er para: observation specifique {req.company}.
2e para: valeur concrete (1-2 missions reelles). 3e para: CTA simple. Pas de formules creuses.
JSON:{{"subject":"<objet max 60 chars>","body":"<corps paragraphes separes par deux retours>",
"personalization_hook":"<ce qui rend email specifique>","best_send_time":"<jour/heure>","follow_up_day":<jours>}}
JSON seul.""", 1000)
    body_html = make_html_email(result.get("body",""))
    subject   = result.get("subject", f"Collaboration Data — {req.company}")
    with db_conn() as conn:
        cur = db_insert(conn,
            "INSERT INTO emails(to_name,to_email,company,role,subject,body_text,body_html,status,provider) VALUES(?,?,?,?,?,?,?,?,?)",
            (req.to_name, req.to_email, req.company, req.role, subject,
             result.get("body",""), body_html, "draft", EMAIL_PROVIDER))
        email_id = cur.lastrowid
    log_act("📧", f"Compose: {req.to_name} ({req.company})", "email")
    return {"email_id": email_id, "to": f"{req.to_name} <{req.to_email}>",
            "subject": subject, "body": result.get("body",""),
            "hook": result.get("personalization_hook"),
            "best_send": result.get("best_send_time"),
            "follow_up_in": result.get("follow_up_day", 5),
            "status": "draft — POST /api/email/send pour envoyer"}

@app.post("/api/email/send", tags=["Email Prospection"])
async def send_email_ep(req: EmailSendReq, _=Depends(verify_api_key)):
    if not req.confirm:
        raise HTTPException(400, "confirm=true requis")
    with db_conn() as conn:
        row = db_exec(
            conn,
            "SELECT to_name,to_email,subject,body_text,body_html,status FROM emails WHERE id=?",
            (req.email_id,),
        ).fetchone()
    if not row:
        raise HTTPException(404, "Email introuvable")
    if row[5] == "sent":
        raise HTTPException(400, "Deja envoye")
    to_name, to_email, subject, body_text, body_html, _ = row

    # ── Étape 1 : envoi SMTP / Resend (isolé pour ne pas polluer le statut DB) ──
    try:
        await send_email(to_email, to_name, subject, body_html, body_text)
    except Exception as e:
        # L'envoi a échoué → on marque 'failed' et on remonte l'erreur
        try:
            with db_conn() as conn:
                db_exec(conn, "UPDATE emails SET status='failed' WHERE id=?", (req.email_id,))
        except Exception:
            pass  # la DB n'est pas critique ici, l'erreur d'envoi est prioritaire
        raise HTTPException(500, f"Echec envoi: {e}")

    # ── Étape 2 : l'email est parti — on met à jour le statut (erreur DB non fatale) ──
    try:
        with db_conn() as conn:
            db_exec(conn,
                "UPDATE emails SET status='sent', sent_at=?, provider=? WHERE id=?",
                (datetime.now().isoformat(), EMAIL_PROVIDER, req.email_id))
        log_act("📤", f"Envoye: {to_name} ({to_email})", "email")
    except Exception as db_err:
        # L'email est bien parti, on log l'erreur DB mais on répond 'sent' quand même
        logger.error(f"send_email_ep DB update failed (email was sent): {db_err}")

    return {"status": "sent", "to": f"{to_name} <{to_email}>",
            "subject": subject, "provider": EMAIL_PROVIDER}

@app.post("/api/email/campaign", tags=["Email Prospection"])
async def email_campaign(request: Request, req: EmailBatchReq, _=Depends(verify_api_key)):
    _check_rate(request, "email_campaign", 5, 3600)
    if len(req.contacts) > 20:
        raise HTTPException(400, "Max 20 contacts par campagne")
    results = []
    for c in req.contacts:
        try:
            composed = await compose_email(EmailComposeReq(
                to_name=c.get("name",""), to_email=c.get("email",""),
                company=c.get("company",""), role=c.get("role",""),
                context=c.get("context", req.context), tone=req.tone))
            sent = False
            if req.auto_send and composed.get("email_id"):
                try:
                    await send_email_ep(EmailSendReq(email_id=composed["email_id"], confirm=True))
                    sent = True
                except Exception as e:
                    logger.error(f"Send {c.get('email')}: {e}")
            results.append({"contact": c.get("email"), "email_id": composed.get("email_id"),
                             "subject": composed.get("subject"), "sent": sent,
                             "status": "sent" if sent else "draft"})
        except Exception as e:
            results.append({"contact": c.get("email"), "error": str(e)})
    total = sum(1 for r in results if r.get("sent"))
    log_act("📢", f"Campagne: {total}/{len(req.contacts)} envoyes", "email")
    return {"campaign_size": len(req.contacts), "sent": total,
            "draft": len(req.contacts)-total, "results": results}

@app.patch("/api/email/{email_id}/replied", tags=["Email Prospection"])
async def mark_email_replied(email_id: int, _=Depends(verify_api_key)):
    _rnow = datetime.now().isoformat()   # FIX: Python datetime compatible SQLite+PG
    with db_conn() as conn:
        db_exec(conn,
            "UPDATE emails SET replied_at=?, status='replied' WHERE id=?",
            (_rnow, email_id))
    return {"email_id": email_id, "status": "replied"}

@app.get("/api/email/history", tags=["Email Prospection"])
async def email_history(status: Optional[str] = None, limit: int = 50, offset: int = 0, _=Depends(verify_api_key)):
    with db_conn() as conn:
        if status:
            rows = db_exec(conn,
                "SELECT id,to_name,to_email,company,role,subject,status,sent_at,replied_at,created_at FROM emails WHERE status=? ORDER BY created_at DESC LIMIT ?",
                (status, limit)).fetchall()
        else:
            rows = db_exec(conn,
                "SELECT id,to_name,to_email,company,role,subject,status,sent_at,replied_at,created_at FROM emails ORDER BY created_at DESC LIMIT ?",
                (limit,)).fetchall()
    return [{"id": r[0], "to_name": r[1], "to_email": r[2], "company": r[3], "role": r[4],
             "subject": r[5], "status": r[6], "sent_at": str(r[7]),
             "replied_at": str(r[8]), "created_at": str(r[9])} for r in rows]

@app.get("/api/email/stats", tags=["Email Prospection"])
async def email_stats(_=Depends(verify_api_key)):
    with db_conn() as conn:
        def cnt(q, p=()):
            return db_exec(conn, q, p).fetchone()[0]
        total   = cnt("SELECT COUNT(*) FROM emails")
        sent    = cnt("SELECT COUNT(*) FROM emails WHERE status='sent'")
        replied = cnt("SELECT COUNT(*) FROM emails WHERE replied_at IS NOT NULL")
        by_co   = db_exec(
            conn,
            "SELECT company,COUNT(*) n FROM emails GROUP BY company ORDER BY n DESC LIMIT 10",
        ).fetchall()
    return {"total": total, "sent": sent, "replied": replied,
            "reply_rate": f"{round(replied/sent*100)}%" if sent > 0 else "0%",
            "top_companies": [{"company": r[0], "count": r[1]} for r in by_co],
            "provider": EMAIL_PROVIDER}

@app.post("/api/email/contacts", tags=["Email Prospection"])
async def add_contact(req: ContactReq, _=Depends(verify_api_key)):
    with db_conn() as conn:
        cur = db_insert(conn,
            "INSERT INTO email_contacts(name,email,company,role,source,notes) VALUES(?,?,?,?,?,?)",
            (req.name, req.email, req.company, req.role, req.source, req.notes))
        cid = cur.lastrowid
    return {"contact_id": cid, "status": "added"}

@app.get("/api/email/contacts", tags=["Email Prospection"])
async def list_contacts(_=Depends(verify_api_key)):
    with db_conn() as conn:
        rows = db_exec(
            conn,
            "SELECT id,name,email,company,role,source,status,created_at FROM email_contacts ORDER BY created_at DESC",
        ).fetchall()
    return [{"id": r[0], "name": r[1], "email": r[2], "company": r[3],
             "role": r[4], "source": r[5], "status": r[6], "created_at": str(r[7])} for r in rows]

# ══════════════════════════════════════════════════════
#  COPILOT ENDPOINTS
# ══════════════════════════════════════════════════════

@app.post("/api/copilot/run", tags=["Copilot Quotidien"])
async def trigger_copilot(bt: BackgroundTasks, _=Depends(verify_api_key)):
    bt.add_task(run_copilot)
    return {"status": "started", "mode": "apify" if APIFY_TOKEN else "mock",
            "message": "Le copilot tourne en arrière-plan. Vérifie /api/copilot/history dans 60s."}

@app.post("/api/copilot/run/sync", tags=["Copilot Quotidien"])
async def trigger_copilot_sync(request: Request, _=Depends(verify_api_key)):
    """Lance le copilot en mode synchrone. Timeout 120s pour les offres mockées, 300s avec Apify."""
    _check_rate(request, "trigger_copilot_sync", 3, 3600)
    timeout_sec = 300 if APIFY_TOKEN else 120
    try:
        result = await asyncio.wait_for(run_copilot(), timeout=timeout_sec)
        return result
    except asyncio.TimeoutError:
        logger.error(f"Copilot sync timeout après {timeout_sec}s")
        raise HTTPException(504, {
            "error": "Copilot timeout",
            "message": f"Le copilot a dépassé {timeout_sec}s. Les offres ont peut-être été partiellement analysées.",
            "hint": "Lance /api/copilot/run (async en background) pour éviter le timeout.",
            "dashboard": "/dashboard"
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Copilot sync crash: {e}", exc_info=True)
        raise HTTPException(500, {
            "error": str(type(e).__name__),
            "message": str(e),
            "hint": "Vérifie GROQ_API_KEY dans les variables d'env Render. Lance /health pour diagnostiquer.",
        })

@app.get("/api/copilot/history", tags=["Copilot Quotidien"])
async def copilot_history(_=Depends(verify_api_key)):
    with db_conn() as conn:
        rows = db_exec(
            conn,
            "SELECT id,offers_found,top_offer,top_score,post_topic,digest_sent,auto_applied,run_at FROM copilot_runs ORDER BY run_at DESC LIMIT 20",
        ).fetchall()
    return [{"id": r[0], "offers_found": r[1], "top_offer": r[2], "top_score": r[3],
             "post_topic": r[4], "digest_sent": bool(r[5]),
             "auto_applied": r[6], "run_at": str(r[7])} for r in rows]

@app.get("/api/copilot/config", tags=["Copilot Quotidien"])
async def copilot_config(_=Depends(verify_api_key)):
    return {"copilot_email": COPILOT_EMAIL, "hour": COPILOT_HOUR,
            "auto_apply": AUTO_APPLY_ENABLED, "min_score": AUTO_APPLY_MIN_SCORE,
            "followup_days": FOLLOWUP_DAYS,
            "scraping": "apify" if APIFY_TOKEN else "mock",
            "jobs": [{"id": j.id, "next": str(j.next_run_time)} for j in scheduler.get_jobs()]}

# ══════════════════════════════════════════════════════
#  AGENT ENDPOINTS
# ══════════════════════════════════════════════════════

class AutoApplyReq(BaseModel):
    offer_title: str; company: str = ""; company_email: str = ""
    description: str; force_send: bool = False

@app.post("/api/agent/apply", tags=["Agent Candidature Auto"])
async def agent_apply(req: AutoApplyReq, request: Request, _=Depends(verify_api_key)):
    _check_rate(request, "agent_apply", 20, 3600)
    _rate_limit(request, max_calls=10, window_seconds=3600)  # 10 candidatures/heure
    analysis = await ask_json(f"""Sekouna KABA Data Architect/Engineer Senior. TJM 500-900euro.
{json.dumps(PROFILE)}\nOffre: {req.description}
JSON:{{"match_score":<0-100>,"tjm_negotiate":"<TJM 500-900euro>",
"urgency":"<Postuler maintenant/Peut attendre>","match_reason":"<raison>"}}
JSON seul.""", 500)
    score = analysis.get("match_score", 0)
    if score < 60:
        return {"match_score": score, "decision": "Score trop faible (<60%) — pas de candidature générée",
                "analysis": analysis}
    # FIX: cree un offer_id reel via context manager (jamais 0)
    with db_conn() as _oc:
        _ocur = db_insert(_oc, "INSERT INTO offers(title,source,description,match_score,tjm_negotiate,urgency,status) VALUES(?,?,?,?,?,?,?)",
            (req.offer_title,"manual",req.description[:1000],score,analysis.get("tjm_negotiate",""),analysis.get("urgency",""),"analyzed"))
        offer_id = _ocur.lastrowid
    result = await run_auto_apply(offer_id, req.offer_title, req.company, req.description, analysis, req.company_email or "")
    if (AUTO_APPLY_ENABLED or req.force_send) and req.company_email:
        # pdf_bytes récupéré depuis la DB (result ne contient plus de bytes bruts)
        _pdf_b64 = result.get("pdf_b64") or ""
        try:
            with db_conn() as _pc:
                _pr = db_exec(_pc, "SELECT cv_pdf_b64 FROM auto_applications WHERE id=?", (result["app_id"],)).fetchone()
                _pdf_b64 = _pr[0] if _pr and _pr[0] else _pdf_b64
        except Exception:
            pass
        pdf_bytes = base64.b64decode(_pdf_b64) if _pdf_b64 else b""
        body_html = make_html_email(result.get("email_body", ""))
        att_name  = f"CV_Sekouna_KABA_{req.offer_title[:25].replace(' ','_')}.pdf" if pdf_bytes else None
        # ── Envoi isolé du update DB ──
        try:
            await send_email(req.company_email, req.company or "Recruteur",
                             result.get("subject", "Candidature freelance Data"),
                             body_html, result.get("email_body", ""),
                             att_bytes=pdf_bytes or None, att_name=att_name)
            result["email_sent"]   = True
            result["pdf_attached"] = bool(pdf_bytes)
        except Exception as e:
            result["email_error"] = str(e)
        # ── Update DB seulement si l'envoi a réussi ──
        if result.get("email_sent"):
            try:
                with db_conn() as conn:
                    db_exec(conn,
                        "UPDATE auto_applications SET status='sent', applied_at=? WHERE id=?",
                        (datetime.now().isoformat(), result["app_id"]))
            except Exception as db_err:
                logger.error(f"agent_apply DB update failed (email was sent): {db_err}")
    return {**analysis, **result, "auto_apply_active": AUTO_APPLY_ENABLED or req.force_send}


@app.get("/api/agent/applications/export-csv", tags=["Agent Candidature Auto"])
async def export_applications_csv(_=Depends(verify_api_key)):
    """Exporte toutes les candidatures en CSV."""
    import csv, io as _io
    with db_conn() as conn:
        rows = db_exec(conn,
            "SELECT id,offer_title,company_email,match_score,tjm_negotiate,status,applied_at,followup_at,followup_sent,reply_received,created_at FROM auto_applications ORDER BY created_at DESC"
        ).fetchall()
    out = _io.StringIO()
    w = csv.writer(out)
    w.writerow(["id","poste","email_entreprise","score","tjm","statut","date_candidature","date_relance","relance_envoyée","reponse_recue","creé_le"])
    for r in rows:
        w.writerow(r)
    from fastapi.responses import Response
    return Response(
        content=out.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=candidatures.csv"}
    )

@app.get("/api/agent/applications", tags=["Agent Candidature Auto"])
async def list_applications(status: Optional[str] = None, _=Depends(verify_api_key)):
    with db_conn() as conn:
        q = "SELECT id,offer_title,company_email,match_score,tjm_negotiate,email_subject,status,applied_at,followup_at,followup_sent,reply_received,created_at FROM auto_applications"
        if status:
            rows = db_exec(conn, q + " WHERE status=? ORDER BY created_at DESC LIMIT 30", (status,)).fetchall()
        else:
            rows = db_exec(conn, q + " ORDER BY created_at DESC LIMIT 30").fetchall()
    cols = ["id","offer_title","company_email","match_score","tjm_negotiate","email_subject",
            "status","applied_at","followup_at","followup_sent","reply_received","created_at"]
    return [dict(zip(cols, r)) for r in rows]

@app.patch("/api/agent/applications/{app_id}/replied", tags=["Agent Candidature Auto"])
async def mark_replied_app(app_id: int, _=Depends(verify_api_key)):
    with db_conn() as conn:
        db_exec(conn,
            "UPDATE auto_applications SET reply_received=1, status='replied' WHERE id=?",
            (app_id,))
    log_act("✅", f"Candidature #{app_id} repondue !", "agent")
    return {"app_id": app_id, "status": "replied"}

@app.get("/api/agent/stats", tags=["Agent Candidature Auto"])
async def agent_stats(_=Depends(verify_api_key)):
    with db_conn() as conn:
        def cnt(q, p=()):
            return db_exec(conn, q, p).fetchone()[0]
        total = cnt("SELECT COUNT(*) FROM auto_applications")
        sent  = cnt("SELECT COUNT(*) FROM auto_applications WHERE status='sent'")
        rep   = cnt("SELECT COUNT(*) FROM auto_applications WHERE reply_received=1")
    return {"total": total, "sent": sent, "replied": rep,
            "reply_rate": f"{round(rep/sent*100)}%" if sent > 0 else "0%",
            "auto_apply": f"{'ACTIF (>=' + str(AUTO_APPLY_MIN_SCORE) + '%)' if AUTO_APPLY_ENABLED else 'MANUEL'}",
            "pdf_cv": "actif (reportlab)"}

# ══════════════════════════════════════════════════════
#  NEGOCIATEUR ENDPOINTS
# ══════════════════════════════════════════════════════

class NegotiationReq(BaseModel):
    context: str; current_offer: str; target_tjm: str = "760euro/j"; conversation: str = ""

@app.post("/api/negotiate", tags=["Négociateur TJM"])
async def negotiate(request: Request, req: NegotiationReq, _=Depends(verify_api_key)):
    _check_rate(request, "negotiate", 20, 3600)
    ctx    = req.context + (f"\n\nConversation:\n{req.conversation}" if req.conversation else "")
    result = await run_negotiation(ctx, req.current_offer, req.target_tjm)
    with db_conn() as conn:
        db_exec(conn,
            "INSERT INTO negotiations(context,current_offer,target_tjm,script,counter_offer,arguments) VALUES(?,?,?,?,?,?)",
            (req.context, req.current_offer, req.target_tjm,
             result.get("full_script",""), result.get("counter_offer",""),
             json.dumps(result.get("arguments",[]))))
    log_act("💰", f"{req.current_offer} → {result.get('counter_offer','?')} (confiance {result.get('confidence_score','?')}%)", "negotiate")
    return result

# ══════════════════════════════════════════════════════
#  VALIDATION (Human-in-the-Loop)
# ══════════════════════════════════════════════════════

@app.get("/api/approvals", tags=["Validation"])
async def list_approvals(status: Optional[str] = "pending", _=Depends(verify_api_key)):
    with db_conn() as conn:
        if status == "all":
            rows = db_exec(conn, "SELECT id,type,title,preview,payload,status,approved_at,created_at FROM pending_approvals ORDER BY created_at DESC").fetchall()
        else:
            rows = db_exec(conn, "SELECT id,type,title,preview,payload,status,approved_at,created_at FROM pending_approvals WHERE status=? ORDER BY created_at DESC", (status,)).fetchall()
    return [{"id":r[0],"type":r[1],"title":r[2],"preview":r[3],
             "payload":json.loads(r[4]) if r[4] else {},
             "status":r[5],"approved_at":r[6],"created_at":r[7]} for r in rows]

@app.post("/api/approvals/{approval_id}/approve", tags=["Validation"])
async def approve_item(approval_id: int, _=Depends(verify_api_key)):
    with db_conn() as conn:
        row = db_exec(conn, "SELECT type,title,payload,status FROM pending_approvals WHERE id=?", (approval_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Approbation introuvable")
    if row[3] != "pending":
        raise HTTPException(400, f"Deja traite: {row[3]}")
    item_type = row[0]
    payload   = json.loads(row[2]) if row[2] else {}
    result    = {}
    try:
        if item_type == "linkedin_post":
            with db_conn() as conn:
                db_exec(conn, "UPDATE linkedin_posts SET status='published' WHERE content=?", (payload.get("content",""),))
            log_act("checkmark", f"Post LinkedIn valide: {payload.get('topic','')}", "validation")
            result = {"action": "post_published", "topic": payload.get("topic")}
        elif item_type == "candidature":
            app_id     = payload.get("app_id")
            company    = payload.get("company", "Recruteur")
            subject    = payload.get("subject", "Candidature freelance")
            email_body = payload.get("email_body", "")
            company_email_payload = payload.get("company_email", "")
            with db_conn() as conn:
                app_row = db_exec(conn, "SELECT cv_pdf_b64,company_email,offer_title FROM auto_applications WHERE id=?", (app_id,)).fetchone()

            offer_title   = payload.get("offer_title", "") or (app_row[2] if app_row else "")
            to_email      = (app_row[1] if app_row else "") or company_email_payload

            # Générer le PDF si absent (cas defer_pdf=True du copilot)
            pdf_b64_stored = app_row[0] if app_row else ""
            if not pdf_b64_stored:
                log_act("pdf", f"Génération PDF différée: {offer_title}", "validation")
                try:
                    # Régénérer le CV simplifié pour ce poste
                    cv_data = {
                        "title_adapted": offer_title,
                        "accroche": f"Data Architect & Engineer Senior avec 8 ans d'expérience. Expert Snowflake, dbt, Airflow, PySpark. Missions SACEM, Thales, Accor.",
                        "kpis": ["50M+ événements/jour", "12 pipelines migrés", "3 clouds maîtrisés"],
                        "competences_core": ["Snowflake", "dbt Core", "Apache Airflow", "PySpark", "Python", "Kafka"],
                        "competences_secondaires": ["AWS", "Azure", "GCP", "Terraform", "Docker", "GitLab CI/CD"],
                        "experiences": PROFILE["experiences"],
                        "formations": [{"diplome": "Master Informatique", "ecole": "Université", "annee": "2016"}],
                        "certifications": ["AWS Certified", "GCP Professional Data Engineer", "Snowflake SnowPro"],
                        "langues": [{"langue": "Français", "niveau": "Natif"}, {"langue": "Anglais", "niveau": "Courant"}],
                        "soft_skills": ["Leadership technique", "Communication client", "Autonomie", "Esprit data-driven"],
                        "tjm_suggest": "760€/j",
                    }
                    pdf_bytes_gen = await generate_cv_pdf(cv_data, offer_title)
                    pdf_b64_stored = base64.b64encode(pdf_bytes_gen).decode() if pdf_bytes_gen else ""
                    if pdf_b64_stored:
                        with db_conn() as conn:
                            db_exec(conn, "UPDATE auto_applications SET cv_pdf_b64=? WHERE id=?", (pdf_b64_stored, app_id))
                except Exception as e:
                    logger.warning(f"PDF deferred generation failed: {e}")

            if to_email and EMAIL_PROVIDER != "none":
                pdf_bytes = base64.b64decode(pdf_b64_stored) if pdf_b64_stored else b""
                att_name  = f"CV_Sekouna_KABA_{offer_title[:25].replace(' ','_')}.pdf" if pdf_bytes else None
                try:
                    await send_email(to_email, company, subject, make_html_email(email_body), email_body,
                                     att_bytes=pdf_bytes or None, att_name=att_name)
                    with db_conn() as conn:
                        db_exec(conn, "UPDATE auto_applications SET status='sent', applied_at=CURRENT_TIMESTAMP WHERE id=?", (app_id,))
                    log_act("send", f"Candidature envoyee: {offer_title}", "validation")
                    result = {"action": "email_sent", "to": to_email, "subject": subject, "pdf_attached": bool(pdf_bytes)}
                    await notify("📨 Candidature envoyée", f"{offer_title}\nDest: {to_email}\n{subject}", url=DASHBOARD_URL)
                except Exception as e:
                    raise HTTPException(500, f"Erreur envoi email: {e}")
            else:
                with db_conn() as conn:
                    db_exec(conn, "UPDATE auto_applications SET status='approved' WHERE id=?", (app_id,))
                log_act("check", f"Candidature approuvee (pas d'email): {offer_title}", "validation")
                result = {"action": "approved_no_email",
                          "hint": "Ajoute company_email via le dashboard ou configure HUNTER_API_KEY pour l'enrichissement auto."}
        with db_conn() as conn:
            db_exec(conn, "UPDATE pending_approvals SET status='approved', approved_at=CURRENT_TIMESTAMP WHERE id=?", (approval_id,))
        return {"status": "approved", "type": item_type, **result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Erreur approbation: {e}")

@app.post("/api/approvals/{approval_id}/reject", tags=["Validation"])
async def reject_item(approval_id: int, _=Depends(verify_api_key)):
    with db_conn() as conn:
        row = db_exec(conn, "SELECT type,title FROM pending_approvals WHERE id=?", (approval_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Approbation introuvable")
    with db_conn() as conn:
        db_exec(conn, "UPDATE pending_approvals SET status='rejected' WHERE id=?", (approval_id,))
    log_act("x", f"Rejete: {row[1]}", "validation")
    return {"status": "rejected", "type": row[0], "title": row[1]}

@app.get("/api/approvals/stats", tags=["Validation"])
async def approval_stats(_=Depends(verify_api_key)):
    with db_conn() as conn:
        pending  = db_exec(conn, "SELECT COUNT(*) FROM pending_approvals WHERE status='pending'").fetchone()[0]
        approved = db_exec(conn, "SELECT COUNT(*) FROM pending_approvals WHERE status='approved'").fetchone()[0]
        rejected = db_exec(conn, "SELECT COUNT(*) FROM pending_approvals WHERE status='rejected'").fetchone()[0]
    return {"pending": pending, "approved": approved, "rejected": rejected}

@app.get("/api/negotiate/history", tags=["Négociateur TJM"])
async def negotiate_history(_=Depends(verify_api_key)):
    with db_conn() as conn:
        rows = db_exec(
            conn,
            "SELECT id,context,current_offer,target_tjm,counter_offer,outcome,created_at FROM negotiations ORDER BY created_at DESC LIMIT 20",
        ).fetchall()
    return [{"id": r[0], "context": str(r[1])[:80], "current_offer": r[2],
             "target": r[3], "counter_offer": r[4], "outcome": r[5],
             "created_at": str(r[6])} for r in rows]

@app.patch("/api/negotiate/{neg_id}/outcome", tags=["Négociateur TJM"])
async def set_outcome(neg_id: int, outcome: str, _=Depends(verify_api_key)):
    with db_conn() as conn:
        db_exec(conn, "UPDATE negotiations SET outcome=? WHERE id=?", (outcome, neg_id))
    return {"neg_id": neg_id, "outcome": outcome}


# ══════════════════════════════════════════════════════
#  ANALYTICS FREELANCE
# ══════════════════════════════════════════════════════

@app.get("/api/analytics", tags=["Analytics"])
async def analytics(_=Depends(verify_api_key)):
    """Tableau de bord analytique complet — performance, tendances, ROI."""
    with db_conn() as conn:
        def cnt(q, p=()):  return db_exec(conn,q,p).fetchone()[0]
        def rows(q, p=()): return db_exec(conn,q,p).fetchall()

        # ── Candidatures ──────────────────────────────────────────────
        total_apps   = cnt("SELECT COUNT(*) FROM auto_applications")
        sent_apps    = cnt("SELECT COUNT(*) FROM auto_applications WHERE status='sent'")
        replied_apps = cnt("SELECT COUNT(*) FROM auto_applications WHERE reply_received=1")
        reply_rate   = round(replied_apps / sent_apps * 100, 1) if sent_apps > 0 else 0

        # Taux par score IA
        score_brackets = []
        for lo, hi, label in [(85,100,"85-100%"),(70,85,"70-84%"),(60,70,"60-69%")]:
            t = cnt(f"SELECT COUNT(*) FROM auto_applications WHERE match_score>={lo} AND match_score<{hi}")
            r = cnt(f"SELECT COUNT(*) FROM auto_applications WHERE match_score>={lo} AND match_score<{hi} AND reply_received=1")
            score_brackets.append({"range": label, "total": t, "replied": r,
                                    "rate": round(r/t*100,1) if t > 0 else 0})

        # Performance par source
        sources_raw = rows("""
            SELECT o.source, COUNT(a.id) as apps,
                   SUM(CASE WHEN a.reply_received=1 THEN 1 ELSE 0 END) as replies
            FROM auto_applications a
            JOIN offers o ON a.offer_id = o.id
            GROUP BY o.source ORDER BY apps DESC LIMIT 8
        """)
        sources = [{"source": r[0] or "Inconnu", "apps": r[1],
                    "replies": r[2], "rate": round(r[2]/r[1]*100,1) if r[1]>0 else 0}
                   for r in sources_raw]

        # TJM moyen proposé vs marché
        tjm_rows = rows("SELECT tjm_negotiate FROM auto_applications WHERE tjm_negotiate IS NOT NULL AND tjm_negotiate != '' LIMIT 50")
        tjm_vals = []
        import re as _re
        for (t,) in tjm_rows:
            m = _re.search(r'(\d{3,4})', str(t))
            if m: tjm_vals.append(int(m.group(1)))
        tjm_avg     = round(sum(tjm_vals)/len(tjm_vals)) if tjm_vals else 0
        tjm_min     = min(tjm_vals) if tjm_vals else 0
        tjm_max     = max(tjm_vals) if tjm_vals else 0

        # ── LinkedIn ──────────────────────────────────────────────────
        total_posts     = cnt("SELECT COUNT(*) FROM linkedin_posts")
        published_posts = cnt("SELECT COUNT(*) FROM linkedin_posts WHERE status='published'")

        # Top sujets LinkedIn
        topics_raw = rows("""
            SELECT topic, COUNT(*) as cnt FROM linkedin_posts
            GROUP BY topic ORDER BY cnt DESC LIMIT 6
        """)
        top_topics = [{"topic": r[0], "count": r[1]} for r in topics_raw]

        # ── Emails ────────────────────────────────────────────────────
        emails_sent    = cnt("SELECT COUNT(*) FROM emails WHERE status='sent'")
        emails_replied = cnt("SELECT COUNT(*) FROM emails WHERE replied=1")
        email_rate     = round(emails_replied/emails_sent*100,1) if emails_sent>0 else 0

        # ── Offres ────────────────────────────────────────────────────
        total_offers  = cnt("SELECT COUNT(*) FROM offers")
        avg_score     = db_exec(conn,"SELECT AVG(match_score) FROM offers WHERE match_score IS NOT NULL").fetchone()[0]
        avg_score     = round(avg_score, 1) if avg_score else 0
        high_match    = cnt("SELECT COUNT(*) FROM offers WHERE match_score >= 85")
        mid_match     = cnt("SELECT COUNT(*) FROM offers WHERE match_score >= 70 AND match_score < 85")

        # Évolution hebdomadaire (4 dernières semaines)
        weekly_raw = rows("""
            SELECT strftime('%Y-W%W', created_at) as week,
                   COUNT(*) as offers,
                   AVG(match_score) as avg_score
            FROM offers
            WHERE created_at >= datetime('now', '-28 days')
            GROUP BY week ORDER BY week
        """) if not USE_POSTGRES else rows("""
            SELECT TO_CHAR(created_at, 'IYYY-IW') as week,
                   COUNT(*) as offers,
                   AVG(match_score) as avg_score
            FROM offers
            WHERE created_at >= NOW() - INTERVAL '28 days'
            GROUP BY week ORDER BY week
        """)
        weekly = [{"week": r[0], "offers": r[1], "avg_score": round(r[2],1) if r[2] else 0}
                  for r in weekly_raw]

        # ── Copilot ───────────────────────────────────────────────────
        copilot_runs   = cnt("SELECT COUNT(*) FROM copilot_runs")
        digests_sent   = cnt("SELECT COUNT(*) FROM copilot_runs WHERE digest_sent=1")
        auto_applied_t = cnt("SELECT SUM(auto_applied) FROM copilot_runs") or 0

        # ── Validation ────────────────────────────────────────────────
        v_pending  = cnt("SELECT COUNT(*) FROM pending_approvals WHERE status='pending'")
        v_approved = cnt("SELECT COUNT(*) FROM pending_approvals WHERE status='approved'")
        v_rejected = cnt("SELECT COUNT(*) FROM pending_approvals WHERE status='rejected'")
        v_rate     = round(v_approved/(v_approved+v_rejected)*100,1) if (v_approved+v_rejected)>0 else 0

        # ── Délai moyen de réponse ────────────────────────────────────
        # (approximation basée sur followup_at - created_at)
        delay_raw = db_exec(conn,"""
            SELECT AVG(
                CAST(julianday(COALESCE(followup_at, datetime('now'))) -
                     julianday(created_at) AS REAL)
            ) FROM auto_applications WHERE reply_received=1
        """).fetchone()[0] if not USE_POSTGRES else None
        avg_reply_delay = round(delay_raw, 1) if delay_raw else None

    return {
        "candidatures": {
            "total": total_apps, "sent": sent_apps, "replied": replied_apps,
            "reply_rate_pct": reply_rate, "by_score": score_brackets,
            "by_source": sources, "avg_reply_delay_days": avg_reply_delay,
        },
        "tjm": {
            "avg_proposed": tjm_avg, "min": tjm_min, "max": tjm_max,
            "target": PROFILE["tjm_target"], "current": PROFILE["tjm_current"],
            "gap_to_target": PROFILE["tjm_target"] - tjm_avg if tjm_avg else 0,
        },
        "linkedin": {
            "total": total_posts, "published": published_posts,
            "top_topics": top_topics,
        },
        "emails": {
            "sent": emails_sent, "replied": emails_replied, "reply_rate_pct": email_rate,
        },
        "offers": {
            "total": total_offers, "avg_score": avg_score,
            "high_match": high_match, "mid_match": mid_match, "weekly": weekly,
        },
        "copilot": {
            "runs": copilot_runs, "digests_sent": digests_sent,
            "auto_applied_total": int(auto_applied_t),
        },
        "validation": {
            "pending": v_pending, "approved": v_approved,
            "rejected": v_rejected, "approval_rate_pct": v_rate,
        },
    }


# ══════════════════════════════════════════════════════
#  BACKUP ENDPOINTS
# ══════════════════════════════════════════════════════

@app.post("/api/backup/run", tags=["Système"])
async def trigger_backup(_=Depends(verify_api_key)):
    """Déclenche un backup immédiat de la base de données."""
    result = await backup_database()
    return result

@app.get("/api/backup/list", tags=["Système"])
async def get_backups(_=Depends(verify_api_key)):
    """Liste les backups disponibles (7 derniers)."""
    backups = await list_backups()
    return {"backups": backups, "count": len(backups), "storage": "/tmp/datalinkedai_backups"}

# ══════════════════════════════════════════════════════
#  GMAIL PARSING ENDPOINTS
# ══════════════════════════════════════════════════════

@app.post("/api/gmail/parse-replies", tags=["Gmail Parsing"])
async def manual_gmail_parse(_=Depends(verify_api_key)):
    """Déclenche manuellement le scan Gmail pour détecter les réponses."""
    result = await parse_gmail_replies()
    return result

@app.get("/api/gmail/status", tags=["Gmail Parsing"])
async def gmail_status(_=Depends(verify_api_key)):
    """Vérifie si le parsing Gmail est configuré et actif."""
    return {
        "enabled": GMAIL_PARSE_ENABLED,
        "gmail_configured": bool(GMAIL_USER and GMAIL_APP_PASS),
        "gmail_user": GMAIL_USER if GMAIL_USER else None,
        "scan_interval": "toutes les 2h (automatique)",
        "hunter_configured": bool(HUNTER_API_KEY),
    }

# ══════════════════════════════════════════════════════
#  MONITORING ENDPOINTS
# ══════════════════════════════════════════════════════

@app.get("/api/rate-limit/status", tags=["Système"])
async def rate_limit_status(request: Request, _=Depends(verify_api_key)):
    """Voir ton quota d'appels restants par IP."""
    ip  = request.client.host if request.client else "unknown"
    now = time.time()
    LIMITS = {
        "copilot/run/sync": (3, 3600), "cv/adapt": (10, 3600),
        "linkedin/generate": (20, 3600), "agent/apply": (10, 3600),
        "proposal/generate": (10, 3600),
    }
    calls = len([t for t in _rate_store.get(ip, []) if now - t < 3600])
    return {"ip": ip, "calls_last_hour": calls,
            "limits": {k: {"max": v[0], "window_seconds": v[1]} for k, v in LIMITS.items()}}

@app.delete("/api/rate-limit/reset", tags=["Système"])
async def rate_limit_reset(request: Request, _=Depends(verify_api_key)):
    """Réinitialise le compteur de rate limiting (admin uniquement)."""
    ip = request.client.host if request.client else "unknown"
    _rate_store.pop(ip, None)
    return {"status": "reset", "ip": ip}


# ══════════════════════════════════════════════════════
#  PAGE PROFIL PUBLIQUE
# ══════════════════════════════════════════════════════

@app.get("/profil", include_in_schema=False)
async def public_profile():
    """Page profil publique — à partager aux recruteurs."""
    from fastapi.responses import HTMLResponse
    skills_html = "".join(f'<span class="skill-tag">{s}</span>' for s in PROFILE["skills"])
    exp_html = ""
    for e in PROFILE["experiences"]:
        stack_tags = "".join(f'<span class="stack-tag">{t.strip()}</span>'
                              for t in e.get("stack","").split(",") if t.strip())[:6*60]
        exp_html += f"""
        <div class="exp-card">
          <div class="exp-header">
            <div>
              <div class="exp-company">{e['company']}</div>
              <div class="exp-role">{e['role']}</div>
            </div>
            <div class="exp-period">{e['period']}</div>
          </div>
          <div class="stack-tags">{stack_tags}</div>
        </div>"""

    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta property="og:title" content="Sekouna KABA — Data Architect Freelance">
<meta property="og:description" content="Data Architect / Engineer Senior • TJM 500-900€/j • Remote">
<meta property="og:type" content="profile">
<title>Sekouna KABA — Data Architect Freelance</title>
<link href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600;12..96,700;12..96,800&family=Instrument+Sans:wght@400;500;600&display=swap" rel="stylesheet">
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:'Instrument Sans',sans-serif;background:#F4F6FB;color:#0C1630;min-height:100vh}}
  .hero{{background:linear-gradient(135deg,#1A3A6B 0%,#2563EB 60%,#7C3AED 100%);padding:4rem 2rem 6rem;text-align:center;position:relative;overflow:hidden}}
  .hero::before{{content:'';position:absolute;inset:0;background:url("data:image/svg+xml,%3Csvg width='60' height='60' viewBox='0 0 60 60' xmlns='http://www.w3.org/2000/svg'%3E%3Cg fill='none' fill-rule='evenodd'%3E%3Cg fill='%23ffffff' fill-opacity='0.03'%3E%3Cpath d='M36 34v-4h-2v4h-4v2h4v4h2v-4h4v-2h-4zm0-30V0h-2v4h-4v2h4v4h2V6h4V4h-4zM6 34v-4H4v4H0v2h4v4h2v-4h4v-2H6zM6 4V0H4v4H0v2h4v4h2V6h4V4H6z'/%3E%3C/g%3E%3C/g%3E%3C/svg%3E")}}
  .avatar{{width:96px;height:96px;border-radius:50%;background:linear-gradient(135deg,#93C5FD,#7C3AED);display:flex;align-items:center;justify-content:center;font-family:'Bricolage Grotesque',sans-serif;font-size:2.2rem;font-weight:800;color:white;margin:0 auto 1.5rem;border:3px solid rgba(255,255,255,.3)}}
  .hero h1{{font-family:'Bricolage Grotesque',sans-serif;font-size:clamp(2rem,5vw,3rem);font-weight:800;color:white;margin-bottom:.5rem}}
  .hero .subtitle{{font-size:1.1rem;color:rgba(255,255,255,.85);margin-bottom:1.5rem}}
  .hero .badges{{display:flex;gap:.75rem;justify-content:center;flex-wrap:wrap;margin-bottom:2rem}}
  .hero .badge{{background:rgba(255,255,255,.15);backdrop-filter:blur(8px);color:white;padding:.4rem 1rem;border-radius:20px;font-size:.85rem;border:1px solid rgba(255,255,255,.25)}}
  .hero .badge.gold{{background:rgba(245,158,11,.25);border-color:rgba(245,158,11,.5);color:#FDE68A}}
  .cta-bar{{background:white;max-width:680px;margin:-2.5rem auto 0;border-radius:16px;padding:1.5rem 2rem;box-shadow:0 20px 60px rgba(0,0,0,.12);display:flex;gap:1rem;align-items:center;flex-wrap:wrap;justify-content:center;position:relative;z-index:10}}
  .cta-btn{{padding:.75rem 1.75rem;border-radius:8px;font-weight:600;text-decoration:none;font-size:.95rem;transition:.2s}}
  .cta-primary{{background:linear-gradient(135deg,#2563EB,#7C3AED);color:white}}
  .cta-secondary{{background:#F4F6FB;color:#0C1630;border:1px solid #DDE3F0}}
  .cta-btn:hover{{opacity:.88;transform:translateY(-1px)}}
  .container{{max-width:900px;margin:0 auto;padding:4rem 2rem}}
  .section{{margin-bottom:3.5rem}}
  .section-title{{font-family:'Bricolage Grotesque',sans-serif;font-size:1.5rem;font-weight:700;color:#0C1630;margin-bottom:1.5rem;padding-bottom:.75rem;border-bottom:2px solid #EEF2FF}}
  .kpi-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:1rem;margin-bottom:2rem}}
  .kpi-card{{background:white;border-radius:12px;padding:1.25rem;text-align:center;box-shadow:0 2px 12px rgba(0,0,0,.06)}}
  .kpi-val{{font-family:'Bricolage Grotesque',sans-serif;font-size:2rem;font-weight:800;background:linear-gradient(135deg,#2563EB,#7C3AED);-webkit-background-clip:text;-webkit-text-fill-color:transparent}}
  .kpi-label{{font-size:.78rem;color:#7B8DB5;margin-top:.25rem;text-transform:uppercase;letter-spacing:.5px}}
  .skills-grid{{display:flex;flex-wrap:wrap;gap:.6rem}}
  .skill-tag{{background:white;border:1px solid #DDE3F0;color:#2563EB;padding:.4rem .9rem;border-radius:20px;font-size:.85rem;font-weight:500}}
  .exp-card{{background:white;border-radius:12px;padding:1.25rem 1.5rem;margin-bottom:1rem;box-shadow:0 2px 12px rgba(0,0,0,.05);border-left:3px solid #2563EB}}
  .exp-header{{display:flex;justify-content:space-between;align-items:flex-start;gap:1rem;margin-bottom:.75rem;flex-wrap:wrap}}
  .exp-company{{font-family:'Bricolage Grotesque',sans-serif;font-weight:700;font-size:1.05rem}}
  .exp-role{{color:#2563EB;font-size:.9rem;margin-top:.2rem}}
  .exp-period{{color:#7B8DB5;font-size:.8rem;font-family:monospace;flex-shrink:0}}
  .stack-tags{{display:flex;flex-wrap:wrap;gap:.4rem}}
  .stack-tag{{background:#EEF2FF;color:#2563EB;padding:.2rem .6rem;border-radius:4px;font-size:.75rem;font-weight:500}}
  .contact-card{{background:linear-gradient(135deg,#1A3A6B,#2563EB);border-radius:16px;padding:2.5rem;text-align:center;color:white}}
  .contact-card h3{{font-family:'Bricolage Grotesque',sans-serif;font-size:1.6rem;font-weight:800;margin-bottom:.5rem}}
  .contact-card p{{color:rgba(255,255,255,.8);margin-bottom:1.5rem}}
  .contact-links{{display:flex;gap:1rem;justify-content:center;flex-wrap:wrap}}
  .contact-link{{background:rgba(255,255,255,.15);color:white;padding:.65rem 1.5rem;border-radius:8px;text-decoration:none;border:1px solid rgba(255,255,255,.25);font-weight:500}}
  .contact-link:hover{{background:rgba(255,255,255,.25)}}
  .avail-badge{{display:inline-flex;align-items:center;gap:.5rem;background:#ECFDF5;color:#059669;padding:.5rem 1.25rem;border-radius:20px;font-weight:600;font-size:.9rem;border:1px solid #A7F3D0}}
  .avail-dot{{width:8px;height:8px;background:#10B981;border-radius:50%;animation:pulse 2s infinite}}
  @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.4}}}}
  footer{{text-align:center;padding:2rem;color:#7B8DB5;font-size:.8rem;border-top:1px solid #EEF2FF}}
  @media(max-width:600px){{.cta-bar{{flex-direction:column}}.exp-header{{flex-direction:column}}}}
</style>
</head>
<body>

<div class="hero">
  <div class="avatar">SK</div>
  <h1>Sekouna KABA</h1>
  <p class="subtitle">Data Architect &amp; Engineer Senior · Freelance</p>
  <div class="badges">
    <span class="badge gold">TJM 500–900€/j</span>
    <span class="badge">🌍 Full Remote</span>
    <span class="badge">📍 Île-de-France</span>
    <span class="badge">8 ans d'expérience</span>
  </div>
  <div style="display:flex;justify-content:center">
    <span class="avail-badge"><span class="avail-dot"></span>Disponible pour missions</span>
  </div>
</div>

<div class="cta-bar">
  <a href="mailto:kaba.sekouna@gmail.com" class="cta-btn cta-primary">📧 Me contacter</a>
  <a href="https://linkedin.com/in/sekouna-kaba" class="cta-btn cta-secondary" target="_blank">LinkedIn</a>
  <a href="https://github.com/cheick-sk" class="cta-btn cta-secondary" target="_blank">GitHub</a>
</div>

<div class="container">

  <div class="section">
    <div class="kpi-grid">
      <div class="kpi-card"><div class="kpi-val">8+</div><div class="kpi-label">Années d'expérience</div></div>
      <div class="kpi-card"><div class="kpi-val">3</div><div class="kpi-label">Clouds maîtrisés</div></div>
      <div class="kpi-card"><div class="kpi-val">5+</div><div class="kpi-label">Missions grands comptes</div></div>
      <div class="kpi-card"><div class="kpi-val">760€</div><div class="kpi-label">TJM cible</div></div>
    </div>
  </div>

  <div class="section">
    <div class="section-title">À propos</div>
    <p style="line-height:1.8;color:#2D3C60;font-size:1rem">
      Data Architect & Engineer Senior avec 8 ans d'expérience sur des projets à fort enjeu pour
      <strong>SACEM, Thales Group, Accor</strong> et d'autres grands comptes.
      Spécialisé dans la conception d'architectures data modernes (Medallion, Lakehouse, Data Mesh),
      la migration de legacy systems, et les pipelines temps réel haute volumétrie.
      Expert certifié <strong>Snowflake, AWS et GCP</strong>.
      Full Remote · Disponible immédiatement.
    </p>
  </div>

  <div class="section">
    <div class="section-title">Stack technique</div>
    <div class="skills-grid">{skills_html}</div>
  </div>

  <div class="section">
    <div class="section-title">Expériences récentes</div>
    {exp_html}
  </div>

  <div class="section">
    <div class="section-title">Formation & Certifications</div>
    <div style="display:flex;flex-wrap:wrap;gap:.75rem">
      <div class="skill-tag" style="background:white">🎓 Master Informatique / Data Engineering</div>
      <div class="skill-tag" style="background:white">☁️ AWS Certified</div>
      <div class="skill-tag" style="background:white">☁️ GCP Professional Data Engineer</div>
      <div class="skill-tag" style="background:white">❄️ Snowflake SnowPro Core</div>
    </div>
  </div>

  <div class="contact-card">
    <h3>Discutons de votre projet</h3>
    <p>Disponible pour missions freelance à partir de maintenant · Remote ou Île-de-France</p>
    <div class="contact-links">
      <a href="mailto:kaba.sekouna@gmail.com" class="contact-link">📧 kaba.sekouna@gmail.com</a>
      <a href="tel:+33659022157" class="contact-link">📱 +33 06 59 02 21 57</a>
    </div>
  </div>

</div>
<footer>Sekouna KABA · Data Architect & Engineer Senior Freelance · Powered by DataLinkedAI</footer>
</body></html>"""
    return HTMLResponse(content=html)

# ══════════════════════════════════════════════════════
#  LANDING PAGE
# ══════════════════════════════════════════════════════

@app.get("/", include_in_schema=False)
async def root():
    db_chip  = ("ok", "PostgreSQL Supabase ✓")         if USE_POSTGRES   else ("warn", "SQLite (config DATABASE_URL)")
    sc_chip  = ("ok", "Scraping Apify ✓")              if APIFY_TOKEN    else ("warn", "Scraping Mock (config APIFY_TOKEN)")
    sec_chip = ("ok", "Sécurité API Key ✓")            if API_KEY        else ("warn", "Sans auth ⚠️")
    em_chip  = ("ok", f"Email {EMAIL_PROVIDER} ✓")     if EMAIL_PROVIDER != "none" else ("warn", "Email non configuré")
    ai_chip  = ("ok", f"IA {AI_PROVIDER} ✓")           if AI_PROVIDER != "none"    else ("warn", "IA non configurée ⚠️")

    def chip(cls, label):
        return f'<span class="chip {cls}">{label}</span>'

    chips = "".join([chip(*db_chip), chip(*sc_chip), chip(*sec_chip), chip(*em_chip), chip(*ai_chip),
                     chip("ok", "CV PDF ✓")])

    return HTMLResponse(f"""<!DOCTYPE html><html lang="fr"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>DataLinkedAI v3.0</title>
<link href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,700;12..96,800&family=Instrument+Sans:wght@400;600&family=Fira+Code:wght@400&display=swap" rel="stylesheet">
<style>*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Instrument Sans',sans-serif;background:linear-gradient(135deg,#F0F4FF,#fff,#F4F0FF);min-height:100vh;display:flex;align-items:center;justify-content:center;padding:2rem}}
.w{{max-width:900px;width:100%}}.hd{{text-align:center;margin-bottom:2.5rem}}
.logo{{width:52px;height:52px;background:linear-gradient(135deg,#1A56FF,#7C3AED);border-radius:13px;display:flex;align-items:center;justify-content:center;font-family:'Bricolage Grotesque';font-weight:800;color:#fff;font-size:1rem;margin:0 auto 1rem}}
h1{{font-family:'Bricolage Grotesque';font-size:clamp(1.8rem,4vw,2.8rem);color:#0C1630;letter-spacing:-.02em;margin-bottom:.5rem}}
h1 span{{background:linear-gradient(135deg,#1A56FF,#7C3AED);-webkit-background-clip:text;-webkit-text-fill-color:transparent}}
.sub{{color:#7B8DB5;font-size:.92rem;max-width:480px;margin:0 auto 1.25rem}}
.chips{{display:flex;gap:.4rem;justify-content:center;flex-wrap:wrap;margin-bottom:1.5rem}}
.chip{{font-family:'Fira Code';font-size:.62rem;padding:.28rem .7rem;border-radius:100px}}
.ok{{background:#E8FBF2;color:#00B96B;border:1px solid #00B96B25}}
.warn{{background:#FFFBEB;color:#F59E0B;border:1px solid #F59E0B25}}
.btns{{display:flex;gap:.65rem;justify-content:center;flex-wrap:wrap;margin-bottom:2.5rem}}
.btn{{padding:.65rem 1.3rem;border-radius:10px;font-weight:600;font-size:.85rem;cursor:pointer;text-decoration:none;display:inline-flex;align-items:center;gap:.4rem}}
.bp{{background:#1A56FF;color:#fff;box-shadow:0 4px 14px #1A56FF35}}.bs{{background:#EEF2FF;color:#1A56FF}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:.8rem;margin-bottom:2rem}}
.card{{background:#fff;border:1.5px solid #DDE3F0;border-radius:11px;padding:1.1rem;transition:all .2s}}
.card:hover{{border-color:#1A56FF50;box-shadow:0 6px 20px rgba(26,86,255,.09)}}
.card h3{{font-family:'Bricolage Grotesque';font-size:.85rem;color:#0C1630;margin-bottom:.25rem}}
.card p{{font-size:.72rem;color:#7B8DB5;line-height:1.5}}
.ep{{font-family:'Fira Code';font-size:.62rem;color:#1A56FF;margin-top:.4rem}}
footer{{text-align:center;font-family:'Fira Code';font-size:.6rem;color:#A0AFCF}}</style></head>
<body><div class="w"><div class="hd">
<div class="logo">DL</div>
<h1>DataLinked<span>AI</span></h1>
<p class="sub">Plateforme freelance data automatisée · Sekouna KABA · TJM 500-900€/j</p>
<div class="chips">{chips}</div>
<div class="btns">
<a href="/dashboard" class="btn bp">🖥️ Dashboard</a>
<a href="/docs" class="btn bs">📖 API Docs</a>
<a href="/health" class="btn bs">💚 Health</a>
</div></div>
<div class="grid">
<div class="card"><h3>🌅 Copilot Quotidien</h3><p>Digest lun-ven à 8h. Offres scorées + post LinkedIn.</p><div class="ep">POST /api/copilot/run/sync</div></div>
<div class="card"><h3>🤖 Agent Candidature</h3><p>Score 85% → CV adapté → email + PDF → relance J+5.</p><div class="ep">POST /api/agent/apply</div></div>
<div class="card"><h3>💰 Négociateur TJM</h3><p>Script exact pour passer de 650€ à 760€/j.</p><div class="ep">POST /api/negotiate</div></div>
<div class="card"><h3>📋 Analyse d'offres</h3><p>Score de match, TJM optimal, urgence, techs.</p><div class="ep">POST /api/offers/analyze</div></div>
<div class="card"><h3>📄 CV Adaptatif + PDF</h3><p>CV réécrit, cover letter, fichier PDF généré.</p><div class="ep">POST /api/cv/adapt</div></div>
<div class="card"><h3>💼 LinkedIn Studio</h3><p>Posts à la voix de Sekouna. 12 sujets × 6 formats.</p><div class="ep">POST /api/linkedin/generate</div></div>
<div class="card"><h3>📧 Prospection Email</h3><p>Email personnalisé par IA, envoi réel, suivi.</p><div class="ep">POST /api/email/compose</div></div>
<div class="card"><h3>📑 Proposition Client</h3><p>Devis professionnel complet en 60 secondes.</p><div class="ep">POST /api/proposal/generate</div></div>
</div>
<footer>DataLinkedAI v3.0 · Sekouna KABA · kaba.sekouna@gmail.com · <a href="/docs" style="color:#1A56FF">API Docs</a></footer>
</div></body></html>""")
