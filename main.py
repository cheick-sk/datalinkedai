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

import os, json, sqlite3, httpx, logging, smtplib, ssl, asyncio, io, re, base64, random, pathlib, hashlib, hmac, secrets
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
SKIP_AUTH      = {"/", "/health", "/docs", "/openapi.json", "/redoc", "/dashboard", "/favicon.ico"}
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

RESEND_API_KEY  = os.getenv("RESEND_API_KEY", "")
GMAIL_USER      = os.getenv("GMAIL_USER", "")
GMAIL_APP_PASS  = os.getenv("GMAIL_APP_PASS", "")
GMAIL_IMAP_HOST = "imap.gmail.com"
GMAIL_IMAP_PORT = 993
GMAIL_CHECK_REPLIES = os.getenv("GMAIL_CHECK_REPLIES", "true").lower() == "true"
EMAIL_FROM_NAME = os.getenv("EMAIL_FROM_NAME", "")
EMAIL_FROM_ADDR = os.getenv("EMAIL_FROM_ADDR", GMAIL_USER or "sekouna@datalinkedai.com")
EMAIL_PROVIDER  = "resend" if RESEND_API_KEY else ("gmail" if GMAIL_USER and GMAIL_APP_PASS else "none")

COPILOT_EMAIL        = os.getenv("COPILOT_EMAIL", GMAIL_USER or "kaba.sekouna@gmail.com")
COPILOT_HOUR         = int(os.getenv("COPILOT_HOUR", "8"))
AUTO_APPLY_ENABLED   = os.getenv("AUTO_APPLY", "false").lower() == "true"
AUTO_APPLY_MIN_SCORE = int(os.getenv("AUTO_APPLY_MIN_SCORE", "85"))
FOLLOWUP_DAYS        = int(os.getenv("FOLLOWUP_DAYS", "5"))
GMAIL_PARSE_ENABLED  = os.getenv("GMAIL_PARSE_ENABLED", "true").lower() == "true"
APIFY_TOKEN          = os.getenv("APIFY_TOKEN", "")
PUSHOVER_TOKEN       = os.getenv("PUSHOVER_TOKEN", "")
PUSHOVER_USER        = os.getenv("PUSHOVER_USER", "")
HUNTER_API_KEY       = os.getenv("HUNTER_API_KEY", "")
DASHBOARD_URL        = os.getenv("DASHBOARD_URL", "https://datalinkedai.onrender.com/dashboard")

# ── Notifications multi-canal ─────────────────────────────────────
# Twilio — SMS & WhatsApp (gratuit avec sandbox WhatsApp)
TWILIO_SID          = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_TOKEN        = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_FROM_SMS     = os.getenv("TWILIO_FROM_SMS", "")        # ex: +15551234567
TWILIO_FROM_WA      = os.getenv("TWILIO_FROM_WHATSAPP", "whatsapp:+14155238886")  # sandbox Twilio
NOTIF_PHONE         = os.getenv("NOTIF_PHONE", "")            # ton numéro: +33612345678
NOTIF_WHATSAPP      = os.getenv("NOTIF_WHATSAPP", "")         # ton numéro WA: +33612345678
# Canaux activés (virgule-séparés): email,whatsapp,sms
NOTIF_CHANNELS      = os.getenv("NOTIF_CHANNELS", "email").lower().split(",")
# Secret pour les liens de validation en un clic (généré au démarrage si absent)
# Stable fallback: derived from API_KEY so tokens survive restarts
_approval_fallback = __import__("hashlib").sha256((os.getenv("API_KEY","datalinked") + "-approval").encode()).hexdigest()[:32]
APPROVAL_SECRET     = os.getenv("APPROVAL_SECRET", _approval_fallback)

# ── LinkedIn OAuth (publication automatique) ────────────────────────
LINKEDIN_CLIENT_ID     = os.getenv("LINKEDIN_CLIENT_ID", "")
LINKEDIN_CLIENT_SECRET = os.getenv("LINKEDIN_CLIENT_SECRET", "")
LINKEDIN_ACCESS_TOKEN  = os.getenv("LINKEDIN_ACCESS_TOKEN", "")   # token long-lived (60j)
LINKEDIN_PERSON_ID     = os.getenv("LINKEDIN_PERSON_ID", "")      # urn:li:person:XXXXXX
LINKEDIN_AUTO_POST     = os.getenv("LINKEDIN_AUTO_POST", "false").lower() == "true"
LINKEDIN_POST_HOUR     = int(os.getenv("LINKEDIN_POST_HOUR", "9"))   # heure de publication auto
LINKEDIN_POST_DAYS     = os.getenv("LINKEDIN_POST_DAYS", "mon,wed,fri")  # jours de publication

# ── Scrapers supplémentaires ─────────────────────────────────────────
ADZUNA_APP_ID  = os.getenv("ADZUNA_APP_ID", "")    # gratuit : adzuna.com/api
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY", "")
SCRAPERS_ENABLED = os.getenv("SCRAPERS_ENABLED",
    "wttj,indeed,remotive,freelance_com,adzuna,jobijoba,pole_emploi,hellowork,malt,comet,github_jobs"
).split(",")   # désactiver un scraper : retirer son nom de la liste

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
# ── Profil par défaut (écrasé dynamiquement depuis la DB après init_db) ──────
DEFAULT_PROFILE = {
    "name": "KABA Sekouna", "title": "Data Engineer Senior — Lead Tech",
    "email": "kaba.sekouna@gmail.com", "phone": "+33 06 59 02 21 57",
    "location": "Montreuil, Ile-de-France",
    "sector": "Data & Tech",
    "work_type": "freelance",          # freelance | salarie | both
    "availability": "Disponible",
    "tjm_min": 500, "tjm_max": 900, "tjm_current": 650, "tjm_target": 760,
    "salary_min": 0, "salary_max": 0,  # pour mode salarié
    "years_xp": 8,
    "bio": "Data Architect & Engineer Senior freelance avec 8 ans d'expérience sur des missions grands comptes (SACEM, Thales, Accor). Spécialisé cloud data platforms, Snowflake, dbt, Airflow.",
    "linkedin_url": "",
    "github_url": "",
    "portfolio_url": "",
    "experiences": [
        {"company": "SACEM",          "period": "Juin 2024 → Present",  "role": "Data Architect/Engineer",
         "stack": "Snowflake, PySpark, Python, Airflow, DBT Core, S3, Kafka, Lambda AWS, SnowPipe, CI/CD GitLab"},
        {"company": "MSO-SOFT",       "period": "Dec 2023 - Juil 2024", "role": "Data Architect/Engineer",
         "stack": "Docker, DBT-Core, Airflow, Apache Superset, Minio, PostgreSQL, GitLab CI/CD"},
        {"company": "Thales Group",   "period": "Oct 2022 - Dec 2023",  "role": "Data Architect Senior",
         "stack": "GCP BigQuery, PySpark, Talend, Dataiku, Azure ADLS Gen2, AWS RDS"},
        {"company": "Accor",          "period": "Nov 2021 - Oct 2022",  "role": "Consultant Data Engineer/BI",
         "stack": "Snowflake, DBT Core, Terraform, Talend Cloud, AWS Glue, Azure"},
        {"company": "ARCADE (Keyrus)","period": "Fev 2021 - Oct 2021",  "role": "Tech Lead BI/Data Engineer",
         "stack": "Azure DevOps, Snowflake, Azure Storage, Power BI, Talend 7"},
    ],
    "skills": ["Snowflake", "DBT Core", "Apache Airflow", "PySpark", "Python", "Kafka",
               "AWS", "Azure", "GCP", "Terraform", "GitLab CI/CD", "Docker",
               "PostgreSQL", "Power BI", "Tableau", "Dataiku"],
    "certifications": ["AWS Certified", "GCP Professional Data Engineer", "Snowflake SnowPro Core"],
    "languages": [{"lang": "Français", "level": "Natif"}, {"lang": "Anglais", "level": "Courant"}],
    "education": [{"degree": "Master Informatique / Data Engineering", "school": "", "year": "2016"}],
    "soft_skills": ["Leadership technique", "Communication client", "Adaptabilité", "Rigueur"],
    "remote_ok": True,
    "relocation_ok": False,
    "keywords": [],   # mots-clés supplémentaires pour le scraping
}

PROFILE: dict = dict(DEFAULT_PROFILE)   # sera écrasé par load_profile() après init_db

def load_profile():
    """Charge le profil depuis la DB (table user_profile). Fallback sur DEFAULT_PROFILE."""
    global PROFILE
    try:
        with db_conn() as conn:
            row = db_exec(conn, "SELECT data FROM user_profile WHERE id=1").fetchone()
            if row:
                PROFILE = json.loads(row[0])
                logger.info(f"Profil chargé depuis DB: {PROFILE.get('name','?')}")
                return
    except Exception as e:
        logger.warning(f"load_profile: {e} — utilise DEFAULT_PROFILE")
    PROFILE = dict(DEFAULT_PROFILE)

# ── Presets sectoriels ────────────────────────────────────────────────────────
SECTOR_PRESETS = {
    "Data & Tech": {
        "search_queries": ["Data Engineer freelance France", "Data Architect senior remote", "Lead Data Engineer Snowflake DBT"],
        "platforms": ["LinkedIn", "WTTJ", "Malt", "Comet", "RemoteOK"],
        "tjm_range": [400, 1000],
        "post_topics": ["Snowflake vs Databricks", "dbt Core en production", "Architecture Medallion", "PySpark optimisation", "Kafka streaming"],
    },
    "Développement Web & Mobile": {
        "search_queries": ["Développeur React freelance", "Lead Dev fullstack remote France", "Architecte logiciel senior freelance"],
        "platforms": ["LinkedIn", "WTTJ", "Malt", "Comet", "Upwork"],
        "tjm_range": [350, 850],
        "post_topics": ["Next.js 14 en prod", "React vs Vue en 2025", "API REST vs GraphQL", "Micro-frontends retour d'expérience"],
    },
    "Design & UX": {
        "search_queries": ["UX Designer freelance France", "Product Designer senior remote", "UI/UX Lead freelance"],
        "platforms": ["LinkedIn", "WTTJ", "Malt", "Dribbble", "Behance"],
        "tjm_range": [300, 750],
        "post_topics": ["Design System en pratique", "UX Research vs assumption", "Figma vs Sketch en 2025", "Comment convaincre un dev"],
    },
    "Marketing & Growth": {
        "search_queries": ["Growth Hacker freelance France", "CMO freelance remote", "SEO consultant senior"],
        "platforms": ["LinkedIn", "WTTJ", "Malt", "Indeed"],
        "tjm_range": [300, 700],
        "post_topics": ["Growth loops qui marchent vraiment", "SEO technique en 2025", "Acquisition payante vs organique", "Email marketing vs social"],
    },
    "Finance & Conseil": {
        "search_queries": ["Consultant finance freelance", "Expert comptable freelance", "Analyste financier senior remote"],
        "platforms": ["LinkedIn", "Indeed", "Malt"],
        "tjm_range": [400, 1200],
        "post_topics": ["Due diligence en pratique", "Modélisation financière avancée", "Freelance vs cabinet", "Lever du capital en 2025"],
    },
    "Santé & Biotech": {
        "search_queries": ["Consultant santé freelance", "Data Scientist médical remote", "Chef de projet Santé freelance"],
        "platforms": ["LinkedIn", "Indeed", "WTTJ"],
        "tjm_range": [400, 900],
        "post_topics": ["IA en médecine réalités", "RGPD données de santé", "Interopérabilité HL7/FHIR", "Essais cliniques et data"],
    },
    "Industrie & Ingénierie": {
        "search_queries": ["Ingénieur industriel freelance", "Chef de projet industrie remote", "Consultant lean manufacturing"],
        "platforms": ["LinkedIn", "Indeed", "Malt"],
        "tjm_range": [400, 900],
        "post_topics": ["Industrie 4.0 retour terrain", "Lean vs Agile en production", "IoT industriel en pratique", "Supply chain resilience"],
    },
    "Juridique & Compliance": {
        "search_queries": ["Juriste freelance France", "DPO freelance remote", "Compliance officer consultant"],
        "platforms": ["LinkedIn", "Indeed", "Malt"],
        "tjm_range": [450, 1100],
        "post_topics": ["RGPD 5 ans après", "AI Act impacts pratiques", "Contrat freelance pièges", "Compliance vs business"],
    },
    "RH & Management": {
        "search_queries": ["DRH freelance France", "Consultant RH remote", "Coach professionnel certifié freelance"],
        "platforms": ["LinkedIn", "Indeed", "Malt"],
        "tjm_range": [350, 800],
        "post_topics": ["Recrutement en 2025", "Télétravail leçons apprises", "Manager à distance", "Talent retention stratégies"],
    },
    "Autre / Personnalisé": {
        "search_queries": [],
        "platforms": ["LinkedIn", "Indeed", "WTTJ", "Malt"],
        "tjm_range": [300, 900],
        "post_topics": ["Mon domaine en 2025", "Freelance conseils pratiques", "Expertise rare = valeur"],
    },
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
        "CREATE TABLE IF NOT EXISTS pending_approvals(id SERIAL PRIMARY KEY,type TEXT,title TEXT,preview TEXT,payload TEXT,status TEXT DEFAULT 'pending',token TEXT,approved_at TIMESTAMP,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        # ── NOUVELLES TABLES v10 ──────────────────────────────────────────────
        # Profil utilisateur configurable (1 seule ligne, id=1)
        "CREATE TABLE IF NOT EXISTS user_profile(id INTEGER PRIMARY KEY DEFAULT 1,data TEXT NOT NULL,updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        # Sessions de mock interview
        "CREATE TABLE IF NOT EXISTS interview_sessions(id SERIAL PRIMARY KEY,offer_title TEXT,offer_description TEXT,difficulty TEXT DEFAULT 'medium',messages TEXT DEFAULT '[]',score INTEGER,feedback TEXT,status TEXT DEFAULT 'active',created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        # Cache scraping
        "CREATE TABLE IF NOT EXISTS scrape_cache(id SERIAL PRIMARY KEY,source TEXT,query_hash TEXT UNIQUE,results TEXT,scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        # ── NOUVELLES TABLES v11 ──────────────────────────────────────────────
        # Calendrier éditorial LinkedIn — posts planifiés
        "CREATE TABLE IF NOT EXISTS linkedin_schedule(id SERIAL PRIMARY KEY,post_id INTEGER,content TEXT NOT NULL,topic TEXT,format TEXT,scheduled_at TIMESTAMP NOT NULL,published_at TIMESTAMP,status TEXT DEFAULT 'scheduled',linkedin_post_id TEXT,error TEXT,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        # Statistiques des scrapers par source (succès, nb offres, latence)
        "CREATE TABLE IF NOT EXISTS scraper_stats(id SERIAL PRIMARY KEY,source TEXT,run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,offers_found INTEGER DEFAULT 0,latency_ms INTEGER DEFAULT 0,success INTEGER DEFAULT 1,error TEXT)",
        # Token LinkedIn OAuth persisté en DB (survit aux redémarrages)
        "CREATE TABLE IF NOT EXISTS linkedin_tokens(id INTEGER PRIMARY KEY DEFAULT 1,access_token TEXT,person_id TEXT,expires_at TEXT,updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    ]
    if not USE_POSTGRES:
        tables = [
            t.replace("SERIAL PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT")
             .replace("TIMESTAMP DEFAULT CURRENT_TIMESTAMP", "TEXT DEFAULT(datetime('now'))")
             .replace("TIMESTAMP,", "TEXT,")
             .replace("TIMESTAMP)", "TEXT)")
             .replace("INTEGER PRIMARY KEY DEFAULT 1", "INTEGER PRIMARY KEY")
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
    # ── Migrations colonnes v12 (ALTER TABLE idempotent) ──────────────
    _migrations = [
        "ALTER TABLE pending_approvals ADD COLUMN token TEXT",
        "ALTER TABLE pending_approvals ADD COLUMN notif_sent INTEGER DEFAULT 0",
        "ALTER TABLE pending_approvals ADD COLUMN rejected_at TIMESTAMP",
    ]
    with db_conn() as conn:
        for mig in _migrations:
            try: db_exec(conn, mig)
            except Exception: pass  # colonne existe déjà → OK

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


def _make_approval_token(approval_id: int) -> str:
    """Génère un token HMAC-SHA256 signé — unique par approbation."""
    msg = f"{approval_id}:{APPROVAL_SECRET}".encode()
    return hmac.new(APPROVAL_SECRET.encode(), msg, hashlib.sha256).hexdigest()[:32]

def _verify_approval_token(approval_id: int, token: str) -> bool:
    expected = _make_approval_token(approval_id)
    return hmac.compare_digest(expected, token)

# ── Twilio SMS / WhatsApp ──────────────────────────────────────────
async def send_sms(to: str, body: str) -> bool:
    """Envoie un SMS via Twilio."""
    if not (TWILIO_SID and TWILIO_TOKEN and TWILIO_FROM_SMS and to):
        return False
    try:
        async with httpx.AsyncClient(timeout=10.0) as cl:
            r = await cl.post(
                f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json",
                auth=(TWILIO_SID, TWILIO_TOKEN),
                data={"From": TWILIO_FROM_SMS, "To": to, "Body": body[:1600]},
            )
            ok = r.status_code in (200, 201)
            if not ok: logger.warning(f"SMS Twilio error {r.status_code}: {r.text[:200]}")
            return ok
    except Exception as e:
        logger.warning(f"send_sms error: {e}")
        return False

async def send_whatsapp(to: str, body: str) -> bool:
    """Envoie un message WhatsApp via Twilio Sandbox."""
    if not (TWILIO_SID and TWILIO_TOKEN and to):
        return False
    # Normalise le numéro au format whatsapp:+33...
    wa_to = f"whatsapp:{to}" if not to.startswith("whatsapp:") else to
    try:
        async with httpx.AsyncClient(timeout=10.0) as cl:
            r = await cl.post(
                f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json",
                auth=(TWILIO_SID, TWILIO_TOKEN),
                data={"From": TWILIO_FROM_WA, "To": wa_to, "Body": body[:1600]},
            )
            ok = r.status_code in (200, 201)
            if not ok: logger.warning(f"WhatsApp Twilio error {r.status_code}: {r.text[:200]}")
            return ok
    except Exception as e:
        logger.warning(f"send_whatsapp error: {e}")
        return False

async def notify(title: str, message: str, url: str = "", priority: int = 0) -> bool:
    """Pushover legacy — remplacé par notify_all() mais conservé pour compatibilité."""
    if not PUSHOVER_TOKEN or not PUSHOVER_USER:
        return False
    try:
        async with httpx.AsyncClient(timeout=10.0) as cl:
            payload = {"token": PUSHOVER_TOKEN, "user": PUSHOVER_USER,
                       "title": title[:100], "message": message[:512], "priority": priority}
            if url: payload["url"] = url; payload["url_title"] = "Ouvrir le Dashboard"
            r = await cl.post("https://api.pushover.net/1/messages.json", data=payload)
            return r.status_code == 200
    except Exception as e:
        logger.warning(f"Pushover failed: {e}")
        return False

def _approval_email_html(approvals: list, offers_html: str, post_preview: str, now_str: str, dashboard_url: str) -> str:
    """Email de digest avec boutons ✅/❌ en un clic par item."""
    items_html = ""
    for a in approvals:
        token   = a.get("token","")
        aid     = a.get("id",0)
        title   = a.get("title","")
        preview = a.get("preview","")[:300]
        atype   = a.get("type","")
        icon    = "💼" if atype == "linkedin_post" else "📨"
        score   = f'<span style="background:#00B96B;color:white;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:700">{a.get("score","")}%</span>' if a.get("score") else ""
        base    = DASHBOARD_URL.replace("/dashboard","")
        approve_url = f"{base}/approve/{aid}/{token}"
        reject_url  = f"{base}/reject/{aid}/{token}"
        items_html += f"""
<div style="background:#F8FAFF;border:1px solid #E0E7FF;border-radius:10px;padding:16px;margin-bottom:12px">
  <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
    <span style="font-size:1.2em">{icon}</span>
    <strong style="font-size:14px;color:#1a1a2e">{title}</strong>
    {score}
  </div>
  <div style="font-size:12px;color:#555;background:#fff;padding:10px;border-radius:6px;border-left:3px solid #1A56FF;white-space:pre-wrap;margin-bottom:12px;max-height:100px;overflow:hidden">{preview}</div>
  <div style="display:flex;gap:8px">
    <a href="{approve_url}" style="background:#00B96B;color:white;padding:10px 20px;border-radius:8px;text-decoration:none;font-weight:700;font-size:13px;display:inline-block">✅ VALIDER ET ENVOYER</a>
    <a href="{reject_url}" style="background:#F3F4F6;color:#555;padding:10px 20px;border-radius:8px;text-decoration:none;font-weight:600;font-size:13px;display:inline-block;border:1px solid #ddd">❌ Rejeter</a>
  </div>
</div>"""

    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:600px;margin:0 auto;padding:20px;background:#f5f7fa;color:#1a1a2e">
  <div style="background:linear-gradient(135deg,#1A56FF,#7C3AED);padding:24px;border-radius:14px;margin-bottom:20px;text-align:center">
    <div style="font-size:28px;margin-bottom:6px">🤖</div>
    <h1 style="color:white;margin:0;font-size:20px">DataLinkedAI — Validation requise</h1>
    <p style="color:rgba(255,255,255,.85);margin:6px 0 0;font-size:13px">{len(approvals)} élément(s) en attente · {now_str}</p>
  </div>

  <div style="background:white;border-radius:12px;padding:20px;margin-bottom:16px;box-shadow:0 1px 3px rgba(0,0,0,.08)">
    <h2 style="font-size:15px;color:#1A56FF;margin:0 0 14px">👇 Valide ou rejette en un clic</h2>
    {items_html}
  </div>

  {"<div style='background:white;border-radius:12px;padding:20px;margin-bottom:16px'><h3 style='font-size:14px;color:#1a1a2e;margin:0 0 10px'>📊 Top offres analysées</h3>" + offers_html + "</div>" if offers_html else ""}
  {"<div style='background:white;border-radius:12px;padding:16px;margin-bottom:16px'><h3 style='font-size:14px;color:#1a1a2e;margin:0 0 8px'>💼 Post LinkedIn</h3><div style='font-size:12px;color:#555;white-space:pre-wrap;background:#F4F6FB;padding:12px;border-radius:6px'>" + post_preview[:400] + "...</div></div>" if post_preview else ""}

  <div style="text-align:center;margin:20px 0">
    <a href="{dashboard_url}" style="background:#1A56FF;color:white;padding:12px 28px;border-radius:8px;text-decoration:none;font-size:13px;font-weight:600">📊 Ouvrir le Dashboard</a>
  </div>
  <p style="text-align:center;font-size:11px;color:#aaa">DataLinkedAI · Plateforme freelance automatisée</p>
</body></html>"""

async def notify_all(approvals: list, offers_html: str = "", post_preview: str = "", now_str: str = "") -> dict:
    """Envoie les notifications de validation sur tous les canaux configurés.
    Retourne un dict avec le statut de chaque canal."""
    if not approvals:
        return {}
    results = {}
    now_str = now_str or datetime.now().strftime("%A %d %B %Y")
    n = len(approvals)
    cands  = [a for a in approvals if a["type"] == "candidature"]
    posts  = [a for a in approvals if a["type"] == "linkedin_post"]
    dashboard_url = os.getenv("DASHBOARD_URL", DASHBOARD_URL)

    # ── 1. EMAIL (toujours tenté si configuré) ──────────────────────────
    if "email" in NOTIF_CHANNELS and COPILOT_EMAIL and EMAIL_PROVIDER != "none":
        try:
            html = _approval_email_html(approvals, offers_html, post_preview, now_str, dashboard_url)
            txt  = f"[DataLinkedAI] {n} élément(s) à valider — {now_str}\n\n"
            txt += "\n".join(f"- {a['title']}" for a in approvals)
            txt += f"\n\nDashboard : {dashboard_url}"
            await send_email(
                COPILOT_EMAIL, PROFILE.get("name","Utilisateur"),
                f"✅ [{n} validation(s)] DataLinkedAI — {now_str}",
                html, txt,
            )
            results["email"] = "✅ envoyé"
        except Exception as e:
            results["email"] = f"❌ {e}"

    # ── 2. WHATSAPP ───────────────────────────────────────────────────
    if "whatsapp" in NOTIF_CHANNELS and NOTIF_WHATSAPP:
        try:
            base = DASHBOARD_URL.replace("/dashboard","")
            wa_body = f"🤖 *DataLinkedAI* — {n} validation(s) en attente\n\n"
            for a in approvals:
                token = a.get("token","")
                aid   = a.get("id",0)
                icon  = "💼" if a["type"] == "linkedin_post" else "📨"
                wa_body += f"{icon} {a['title']}\n"
                if token:
                    wa_body += f"✅ Valider : {base}/approve/{aid}/{token}\n"
                    wa_body += f"❌ Rejeter : {base}/reject/{aid}/{token}\n"
                wa_body += "\n"
            wa_body += f"📊 Dashboard : {dashboard_url}"
            ok = await send_whatsapp(NOTIF_WHATSAPP, wa_body)
            results["whatsapp"] = "✅ envoyé" if ok else "❌ échec (vérifie TWILIO_ACCOUNT_SID)"
        except Exception as e:
            results["whatsapp"] = f"❌ {e}"

    # ── 3. SMS ────────────────────────────────────────────────────────
    if "sms" in NOTIF_CHANNELS and NOTIF_PHONE:
        try:
            base = DASHBOARD_URL.replace("/dashboard","")
            sms_body = f"DataLinkedAI: {n} validation(s) en attente.\n"
            for a in approvals[:2]:  # SMS limité en longueur
                token = a.get("token","")
                aid   = a.get("id",0)
                sms_body += f"- {a['title'][:50]}\n"
                if token:
                    sms_body += f"OK: {base}/approve/{aid}/{token}\n"
            sms_body += f"Dashboard: {dashboard_url}"
            ok = await send_sms(NOTIF_PHONE, sms_body)
            results["sms"] = "✅ envoyé" if ok else "❌ échec (vérifie TWILIO credentials)"
        except Exception as e:
            results["sms"] = f"❌ {e}"

    # ── 4. Pushover (legacy) ──────────────────────────────────────────
    if "pushover" in NOTIF_CHANNELS and PUSHOVER_TOKEN and PUSHOVER_USER:
        ok = await notify(
            f"DataLinkedAI — {n} élément(s) à valider",
            f"{len(cands)} candidature(s) + {len(posts)} post(s) LinkedIn",
            url=dashboard_url, priority=0,
        )
        results["pushover"] = "✅ envoyé" if ok else "❌ échec"

    logger.info(f"notify_all: {results}")
    return results

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
    <strong>{cv_name}</strong><br>{cv_title}<br>
    TJM 500-900€/j · Full Remote · Ile-de-France<br>
    kaba.sekouna@gmail.com | +33 06 59 02 21 57
  </div>
</body></html>"""

# ══════════════════════════════════════════════════════
#  FIX-C : CV PDF — async (ne bloque plus l'event loop)
# ══════════════════════════════════════════════════════

def _generate_cv_pdf_sync(cv_data: dict, offer_title: str = "") -> bytes:
    """CV PDF professionnel — layout 2 colonnes stable (sidebar + contenu)."""
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles    import ParagraphStyle
        from reportlab.lib.units     import cm
        from reportlab.lib           import colors
        from reportlab.platypus      import (SimpleDocTemplate, Paragraph, Spacer,
                                             HRFlowable, Table, TableStyle,
                                             KeepInFrame)
        from reportlab.lib.enums     import TA_LEFT, TA_CENTER, TA_JUSTIFY, TA_RIGHT
    except ImportError:
        logger.warning("reportlab non installe — PDF desactive")
        return b""

    # ── Couleurs ────────────────────────────────────────────────────────
    C_BLUE   = colors.HexColor("#1A3A6B")
    C_ACCENT = colors.HexColor("#2563EB")
    C_GOLD   = colors.HexColor("#F59E0B")
    C_DARK   = colors.HexColor("#0F172A")
    C_MUTED  = colors.HexColor("#64748B")
    C_WHITE  = colors.white
    C_DIV    = colors.HexColor("#CBD5E1")
    C_LIGHT_BLUE = colors.HexColor("#BFDBFE")
    C_SKY    = colors.HexColor("#93C5FD")

    W, H = A4   # 595 x 842 pts
    SIDEBAR_W = 175
    MARGIN    = 14
    HDR_H     = 95

    # ── Données (dynamiques depuis PROFILE) ─────────────────────────────
    p_name   = PROFILE.get("name", "Candidat")
    p_title  = PROFILE.get("title", "Consultant Freelance")
    p_email  = PROFILE.get("email", "")
    p_phone  = PROFILE.get("phone", "")
    p_city   = PROFILE.get("location", "")
    p_github = PROFILE.get("github", "")
    p_linkedin = PROFILE.get("linkedin", "")

    title_adapted  = cv_data.get("title_adapted", p_title)
    accroche       = cv_data.get("accroche", PROFILE.get("bio",""))
    kpis           = cv_data.get("kpis", ["Expérience confirmée", "Disponible rapidement", "Résultats mesurables"])
    comp_core      = cv_data.get("competences_core", cv_data.get("competences", PROFILE.get("skills",[])))[:8]
    comp_sec       = cv_data.get("competences_secondaires", PROFILE.get("skills",[]))[:8]
    experiences    = cv_data.get("experiences", PROFILE.get("experiences", []))
    formations     = cv_data.get("formations", PROFILE.get("formations", []))
    certifications = cv_data.get("certifications", PROFILE.get("certifications", []))
    langues        = cv_data.get("langues", PROFILE.get("languages", [{"langue":"Français","niveau":"Natif"}]))
    soft_skills    = cv_data.get("soft_skills", ["Autonomie","Communication","Rigueur","Adaptabilité"])
    tjm            = cv_data.get("tjm_suggest", f"{PROFILE.get('tjm_min',500)}-{PROFILE.get('tjm_max',700)}€/j")

    # Normalise les expériences
    exps = []
    for e in (experiences or [])[:5]:
        exps.append({
            "company": e.get("company",""),
            "role":    e.get("role", e.get("title","")),
            "period":  e.get("period", e.get("date","")),
            "context": e.get("context", e.get("pitch","")),
            "missions": e.get("missions", [e.get("pitch","")]) if isinstance(e.get("missions"), list) else [e.get("missions","")],
            "stack":   e.get("stack",""),
        })

    # ── Style helpers ────────────────────────────────────────────────────
    def PS(name, **kw):
        return ParagraphStyle(name, **kw)

    sName   = PS("name",  fontSize=22, fontName="Helvetica-Bold", textColor=C_WHITE,  leading=26)
    sTitle  = PS("title", fontSize=10, fontName="Helvetica",      textColor=C_SKY,   leading=14)
    sTjm    = PS("tjm",   fontSize=10, fontName="Helvetica-Bold", textColor=C_GOLD,  leading=13)
    sInfo   = PS("info",  fontSize=8,  fontName="Helvetica",      textColor=C_LIGHT_BLUE, leading=11)
    sGit    = PS("git",   fontSize=7,  fontName="Helvetica",      textColor=C_SKY,   leading=10)

    sSidH   = PS("sidH",  fontSize=7.5,fontName="Helvetica-Bold", textColor=C_SKY,   spaceBefore=12,spaceAfter=4,leading=11)
    sSidTxt = PS("sidTxt",fontSize=8,  fontName="Helvetica",      textColor=C_WHITE, leading=12,spaceAfter=2)
    sSidMut = PS("sidMut",fontSize=7,  fontName="Helvetica",      textColor=colors.HexColor("#94A3B8"),leading=10,spaceAfter=2)

    sSecH   = PS("secH",  fontSize=8.5,fontName="Helvetica-Bold", textColor=C_ACCENT,spaceBefore=12,spaceAfter=3,leading=11)
    sAccr   = PS("accr",  fontSize=8.5,fontName="Helvetica",      textColor=C_DARK,  leading=13,spaceAfter=4,alignment=TA_JUSTIFY)
    sExpCo  = PS("expCo", fontSize=10, fontName="Helvetica-Bold", textColor=C_DARK,  leading=13,spaceAfter=1)
    sExpRo  = PS("expRo", fontSize=8.5,fontName="Helvetica",      textColor=C_ACCENT,leading=11,spaceAfter=1)
    sExpDt  = PS("expDt", fontSize=7.5,fontName="Helvetica",      textColor=C_MUTED, leading=10,spaceAfter=2)
    sExpCtx = PS("expCtx",fontSize=8,  fontName="Helvetica",      textColor=C_MUTED, leading=11,spaceAfter=2,alignment=TA_JUSTIFY)
    sMiss   = PS("miss",  fontSize=8,  fontName="Helvetica",      textColor=C_DARK,  leading=12,leftIndent=8,spaceAfter=1)
    sStack  = PS("stack", fontSize=7.5,fontName="Helvetica-Bold", textColor=C_ACCENT,leading=10,spaceAfter=5)
    sFormD  = PS("formD", fontSize=9,  fontName="Helvetica-Bold", textColor=C_DARK,  leading=12,spaceAfter=1)
    sFormS  = PS("formS", fontSize=8,  fontName="Helvetica",      textColor=C_MUTED, leading=11,spaceAfter=3)

    def HR_main(): return HRFlowable(width="100%", thickness=0.5, color=C_DIV, spaceAfter=5, spaceBefore=0)
    def HR_sid():  return HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#2D4A7A"), spaceAfter=4, spaceBefore=0)

    def safe(txt):
        """Échappe les caractères XML pour ReportLab."""
        return str(txt).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

    # ── Contenu HEADER ───────────────────────────────────────────────────
    contact_parts = [p for p in [p_email, p_phone, p_city] if p]
    contact_str   = "  |  ".join(contact_parts) if contact_parts else ""
    link_parts    = [p for p in [p_github, p_linkedin] if p]
    link_str      = "  |  ".join(link_parts) if link_parts else ""

    hdr_left_items = [
        Paragraph(safe(p_name), sName),
        Paragraph(safe(title_adapted), sTitle),
    ]
    hdr_right_items = [
        Paragraph(safe(f"TJM : {tjm}"), sTjm),
    ]
    if contact_str:
        hdr_right_items.append(Spacer(1, 3))
        hdr_right_items.append(Paragraph(safe(contact_str), sInfo))
    if link_str:
        hdr_right_items.append(Paragraph(safe(link_str), sGit))

    HDR_W  = W - 2 * MARGIN
    HDR_HH = HDR_H - 2 * MARGIN
    hdr_l_frame = KeepInFrame(HDR_W * 0.62, HDR_HH, hdr_left_items,  mode="shrink")
    hdr_r_frame = KeepInFrame(HDR_W * 0.35, HDR_HH, hdr_right_items, mode="shrink")
    hdr_table   = Table([[hdr_l_frame, hdr_r_frame]],
                        colWidths=[HDR_W * 0.62, HDR_W * 0.38])
    hdr_table.setStyle(TableStyle([
        ("VALIGN",       (0,0),(-1,-1),"MIDDLE"),
        ("LEFTPADDING",  (0,0),(-1,-1), 0),
        ("RIGHTPADDING", (0,0),(-1,-1), 0),
        ("TOPPADDING",   (0,0),(-1,-1), 0),
        ("BOTTOMPADDING",(0,0),(-1,-1), 0),
        ("ALIGN",        (1,0),(1,0),  "RIGHT"),
    ]))

    # ── Contenu SIDEBAR ──────────────────────────────────────────────────
    def sid_sec(title):
        return [Paragraph(title.upper(), sSidH), HR_sid()]

    sid = []
    sid += sid_sec("Chiffres clés")
    for k in kpis[:3]:
        sid.append(Paragraph(f"• {safe(k)}", sSidTxt))

    sid += sid_sec("Compétences")
    for c in comp_core:
        sid.append(Paragraph(f"▸ {safe(c)}", sSidTxt))

    if comp_sec:
        sid += sid_sec("Autres")
        sid.append(Paragraph("  ·  ".join(safe(c) for c in comp_sec), sSidMut))

    if soft_skills:
        sid += sid_sec("Soft skills")
        for s in soft_skills:
            sid.append(Paragraph(f"◦ {safe(s)}", sSidMut))

    if langues:
        sid += sid_sec("Langues")
        for lang in (langues if isinstance(langues, list) else []):
            if isinstance(lang, dict):
                sid.append(Paragraph(f"{safe(lang.get('langue',''))} — {safe(lang.get('niveau',''))}", sSidTxt))
            else:
                sid.append(Paragraph(safe(str(lang)), sSidTxt))

    if certifications:
        sid += sid_sec("Certifications")
        for cert in (certifications if isinstance(certifications, list) else [])[:4]:
            sid.append(Paragraph(f"✓ {safe(cert)}", sSidMut))

    # ── Contenu PRINCIPAL ────────────────────────────────────────────────
    def main_sec(title):
        return [Paragraph(title.upper(), sSecH), HR_main()]

    main = []

    if accroche:
        main += main_sec("Profil")
        main.append(Paragraph(safe(accroche), sAccr))

    main += main_sec("Expériences Professionnelles")
    for exp in exps:
        main.append(Paragraph(safe(exp["company"]), sExpCo))
        if exp["role"]:
            main.append(Paragraph(safe(exp["role"]), sExpRo))
        if exp["period"]:
            main.append(Paragraph(safe(exp["period"]), sExpDt))
        if exp["context"]:
            main.append(Paragraph(safe(exp["context"]), sExpCtx))
        for m in (exp["missions"] or [])[:4]:
            if m and str(m).strip():
                main.append(Paragraph(f"→  {safe(str(m))}", sMiss))
        if exp["stack"]:
            main.append(Paragraph(f"Stack : {safe(exp['stack'])}", sStack))
        main.append(Spacer(1, 4))

    if formations:
        main += main_sec("Formation")
        for f in (formations if isinstance(formations, list) else []):
            d = f.get("diplome","") if isinstance(f, dict) else str(f)
            main.append(Paragraph(safe(d), sFormD))
            if isinstance(f, dict):
                ec = f.get("ecole",""); an = f.get("annee","")
                if ec or an:
                    main.append(Paragraph(safe(f"{ec}  {an}".strip()), sFormS))

    # ── Canvas callback (dessin fond) ────────────────────────────────────
    def draw_bg(canv, doc):
        canv.saveState()
        canv.setFillColor(C_BLUE)
        canv.rect(0, 0, SIDEBAR_W, H, fill=1, stroke=0)
        canv.setFillColor(C_ACCENT)
        canv.rect(0, H - HDR_H, W, HDR_H, fill=1, stroke=0)
        canv.setStrokeColor(colors.HexColor("#1E40AF"))
        canv.setLineWidth(0.5)
        canv.line(SIDEBAR_W, 0, SIDEBAR_W, H - HDR_H)
        canv.restoreState()

    # ── Assembly — KeepInFrame dans Table (sans KeepTogether imbriqué) ───
    SIDE_W = SIDEBAR_W - 2 * MARGIN
    SIDE_H = H - HDR_H - 2 * MARGIN
    MAIN_W = W - SIDEBAR_W - 2 * MARGIN
    MAIN_H = H - HDR_H - 2 * MARGIN

    # NOTE: KeepInFrame accepte une liste plate de Flowables — pas de KeepTogether ici
    sid_frame  = KeepInFrame(SIDE_W, SIDE_H, sid,  mode="shrink")
    main_frame = KeepInFrame(MAIN_W, MAIN_H, main, mode="shrink")

    body_table = Table([[sid_frame, main_frame]],
                       colWidths=[SIDE_W, MAIN_W])
    body_table.setStyle(TableStyle([
        ("VALIGN",       (0,0),(-1,-1),"TOP"),
        ("LEFTPADDING",  (0,0),(0,0),   0),
        ("RIGHTPADDING", (0,0),(0,0),   6),
        ("LEFTPADDING",  (1,0),(1,0),   10),
        ("RIGHTPADDING", (1,0),(1,0),   0),
        ("TOPPADDING",   (0,0),(-1,-1), 0),
        ("BOTTOMPADDING",(0,0),(-1,-1), 0),
    ]))

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=MARGIN, rightMargin=MARGIN,
        topMargin=MARGIN,  bottomMargin=MARGIN,
    )
    doc.build([hdr_table, Spacer(1, 6), body_table],
              onFirstPage=draw_bg, onLaterPages=draw_bg)
    return buf.getvalue()


async def generate_cv_pdf(cv_data: dict, offer_title: str = "") -> bytes:
    """Wrapper async — délègue à un thread pour ne pas bloquer l'event loop."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _generate_cv_pdf_sync, cv_data, offer_title)

# ══════════════════════════════════════════════════════
#  SCRAPING APIFY
# ══════════════════════════════════════════════════════

def _build_mock_offers() -> list:
    """Génère des offres mock adaptées au secteur du profil actuel."""
    sector = PROFILE.get("sector", "Data & Tech")
    preset = SECTOR_PRESETS.get(sector, SECTOR_PRESETS["Data & Tech"])
    name   = PROFILE.get("name", "").split()[-1] if PROFILE.get("name") else "vous"
    title  = PROFILE.get("title", "Consultant Senior")
    skills = PROFILE.get("skills", [])[:5]
    tjm_min = PROFILE.get("tjm_min", 300)
    tjm_max = PROFILE.get("tjm_max", 900)
    platforms = preset["platforms"]
    queries   = preset["search_queries"] or [f"{title} freelance France"]

    mocks = []
    for i, q in enumerate(queries[:5]):
        kw = skills[i % len(skills)] if skills else "expertise"
        mocks.append({
            "title": f"{q.replace(' freelance France','').replace(' remote','').strip()} — Mission {i+1}",
            "company": random.choice(["Scale-up Paris", "Groupe Industriel", "Cabinet Conseil", "Startup SaaS", "Grande Entreprise CAC40"]),
            "source": platforms[i % len(platforms)],
            "url": f"https://example.com/offre-{i+1}",
            "description": (
                f"Mission freelance 6-12 mois, full remote possible. "
                f"Secteur: {sector}. Profil recherché: {title}. "
                f"Compétences clés: {', '.join(skills[:4]) if skills else kw}. "
                f"TJM {tjm_min}-{tjm_max}€/j selon profil. "
                f"Démarrage dès que possible."
            ),
        })
    return mocks

MOCK_OFFERS: list = []  # rempli dynamiquement

# ══════════════════════════════════════════════════════
#  LINKEDIN — PUBLICATION AUTOMATIQUE VIA API
# ══════════════════════════════════════════════════════

def _get_linkedin_token() -> tuple:
    """Retourne (access_token, person_id) depuis DB ou variables d'env."""
    try:
        with db_conn() as conn:
            row = db_exec(conn, "SELECT access_token, person_id FROM linkedin_tokens WHERE id=1").fetchone()
            if row and row[0]:
                return row[0], row[1] or LINKEDIN_PERSON_ID
    except Exception:
        pass
    return LINKEDIN_ACCESS_TOKEN, LINKEDIN_PERSON_ID

async def linkedin_publish(content: str) -> dict:
    """Publie un post sur LinkedIn via l'API v2 UGC."""
    access_token, person_id = _get_linkedin_token()
    if not access_token or not person_id:
        return {"success": False, "error": "LINKEDIN_ACCESS_TOKEN ou LINKEDIN_PERSON_ID manquant — configure dans ⚙️ Config."}
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "X-Restli-Protocol-Version": "2.0.0",
    }
    payload = {
        "author": f"urn:li:person:{person_id}",
        "lifecycleState": "PUBLISHED",
        "specificContent": {
            "com.linkedin.ugc.ShareContent": {
                "shareCommentary": {"text": content},
                "shareMediaCategory": "NONE",
            }
        },
        "visibility": {"com.linkedin.ugc.MemberNetworkVisibility": "PUBLIC"},
    }
    try:
        async with httpx.AsyncClient(timeout=20.0) as cl:
            r = await cl.post("https://api.linkedin.com/v2/ugcPosts", headers=headers, json=payload)
            if r.status_code in (200, 201):
                post_id = r.headers.get("x-restli-id", r.json().get("id", ""))
                logger.info(f"LinkedIn post publié: {post_id}")
                return {"success": True, "post_id": post_id}
            err = r.json().get("message", r.text[:200])
            logger.error(f"LinkedIn API {r.status_code}: {err}")
            return {"success": False, "error": f"{r.status_code}: {err}"}
    except Exception as e:
        return {"success": False, "error": str(e)}

async def run_linkedin_scheduler():
    """Tâche schedulée: publie les posts dont l'heure est passée."""
    if not LINKEDIN_AUTO_POST:
        return
    now = datetime.now().isoformat()
    try:
        with db_conn() as conn:
            due = db_exec(conn,
                "SELECT id, content, topic FROM linkedin_schedule WHERE status='scheduled' AND scheduled_at <= ?",
                (now,)).fetchall()
        for row in due:
            sched_id, content, topic = row
            result = await linkedin_publish(content)
            with db_conn() as conn:
                if result["success"]:
                    db_exec(conn,
                        "UPDATE linkedin_schedule SET status='published', published_at=?, linkedin_post_id=? WHERE id=?",
                        (datetime.now().isoformat(), result.get("post_id",""), sched_id))
                    log_act("💼", f"Post LinkedIn publié auto: {topic}", "linkedin")
                else:
                    db_exec(conn,
                        "UPDATE linkedin_schedule SET status='error', error=? WHERE id=?",
                        (result["error"][:300], sched_id))
    except Exception as e:
        logger.error(f"run_linkedin_scheduler: {e}")

def _schedule_linkedin_post(content: str, topic: str, fmt: str, delay_hours: int = 0) -> int:
    """Enregistre un post pour publication automatique (ou manuelle si LINKEDIN_AUTO_POST=false)."""
    publish_time = datetime.now() + timedelta(hours=delay_hours)
    status = "scheduled" if LINKEDIN_AUTO_POST else "manual"
    try:
        with db_conn() as conn:
            cur = db_insert(conn,
                "INSERT INTO linkedin_schedule(content,topic,format,scheduled_at,status) VALUES(?,?,?,?,?)",
                (content, topic, fmt, publish_time.isoformat(), status))
            return cur.lastrowid
    except Exception as e:
        logger.error(f"_schedule_linkedin_post: {e}")
        return 0

# ══════════════════════════════════════════════════════
#  SCRAPERS — 10 SOURCES EN PARALLÈLE
# ══════════════════════════════════════════════════════

_HDR = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
        "Accept-Language": "fr-FR,fr;q=0.9"}

def _rss_parse(xml: str, source: str, max_items: int = 6) -> list:
    """Parse générique RSS → liste d'offres."""
    offers = []
    for item in re.findall(r"<item>(.*?)</item>", xml, re.DOTALL)[:max_items]:
        def _g(tag):
            m = re.search(rf"<{tag}[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</{tag}>", item, re.DOTALL)
            return re.sub(r"<[^>]+>", " ", m.group(1)).strip()[:600] if m else ""
        t = _g("title"); url = _g("link"); desc = _g("description")
        co_m = re.search(r"<source[^>]*>(.*?)</source>", item)
        co = re.sub(r"<[^>]+>", "", co_m.group(1)).strip() if co_m else ""
        if t: offers.append({"title": t, "company": co, "source": source,
                             "url": url, "description": f"{t}. {desc}"[:800]})
    return offers

async def _timed(coro, source: str) -> list:
    """Wrapper: exécute un scraper, mesure la latence, persiste les stats."""
    t0 = time.time()
    try:
        result = await coro
        ms = int((time.time() - t0) * 1000)
        try:
            with db_conn() as conn:
                db_exec(conn,
                    "INSERT INTO scraper_stats(source,offers_found,latency_ms,success) VALUES(?,?,?,1)",
                    (source, len(result), ms))
        except Exception: pass
        return result
    except Exception as e:
        ms = int((time.time() - t0) * 1000)
        try:
            with db_conn() as conn:
                db_exec(conn,
                    "INSERT INTO scraper_stats(source,offers_found,latency_ms,success,error) VALUES(?,0,?,0,?)",
                    (source, ms, str(e)[:200]))
        except Exception: pass
        logger.debug(f"Scraper {source}: {e}")
        return []

# 1 — WTTJ ────────────────────────────────────────────
async def _wttj(queries: list) -> list:
    offers = []
    async with httpx.AsyncClient(timeout=15.0, headers=_HDR) as cl:
        for q in queries[:3]:
            try:
                r = await cl.get("https://www.welcometothejungle.com/api/v1/jobs",
                    params={"query": q, "page": 1, "per_page": 5})
                if r.status_code == 200:
                    for job in r.json().get("jobs", [])[:5]:
                        t = job.get("name",""); co = job.get("company",{}).get("name","")
                        slug_co = job.get("company",{}).get("slug","")
                        slug_j  = job.get("slug","")
                        url = f"https://www.welcometothejungle.com/fr/companies/{slug_co}/jobs/{slug_j}"
                        desc = (job.get("description","") or "")[:500]
                        if t: offers.append({"title":t,"company":co,"source":"WTTJ","url":url,
                            "description":f"{t} chez {co}. {desc}"})
            except Exception: pass
    return offers

# 2 — Indeed RSS ──────────────────────────────────────
async def _indeed(queries: list, location: str) -> list:
    offers = []
    async with httpx.AsyncClient(timeout=15.0, headers=_HDR) as cl:
        for q in queries[:3]:
            try:
                r = await cl.get("https://fr.indeed.com/rss",
                    params={"q": q, "l": location, "sort": "date", "limit": 10})
                if r.status_code == 200:
                    offers.extend(_rss_parse(r.text, "Indeed"))
            except Exception: pass
    return offers

# 3 — Remotive JSON ───────────────────────────────────
async def _remotive(queries: list) -> list:
    offers = []
    async with httpx.AsyncClient(timeout=15.0) as cl:
        for q in queries[:2]:
            try:
                r = await cl.get("https://remotive.com/api/remote-jobs", params={"search": q, "limit": 5})
                if r.status_code == 200:
                    for job in r.json().get("jobs", [])[:5]:
                        t = job.get("title",""); co = job.get("company_name","")
                        desc = re.sub(r"<[^>]+>"," ", job.get("description","")).strip()[:500]
                        if t: offers.append({"title":t,"company":co,"source":"Remotive",
                            "url":job.get("url",""),"description":f"{t} chez {co}. {desc}"})
            except Exception: pass
    return offers

# 4 — Freelance.com RSS ───────────────────────────────
async def _freelance_com(queries: list) -> list:
    offers = []
    async with httpx.AsyncClient(timeout=15.0, headers=_HDR) as cl:
        for q in queries[:2]:
            try:
                r = await cl.get(f"https://www.freelance.com/rss.php?mot={q.replace(' ','+')}")
                if r.status_code == 200:
                    offers.extend(_rss_parse(r.text, "Freelance.com"))
            except Exception: pass
    return offers

# 5 — Adzuna API (gratuit avec clé) ──────────────────
async def _adzuna(queries: list) -> list:
    if not ADZUNA_APP_ID or not ADZUNA_APP_KEY:
        return []
    offers = []
    async with httpx.AsyncClient(timeout=15.0) as cl:
        for q in queries[:2]:
            try:
                r = await cl.get("https://api.adzuna.com/v1/api/jobs/fr/search/1",
                    params={"app_id":ADZUNA_APP_ID,"app_key":ADZUNA_APP_KEY,
                            "what":q,"results_per_page":5,"sort_by":"date","contract_type":"contract"})
                if r.status_code == 200:
                    for job in r.json().get("results",[])[:5]:
                        t = job.get("title",""); co = job.get("company",{}).get("display_name","")
                        desc = re.sub(r"<[^>]+>"," ", job.get("description","")).strip()[:500]
                        if t: offers.append({"title":t,"company":co,"source":"Adzuna",
                            "url":job.get("redirect_url",""),"description":f"{t} chez {co}. {desc}"})
            except Exception: pass
    return offers

# 6 — Jobijoba RSS ────────────────────────────────────
async def _jobijoba(queries: list) -> list:
    offers = []
    async with httpx.AsyncClient(timeout=15.0, headers=_HDR) as cl:
        for q in queries[:2]:
            try:
                r = await cl.get(f"https://www.jobijoba.com/fr/rss/?what={q.replace(' ','+')}")
                if r.status_code == 200:
                    offers.extend(_rss_parse(r.text, "Jobijoba"))
            except Exception: pass
    return offers

# 7 — France Travail RSS ──────────────────────────────
async def _france_travail(queries: list) -> list:
    offers = []
    async with httpx.AsyncClient(timeout=15.0, headers=_HDR, follow_redirects=True) as cl:
        for q in queries[:2]:
            try:
                r = await cl.get(
                    f"https://candidat.francetravail.fr/offres/recherche/rss?motsCles={q.replace(' ','+')}",
                    headers=_HDR)
                if r.status_code == 200:
                    offers.extend(_rss_parse(r.text, "France Travail"))
            except Exception: pass
    return offers

# 8 — HelloWork ───────────────────────────────────────
async def _hellowork(queries: list) -> list:
    offers = []
    async with httpx.AsyncClient(timeout=15.0, headers=_HDR, follow_redirects=True) as cl:
        for q in queries[:2]:
            try:
                r = await cl.get("https://www.hellowork.com/fr-fr/emploi/recherche.html",
                    params={"k":q,"c":"Freelance,Mission","s":"date"})
                if r.status_code == 200:
                    titles   = re.findall(r'<h2[^>]*class="[^"]*job-title[^"]*"[^>]*>(.*?)</h2>', r.text, re.DOTALL)
                    companies= re.findall(r'<span[^>]*class="[^"]*company[^"]*"[^>]*>(.*?)</span>', r.text, re.DOTALL)
                    links    = re.findall(r'href="(/fr-fr/emploi/[^"?]+)"', r.text)
                    for i, raw_t in enumerate(titles[:5]):
                        t  = re.sub(r"<[^>]+>","",raw_t).strip()
                        co = re.sub(r"<[^>]+>","",companies[i]).strip() if i < len(companies) else ""
                        url= f"https://www.hellowork.com{links[i]}" if i < len(links) else ""
                        if t: offers.append({"title":t,"company":co,"source":"HelloWork","url":url,
                            "description":f"{t} chez {co}."})
            except Exception: pass
    return offers

# 9 — LinkedIn Jobs RSS (sans auth) ───────────────────
async def _linkedin_rss(queries: list, location: str) -> list:
    offers = []
    async with httpx.AsyncClient(timeout=15.0, headers=_HDR, follow_redirects=True) as cl:
        for q in queries[:2]:
            try:
                url = f"https://www.linkedin.com/jobs/search?keywords={q.replace(' ','%20')}&location={location}&f_JT=C&format=rss"
                r = await cl.get(url)
                if r.status_code == 200:
                    offers.extend(_rss_parse(r.text, "LinkedIn"))
                else:
                    # Fallback: scrape page HTML
                    r2 = await cl.get("https://www.linkedin.com/jobs/search/",
                        params={"keywords":q,"location":location,"f_JT":"C","sortBy":"DD"})
                    items = re.findall(r'<li[^>]*class="[^"]*result-card[^"]*"[^>]*>(.*?)</li>', r2.text, re.DOTALL)
                    for item in items[:5]:
                        t_m  = re.search(r'<h3[^>]*>(.*?)</h3>', item, re.DOTALL)
                        co_m = re.search(r'<h4[^>]*>(.*?)</h4>', item, re.DOTALL)
                        l_m  = re.search(r'href="([^"]+/jobs/view/[^"]+)"', item)
                        t    = re.sub(r"<[^>]+>","",t_m.group(1)).strip()  if t_m  else ""
                        co   = re.sub(r"<[^>]+>","",co_m.group(1)).strip() if co_m else ""
                        link = l_m.group(1).split("?")[0]                  if l_m  else ""
                        if t: offers.append({"title":t,"company":co,"source":"LinkedIn",
                            "url":link,"description":f"{t} chez {co}."})
            except Exception: pass
    return offers

# 10 — RemoteOK JSON ──────────────────────────────────
async def _remoteok(queries: list) -> list:
    offers = []
    async with httpx.AsyncClient(timeout=15.0, headers={**_HDR,"Accept":"application/json"}) as cl:
        try:
            r = await cl.get("https://remoteok.com/api")
            if r.status_code == 200:
                jobs = [j for j in r.json() if isinstance(j, dict) and j.get("position")]
                kws  = [s.lower() for s in PROFILE.get("skills",[])[:5]]
                kws += [q.split()[0].lower() for q in queries[:3]]
                for job in jobs[:60]:
                    text = f"{job.get('position','').lower()} {' '.join(job.get('tags',[])).lower()}"
                    if any(k in text for k in kws):
                        t   = job.get("position",""); co = job.get("company","")
                        url = job.get("url","") or f"https://remoteok.com/l/{job.get('id','')}"
                        desc= re.sub(r"<[^>]+>"," ", job.get("description","")).strip()[:400]
                        offers.append({"title":t,"company":co,"source":"RemoteOK","url":url,
                            "description":f"{t} chez {co}. {desc}"})
                    if len(offers) >= 5: break
        except Exception: pass
    return offers

# ── Orchestrateur ─────────────────────────────────────
async def scrape_offers_free() -> list:
    """10 scrapers en parallèle → dédoublonnage → tri."""
    sector   = PROFILE.get("sector","Data & Tech")
    preset   = SECTOR_PRESETS.get(sector, SECTOR_PRESETS["Data & Tech"])
    queries  = list(preset["search_queries"]) or [PROFILE.get("title","Consultant freelance")]
    extra    = PROFILE.get("keywords",[])
    if extra: queries += [f"{queries[0]} {k}" for k in extra[:2]]
    location = (PROFILE.get("location","France").split(",")[0].strip()) or "France"
    enabled  = set(SCRAPERS_ENABLED)

    logger.info(f"Scraping ×10 — secteur={sector} | {len(queries)} requêtes | location={location}")

    coros = []
    names = []
    pairs = [
        ("wttj",          _wttj(queries)),
        ("indeed",        _indeed(queries, location)),
        ("remotive",      _remotive(queries)),
        ("freelance_com", _freelance_com(queries)),
        ("adzuna",        _adzuna(queries)),
        ("jobijoba",      _jobijoba(queries)),
        ("france_travail",_france_travail(queries)),
        ("hellowork",     _hellowork(queries)),
        ("linkedin",      _linkedin_rss(queries, location)),
        ("remoteok",      _remoteok(queries)),
    ]
    for name, coro in pairs:
        if name in enabled:
            coros.append(_timed(coro, name))
            names.append(name)

    results = await asyncio.gather(*coros, return_exceptions=True)
    all_offers: list = []
    for r in results:
        if isinstance(r, list): all_offers.extend(r)

    if all_offers:
        seen, deduped = set(), []
        for o in all_offers:
            key = o["title"].lower()[:45]
            if key not in seen:
                seen.add(key); deduped.append(o)
        logger.info(f"Scraping terminé: {len(deduped)} offres uniques depuis {len(names)} sources actives")
        return deduped[:20]

    logger.warning("Tous scrapers vides — fallback mock")
    return _build_mock_offers()

async def scrape_offers_apify() -> list:
    """Scraping via Apify — requêtes construites depuis le profil utilisateur."""
    logger.info("Scraping via Apify...")
    sector  = PROFILE.get("sector","Data & Tech")
    preset  = SECTOR_PRESETS.get(sector, SECTOR_PRESETS["Data & Tech"])
    queries = list(preset["search_queries"])[:3] or [PROFILE.get("title","Consultant freelance")]
    location = (PROFILE.get("location","France").split(",")[0].strip()) or "France"
    all_offers = []
    async with httpx.AsyncClient(timeout=120.0) as cl:
        for q in queries:
            try:
                run_r = await cl.post(
                    "https://api.apify.com/v2/acts/hMvNSpz3JnHgl5jkh/runs",
                    headers={"Authorization": f"Bearer {APIFY_TOKEN}"},
                    params={"token": APIFY_TOKEN},
                    json={"queries": q, "location": location, "maxResults": 5,
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
    return _build_mock_offers()

async def get_offers_today() -> list:
    """Retourne les offres du jour. Priorité: Apify > scrapers gratuits > mock."""
    if APIFY_TOKEN:
        return await scrape_offers_apify()
    offers = await scrape_offers_free()
    return offers if offers else _build_mock_offers()

# ══════════════════════════════════════════════════════
#  COPILOT ENGINE
# ══════════════════════════════════════════════════════

async def run_copilot():
    logger.info("Copilot démarré...")
    now_str    = datetime.now().strftime("%A %d %B")
    _copilot_status["step"] = "Récupération des offres…"
    raw_offers = await get_offers_today()
    analyzed, auto_applied = [], 0
    total = len(raw_offers)
    name  = PROFILE.get("name", "Utilisateur")
    title = PROFILE.get("title", "Consultant Senior")
    tjm_min = PROFILE.get("tjm_min", 300)
    tjm_max = PROFILE.get("tjm_max", 900)

    for idx, raw in enumerate(raw_offers):
        try:
            _copilot_status["step"] = f"Analyse offre {idx+1}/{total}: {raw.get('title','')[:40]}…"
            result = await ask_json(f"""{name} — {title}. TJM {tjm_min}-{tjm_max}€.
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
    _copilot_status["step"] = "Génération du post LinkedIn…"
    sector  = PROFILE.get("sector", "Data & Tech")
    preset  = SECTOR_PRESETS.get(sector, SECTOR_PRESETS["Data & Tech"])
    topics  = preset.get("post_topics", ["Mon expertise en 2025", "Freelance conseils pratiques"])
    topic   = random.choice(topics)
    exps    = PROFILE.get("experiences", [])
    recent_companies = ", ".join([e["company"] for e in exps[:3]]) if exps else "mes missions récentes"
    fmt = random.choice([
        "Hook choc + histoire personnelle + leçon", "Liste numerotée contre-intuitive",
        "Confession professionnelle + twist", "Storytelling mission avec chiffres"
    ])

    try:
        post = await ask(f"""Tu es {name}, {title} freelance ({recent_companies}).
{json.dumps(PROFILE)}

Ecris un post LinkedIn en FRANÇAIS qui va exploser en engagement.

SUJET: {topic}
FORMAT: {fmt}
SECTEUR: {sector}

RÈGLES ABSOLUES pour un post viral:
1. LIGNE 1 = hook IRRÉSISTIBLE (question provocatrice, stat choc, ou phrase courte qui force le "voir plus")
2. Ligne 2 = ligne vide (pause dramatique)
3. Corps = histoire CONCRÈTE avec les missions réelles mentionnées dans le profil + chiffres précis
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
        # ── Planifier la publication automatique ─────────────────────────────
        # Délai : publié demain matin à LINKEDIN_POST_HOUR si AUTO, sinon créé comme "manual"
        delay_h = max(0, LINKEDIN_POST_HOUR - datetime.now().hour + 24 if datetime.now().hour >= LINKEDIN_POST_HOUR else LINKEDIN_POST_HOUR - datetime.now().hour)
        sched_id = _schedule_linkedin_post(post.strip(), topic, fmt, delay_hours=int(delay_h))
    except Exception as e:
        post = f"[Erreur post: {e}]"

    # ── Créer les approbations avec tokens signés ──────────────────────
    approvals_created = []

    # 1. Post LinkedIn → approval + token
    if post and not post.startswith("[Erreur"):
        with db_conn() as conn:
            cur = db_insert(conn,
                "INSERT INTO pending_approvals(type,title,preview,payload,status) VALUES(?,?,?,?,?)",
                ("linkedin_post", f"Post LinkedIn: {topic}", post[:200],
                 json.dumps({"content": post, "topic": topic, "format": fmt}), "pending"))
            aid = cur.lastrowid
            token = _make_approval_token(aid)
            db_exec(conn, "UPDATE pending_approvals SET token=? WHERE id=?", (token, aid))
        approvals_created.append({"type": "linkedin_post", "id": aid, "title": f"Post LinkedIn: {topic}",
                                   "preview": post[:200], "token": token})

    # 2. Candidatures score ≥ 60 → approvals + tokens (PDF différé)
    candidatures_this_run = 0
    for entry in analyzed:
        if candidatures_this_run >= 3:
            break
        if entry.get("match_score", 0) >= 60:
            try:
                apply_result = await run_auto_apply(
                    entry["offer_id"], entry.get("title",""),
                    entry.get("company",""), entry.get("description",""),
                    entry, entry.get("company_email",""), defer_pdf=True
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
                    aid = cur.lastrowid
                    token = _make_approval_token(aid)
                    db_exec(conn, "UPDATE pending_approvals SET token=? WHERE id=?", (token, aid))
                approvals_created.append({
                    "type": "candidature", "id": aid, "token": token,
                    "title": f"Candidature: {entry.get('title','')} ({entry.get('match_score',0)}%)",
                    "preview": apply_result.get("email_body","")[:200],
                    "score": entry.get("match_score",0),
                })
                candidatures_this_run += 1
            except Exception as e:
                logger.error(f"Approval candidature error: {e}")

    top = top3[0] if top3 else {}

    # ── Notification multi-canal (Email + WhatsApp + SMS) ──────────────
    digest_sent = False
    notif_results = {}
    if approvals_created:
        offers_html = "".join(
            f"<div style='border-left:4px solid {'#00B96B' if o.get('match_score',0)>=85 else '#F59E0B'};"
            f"padding:10px 14px;margin-bottom:8px;background:#F9FAFB'>"
            f"<strong>{o.get('title','')}</strong> "
            f"<span style='color:{'#00B96B' if o.get('match_score',0)>=85 else '#F59E0B'}'>"
            f"{o.get('match_score',0)}%</span><br>"
            f"<small>{o.get('company','')} | TJM: {o.get('tjm_negotiate','?')}</small><br>"
            f"<small>{o.get('match_reason','')}</small></div>"
            for o in top3
        )
        try:
            notif_results = await notify_all(
                approvals_created, offers_html=offers_html,
                post_preview=post if not post.startswith("[Erreur") else "",
                now_str=now_str,
            )
            digest_sent = "email" in notif_results and "✅" in notif_results.get("email","")
        except Exception as e:
            logger.error(f"notify_all error: {e}")

    with db_conn() as conn:
        db_exec(conn,
            "INSERT INTO copilot_runs(offers_found,top_offer,top_score,post_topic,digest_sent,auto_applied) VALUES(?,?,?,?,?,?)",
            (len(analyzed), top.get("title",""), top.get("match_score",0), topic, int(digest_sent), auto_applied))
    log_act("🌅", f"{len(analyzed)} offres analysees, top {top.get('match_score',0)}%", "copilot")
    return {"offers_analyzed": len(analyzed), "top_offers": top3, "post_topic": topic,
            "auto_applied": auto_applied, "digest_sent": digest_sent,
            "notifications": notif_results,
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

    _name  = PROFILE.get("name", "Candidat")
    _title = PROFILE.get("title", "Consultant Freelance")
    _exps  = PROFILE.get("experiences", [])
    _recent_co = ", ".join([e.get("company","") for e in _exps[:3]]) if _exps else "mes missions récentes"
    _tjm   = f"{PROFILE.get('tjm_min',500)}-{PROFILE.get('tjm_max',900)}€"
    _skills_str = ", ".join(PROFILE.get("skills",[])[:10])
    _certs_str  = ", ".join(PROFILE.get("certifications",[])[:4])

    # Prompt compact pour éviter truncature sur modèles limités (Groq 8k)
    cv = await ask_json(f"""Expert RH. CV JSON pour {_name} ({_title}).
Profil: {json.dumps({k:v for k,v in PROFILE.items() if k not in ['bio','keywords']}, ensure_ascii=False)[:1200]}
Offre cible: {description[:600]}
Réponds UNIQUEMENT avec ce JSON (complète chaque champ) :
{{"title_adapted":"<titre adapté à l offre>","accroche":"<3 phrases percutantes 1ère personne, missions: {_recent_co}>","kpis":["<kpi1>","<kpi2>","<kpi3>"],"competences_core":[<liste 6 skills de {_skills_str} liés à l offre>],"competences_secondaires":[<liste 5 autres skills>],"experiences":[{{"company":"<nom>","period":"<période>","role":"<titre>","context":"<1 phrase>","missions":["<mission>","<mission>","<mission>"],"stack":"<techs>"}}],"formations":[{{"diplome":"<diplôme>","ecole":"<école>","annee":"<année>"}}],"certifications":[{_certs_str!r}],"langues":[{{"langue":"Français","niveau":"Natif"}}],"soft_skills":["<s1>","<s2>","<s3>","<s4>"],"tjm_suggest":"<TJM {_tjm}>€/j"}}
JSON seul.""", 2000, fallback={
        "title_adapted": title, "accroche": f"{_name}, {_title}.",
        "kpis": ["Expérience confirmée","Missions réussies","Disponible rapidement"],
        "competences_core": PROFILE.get("skills",[])[:6],
        "competences_secondaires": PROFILE.get("skills",[])[6:11],
        "experiences": _exps[:3],
        "formations": PROFILE.get("formations",[]),
        "certifications": PROFILE.get("certifications",[]),
        "langues": [{"langue":"Français","niveau":"Natif"}],
        "soft_skills": ["Autonomie","Rigueur","Communication","Adaptabilité"],
        "tjm_suggest": f"{PROFILE.get('tjm_max',700)}€/j",
    })

    # FIX-C : génération PDF async — différée si mode copilot (évite timeout Render 30s)
    if defer_pdf:
        pdf_bytes = b""
        pdf_b64   = ""   # sera généré à l'approbation dans approve_item()
    else:
        pdf_bytes = await generate_cv_pdf(cv, title)
        pdf_b64   = base64.b64encode(pdf_bytes).decode() if pdf_bytes else ""

    email_body = await ask(f"""Tu es {_name}, {_title} freelance.
Email candidature pour: {title} chez {company or "l'entreprise"}.
TJM: {analysis.get('tjm_negotiate', _tjm)}
Missions récentes: {_recent_co}
100-130 mots, direct, 1ere personne, mentionne une ou deux missions récentes, mentionne CV en PJ.
UNIQUEMENT le texte de l'email.""", 500)

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

        loop = asyncio.get_running_loop()
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
                f"Tu es {PROFILE.get('name','Candidat')}, {PROFILE.get('title','Consultant freelance')} freelance. "
                f"Écris un email de relance court (50-70 mots) pour la candidature: {title}. "
                f"TJM proposé: {tjm or str(PROFILE.get('tjm_max',800))+'€/j'}. "
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
    return await ask_json(f"""{PROFILE.get("name","Candidat")}, {PROFILE.get("title","Consultant freelance")}. TJM {PROFILE.get("tjm_min",500)}-{PROFILE.get("tjm_max",900)}€.
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
    logger.info("DataLinkedAI v11.0 — Démarrage")
    logger.info(f"  IA           : {AI_PROVIDER} / {AI_MODEL or 'non configuré'}")
    logger.info(f"  DB           : {'PostgreSQL Supabase' if USE_POSTGRES else 'SQLite /tmp (non persistant)'}")
    logger.info(f"  Email        : {EMAIL_PROVIDER}")
    logger.info(f"  Scraping     : {'Apify (réel)' if APIFY_TOKEN else f'×10 gratuits ({len(SCRAPERS_ENABLED)} actifs)'}")
    logger.info(f"  LinkedIn auto: {'ACTIF (' + LINKEDIN_POST_DAYS + ' à ' + str(LINKEDIN_POST_HOUR) + 'h)' if LINKEDIN_AUTO_POST else 'Manuel (validation requise)'}")
    logger.info(f"  Sécurité     : {'API Key ON' if API_KEY else '⚠️  DESACTIVEE — configure API_KEY'}")
    logger.info(f"  Auto-apply   : {'ACTIF (>=' + str(AUTO_APPLY_MIN_SCORE) + '%)' if AUTO_APPLY_ENABLED else 'Manuel'}")
    if AI_PROVIDER == "none":
        logger.warning("⚠️  Aucune clé IA — les endpoints IA renverront une erreur 500")
    logger.info("=" * 60)
    init_db()
    load_profile()
    # Jobs planifiés
    scheduler.add_job(run_copilot,   CronTrigger(day_of_week="mon-fri", hour=COPILOT_HOUR, minute=0),
                      id="copilot",  replace_existing=True)
    scheduler.add_job(backup_database, CronTrigger(hour=2, minute=0),
                      id="daily_backup", misfire_grace_time=600)
    scheduler.add_job(parse_gmail_replies, "interval", hours=2, id="gmail_parse",
                      misfire_grace_time=300)
    scheduler.add_job(run_followups, CronTrigger(hour=10, minute=0),
                      id="followups", replace_existing=True)
    # LinkedIn auto-post : toutes les 15 min vérifier les posts planifiés
    scheduler.add_job(run_linkedin_scheduler, "interval", minutes=15,
                      id="linkedin_scheduler", misfire_grace_time=300)
    scheduler.start()
    yield
    # Shutdown
    scheduler.shutdown()
    logger.info("DataLinkedAI arrêté proprement.")

app = FastAPI(
    title="DataLinkedAI API v8.0",
    description="Plateforme freelance data automatisée | Sekouna KABA | TJM 500-900€",
    version="8.0.0",
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

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    f = _static / "favicon.ico"
    if f.exists():
        return FileResponse(str(f), media_type="image/x-icon")
    return HTMLResponse("", status_code=204)

# ── Health ───────────────────────────────────────────
@app.get("/health", tags=["Système"])
async def health():
    token, pid = _get_linkedin_token()
    return {
        "status": "ok", "version": "11.0.0",
        "ai": AI_PROVIDER, "model": AI_MODEL,
        "db": "postgresql" if USE_POSTGRES else "sqlite",
        "email": EMAIL_PROVIDER,
        "scraping": "apify" if APIFY_TOKEN else f"free_x10 ({len(SCRAPERS_ENABLED)} sources)",
        "security": "api_key" if API_KEY else "none",
        "auto_apply": AUTO_APPLY_ENABLED,
        "linkedin_auto": LINKEDIN_AUTO_POST,
        "linkedin_token": bool(token),
        "jobs": [{"id": j.id, "next": str(j.next_run_time)} for j in scheduler.get_jobs()],
    }

@app.get("/api/profile", tags=["Système"])
async def profile(_=Depends(verify_api_key)):
    return PROFILE

@app.put("/api/profile", tags=["Profil Utilisateur"])
async def update_profile(request: Request, _=Depends(verify_api_key)):
    """Met à jour le profil utilisateur et le persist en DB.
    Accepte un JSON partiel — seuls les champs fournis sont mis à jour."""
    global PROFILE
    body = await request.json()
    if not isinstance(body, dict):
        raise HTTPException(400, "Body doit être un objet JSON")
    # Merge : on ne remplace que les clés fournies
    PROFILE = {**DEFAULT_PROFILE, **PROFILE, **body}
    with db_conn() as conn:
        # Upsert — SQLite et PostgreSQL compat
        existing = db_exec(conn, "SELECT id FROM user_profile WHERE id=1").fetchone()
        if existing:
            db_exec(conn,
                "UPDATE user_profile SET data=?, updated_at=? WHERE id=1",
                (json.dumps(PROFILE), datetime.now().isoformat()))
        else:
            db_exec(conn,
                "INSERT INTO user_profile(id,data,updated_at) VALUES(1,?,?)",
                (json.dumps(PROFILE), datetime.now().isoformat()))
    log_act("👤", f"Profil mis à jour: {PROFILE.get('name','?')}", "profile")
    return {"status": "updated", "profile": PROFILE}

@app.get("/api/sectors", tags=["Profil Utilisateur"])
async def list_sectors(_=Depends(verify_api_key)):
    """Retourne les presets sectoriels disponibles."""
    return {
        "sectors": list(SECTOR_PRESETS.keys()),
        "presets": {k: {"search_queries": v["search_queries"], "tjm_range": v["tjm_range"],
                        "platforms": v["platforms"]} for k, v in SECTOR_PRESETS.items()},
        "current_sector": PROFILE.get("sector", "Data & Tech"),
    }

# ══════════════════════════════════════════════════════
#  MOCK INTERVIEW IA
# ══════════════════════════════════════════════════════

class InterviewStartReq(BaseModel):
    offer_title: str
    offer_description: str = ""
    difficulty: str = "medium"   # easy | medium | hard

class InterviewAnswerReq(BaseModel):
    session_id: int
    answer: str

@app.post("/api/interview/start", tags=["Mock Interview"])
async def interview_start(req: InterviewStartReq, request: Request, _=Depends(verify_api_key)):
    """Démarre une session de mock interview. L'IA joue le recruteur."""
    _check_rate(request, "interview_start", 10, 3600)
    diff_label = {"easy": "débutant — questions générales", "medium": "intermédiaire — questions techniques et comportementales",
                  "hard": "expert — questions techniques poussées, mise en situation, stress test"}.get(req.difficulty, "intermédiaire")

    system = f"""Tu es un recruteur expert pour le poste: {req.offer_title}.
Profil candidat: {json.dumps(PROFILE)}
Niveau: {diff_label}
Secteur: {PROFILE.get('sector', 'Tech')}

RÈGLES:
- Commence TOUJOURS par te présenter brièvement et poser LA PREMIÈRE QUESTION uniquement
- Pose 1 question à la fois, attends la réponse
- Après chaque réponse: donne un feedback court (1-2 phrases), note /10 entre balises [SCORE:X/10], puis pose la suivante
- Après 5 questions, conclus avec: bilan global, points forts, axes d'amélioration, note finale [FINAL:X/10]
- Adapte les questions à l'offre ET au profil du candidat
- Mélange: motivation, technique, situations passées, mises en situation
- Ton: professionnel mais humain"""

    first_msg = await ask(f"""{system}

C'est le début de l'entretien. Présente-toi brièvement et pose ta première question.""", 600)

    messages = [{"role": "assistant", "content": first_msg}]
    with db_conn() as conn:
        cur = db_insert(conn,
            "INSERT INTO interview_sessions(offer_title,offer_description,difficulty,messages,status) VALUES(?,?,?,?,?)",
            (req.offer_title, req.offer_description[:1000], req.difficulty, json.dumps(messages), "active"))
        session_id = cur.lastrowid

    log_act("🎤", f"Interview démarré: {req.offer_title}", "interview")
    return {"session_id": session_id, "message": first_msg, "difficulty": req.difficulty, "messages": messages}

@app.post("/api/interview/answer", tags=["Mock Interview"])
async def interview_answer(req: InterviewAnswerReq, request: Request, _=Depends(verify_api_key)):
    """Envoie une réponse et obtient la réaction + question suivante du recruteur IA."""
    _check_rate(request, "interview_answer", 50, 3600)
    with db_conn() as conn:
        row = db_exec(conn, "SELECT offer_title,offer_description,difficulty,messages,status FROM interview_sessions WHERE id=?",
                      (req.session_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Session introuvable")
    if row[4] == "completed":
        raise HTTPException(400, "Cette session est terminée")

    offer_title, offer_desc, difficulty, msgs_raw, _ = row
    messages: list = json.loads(msgs_raw)
    messages.append({"role": "user", "content": req.answer})

    diff_label = {"easy": "débutant", "medium": "intermédiaire", "hard": "expert"}.get(difficulty, "intermédiaire")
    system = f"""Tu es un recruteur expert pour: {offer_title}. Profil candidat: {json.dumps(PROFILE)}. Niveau: {diff_label}.
Règles: feedback 1-2 phrases + [SCORE:X/10] + question suivante OU bilan final [FINAL:X/10] si c'est la 5e réponse."""

    # Construire le contexte conversationnel
    conv_text = "\n".join([f"{'Recruteur' if m['role']=='assistant' else 'Candidat'}: {m['content']}" for m in messages[-8:]])
    response = await ask(f"{system}\n\nConversation:\n{conv_text}\n\nRéponds maintenant (recruteur):", 500)

    messages.append({"role": "assistant", "content": response})
    is_final = "[FINAL:" in response
    status   = "completed" if is_final else "active"

    # Extraire le score final si présent
    final_score = None
    if is_final:
        m = re.search(r"\[FINAL:(\d+)/10\]", response)
        if m:
            final_score = int(m.group(1))

    with db_conn() as conn:
        db_exec(conn,
            "UPDATE interview_sessions SET messages=?, status=?, score=? WHERE id=?",
            (json.dumps(messages), status, final_score, req.session_id))

    if is_final:
        log_act("🎤", f"Interview terminé: {offer_title} — score {final_score}/10", "interview")

    return {"message": response, "messages": messages, "is_final": is_final,
            "final_score": final_score, "status": status}

@app.get("/api/interview/sessions", tags=["Mock Interview"])
async def list_interview_sessions(_=Depends(verify_api_key)):
    with db_conn() as conn:
        rows = db_exec(conn,
            "SELECT id,offer_title,difficulty,score,status,created_at FROM interview_sessions ORDER BY created_at DESC LIMIT 20"
        ).fetchall()
    return [{"id": r[0], "offer_title": r[1], "difficulty": r[2], "score": r[3],
             "status": r[4], "created_at": str(r[5])} for r in rows]

@app.get("/api/interview/{session_id}", tags=["Mock Interview"])
async def get_interview_session(session_id: int, _=Depends(verify_api_key)):
    with db_conn() as conn:
        row = db_exec(conn,
            "SELECT id,offer_title,offer_description,difficulty,messages,score,feedback,status,created_at FROM interview_sessions WHERE id=?",
            (session_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Session introuvable")
    return {"id": row[0], "offer_title": row[1], "offer_description": row[2], "difficulty": row[3],
            "messages": json.loads(row[4]), "score": row[5], "feedback": row[6],
            "status": row[7], "created_at": str(row[8])}

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
    name  = PROFILE.get("name","Consultant")
    title = PROFILE.get("title","Consultant Senior")
    tjm_min = PROFILE.get("tjm_min",300); tjm_max = PROFILE.get("tjm_max",900)
    r = await ask_json(f"""{name} — {title}. TJM {tjm_min}-{tjm_max}€.
Profil:{json.dumps(PROFILE)}\nOffre:---{req.text}---
JSON:{{"match_score":<0-100>,"title":"<titre>","tjm_range":"<fourchette>","tjm_negotiate":"<TJM {tjm_min}-{tjm_max}€>",
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
            f"SELECT id,title,source,match_score,tjm_negotiate,urgency,status,url,created_at,description FROM offers ORDER BY created_at DESC LIMIT {min(limit,200)} OFFSET {offset}",
        ).fetchall()
    return {"total": total, "offset": offset, "limit": limit,
            "items": [{"id": r[0], "title": r[1], "source": r[2], "match_score": r[3],
             "tjm_negotiate": r[4], "urgency": r[5], "status": r[6],
             "url": r[7], "created_at": str(r[8]),
             "description": r[9] or ""} for r in rows]}

@app.post("/api/offers/scrape", tags=["Offres Freelance"])
async def scrape_and_analyze_now(request: Request, _=Depends(verify_api_key)):
    """Lance un scraping immédiat + analyse IA de chaque offre. Stocke en DB."""
    _check_rate(request, "offers_scrape", 3, 3600)
    raw = await get_offers_today()
    analyzed = []
    name  = PROFILE.get("name","Candidat")
    title = PROFILE.get("title","Consultant")
    tjm_min = PROFILE.get("tjm_min",300); tjm_max = PROFILE.get("tjm_max",900)
    for raw_o in raw[:12]:
        try:
            result = await ask_json(
                f"""{name} — {title}. TJM {tjm_min}-{tjm_max}€. Compétences: {", ".join(PROFILE.get("skills",[])[:8])}
Offre: {raw_o.get("description","")[:600]}
JSON:{{"match_score":<0-100>,"tjm_negotiate":"<TJM>","urgency":"<Postuler maintenant/Peut attendre>","match_reason":"<raison>","key_techs":["<t1>","<t2>"]}}
JSON seul.""", 400, fallback={"match_score":50,"tjm_negotiate":f"{tjm_max}€/j","urgency":"Peut attendre","match_reason":"","key_techs":[]})
            score = result.get("match_score",0)
            with db_conn() as conn:
                db_insert(conn,
                    "INSERT OR IGNORE INTO offers(title,source,description,match_score,tjm_negotiate,urgency,status,url) VALUES(?,?,?,?,?,?,?,?)",
                    (raw_o.get("title",""), raw_o.get("source","scraper"),
                     raw_o.get("description","")[:1000], score,
                     result.get("tjm_negotiate",""), result.get("urgency",""),
                     "analyzed", raw_o.get("url","")))
            analyzed.append({**raw_o, **result})
        except Exception as e:
            logger.debug(f"Analyze offer error: {e}")
            analyzed.append(raw_o)
    log_act("🕷️", f"Scraping live: {len(analyzed)} offres", "scraping")
    return {"count": len(analyzed), "offers": sorted(analyzed, key=lambda x: x.get("match_score",0), reverse=True)}

class EnrichReq(BaseModel):
    company: str
    offer_text: str = ""

@app.post("/api/offers/enrich-contact", tags=["Offres Freelance"])
async def enrich_contact(req: EnrichReq, _=Depends(verify_api_key)):
    """Enrichit automatiquement le contact recruteur depuis le nom de l'entreprise + texte offre."""
    result = {"company": req.company, "email": "", "linkedin": "", "source": ""}

    # 1. Extraire email directement dans le texte de l'offre
    if req.offer_text:
        emails = re.findall(r'[\w.+-]+@[\w-]+\.[\w.]+', req.offer_text)
        if emails:
            result["email"] = emails[0]
            result["source"] = "extracted_from_offer"

    # 2. Extraire URL LinkedIn de l'offre
    if req.offer_text:
        li = re.findall(r'linkedin\.com/in/[\w-]+', req.offer_text)
        if li:
            result["linkedin"] = "https://www." + li[0]

    # 3. Hunter.io si pas d'email trouvé
    if not result["email"] and req.company and HUNTER_API_KEY:
        found = await enrich_company_email(req.company)
        if found:
            result["email"] = found
            result["source"] = "hunter_io"

    # 4. Guess pattern basique si domaine connu
    if not result["email"] and req.company:
        domain_guess = req.company.lower().replace(" ","-").replace("&","").replace("'","")[:30] + ".com"
        result["email_guess"] = f"contact@{domain_guess}"
        result["source"] = result["source"] or "guess"

    # 5. IA : extraire infos de contact du texte de l'offre
    if req.offer_text and not result["email"]:
        contact_info = await ask_json(
            f"""Extrais les infos de contact depuis ce texte d'offre.
Texte: {req.offer_text[:800]}
JSON:{{"email":"<email ou vide>","nom_recruteur":"<nom ou vide>","entreprise":"<entreprise exacte>","linkedin":"<url linkedin ou vide>","telephone":"<tel ou vide>"}}
JSON seul.""", 300, fallback={"email":"","nom_recruteur":"","entreprise":req.company,"linkedin":"","telephone":""})
        for k in ["email","nom_recruteur","linkedin","telephone"]:
            if contact_info.get(k) and not result.get(k):
                result[k] = contact_info[k]
                if k == "email": result["source"] = "ai_extracted"

    return result

# ── CV ───────────────────────────────────────────────
class CVReq(BaseModel):
    offer_text: str
    generate_pdf: bool = True

@app.post("/api/cv/adapt", tags=["CV Adaptatif"])
async def adapt_cv(req: CVReq, request: Request, _=Depends(verify_api_key)):
    _check_rate(request, "adapt_cv", 20, 3600)
    _rate_limit(request, max_calls=10, window_seconds=3600)
    name  = PROFILE.get("name","Consultant")
    title = PROFILE.get("title","Consultant Senior")
    tjm_min = PROFILE.get("tjm_min",300); tjm_max = PROFILE.get("tjm_max",900)
    r = await ask_json(f"""{name} — {title}. TJM {tjm_min}-{tjm_max}€.
CV:{json.dumps(PROFILE)}\nOffre:---{req.offer_text}---
JSON:{{"score":<0-100>,"title_adapted":"<titre>","accroche":"<3 phrases 1ere personne>",
"competences":["<s1>","<s2>","<s3>","<s4>","<s5>","<s6>"],
"experiences":[{{"company":"<n>","period":"<p>","role":"<r>","pertinence":<0-100>,"pitch":"<1 phrase>","missions":["<m1>","<m2>","<m3>"]}}],
"cover_letter":"<3 paragraphes directs>","strengths":["<f1>","<f2>","<f3>"],
"advice":"<conseil 2 phrases>","tjm_suggest":"<TJM {tjm_min}-{tjm_max}€ et pourquoi>"}}
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
    _rate_limit(request, max_calls=20, window_seconds=3600)
    name  = PROFILE.get("name","Consultant")
    title = PROFILE.get("title","Consultant Senior")
    exps  = PROFILE.get("experiences",[])
    recent = ", ".join([e["company"] for e in exps[:3]]) if exps else "mes missions récentes"
    sector = PROFILE.get("sector","Tech")
    content = await ask(f"""Tu es {name}, {title} freelance ({recent}).
{json.dumps(PROFILE)}

Ecris un post LinkedIn VIRAL en français.
Sujet: {req.topic}
Format demandé: {req.format}
Ton: {req.tone}
Secteur: {sector}
{f'Angle spécifique: {req.angle}' if req.angle else ''}

STRUCTURE VIRALE OBLIGATOIRE:
— Ligne 1 : hook IRRÉSISTIBLE (max 12 mots) qui force le clic "voir plus"
  Types de hooks qui marchent: chiffre choc, question provocatrice, confession, prise de position tranchée
— Ligne 2 : vide
— Corps : histoire concrète avec les missions réelles du profil + métriques précises
  Sauts de ligne fréquents — 1-2 phrases par paragraphe max
— Fin : question qui APPELLE les commentaires OU CTA fort
— 4-6 emojis positionnés stratégiquement
— 6-8 hashtags pertinents en toute fin
— 200-260 mots au total

TON: expert accessible, direct, authentique
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

@app.get("/api/linkedin/schedule", tags=["LinkedIn Studio"])
async def list_linkedin_schedule(_=Depends(verify_api_key)):
    """Calendrier éditorial — posts planifiés / publiés / en erreur."""
    with db_conn() as conn:
        rows = db_exec(conn,
            "SELECT id,topic,format,content,scheduled_at,published_at,status,linkedin_post_id,error,created_at FROM linkedin_schedule ORDER BY scheduled_at DESC LIMIT 30"
        ).fetchall()
    return [{"id":r[0],"topic":r[1],"format":r[2],"content":r[3],"scheduled_at":str(r[4]),
             "published_at":str(r[5]),"status":r[6],"linkedin_post_id":r[7],
             "error":r[8],"created_at":str(r[9])} for r in rows]

class LinkedInPostNowReq(BaseModel):
    content: str
    topic: str = ""

@app.post("/api/linkedin/publish", tags=["LinkedIn Studio"])
async def publish_linkedin_now(req: LinkedInPostNowReq, request: Request, _=Depends(verify_api_key)):
    """Publie immédiatement un post sur LinkedIn via l'API."""
    _check_rate(request, "linkedin_publish", 5, 3600)
    result = await linkedin_publish(req.content)
    if result["success"]:
        with db_conn() as conn:
            db_exec(conn, "UPDATE linkedin_posts SET status='published' WHERE content=?", (req.content,))
            try:
                db_exec(conn,
                    "UPDATE linkedin_schedule SET status='published', published_at=?, linkedin_post_id=? WHERE content=?",
                    (datetime.now().isoformat(), result.get("post_id",""), req.content))
            except Exception: pass
        log_act("💼", f"Post LinkedIn publié manuellement: {req.topic}", "linkedin")
    return {**result, "topic": req.topic}

class LinkedInScheduleReq(BaseModel):
    content: str
    topic: str = ""
    format: str = ""
    scheduled_at: str   # ISO8601 ex: "2025-06-16T09:00:00"

@app.post("/api/linkedin/schedule", tags=["LinkedIn Studio"])
async def schedule_linkedin_post(req: LinkedInScheduleReq, _=Depends(verify_api_key)):
    """Planifie un post pour publication automatique à une date/heure précise."""
    try:
        with db_conn() as conn:
            cur = db_insert(conn,
                "INSERT INTO linkedin_schedule(content,topic,format,scheduled_at,status) VALUES(?,?,?,?,?)",
                (req.content, req.topic, req.format, req.scheduled_at,
                 "scheduled" if LINKEDIN_AUTO_POST else "manual"))
            sched_id = cur.lastrowid
        log_act("💼", f"Post planifié: {req.topic} → {req.scheduled_at}", "linkedin")
        return {"status": "scheduled", "id": sched_id, "scheduled_at": req.scheduled_at,
                "will_auto_publish": LINKEDIN_AUTO_POST}
    except Exception as e:
        raise HTTPException(500, str(e))

class LinkedInTokenReq(BaseModel):
    access_token: str
    person_id: str

@app.post("/api/linkedin/token", tags=["LinkedIn Studio"])
async def set_linkedin_token(req: LinkedInTokenReq, _=Depends(verify_api_key)):
    """Persiste le token LinkedIn OAuth en DB (survit aux redémarrages)."""
    with db_conn() as conn:
        existing = db_exec(conn, "SELECT id FROM linkedin_tokens WHERE id=1").fetchone()
        if existing:
            db_exec(conn, "UPDATE linkedin_tokens SET access_token=?, person_id=?, updated_at=? WHERE id=1",
                    (req.access_token, req.person_id, datetime.now().isoformat()))
        else:
            db_exec(conn, "INSERT INTO linkedin_tokens(id,access_token,person_id,updated_at) VALUES(1,?,?,?)",
                    (req.access_token, req.person_id, datetime.now().isoformat()))
    log_act("🔑", "Token LinkedIn mis à jour", "linkedin")
    return {"status": "saved", "person_id": req.person_id}

@app.get("/api/linkedin/status", tags=["LinkedIn Studio"])
async def linkedin_status(_=Depends(verify_api_key)):
    """Statut de la connexion LinkedIn (token présent, dernière publication, etc.)."""
    token, pid = _get_linkedin_token()
    with db_conn() as conn:
        last_pub = db_exec(conn,
            "SELECT topic, published_at FROM linkedin_schedule WHERE status='published' ORDER BY published_at DESC LIMIT 1"
        ).fetchone()
        scheduled = db_exec(conn,
            "SELECT COUNT(*) FROM linkedin_schedule WHERE status='scheduled'"
        ).fetchone()[0]
    return {
        "token_configured": bool(token),
        "person_id_configured": bool(pid),
        "auto_post_enabled": LINKEDIN_AUTO_POST,
        "post_days": LINKEDIN_POST_DAYS,
        "post_hour": LINKEDIN_POST_HOUR,
        "pending_scheduled": scheduled,
        "last_published": {"topic": last_pub[0], "at": str(last_pub[1])} if last_pub else None,
    }

@app.get("/api/scrapers/stats", tags=["Scraping"])
async def scraper_stats(_=Depends(verify_api_key)):
    """Statistiques de performance par source de scraping."""
    with db_conn() as conn:
        rows = db_exec(conn, """
            SELECT source,
                   COUNT(*) as runs,
                   SUM(offers_found) as total_offers,
                   AVG(latency_ms) as avg_ms,
                   SUM(success) as successes,
                   MAX(run_at) as last_run
            FROM scraper_stats
            GROUP BY source
            ORDER BY total_offers DESC
        """).fetchall()
    return {
        "sources": [{"source":r[0],"runs":r[1],"total_offers":r[2],
                     "avg_latency_ms":round(r[3] or 0),"successes":r[4],
                     "success_rate": round(100*r[4]/max(r[1],1)),
                     "last_run":str(r[5])} for r in rows],
        "enabled": SCRAPERS_ENABLED,
        "apify_active": bool(APIFY_TOKEN),
    }

@app.get("/api/scrapers/run", tags=["Scraping"])
async def trigger_scraping(_=Depends(verify_api_key)):
    """Lance un scraping immédiat (toutes sources actives) et retourne les offres brutes."""
    offers = await scrape_offers_free()
    return {"count": len(offers), "offers": offers}

# ── Prospection ──────────────────────────────────────
class ProspectReq(BaseModel):
    target_context: str

@app.post("/api/prospecting/generate", tags=["Prospection"])
async def gen_prospect(request: Request, req: ProspectReq, _=Depends(verify_api_key)):
    _check_rate(request, "gen_prospect", 15, 3600)
    r = await ask_json(f"""{PROFILE.get("name","Candidat")} {PROFILE.get("title","Consultant freelance")}.{json.dumps(PROFILE)}
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
    name=PROFILE.get("name","Consultant"); title=PROFILE.get("title","Consultant Senior")
    tjm_min=PROFILE.get("tjm_min",300); tjm_max=PROFILE.get("tjm_max",900)
    sector=PROFILE.get("sector","Tech")
    r = await ask_json(f"""{name} — {title} freelance. TJM {tjm_min}-{tjm_max}€. Secteur: {sector}.
{json.dumps(PROFILE)}
Contexte:{req.context or f"Mission générale {title} full remote"}
JSON:{{"tjm_market":<median>,"tjm_top":<top>,"tjm_recommend":<{tjm_min}-{tjm_max}>,"summary":"<2 phrases>",
"value_drivers":["<v1>","<v2>","<v3>"],"negotiation_script":"<script 150 mots>",
"skills_premium":["<s1>","<s2>","<s3>"],
"benchmark":[{{"role":"<r1>","tjm":<n>}},{{"role":"<r2>","tjm":<n>}},{{"role":"<r3>","tjm":<n>}}]}}
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
    r = await ask_json(f"""{PROFILE.get("name","Candidat")} {PROFILE.get("title","Consultant freelance")}. TJM {PROFILE.get("tjm_min",500)}-{PROFILE.get("tjm_max",900)}€.{json.dumps(PROFILE)}
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
    r = await ask_json(f"""Audit profil LinkedIn {PROFILE.get("name","Candidat")} freelance.{json.dumps(PROFILE)}
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
    result = await ask_json(f"""Tu es {PROFILE.get("name","Candidat")}, {PROFILE.get("title","Consultant freelance")}.{json.dumps(PROFILE)}
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

# ── État en mémoire du copilot (singleton process) ────────────────────────
_copilot_status: dict = {
    "running": False, "step": "idle", "started_at": None,
    "result": None, "error": None,
}

async def _run_copilot_tracked():
    """Wrapper autour de run_copilot() qui met à jour _copilot_status."""
    _copilot_status["running"] = True
    _copilot_status["step"] = "Récupération des offres…"
    _copilot_status["error"] = None
    _copilot_status["result"] = None
    try:
        result = await run_copilot()
        _copilot_status["result"] = result
        _copilot_status["step"] = "Terminé ✅"
    except Exception as e:
        logger.error(f"Copilot tracked crash: {e}", exc_info=True)
        _copilot_status["error"] = str(e)
        _copilot_status["step"] = "Erreur ❌"
    finally:
        _copilot_status["running"] = False

@app.post("/api/copilot/run/sync", tags=["Copilot Quotidien"])
async def trigger_copilot_sync(request: Request, bt: BackgroundTasks, _=Depends(verify_api_key)):
    """Lance le copilot en arrière-plan (évite timeout Render 30s).
    Poll /api/copilot/status pour suivre la progression en temps réel.
    """
    _check_rate(request, "trigger_copilot_sync", 3, 3600)
    if _copilot_status["running"]:
        return {
            "status": "already_running",
            "message": "Un copilot est déjà en cours. Attends la fin avant de relancer.",
            "poll_url": "/api/copilot/status",
        }
    _copilot_status["running"] = True
    _copilot_status["started_at"] = datetime.now().isoformat()
    _copilot_status["step"] = "Démarrage…"
    _copilot_status["result"] = None
    _copilot_status["error"] = None
    bt.add_task(_run_copilot_tracked)
    return {
        "status": "started",
        "message": "Copilot lancé en arrière-plan. Poll /api/copilot/status pour suivre.",
        "poll_url": "/api/copilot/status",
        "mode": "apify" if APIFY_TOKEN else "mock",
    }

@app.get("/api/copilot/status", tags=["Copilot Quotidien"])
async def copilot_status_ep(_=Depends(verify_api_key)):
    """Retourne l'état en temps réel du dernier run copilot (polling frontend)."""
    return _copilot_status

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
    _rate_limit(request, max_calls=10, window_seconds=3600)
    try:
        analysis = await ask_json(f"""{PROFILE.get("name","Candidat")} {PROFILE.get("title","Consultant freelance")}. TJM {PROFILE.get("tjm_min",500)}-{PROFILE.get("tjm_max",900)}€.
Profil compétences: {", ".join(PROFILE.get("skills",[])[:8])}
Offre: {req.description[:800]}
JSON:{{"match_score":<0-100>,"tjm_negotiate":"<TJM recommandé>","urgency":"<Postuler maintenant/Peut attendre>","match_reason":"<raison courte>"}}
JSON seul.""", 600, fallback={"match_score": 75, "tjm_negotiate": f"{PROFILE.get('tjm_max',700)}€/j", "urgency": "Postuler maintenant", "match_reason": "Profil compatible"})
        score = analysis.get("match_score", 0)
        if score < 60:
            return {"match_score": score, "decision": "Score trop faible (<60%) — pas de candidature générée", "analysis": analysis}
    except Exception as e:
        logger.error(f"agent_apply analysis error: {e}")
        analysis = {"match_score": 75, "tjm_negotiate": f"{PROFILE.get('tjm_max',700)}€/j", "urgency": "Postuler maintenant", "match_reason": "Analyse partielle"}
        score = 75

    try:
        # Enrichir email si pas fourni
        company_email = req.company_email or ""
        if not company_email and req.company and HUNTER_API_KEY:
            company_email = await enrich_company_email(req.company) or ""
        if not company_email:
            # Extraire email depuis le texte de l'offre
            email_matches = re.findall(r'[\w.+-]+@[\w-]+\.[\w.]+', req.description)
            if email_matches:
                company_email = email_matches[0]
                log_act("🔍", f"Email extrait de l'offre: {company_email}", "agent")

        with db_conn() as _oc:
            _ocur = db_insert(_oc,
                "INSERT INTO offers(title,source,description,match_score,tjm_negotiate,urgency,status) VALUES(?,?,?,?,?,?,?)",
                (req.offer_title,"manual",req.description[:1000],score,
                 analysis.get("tjm_negotiate",""),analysis.get("urgency",""),"analyzed"))
            offer_id = _ocur.lastrowid

        result = await run_auto_apply(offer_id, req.offer_title, req.company, req.description, analysis, company_email)
    except Exception as e:
        logger.error(f"agent_apply CV/email generation error: {e}", exc_info=True)
        raise HTTPException(500, f"Erreur génération candidature: {str(e)[:200]}. Vérifie ta clé IA (GROQ_API_KEY / OPENAI_API_KEY) et que le profil est rempli.")

    if (AUTO_APPLY_ENABLED or req.force_send) and company_email:
        try:
            with db_conn() as _pc:
                _pr = db_exec(_pc, "SELECT cv_pdf_b64 FROM auto_applications WHERE id=?", (result["app_id"],)).fetchone()
                _pdf_b64 = _pr[0] if _pr and _pr[0] else ""
            pdf_bytes = base64.b64decode(_pdf_b64) if _pdf_b64 else b""
            body_html = make_html_email(result.get("email_body",""))
            _cname = PROFILE.get("name","Candidat").replace(" ","_")
            att_name = f"CV_{_cname}_{req.offer_title[:25].replace(' ','_')}.pdf" if pdf_bytes else None
            await send_email(company_email, req.company or "Recruteur",
                             result.get("subject","Candidature freelance"),
                             body_html, result.get("email_body",""),
                             att_bytes=pdf_bytes or None, att_name=att_name)
            result["email_sent"] = True; result["pdf_attached"] = bool(pdf_bytes)
            with db_conn() as conn:
                db_exec(conn, "UPDATE auto_applications SET status='sent', applied_at=? WHERE id=?",
                        (datetime.now().isoformat(), result["app_id"]))
        except Exception as e:
            result["email_error"] = str(e)
    return {**analysis, **result, "auto_apply_active": AUTO_APPLY_ENABLED or req.force_send,
            "company_email_used": company_email or ""}


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
            content = payload.get("content","")
            topic   = payload.get("topic","")
            # Tenter la publication LinkedIn directe si token configuré
            publish_result = {}
            if content:
                publish_result = await linkedin_publish(content)
            if publish_result.get("success"):
                with db_conn() as conn:
                    db_exec(conn, "UPDATE linkedin_posts SET status='published' WHERE content=?", (content,))
                    db_exec(conn, "UPDATE linkedin_schedule SET status='published', published_at=?, linkedin_post_id=? WHERE content=?",
                            (datetime.now().isoformat(), publish_result.get("post_id",""), content))
                log_act("💼", f"Post LinkedIn publié: {topic}", "validation")
                result = {"action": "post_published_linkedin", "topic": topic,
                          "post_id": publish_result.get("post_id",""), "linkedin": True}
            else:
                # Pas de token ou erreur → marquer comme validé pour copier/coller manuel
                with db_conn() as conn:
                    db_exec(conn, "UPDATE linkedin_posts SET status='published' WHERE content=?", (content,))
                log_act("💼", f"Post LinkedIn validé (publication manuelle): {topic}", "validation")
                result = {"action": "post_approved_manual", "topic": topic, "linkedin": False,
                          "hint": publish_result.get("error","Configure LINKEDIN_ACCESS_TOKEN + LINKEDIN_PERSON_ID pour la publication auto.")}
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
                    _p_name  = PROFILE.get("name","Candidat")
                    _p_title = PROFILE.get("title","Consultant Freelance")
                    _p_exps  = PROFILE.get("experiences", [])
                    _p_certs = PROFILE.get("certifications", [])
                    _p_langs = PROFILE.get("languages", [{"langue":"Français","niveau":"Natif"}])
                    _p_skills = PROFILE.get("skills", [])
                    _p_tjm   = f"{PROFILE.get('tjm_min',500)}-{PROFILE.get('tjm_max',900)}€/j"
                    cv_data = {
                        "title_adapted": offer_title,
                        "accroche": f"{_p_title} avec expérience confirmée. {PROFILE.get('bio','')}",
                        "kpis": PROFILE.get("kpis", ["Expérience confirmée", "Missions grands comptes", "Disponible rapidement"]),
                        "competences_core": _p_skills[:8] if _p_skills else ["Voir profil"],
                        "competences_secondaires": _p_skills[8:16] if len(_p_skills)>8 else [],
                        "experiences": _p_exps,
                        "formations": PROFILE.get("formations", [{"diplome":"Formation","ecole":"","annee":""}]),
                        "certifications": _p_certs,
                        "langues": _p_langs if isinstance(_p_langs[0], dict) else [{"langue":l,"niveau":""} for l in _p_langs] if _p_langs else [{"langue":"Français","niveau":"Natif"}],
                        "soft_skills": ["Autonomie", "Communication client", "Rigueur", "Adaptabilité"],
                        "tjm_suggest": _p_tjm,
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
                _cand_name = PROFILE.get("name","Candidat").replace(" ","_")
                att_name  = f"CV_{_cand_name}_{offer_title[:25].replace(' ','_')}.pdf" if pdf_bytes else None
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

# ── Endpoints publics validation en 1 clic (depuis email/WhatsApp/SMS) ──
_CONFIRM_HTML = lambda action, icon, title, msg, color, dashboard_url: f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>DataLinkedAI — {action}</title>
<style>*{{box-sizing:border-box;margin:0;padding:0}}body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f7fa;display:flex;align-items:center;justify-content:center;min-height:100vh;padding:20px}}
.card{{background:white;border-radius:16px;padding:40px 32px;max-width:480px;width:100%;text-align:center;box-shadow:0 4px 24px rgba(0,0,0,.1)}}
.icon{{font-size:56px;margin-bottom:16px}}.title{{font-size:22px;font-weight:700;color:#1a1a2e;margin-bottom:8px}}
.msg{{font-size:14px;color:#666;line-height:1.6;margin-bottom:24px}}.item{{background:#f0f4ff;border-radius:8px;padding:12px 16px;font-size:13px;color:#333;margin-bottom:20px;text-align:left;border-left:3px solid {color}}}
.btn{{display:inline-block;background:{color};color:white;padding:14px 32px;border-radius:8px;text-decoration:none;font-weight:700;font-size:15px}}</style>
</head><body><div class="card">
<div class="icon">{icon}</div>
<div class="title">{action}</div>
<div class="item">{title}</div>
<div class="msg">{msg}</div>
<a href="{dashboard_url}" class="btn">📊 Voir le Dashboard</a>
</div></body></html>"""

@app.get("/approve/{approval_id}/{token}", tags=["Validation publique"], include_in_schema=False)
async def public_approve(approval_id: int, token: str):
    """Approbation en 1 clic depuis email/SMS/WhatsApp — sans authentification."""
    if not _verify_approval_token(approval_id, token):
        return HTMLResponse("<h2>❌ Lien invalide ou expiré.</h2>", status_code=403)
    with db_conn() as conn:
        row = db_exec(conn, "SELECT type,title,payload,status FROM pending_approvals WHERE id=?", (approval_id,)).fetchone()
    if not row:
        return HTMLResponse("<h2>❌ Élément introuvable.</h2>", status_code=404)
    if row[3] != "pending":
        return HTMLResponse(_CONFIRM_HTML("Déjà traité","ℹ️", row[1], f"Cet élément a déjà été {row[3]}.", "#888", DASHBOARD_URL), status_code=200)
    # Exécuter l'approbation
    try:
        item_type = row[0]; payload = json.loads(row[2]) if row[2] else {}
        result_msg = ""
        if item_type == "linkedin_post":
            content = payload.get("content",""); topic = payload.get("topic","")
            pub = await linkedin_publish(content) if content else {}
            with db_conn() as conn:
                db_exec(conn, "UPDATE linkedin_posts SET status='published' WHERE content=?", (content,))
                db_exec(conn, "UPDATE pending_approvals SET status='approved', approved_at=CURRENT_TIMESTAMP WHERE id=?", (approval_id,))
            result_msg = "Post publié sur LinkedIn ✅" if pub.get("success") else "Post validé — publication manuelle requise"
            log_act("💼", f"Post approuvé (1-clic): {topic}", "validation")
        elif item_type == "candidature":
            app_id = payload.get("app_id"); company = payload.get("company","Recruteur")
            subject = payload.get("subject","Candidature"); email_body = payload.get("email_body","")
            offer_title = payload.get("offer_title",""); company_email = payload.get("company_email","")
            with db_conn() as conn:
                app_row = db_exec(conn, "SELECT cv_pdf_b64,company_email FROM auto_applications WHERE id=?", (app_id,)).fetchone()
            to_email = (app_row[1] if app_row else "") or company_email
            pdf_b64  = app_row[0] if app_row else ""
            if not pdf_b64:
                _p = PROFILE; cv_data = {
                    "title_adapted": offer_title, "accroche": _p.get("bio",""),
                    "kpis": _p.get("kpis",["Expérience confirmée"]),
                    "competences_core": _p.get("skills",[])[:8],
                    "competences_secondaires": _p.get("skills",[])[8:14],
                    "experiences": _p.get("experiences",[]),
                    "formations": _p.get("formations",[]),
                    "certifications": _p.get("certifications",[]),
                    "langues": [{"langue":"Français","niveau":"Natif"}],
                    "soft_skills": ["Autonomie","Rigueur","Communication","Adaptabilité"],
                    "tjm_suggest": f"{_p.get('tjm_max',700)}€/j",
                }
                pdf_bytes_gen = await generate_cv_pdf(cv_data, offer_title)
                pdf_b64 = base64.b64encode(pdf_bytes_gen).decode() if pdf_bytes_gen else ""
                if pdf_b64:
                    with db_conn() as conn:
                        db_exec(conn, "UPDATE auto_applications SET cv_pdf_b64=? WHERE id=?", (pdf_b64, app_id))
            if to_email and EMAIL_PROVIDER != "none":
                pdf_bytes = base64.b64decode(pdf_b64) if pdf_b64 else b""
                cname = PROFILE.get("name","Candidat").replace(" ","_")
                att = f"CV_{cname}_{offer_title[:25].replace(' ','_')}.pdf" if pdf_bytes else None
                await send_email(to_email, company, subject, make_html_email(email_body), email_body,
                                 att_bytes=pdf_bytes or None, att_name=att)
                with db_conn() as conn:
                    db_exec(conn, "UPDATE auto_applications SET status='sent', applied_at=CURRENT_TIMESTAMP WHERE id=?", (app_id,))
                result_msg = f"Email envoyé à {to_email} avec CV PDF ✅"
            else:
                with db_conn() as conn:
                    db_exec(conn, "UPDATE auto_applications SET status='approved' WHERE id=?", (app_id,))
                result_msg = "Candidature approuvée — configure l'email pour l'envoi auto"
            with db_conn() as conn:
                db_exec(conn, "UPDATE pending_approvals SET status='approved', approved_at=CURRENT_TIMESTAMP WHERE id=?", (approval_id,))
            log_act("✅", f"Candidature approuvée (1-clic): {offer_title}", "validation")
        return HTMLResponse(_CONFIRM_HTML("Validé !", "✅", row[1], result_msg, "#00B96B", DASHBOARD_URL))
    except Exception as e:
        logger.error(f"public_approve error: {e}", exc_info=True)
        return HTMLResponse(f"<h2>❌ Erreur: {e}</h2>", status_code=500)

@app.get("/reject/{approval_id}/{token}", tags=["Validation publique"], include_in_schema=False)
async def public_reject(approval_id: int, token: str):
    """Rejet en 1 clic depuis email/SMS/WhatsApp — sans authentification."""
    if not _verify_approval_token(approval_id, token):
        return HTMLResponse("<h2>❌ Lien invalide ou expiré.</h2>", status_code=403)
    with db_conn() as conn:
        row = db_exec(conn, "SELECT type,title,status FROM pending_approvals WHERE id=?", (approval_id,)).fetchone()
    if not row:
        return HTMLResponse("<h2>❌ Élément introuvable.</h2>", status_code=404)
    if row[2] != "pending":
        return HTMLResponse(_CONFIRM_HTML("Déjà traité","ℹ️", row[1], f"Cet élément a déjà été {row[2]}.", "#888", DASHBOARD_URL))
    with db_conn() as conn:
        db_exec(conn, "UPDATE pending_approvals SET status='rejected', rejected_at=CURRENT_TIMESTAMP WHERE id=?", (approval_id,))
    log_act("❌", f"Rejeté (1-clic): {row[1]}", "validation")
    return HTMLResponse(_CONFIRM_HTML("Rejeté", "❌", row[1], "Élément rejeté. Aucun envoi effectué.", "#EF4444", DASHBOARD_URL))

@app.post("/api/notifications/test", tags=["Notifications"])
async def test_notifications(_=Depends(verify_api_key)):
    """Envoie une notification de test sur tous les canaux configurés."""
    test_approval = [{
        "type": "test", "id": 0, "token": "test-token",
        "title": "🧪 Test notification DataLinkedAI",
        "preview": "Si tu reçois ce message, les notifications fonctionnent correctement !",
        "score": 99,
    }]
    results = await notify_all(test_approval, now_str=datetime.now().strftime("%A %d %B %Y"))
    channels = {
        "email":     bool(COPILOT_EMAIL and EMAIL_PROVIDER != "none"),
        "whatsapp":  bool(TWILIO_SID and NOTIF_WHATSAPP),
        "sms":       bool(TWILIO_SID and NOTIF_PHONE),
        "pushover":  bool(PUSHOVER_TOKEN and PUSHOVER_USER),
    }
    return {"channels_configured": channels, "channels_active": NOTIF_CHANNELS, "results": results}

@app.get("/api/notifications/status", tags=["Notifications"])
async def notification_status(_=Depends(verify_api_key)):
    """Statut de configuration des canaux de notification."""
    return {
        "channels_active": NOTIF_CHANNELS,
        "email":    {"configured": bool(COPILOT_EMAIL and EMAIL_PROVIDER != "none"), "address": COPILOT_EMAIL, "provider": EMAIL_PROVIDER},
        "whatsapp": {"configured": bool(TWILIO_SID and NOTIF_WHATSAPP), "number": NOTIF_WHATSAPP or "Non configuré"},
        "sms":      {"configured": bool(TWILIO_SID and NOTIF_PHONE), "number": NOTIF_PHONE or "Non configuré"},
        "pushover": {"configured": bool(PUSHOVER_TOKEN and PUSHOVER_USER)},
        "dashboard_url": DASHBOARD_URL,
    }

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
        emails_replied = cnt("SELECT COUNT(*) FROM emails WHERE replied_at IS NOT NULL")
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
<meta property="og:title" content="DataLinkedAI — Freelance Platform">
<meta property="og:description" content="Data Architect / Engineer Senior • TJM 500-900€/j • Remote">
<meta property="og:type" content="profile">
<title>DataLinkedAI — Freelance Platform</title>
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
  <h1 id="landing-name">DataLinkedAI</h1>
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
<footer>DataLinkedAI · Plateforme Freelance IA</footer>
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
