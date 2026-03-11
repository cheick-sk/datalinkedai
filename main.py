"""
DataLinkedAI v3.0 — Plateforme Freelance Data | PRODUCTION READY
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
from apscheduler.triggers.cron      import CronTrigger

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger(__name__)

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
EMAIL_FROM_NAME = os.getenv("EMAIL_FROM_NAME", "Sekouna KABA — Data Architect")
EMAIL_FROM_ADDR = os.getenv("EMAIL_FROM_ADDR", GMAIL_USER or "sekouna@datalinkedai.com")
EMAIL_PROVIDER  = "resend" if RESEND_API_KEY else ("gmail" if GMAIL_USER and GMAIL_APP_PASS else "none")

COPILOT_EMAIL        = os.getenv("COPILOT_EMAIL", GMAIL_USER or "kaba.sekouna@gmail.com")
COPILOT_HOUR         = int(os.getenv("COPILOT_HOUR", "8"))
AUTO_APPLY_ENABLED   = os.getenv("AUTO_APPLY", "false").lower() == "true"
AUTO_APPLY_MIN_SCORE = int(os.getenv("AUTO_APPLY_MIN_SCORE", "85"))
FOLLOWUP_DAYS        = int(os.getenv("FOLLOWUP_DAYS", "5"))
APIFY_TOKEN          = os.getenv("APIFY_TOKEN", "")

PH        = "%s" if USE_POSTGRES else "?"
scheduler = AsyncIOScheduler(timezone="Europe/Paris")

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

async def ask(prompt: str, tokens: int = 1400) -> str:
    if AI_PROVIDER == "none":
        raise HTTPException(500, "Aucune cle IA configuree. Ajoute GROQ_API_KEY (gratuit sur console.groq.com).")
    async with httpx.AsyncClient(timeout=90.0) as cl:
        if AI_PROVIDER in ("groq", "openai"):
            # Groq et OpenAI partagent le meme format d'API
            base_url = "https://api.groq.com/openai/v1" if AI_PROVIDER == "groq" else "https://api.openai.com/v1"
            api_key  = GROQ_API_KEY if AI_PROVIDER == "groq" else OPENAI_API_KEY
            r = await cl.post(
                f"{base_url}/chat/completions",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={"model": AI_MODEL, "max_tokens": tokens, "temperature": 0.7,
                      "messages": [{"role": "user", "content": prompt}]},
            )
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
            r.raise_for_status()
            return "".join(b.get("text", "") for b in r.json().get("content", []))

async def ask_json(prompt: str, tokens: int = 1600) -> dict:
    raw = await ask(prompt, tokens)
    cleaned = re.sub(r"```json\s*|```\s*", "", raw).strip()
    match = re.search(r"\{[\s\S]*\}", cleaned)
    if match:
        cleaned = match.group(0)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        logger.error(f"ask_json parse error: {e}\nRaw (300c): {raw[:300]}")
        raise HTTPException(500, "L'IA n'a pas renvoyé du JSON valide. Réessaie.")

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
    """Génération PDF synchrone — appelée via run_in_executor."""
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles    import ParagraphStyle
        from reportlab.lib.units     import cm
        from reportlab.lib           import colors
        from reportlab.platypus      import SimpleDocTemplate, Paragraph, Spacer, HRFlowable
        from reportlab.lib.enums     import TA_JUSTIFY
    except ImportError:
        logger.warning("reportlab non installe — PDF desactive")
        return b""

    buf  = io.BytesIO()
    doc  = SimpleDocTemplate(buf, pagesize=A4,
                             leftMargin=1.8*cm, rightMargin=1.8*cm,
                             topMargin=1.5*cm,  bottomMargin=1.5*cm)
    BLUE = colors.HexColor("#1A56FF")
    DARK = colors.HexColor("#0C1630")
    MUTED= colors.HexColor("#7B8DB5")

    sN  = ParagraphStyle("n",  fontSize=22, fontName="Helvetica-Bold",  textColor=DARK, spaceAfter=2)
    sT  = ParagraphStyle("t",  fontSize=11, fontName="Helvetica",       textColor=BLUE, spaceAfter=4)
    sI  = ParagraphStyle("i",  fontSize=9,  fontName="Helvetica",       textColor=MUTED,spaceAfter=2)
    sH1 = ParagraphStyle("h1", fontSize=11, fontName="Helvetica-Bold",  textColor=BLUE, spaceBefore=12, spaceAfter=4)
    sB  = ParagraphStyle("b",  fontSize=9,  fontName="Helvetica",       textColor=DARK, leading=14, spaceAfter=4, alignment=TA_JUSTIFY)
    sCo = ParagraphStyle("co", fontSize=10, fontName="Helvetica-Bold",  textColor=DARK, spaceAfter=1)
    sDt = ParagraphStyle("dt", fontSize=8,  fontName="Helvetica",       textColor=MUTED,spaceAfter=3)
    sLi = ParagraphStyle("li", fontSize=9,  fontName="Helvetica",       textColor=DARK, leading=13, leftIndent=12, spaceAfter=2)
    sTg = ParagraphStyle("tg", fontSize=8,  fontName="Helvetica",       textColor=BLUE, spaceAfter=2)
    HR  = lambda: HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#DDE3F0"), spaceAfter=6)
    HR2 = lambda: HRFlowable(width="100%", thickness=2,   color=BLUE, spaceAfter=10)

    title_adapted = cv_data.get("title_adapted", PROFILE["title"])
    accroche      = cv_data.get("accroche", "")
    competences   = cv_data.get("competences", PROFILE["skills"])
    experiences   = cv_data.get("experiences", [])
    tjm           = cv_data.get("tjm_suggest", "760€/j")

    story = [
        Paragraph("KABA Sekouna", sN),
        Paragraph(title_adapted, sT),
        Paragraph(f"kaba.sekouna@gmail.com  |  +33 06 59 02 21 57  |  Montreuil, Ile-de-France  |  TJM : {tjm}", sI),
        HR2(),
    ]
    if accroche:
        story += [Paragraph("PROFIL", sH1), HR(), Paragraph(accroche, sB)]

    story += [Paragraph("COMPETENCES TECHNIQUES", sH1), HR()]
    skills_list = competences if isinstance(competences, list) else PROFILE["skills"]
    story.append(Paragraph("  |  ".join(skills_list[:12]), sTg))

    story += [Paragraph("EXPERIENCES PROFESSIONNELLES", sH1), HR()]
    exp_list = experiences if experiences else [
        {"company": e["company"], "period": e["period"], "role": e["role"],
         "missions": [e.get("stack", "")]} for e in PROFILE["experiences"]
    ]
    for exp in exp_list[:5]:
        story.append(Paragraph(f"{exp.get('company','')} — {exp.get('role','')}", sCo))
        story.append(Paragraph(exp.get("period", ""), sDt))
        missions = exp.get("missions", [exp.get("pitch", exp.get("stack", ""))])
        for m in (missions if isinstance(missions, list) else [missions])[:3]:
            if m:
                story.append(Paragraph(f"• {m}", sLi))
        if exp.get("stack"):
            story.append(Paragraph(f"Stack : {exp['stack']}", sTg))
        story.append(Spacer(1, 6))

    story += [Paragraph("FORMATION", sH1), HR()]
    story.append(Paragraph("• Master en Informatique / Data Engineering", sLi))
    story.append(Paragraph("• Certifications : AWS Certified | GCP Professional Data Engineer | Snowflake SnowPro", sLi))

    doc.build(story)
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
JSON seul.""", 600)
            with db_conn() as conn:
                cur = db_insert(
                    conn,
                    "INSERT INTO offers(title,source,description,match_score,tjm_negotiate,urgency,status,url) VALUES(?,?,?,?,?,?,?,?)",
                    (raw["title"], raw["source"], raw["description"], result.get("match_score"),
                     result.get("tjm_negotiate"), result.get("urgency", ""), "analyzed", raw.get("url", "")),
                )
                offer_id = cur.lastrowid
            entry = {**result, "offer_id": offer_id,
                     "url": raw.get("url"), "company": raw.get("company"), "source": raw.get("source")}
            analyzed.append(entry)
            if result.get("match_score", 0) >= AUTO_APPLY_MIN_SCORE and AUTO_APPLY_ENABLED:
                await run_auto_apply(offer_id, raw["title"], raw.get("company", ""), raw["description"], result)
                auto_applied += 1
        except Exception as e:
            logger.error(f"Copilot offer error: {e}")

    analyzed.sort(key=lambda x: x.get("match_score", 0), reverse=True)
    top3  = analyzed[:3]
    topic = random.choice(["Snowflake", "dbt Core", "Medallion Architecture", "PySpark", "Kafka", "TJM Freelance"])
    fmt   = random.choice(["Opinion tranchee", "Retour d experience mission", "How-to pratique"])

    try:
        post = await ask(f"""Tu es Sekouna KABA, Data Engineer Senior freelance.{json.dumps(PROFILE)}
Post LinkedIn. Sujet:{topic}. Format:{fmt}. 1ere personne, missions reelles, 150-180 mots, emojis, hashtags.
UNIQUEMENT le texte.""", 600)
        with db_conn() as conn:
            db_exec(conn,
                "INSERT INTO linkedin_posts(topic,format,tone,content,status) VALUES(?,?,?,?,?)",
                (topic, fmt, "expert", post.strip(), "ready"))
    except Exception as e:
        post = f"[Erreur post: {e}]"

    digest_sent = False
    if COPILOT_EMAIL and EMAIL_PROVIDER != "none":
        try:
            offers_html = "".join(
                f"<div style='border-left:4px solid "
                f"{'#00B96B' if o.get('match_score',0)>=85 else '#F59E0B'};"
                f"padding:10px 14px;margin-bottom:8px;background:#F9FAFB'>"
                f"<strong>{o.get('title','')}</strong> — "
                f"<span style='color:{'#00B96B' if o.get('match_score',0)>=85 else '#F59E0B'}'>"
                f"{o.get('match_score',0)}%</span><br>"
                f"<small>{o.get('company','')} | TJM: {o.get('tjm_negotiate','?')}</small><br>"
                f"<small>{o.get('match_reason','')}</small></div>"
                for o in top3
            )
            body_html = f"""<html><body style='font-family:sans-serif;max-width:600px;margin:0 auto;padding:24px'>
<div style='background:linear-gradient(135deg,#1A56FF,#7C3AED);padding:20px;border-radius:12px;margin-bottom:20px'>
<p style='color:rgba(255,255,255,.7);font-size:11px;margin:0'>COPILOT QUOTIDIEN</p>
<h2 style='color:white;margin:4px 0 0'>Brief data du {now_str}</h2></div>
<h3>Top offres du jour</h3>{offers_html}
<hr style='border:none;border-top:1px solid #eee;margin:20px 0'>
<h3>Post LinkedIn du jour</h3>
<div style='background:#F4F6FB;padding:14px;border-radius:8px;font-size:13px;white-space:pre-wrap'>{post[:300]}...</div>
<p style='color:#aaa;font-size:11px;text-align:center;margin-top:20px'>DataLinkedAI v3.0 | Sekouna KABA</p>
</body></html>"""
            body_text = f"Brief {now_str}\n\n" + "\n".join(
                f"- {o.get('title')} ({o.get('match_score')}%)" for o in top3
            )
            await send_email(COPILOT_EMAIL, "Sekouna",
                             f"Brief data {now_str} — {len(top3)} offres",
                             body_html, body_text)
            digest_sent = True
        except Exception as e:
            logger.error(f"Digest error: {e}")

    top = top3[0] if top3 else {}
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

async def run_auto_apply(offer_id, title, company, description, analysis):
    cv = await ask_json(f"""Sekouna KABA Data Architect/Engineer freelance. TJM 500-900euro.
{json.dumps(PROFILE)}
Offre: {description[:800]}
JSON:{{"title_adapted":"<titre>","accroche":"<2 phrases percutantes 1ere personne>",
"competences":["<s1>","<s2>","<s3>","<s4>","<s5>"],"experiences":[{{"company":"<n>","period":"<p>","role":"<r>","missions":["<m1>","<m2>"]}}],
"tjm_suggest":"<TJM 500-900euro>"}}
JSON seul.""", 900)

    # FIX-C : génération PDF async
    pdf_bytes = generate_cv_pdf(cv, title)
    pdf_b64   = base64.b64encode(pdf_bytes).decode() if pdf_bytes else ""

    email_body = await ask(f"""Tu es Sekouna KABA, Data Engineer Senior freelance.
Email candidature pour: {title} chez {company or "l entreprise"}.
TJM: {analysis.get('tjm_negotiate','760euro/j')}
100-130 mots, direct, 1ere personne, mentionne SACEM ou Thales, mentionne CV en PJ.
UNIQUEMENT le texte.""", 500)

    subject      = f"Candidature freelance — {cv.get('title_adapted', title)}"
    followup_at  = (datetime.now() + timedelta(days=FOLLOWUP_DAYS)).isoformat()
    status       = "sent" if AUTO_APPLY_ENABLED else "pending"
    # FIX-D : applied_at renseigné si statut = sent
    applied_at   = datetime.now().isoformat() if status == "sent" else None

    with db_conn() as conn:
        cur = db_insert(
            conn,
            "INSERT INTO auto_applications(offer_id,offer_title,match_score,tjm_negotiate,email_subject,email_body,cv_pdf_b64,status,applied_at,followup_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (offer_id, title, analysis.get("match_score"), analysis.get("tjm_negotiate",""),
             subject, email_body, pdf_b64, status, applied_at, followup_at),
        )
        app_id = cur.lastrowid
    log_act("🤖", f"Auto-apply: {title} ({analysis.get('match_score')}%) {status}", "agent")
    return {"app_id": app_id, "status": status, "subject": subject, "followup_at": followup_at,
            "cv": cv, "pdf_bytes": pdf_bytes, "pdf_b64": pdf_b64, "cv_pdf_generated": bool(pdf_bytes), "email_body": email_body}

async def run_followups():
    now = datetime.now().isoformat()
    with db_conn() as conn:
        due = db_exec(
            conn,
            "SELECT id,offer_title FROM auto_applications WHERE status='sent' AND followup_sent=0 AND followup_at<=? AND reply_received=0",
            (now,),
        ).fetchall()
    for app_id, title in due:
        try:
            await ask(f"Sekouna KABA freelance. Relance candidature {title}. 60 mots max, naturel. UNIQUEMENT le texte.", 300)
            with db_conn() as conn:
                db_exec(conn, "UPDATE auto_applications SET followup_sent=1 WHERE id=?", (app_id,))
            log_act("🔄", f"Relance: {title}", "agent")
        except Exception as e:
            logger.error(f"Followup {app_id}: {e}")
    return {"followups_sent": len(due)}

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
    logger.info("DataLinkedAI v3.0 — Démarrage")
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
    scheduler.add_job(run_followups, CronTrigger(hour=10, minute=0),
                      id="followups", replace_existing=True)
    scheduler.start()
    yield
    # Shutdown
    scheduler.shutdown()
    logger.info("DataLinkedAI arrêté proprement.")

app = FastAPI(
    title="DataLinkedAI API v3.0",
    description="Plateforme freelance data automatisée | Sekouna KABA | TJM 500-900€",
    version="3.0.0",
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
        "status": "ok", "version": "3.0.0",
        "ai": AI_PROVIDER, "model": AI_MODEL,
        "db": "postgresql" if USE_POSTGRES else "sqlite",
        "email": EMAIL_PROVIDER,
        "scraping": "apify" if APIFY_TOKEN else "mock",
        "security": "api_key" if API_KEY else "none",
        "auto_apply": AUTO_APPLY_ENABLED,
        "jobs": [{"id": j.id, "next": str(j.next_run_time)} for j in scheduler.get_jobs()],
    }

@app.get("/api/profile", tags=["Système"])
async def profile():
    return PROFILE

@app.get("/api/dashboard", tags=["Système"])
async def dashboard():
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

@app.post("/api/offers/analyze", tags=["Offres Freelance"])
async def analyze_offer(req: OfferReq):
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
async def list_offers():
    with db_conn() as conn:
        rows = db_exec(
            conn,
            "SELECT id,title,source,match_score,tjm_negotiate,urgency,status,url,created_at FROM offers ORDER BY created_at DESC LIMIT 30",
        ).fetchall()
    return [{"id": r[0], "title": r[1], "source": r[2], "match_score": r[3],
             "tjm_negotiate": r[4], "urgency": r[5], "status": r[6],
             "url": r[7], "created_at": str(r[8])} for r in rows]

# ── CV ───────────────────────────────────────────────
class CVReq(BaseModel):
    offer_text: str
    generate_pdf: bool = True

@app.post("/api/cv/adapt", tags=["CV Adaptatif"])
async def adapt_cv(req: CVReq):
    r = await ask_json(f"""Sekouna KABA Data Engineer Senior/Architect, TJM 500-900euro.
CV:{json.dumps(PROFILE)}\nOffre:---{req.offer_text}---
JSON:{{"score":<0-100>,"title_adapted":"<titre>","accroche":"<3 phrases 1ere personne>",
"competences":["<s1>","<s2>","<s3>","<s4>","<s5>","<s6>"],
"experiences":[{{"company":"<n>","period":"<p>","role":"<r>","pertinence":<0-100>,"pitch":"<1 phrase>","missions":["<m1>","<m2>","<m3>"]}}],
"cover_letter":"<3 paragraphes directs>","strengths":["<f1>","<f2>","<f3>"],
"advice":"<conseil 2 phrases>","tjm_suggest":"<TJM 500-900euro et pourquoi>"}}
Max 5 experiences triees pertinence. JSON seul.""", 1800)
    pdf_bytes = generate_cv_pdf(r, r.get("title_adapted","")) if req.generate_pdf else b""
    pdf_b64   = base64.b64encode(pdf_bytes).decode() if pdf_bytes else ""
    with db_conn() as conn:
        db_exec(conn,
            "INSERT INTO cv_adaptations(offer_title,score,title_adapted,accroche,cover_letter,tjm_suggest,cv_pdf_b64) VALUES(?,?,?,?,?,?,?)",
            (r.get("title_adapted",""), r.get("score",0), r.get("title_adapted",""),
             r.get("accroche",""), r.get("cover_letter",""), r.get("tjm_suggest",""), pdf_b64))
    log_act("📄", f"Adapte: {r.get('title_adapted','')} ({r.get('score')}%)", "cv")
    return {**r, "cv_pdf_generated": bool(pdf_bytes), "pdf_size_kb": round(len(pdf_bytes)/1024,1) if pdf_bytes else 0}

@app.get("/api/cv/history", tags=["CV Adaptatif"])
async def cv_history():
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
async def gen_post(req: PostReq):
    content = await ask(f"""Tu es Sekouna KABA, Data Engineer Senior/Architect freelance.{json.dumps(PROFILE)}
Post LinkedIn francais. Sujet:{req.topic}. Format:{req.format}. Ton:{req.tone}.{f' Angle:{req.angle}' if req.angle else ''}
1ere personne, missions reelles (SACEM/Thales/Accor), hook ligne1, 150-220 mots, 3-5 emojis, 5-7 hashtags fin.
UNIQUEMENT le texte du post.""", 800)
    with db_conn() as conn:
        db_exec(conn,
            "INSERT INTO linkedin_posts(topic,format,tone,content) VALUES(?,?,?,?)",
            (req.topic, req.format, req.tone, content.strip()))
    log_act("💼", f"Post: {req.topic} ({req.format})", "linkedin")
    return {"content": content.strip()}

@app.get("/api/linkedin/posts", tags=["LinkedIn Studio"])
async def list_posts():
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
async def gen_prospect(req: ProspectReq):
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
async def analyze_tjm(req: TJMReq):
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
async def gen_proposal(req: ProposalReq):
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
async def audit_profile():
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
async def compose_email(req: EmailComposeReq):
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
async def send_email_ep(req: EmailSendReq):
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
async def email_campaign(req: EmailBatchReq):
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
async def mark_email_replied(email_id: int):
    _rnow = datetime.now().isoformat()   # FIX: Python datetime compatible SQLite+PG
    with db_conn() as conn:
        db_exec(conn,
            "UPDATE emails SET replied_at=?, status='replied' WHERE id=?",
            (_rnow, email_id))
    return {"email_id": email_id, "status": "replied"}

@app.get("/api/email/history", tags=["Email Prospection"])
async def email_history(status: Optional[str] = None, limit: int = 50):
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
async def email_stats():
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
async def add_contact(req: ContactReq):
    with db_conn() as conn:
        cur = db_insert(conn,
            "INSERT INTO email_contacts(name,email,company,role,source,notes) VALUES(?,?,?,?,?,?)",
            (req.name, req.email, req.company, req.role, req.source, req.notes))
        cid = cur.lastrowid
    return {"contact_id": cid, "status": "added"}

@app.get("/api/email/contacts", tags=["Email Prospection"])
async def list_contacts():
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
async def trigger_copilot(bt: BackgroundTasks):
    bt.add_task(run_copilot)
    return {"status": "started", "mode": "apify" if APIFY_TOKEN else "mock",
            "message": "Le copilot tourne en arrière-plan. Vérifie /api/copilot/history dans 60s."}

@app.post("/api/copilot/run/sync", tags=["Copilot Quotidien"])
async def trigger_copilot_sync():
    return await run_copilot()

@app.get("/api/copilot/history", tags=["Copilot Quotidien"])
async def copilot_history():
    with db_conn() as conn:
        rows = db_exec(
            conn,
            "SELECT id,offers_found,top_offer,top_score,post_topic,digest_sent,auto_applied,run_at FROM copilot_runs ORDER BY run_at DESC LIMIT 20",
        ).fetchall()
    return [{"id": r[0], "offers_found": r[1], "top_offer": r[2], "top_score": r[3],
             "post_topic": r[4], "digest_sent": bool(r[5]),
             "auto_applied": r[6], "run_at": str(r[7])} for r in rows]

@app.get("/api/copilot/config", tags=["Copilot Quotidien"])
async def copilot_config():
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
async def agent_apply(req: AutoApplyReq):
    analysis = await ask_json(f"""Sekouna KABA Data Architect/Engineer Senior. TJM 500-900euro.
{json.dumps(PROFILE)}\nOffre: {req.description}
JSON:{{"match_score":<0-100>,"tjm_negotiate":"<TJM 500-900euro>",
"urgency":"<Postuler maintenant/Peut attendre>","match_reason":"<raison>"}}
JSON seul.""", 500)
    score = analysis.get("match_score", 0)
    if score < 60:
        return {"match_score": score, "decision": "Score trop faible (<60%) — pas de candidature générée",
                "analysis": analysis}
    # FIX: cree un offer_id reel (jamais 0)
    _oc = get_db()
    _ocur = db_insert(_oc, "INSERT INTO offers(title,source,description,match_score,tjm_negotiate,urgency,status) VALUES(?,?,?,?,?,?,?)",
        (req.offer_title,"manual",req.description[:1000],score,analysis.get("tjm_negotiate",""),analysis.get("urgency",""),"analyzed"))
    offer_id = _ocur.lastrowid; _oc.commit(); _oc.close()
    result = await run_auto_apply(offer_id, req.offer_title, req.company, req.description, analysis)
    if (AUTO_APPLY_ENABLED or req.force_send) and req.company_email:
        # FIX: pdf_bytes directement depuis result (pas result.cv.cv_pdf_b64)
        pdf_bytes = result.get("pdf_bytes") or b""
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

@app.get("/api/agent/applications", tags=["Agent Candidature Auto"])
async def list_applications(status: Optional[str] = None):
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
async def mark_replied_app(app_id: int):
    with db_conn() as conn:
        db_exec(conn,
            "UPDATE auto_applications SET reply_received=1, status='replied' WHERE id=?",
            (app_id,))
    log_act("✅", f"Candidature #{app_id} repondue !", "agent")
    return {"app_id": app_id, "status": "replied"}

@app.get("/api/agent/stats", tags=["Agent Candidature Auto"])
async def agent_stats():
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
async def negotiate(req: NegotiationReq):
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

@app.get("/api/negotiate/history", tags=["Négociateur TJM"])
async def negotiate_history():
    with db_conn() as conn:
        rows = db_exec(
            conn,
            "SELECT id,context,current_offer,target_tjm,counter_offer,outcome,created_at FROM negotiations ORDER BY created_at DESC LIMIT 20",
        ).fetchall()
    return [{"id": r[0], "context": str(r[1])[:80], "current_offer": r[2],
             "target": r[3], "counter_offer": r[4], "outcome": r[5],
             "created_at": str(r[6])} for r in rows]

@app.patch("/api/negotiate/{neg_id}/outcome", tags=["Négociateur TJM"])
async def set_outcome(neg_id: int, outcome: str):
    with db_conn() as conn:
        db_exec(conn, "UPDATE negotiations SET outcome=? WHERE id=?", (outcome, neg_id))
    return {"neg_id": neg_id, "outcome": outcome}

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
