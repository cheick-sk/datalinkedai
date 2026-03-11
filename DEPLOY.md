# 🚀 DataLinkedAI — Guide déploiement complet

## ÉTAPE 1 — Push GitHub

```bash
git init && git add . && git commit -m "🚀 v2.0 prod"
git remote add origin https://github.com/cheick-sk/datalinkedai.git
git branch -M main && git push -u origin main
# Password = ton token ghp_xxxx
```

## ÉTAPE 2 — Railway

1. railway.app → New Project → Deploy from GitHub → `datalinkedai`
2. Settings → Networking → Generate Domain
3. Ajouter les variables (Étape 3)

## ÉTAPE 3 — Variables Railway (obligatoires)

```
OPENAI_API_KEY  = sk-...
API_KEY         = datalinked-2025
COPILOT_EMAIL   = kaba.sekouna@gmail.com
```

## ÉTAPE 4 — Supabase (persistance données)

1. supabase.com → New project → Name: datalinkedai, Region: West EU
2. Settings → Database → Connection String → URI
3. Copier: postgresql://postgres:[PASS]@db.[REF].supabase.co:5432/postgres
4. Railway Variables → DATABASE_URL = cette URL (avec vrai mot de passe)
5. Vérif: /health → "db":"postgresql"

## ÉTAPE 5 — Gmail App Password

1. myaccount.google.com/security → Mots de passe des applications
2. Créer → copier le code 16 caractères
3. Railway Variables:
   GMAIL_USER      = kaba.sekouna@gmail.com
   GMAIL_APP_PASS  = xxxx xxxx xxxx xxxx

## ÉTAPE 6 — Apify (scraping offres réelles)

1. apify.com → Settings → Integrations → API tokens → Create
2. Railway Variables → APIFY_TOKEN = apify_api_xxxxx
3. Vérif: /health → "scraping":"apify"

## ÉTAPE 7 — Tester

- /health → tous les champs OK
- /dashboard → configurer URL + API_KEY → point vert
- Dashboard → Copilot → Lancer → email reçu ✅

## Variables complètes

```
OPENAI_API_KEY=sk-...
API_KEY=datalinked-2025
DATABASE_URL=postgresql://postgres:[PASS]@db.[REF].supabase.co:5432/postgres
GMAIL_USER=kaba.sekouna@gmail.com
GMAIL_APP_PASS=xxxx xxxx xxxx xxxx
COPILOT_EMAIL=kaba.sekouna@gmail.com
COPILOT_HOUR=8
APIFY_TOKEN=apify_api_...
AUTO_APPLY=false
AUTO_APPLY_MIN_SCORE=85
FOLLOWUP_DAYS=5
```
