# DataLinkedAI POC 🚀
**Backend API gratuit — deploy en 3 minutes**

## Ce que c'est

- **FastAPI** — 8 endpoints IA (offres, CV, LinkedIn, TJM, prospection, proposition, audit)
- **SQLite** — base de données embarquée, zéro configuration
- **Claude Sonnet** — toute l'intelligence artificielle
- **Single container** — un seul fichier `main.py`, une seule image Docker
- **Gratuit** — Railway, Render ou Fly.io

---

## Option 1 — Railway ⭐ (recommandé, le plus simple)

**Gratuit : 5$/mois de crédit offert — largement suffisant pour un POC**

```bash
# 1. Crée un compte : https://railway.app

# 2. Installe la CLI
npm install -g @railway/cli

# 3. Login
railway login

# 4. Dans ce dossier :
railway init
railway up

# 5. Ajoute ta clé API
railway variables set ANTHROPIC_API_KEY=sk-ant-api03-...

# 6. Récupère l'URL publique
railway domain
```

**Ou via GitHub (encore plus simple) :**
1. Push ce dossier sur GitHub
2. railway.app → "New Project" → "Deploy from GitHub"
3. Sélectionne ton repo
4. Add variable : `ANTHROPIC_API_KEY`
5. ✅ C'est en ligne en 2 minutes

---

## Option 2 — Render (gratuit total, se met en veille)

**Gratuit à vie — se réveille en ~30s après inactivité**

```bash
# 1. Crée un compte : https://render.com

# 2. Push ce dossier sur GitHub

# 3. render.com → "New Web Service" → Connect GitHub repo

# 4. Settings :
#    - Runtime: Docker
#    - Environment variable: ANTHROPIC_API_KEY = sk-ant-...
#    - Health check path: /health

# 5. Deploy → URL publique en 3-4 minutes
```

---

## Option 3 — Fly.io (le plus performant gratuit)

**3 machines gratuites à vie, Paris disponible**

```bash
# 1. Installe flyctl
curl -L https://fly.io/install.sh | sh

# 2. Login
fly auth login

# 3. Dans ce dossier :
fly launch --name datalinkedai-poc --region cdg

# 4. Ajoute ta clé API (stockée de façon sécurisée)
fly secrets set ANTHROPIC_API_KEY=sk-ant-api03-...

# 5. Deploy
fly deploy

# 6. URL publique
fly open
```

---

## Test rapide après deploy

```bash
# Remplace [URL] par ton URL publique

# Health check
curl https://[URL]/health

# Analyser une offre
curl -X POST https://[URL]/api/offers/analyze \
  -H "Content-Type: application/json" \
  -d '{"text": "Data Architect freelance full remote 12 mois, Snowflake dbt Airflow, TJM 750-850€"}'

# Adapter le CV
curl -X POST https://[URL]/api/cv/adapt \
  -H "Content-Type: application/json" \
  -d '{"offer_text": "Mission Data Engineer Senior, Snowflake, dbt, Python, full remote"}'

# Générer un post LinkedIn
curl -X POST https://[URL]/api/linkedin/generate \
  -H "Content-Type: application/json" \
  -d '{"topic": "Snowflake", "format": "Retour d'\''XP mission", "tone": "expert"}'

# Benchmark TJM
curl -X POST https://[URL]/api/tjm/analyze \
  -H "Content-Type: application/json" \
  -d '{"context": ""}'
```

---

## Variable d'environnement requise

Une seule clé suffit — **OpenAI OU Anthropic** :

| Variable | Valeur | Provider |
|----------|--------|----------|
| `OPENAI_API_KEY` | `sk-proj-...` | ✅ ChatGPT / GPT-4o |
| `ANTHROPIC_API_KEY` | `sk-ant-...` | ✅ Claude Sonnet |
| `OPENAI_MODEL` | `gpt-4o-mini` | (optionnel, défaut: `gpt-4o`) |

> Si les deux clés sont présentes, **OpenAI est prioritaire**.

### Où récupérer les clés ?
- **OpenAI** : https://platform.openai.com/api-keys
- **Anthropic** : https://console.anthropic.com/settings/keys

---

## API Docs interactifs

Une fois déployé, ouvre : `https://[URL]/docs`

Tu peux tester tous les endpoints directement depuis le navigateur — pas besoin de Postman.

---

## Fichiers

```
datalinkedai-poc/
├── main.py           ← toute l'application (1 fichier)
├── requirements.txt  ← 4 dépendances seulement
├── Dockerfile        ← image légère python:3.12-slim
├── railway.toml      ← config Railway (auto-détectée)
├── render.yaml       ← config Render (auto-détectée)
└── fly.toml          ← config Fly.io (auto-détectée)
```
