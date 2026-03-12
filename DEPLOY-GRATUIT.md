# DataLinkedAI v3.0 — Déploiement 100% GRATUIT
## Stack : Groq + Render + Supabase + Gmail

---

## ÉTAPE 1 — Groq (IA gratuite) — 3 minutes

1. Va sur **console.groq.com**
2. Clique **Sign Up** (Google ou email)
3. Menu gauche → **API Keys** → **Create API Key**
4. Nom : `datalinkedai` → **Submit**
5. Copie la clé : `gsk_xxxxxxxxx...`
6. ⚠️ Sauvegarde-la — elle ne s'affiche qu'une fois

**Limites gratuites Groq :**
- 14 400 requêtes/jour
- 500 000 tokens/minute
- Modèle : Llama 3.3 70B (meilleur que GPT-3.5)

---

## ÉTAPE 2 — Supabase (base de données) — 5 minutes

1. Va sur **supabase.com** → **Start your project**
2. **New project** :
   - Organization : ton compte
   - Name : `datalinkedai`
   - Database Password : invente un mot de passe fort → **copie-le**
   - Region : **West EU (Paris)**
3. Attend 2 minutes que le projet se crée
4. Menu gauche → **Settings** → **Database**
5. Section **Connection String** → onglet **URI**
6. Copie l'URL : `postgresql://postgres:[PASSWORD]@db.[REF].supabase.co:5432/postgres`
7. Remplace `[PASSWORD]` par le mot de passe créé à l'étape 2

---

## ÉTAPE 3 — Gmail App Password (email) — 3 minutes

1. Va sur **myaccount.google.com/security**
2. Section **Connexion à Google** → **Validation en 2 étapes** (active si pas encore fait)
3. En bas de la page → **Mots de passe des applications**
4. Sélectionne : Autre (nom personnalisé) → tape `DataLinkedAI` → **Générer**
5. Copie le code à 16 caractères : `xxxx xxxx xxxx xxxx`

---

## ÉTAPE 4 — GitHub (push du code) — 5 minutes

```bash
# Dans le dossier du projet
cd datalinkedai-poc

git init
git add .
git commit -m "DataLinkedAI v3.0 prod"

# Sur github.com : New repository "datalinkedai" (privé recommandé)
git remote add origin https://github.com/cheick-sk/datalinkedai.git
git branch -M main
git push -u origin main
```

---

## ÉTAPE 5 — Render (hébergement gratuit) — 5 minutes

1. Va sur **render.com** → **Get Started for Free**
2. **New +** → **Web Service**
3. **Connect a repository** → connecte GitHub → sélectionne `datalinkedai`
4. Configure :
   - **Name** : `datalinkedai`
   - **Region** : `Frankfurt (EU Central)`
   - **Branch** : `main`
   - **Runtime** : `Docker`
   - **Instance Type** : **Free** ✅

5. **Environment Variables** → Ajoute une par une :

| Key | Value |
|-----|-------|
| `GROQ_API_KEY` | `gsk_xxx...` (ta clé Groq) |
| `GROQ_MODEL` | `llama-3.3-70b-versatile` |
| `API_KEY` | `datalinked-2025-secret` |
| `DATABASE_URL` | `postgresql://postgres:...` (ton URL Supabase) |
| `GMAIL_USER` | `kaba.sekouna@gmail.com` |
| `GMAIL_APP_PASS` | `xxxx xxxx xxxx xxxx` |
| `COPILOT_EMAIL` | `kaba.sekouna@gmail.com` |
| `COPILOT_HOUR` | `8` |

6. **Create Web Service** → Render build et déploie (~3 minutes)

---

## ÉTAPE 6 — Vérification

Une fois déployé, ton URL sera : `https://datalinkedai.onrender.com`

1. Ouvre : `https://datalinkedai.onrender.com/health`
   ```json
   {
     "status": "ok",
     "ai": "groq",
     "model": "llama-3.3-70b-versatile",
     "db": "postgresql",
     "email": "gmail"
   }
   ```

2. Ouvre le dashboard : `https://datalinkedai.onrender.com/dashboard`
3. Configure dans la topbar :
   - URL : `https://datalinkedai.onrender.com`
   - API Key : `datalinked-2025-secret`
   - Clique **Tester ✓** → point vert ✅

---

## ⚠️ Limite importante de Render Free

Le plan gratuit **met l'app en veille après 15 min d'inactivité**.
Le premier chargement après veille prend ~30 secondes.

**Solution gratuite** : UptimeRobot (gratuit)
1. Va sur **uptimerobot.com** → compte gratuit
2. **New Monitor** → HTTP(s)
3. URL : `https://datalinkedai.onrender.com/health`
4. Interval : **5 minutes**
→ L'app reste éveillée en permanence ✅

---

## Bilan des coûts

| Service | Plan | Coût |
|---------|------|------|
| Groq | Free | 0€ |
| Render | Free | 0€ |
| Supabase | Free | 0€ |
| Gmail SMTP | Gratuit | 0€ |
| UptimeRobot | Free | 0€ |
| **TOTAL** | | **0€/mois** |

---

## Commandes utiles

```bash
# Tester en local
pip install -r requirements.txt
cp .env.example .env
# Edite .env avec tes vraies clés
uvicorn main:app --reload --port 8000
# Ouvre http://localhost:8000/dashboard
```
