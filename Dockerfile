FROM python:3.12-slim

WORKDIR /app

# Deps only
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Code + assets statiques
COPY main.py .
COPY static/ ./static/

# Non-root
RUN useradd -m app && chown -R app /app
USER app

# Cloud Run / Railway / Render injectent PORT automatiquement
ENV PORT=8080
EXPOSE 8080

# Exec form avec sh -c pour permettre l'expansion de $PORT
# Shell form bloque le forwarding SIGTERM → uvicorn ne s'arrête pas proprement
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080} --log-level info"]
