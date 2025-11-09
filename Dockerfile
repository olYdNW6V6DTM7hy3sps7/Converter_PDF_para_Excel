# Multi-stage build para imagem pequena e segura
FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependências de build (mantenha mínimo; remova se não precisar)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Usuário não-root
RUN adduser --disabled-password --gecos "" appuser
WORKDIR /app

# Copia libs instaladas
COPY --from=builder /install /usr/local

# Copia app
COPY main.py /app/main.py
COPY requirements.txt /app/requirements.txt

USER appuser

# Expor porta padrão local; no Render usaremos $PORT
EXPOSE 8000

# Permite sobrescrever via env (Render define $PORT)
# WORKERS é opcional para concorrer melhor em instâncias maiores
ENV PORT=8000
ENV WORKERS=1

# Use sh -c para expandir variáveis de ambiente
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT} --workers ${WORKERS}"]