# Multi-stage build to keep the final image small and secure.

# 1) Builder stage: install dependencies into a staging dir
FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# System deps only if needed for building; pandas/openpyxl/pdfplumber ship wheels typically.
# Keep minimal. Add build-essential only if compilation is required.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# 2) Runtime stage: copy Python deps, create non-root user
FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Create non-root user
RUN adduser --disabled-password --gecos "" appuser

WORKDIR /app

# Copy installed site-packages from builder
COPY --from=builder /install /usr/local

# Copy application code
COPY main.py /app/main.py
COPY requirements.txt /app/requirements.txt

USER appuser

EXPOSE 8000

# Use uvicorn in production (can tune workers based on CPU and workload)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]