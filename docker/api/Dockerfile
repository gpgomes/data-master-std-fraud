FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Apenas o necessário para servir a API — evita a árvore pesada de
# dependências do projeto inteiro (pyspark, great-expectations etc.), que a
# API não usa.
# setuptools só serve para instalar; em runtime ele só carrega cópias vendorizadas
# (jaraco.context, wheel) com CVEs que o trivy acusa (issue #58).
RUN pip install --no-cache-dir \
    fastapi==0.135.1 \
    "uvicorn[standard]==0.29.0" \
    pydantic==2.7.1 \
    pydantic-settings==2.2.1 \
    sqlalchemy==2.0.29 \
    psycopg2-binary==2.9.9 \
    python-dotenv==1.2.2 \
    loguru==0.7.2 \
    && pip uninstall -y setuptools wheel

RUN mkdir -p /app/src/serving
COPY src/common /app/src/common
COPY src/serving/__init__.py /app/src/serving/__init__.py
COPY src/serving/api /app/src/serving/api
# métricas e auditoria de acesso (issues #55/#58): o main.py importa src.observability
COPY src/observability /app/src/observability
RUN touch /app/src/__init__.py

EXPOSE 8000

CMD ["uvicorn", "src.serving.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
