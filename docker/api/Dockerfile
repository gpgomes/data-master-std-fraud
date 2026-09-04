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
RUN pip install --no-cache-dir \
    fastapi==0.111.0 \
    "uvicorn[standard]==0.29.0" \
    pydantic==2.7.1 \
    pydantic-settings==2.2.1 \
    sqlalchemy==2.0.29 \
    psycopg2-binary==2.9.9 \
    python-dotenv==1.0.1 \
    loguru==0.7.2

RUN mkdir -p /app/src/serving
COPY src/common /app/src/common
COPY src/serving/__init__.py /app/src/serving/__init__.py
COPY src/serving/api /app/src/serving/api
RUN touch /app/src/__init__.py

EXPOSE 8000

CMD ["uvicorn", "src.serving.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
