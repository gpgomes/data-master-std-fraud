FROM python:3.11-slim

# Evita prompts interativos
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Dependências do sistema
# apt-get upgrade: atualizações de segurança do SO que saíram depois da imagem base (issue #66;
# ex.: OpenSSL do Debian 13 da python:3.11-slim).
RUN apt-get update && apt-get upgrade -y --no-install-recommends \
    && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Dependências Python — apenas o necessário para os producers
COPY requirements-producer.txt .
# setuptools só serve para instalar; em runtime ele só carrega cópias vendorizadas
# (jaraco.context, wheel) com CVEs que o trivy acusa (issue #58).
RUN pip install --no-cache-dir -r requirements-producer.txt \
    && pip uninstall -y setuptools wheel

# PRODUCER_TYPE controla qual producer rodar: "transactions" ou "market"
ENV PRODUCER_TYPE=transactions \
    KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    PRODUCER_RATE_TPS=10 \
    MARKET_PRODUCER_RATE_TPS=50 \
    SOURCE_SYSTEM=simulator \
    GENERATOR_SEED=0 \
    LOG_LEVEL=INFO

ENTRYPOINT ["sh", "-c", "\
  if [ \"$PRODUCER_TYPE\" = \"market\" ]; then \
    python -m src.ingestion.streaming.kafka_producer_market; \
  else \
    python -m src.ingestion.streaming.kafka_producer_transactions; \
  fi \
"]
