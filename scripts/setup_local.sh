#!/usr/bin/env bash
# setup_local.sh — Inicializa buckets MinIO e tópicos Kafka após docker-compose up
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

# Carrega variáveis de ambiente
if [ -f "$ROOT_DIR/.env" ]; then
    set -a && source "$ROOT_DIR/.env" && set +a
fi

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:29092}"

echo "=== Data Master — Setup Local ==="
echo ""

# ── MinIO ─────────────────────────────────────────────────────────────────────
echo "[1/2] Configurando MinIO..."

wait_for_minio() {
    local max_attempts=30
    local attempt=0
    echo "  Aguardando MinIO ficar disponível..."
    until curl -sf "$MINIO_ENDPOINT/minio/health/live" > /dev/null 2>&1; do
        attempt=$((attempt + 1))
        if [ $attempt -ge $max_attempts ]; then
            echo "  ERRO: MinIO não respondeu após ${max_attempts} tentativas."
            exit 1
        fi
        sleep 2
    done
    echo "  MinIO disponível."
}

wait_for_minio

# Configura alias mc
docker compose exec -T minio mc alias set local "$MINIO_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" --quiet 2>/dev/null || true

# Cria buckets
for bucket in bronze silver gold checkpoints; do
    if docker compose exec -T minio mc ls "local/$bucket" > /dev/null 2>&1; then
        echo "  Bucket '$bucket' já existe."
    else
        docker compose exec -T minio mc mb "local/$bucket" --quiet
        echo "  Bucket '$bucket' criado."
    fi
done

# Configura políticas de acesso
docker compose exec -T minio mc anonymous set download local/bronze > /dev/null 2>&1 || true

echo "  MinIO configurado com sucesso."
echo ""

# ── Kafka ─────────────────────────────────────────────────────────────────────
echo "[2/2] Configurando Kafka..."

wait_for_kafka() {
    local max_attempts=30
    local attempt=0
    echo "  Aguardando Kafka ficar disponível..."
    until docker compose exec -T kafka kafka-topics.sh \
        --bootstrap-server kafka:9092 \
        --list > /dev/null 2>&1; do
        attempt=$((attempt + 1))
        if [ $attempt -ge $max_attempts ]; then
            echo "  ERRO: Kafka não respondeu após ${max_attempts} tentativas."
            exit 1
        fi
        sleep 2
    done
    echo "  Kafka disponível."
}

wait_for_kafka

# Cria tópicos
declare -A TOPICS=(
    ["raw-transactions"]="3:1"
    ["raw-market-data"]="3:1"
    ["enriched-transactions"]="3:1"
    ["fraud-alerts"]="1:1"
)

for topic in "${!TOPICS[@]}"; do
    partitions="${TOPICS[$topic]%%:*}"
    replication="${TOPICS[$topic]##*:}"

    if docker compose exec -T kafka kafka-topics.sh \
        --bootstrap-server kafka:9092 \
        --describe --topic "$topic" > /dev/null 2>&1; then
        echo "  Tópico '$topic' já existe."
    else
        docker compose exec -T kafka kafka-topics.sh \
            --bootstrap-server kafka:9092 \
            --create \
            --topic "$topic" \
            --partitions "$partitions" \
            --replication-factor "$replication" \
            --if-not-exists
        echo "  Tópico '$topic' criado (partitions=$partitions, replication=$replication)."
    fi
done

echo "  Kafka configurado com sucesso."
echo ""
echo "=== Setup concluído! ==="
echo ""
echo "Próximos passos:"
echo "  make seed-data           — Gerar dados sintéticos"
echo "  make spark-submit-batch  — Executar pipeline batch"
echo "  make spark-submit-stream — Iniciar processamento streaming"
