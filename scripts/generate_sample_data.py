"""CLI para geração de dados sintéticos de transações financeiras.

Uso:
    python -m scripts.generate_sample_data
    python -m scripts.generate_sample_data --transactions 10000 --customers 1000
    python -m scripts.generate_sample_data --seed 123 --output-dir data/sample
"""

import argparse
import csv
import json
import sys
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

# Garante que o pacote raiz está no sys.path ao rodar como script direto
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.common.data_generator import MARKET_SYMBOLS, DataGenerator

# ── I/O helpers ────────────────────────────────────────────────────────────────


def _write_csv(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not records:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(records[0].keys()))
        writer.writeheader()
        writer.writerows(records)


def _write_json(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, default=str)


def save_customers(customers: list[dict], output_dir: Path) -> None:
    path = output_dir / "customers" / "customers.csv"
    _write_csv(path, customers)
    print(f"  Clientes salvos: {path}  ({len(customers):,} registros)")


def save_transactions(transactions: list[dict], output_dir: Path) -> None:
    """Particiona transações por data (YYYY/MM/DD) e salva como CSV."""
    by_date: dict[str, list[dict]] = {}
    for tx in transactions:
        date_str = tx["timestamp"][:10]  # "YYYY-MM-DD"
        by_date.setdefault(date_str, []).append(tx)

    for date_str, records in by_date.items():
        year, month, day = date_str.split("-")
        path = output_dir / "transactions" / year / month / day / "transactions.csv"
        _write_csv(path, records)

    total_partitions = len(by_date)
    print(
        f"  Transações salvas: {output_dir / 'transactions'}  "
        f"({len(transactions):,} registros em {total_partitions} partições)"
    )


def save_ground_truth(ground_truth: list[dict], output_dir: Path) -> None:
    """Sidecar de ground truth (episódio, cenário, stealth, hard negatives) por transaction_id.

    Fica em `ground_truth/`, fora de `transactions/` (que o loader Bronze varre por `*.csv`), e
    fora do `TransactionEvent`: só o avaliador do detector (issue #44) o consome.
    """
    path = output_dir / "ground_truth" / "ground_truth.csv"
    _write_csv(path, ground_truth)
    print(f"  Ground truth salvo: {path}  ({len(ground_truth):,} registros)")


def save_market_data(market_records: list[dict], output_dir: Path) -> None:
    """Particiona cotações por data (YYYY/MM/DD) e salva como CSV."""
    by_date: dict[str, list[dict]] = {}
    for rec in market_records:
        date_str = rec["date"]  # "YYYY-MM-DD"
        by_date.setdefault(date_str, []).append(rec)

    for date_str, records in by_date.items():
        year, month, day = date_str.split("-")
        path = output_dir / "market_data" / year / month / day / "market.csv"
        _write_csv(path, records)

    print(
        f"  Cotações salvas:   {output_dir / 'market_data'}  "
        f"({len(market_records):,} registros em {len(by_date)} partições)"
    )


# ── Estatísticas ───────────────────────────────────────────────────────────────


def print_stats(
    customers: list[dict],
    transactions: list[dict],
    market_records: list[dict],
    ground_truth: list[dict] | None = None,
) -> None:
    total = len(transactions)
    fraud_count = sum(1 for t in transactions if t["is_fraud"])
    fraud_pct = fraud_count / total * 100 if total else 0

    type_counter: Counter = Counter(t["transaction_type"] for t in transactions)
    cat_counter: Counter = Counter(t["merchant_category"] for t in transactions)
    fraud_type_counter: Counter = Counter(
        t["fraud_type"] for t in transactions if t["is_fraud"]
    )

    print("\n" + "=" * 60)
    print("  ESTATÍSTICAS DOS DADOS GERADOS")
    print("=" * 60)
    print(f"  Clientes:          {len(customers):>10,}")
    print(f"  Transações:        {total:>10,}")
    print(f"  Transações fraude: {fraud_count:>10,}  ({fraud_pct:.2f}%)")
    print(f"  Cotações (OHLCV):  {len(market_records):>10,}")
    print()
    print("  Distribuição por tipo de transação:")
    for tx_type, count in type_counter.most_common():
        print(f"    {tx_type:<25} {count:>8,}  ({count / total * 100:.1f}%)")
    print()
    print("  Top 5 categorias de merchant:")
    for cat, count in cat_counter.most_common(5):
        print(f"    {cat:<25} {count:>8,}  ({count / total * 100:.1f}%)")
    print()
    print("  Distribuição por tipo de fraude:")
    for ft, count in fraud_type_counter.most_common():
        print(f"    {ft:<30} {count:>6,}")
    if ground_truth:
        episodes = {r["episode_id"] for r in ground_truth if r["episode_id"]}
        stealth_episodes = {r["episode_id"] for r in ground_truth if r["episode_id"] and r["stealth"]}
        hard_negatives: Counter = Counter(
            kind for r in ground_truth for kind in r["hard_negative"].split(";") if kind
        )
        print()
        print(f"  Episódios de fraude: {len(episodes):>8,}  (stealth: {len(stealth_episodes):,})")
        print("  Hard negatives (legítimos que imitam fraude):")
        for kind, count in hard_negatives.most_common():
            print(f"    {kind:<25} {count:>8,}  ({count / total * 100:.2f}%)")
    print("=" * 60 + "\n")


# ── CLI ────────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gera datasets sintéticos de transações financeiras para o Data Master."
    )
    parser.add_argument(
        "--transactions",
        type=int,
        default=500_000,
        metavar="N",
        help="Número de transações a gerar (padrão: 500000)",
    )
    parser.add_argument(
        "--customers",
        type=int,
        default=10_000,
        metavar="N",
        help="Número de clientes a gerar (padrão: 10000)",
    )
    parser.add_argument(
        "--market-days",
        type=int,
        default=100,
        metavar="N",
        help="Número de dias de cotações por ativo (padrão: 100)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Seed para reprodutibilidade (padrão: 42)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/sample",
        metavar="PATH",
        help="Diretório de saída (padrão: data/sample)",
    )
    parser.add_argument(
        "--months",
        type=int,
        default=6,
        help="Período em meses para as transações (padrão: 6)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)

    print(f"\nIniciando geração de dados sintéticos (seed={args.seed})...")
    print(f"  Destino: {output_dir.resolve()}\n")

    gen = DataGenerator(seed=args.seed)

    end_date = datetime.now(tz=UTC)
    start_date = end_date - timedelta(days=args.months * 30)

    # 1. Clientes
    print(f"[1/3] Gerando {args.customers:,} clientes...")
    customers = gen.generate_customers(n=args.customers)
    save_customers(customers, output_dir)

    # 2. Transações
    print(f"[2/3] Gerando {args.transactions:,} transações ({args.months} meses)...")
    transactions = gen.generate_transactions(
        customers=customers,
        n=args.transactions,
        start_date=start_date,
        end_date=end_date,
    )
    save_transactions(transactions, output_dir)
    save_ground_truth(gen.last_ground_truth, output_dir)

    # 3. Cotações
    total_market = len(MARKET_SYMBOLS) * args.market_days
    print(f"[3/3] Gerando {total_market:,} registros de cotações...")
    market_records = gen.generate_market_data(n_days=args.market_days)
    save_market_data(market_records, output_dir)

    print_stats(customers, transactions, market_records, gen.last_ground_truth)
    print("Concluído.")


if __name__ == "__main__":
    main()
