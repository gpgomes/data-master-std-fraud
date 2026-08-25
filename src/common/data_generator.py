"""Gerador de dados sintéticos para transações financeiras e dados de mercado."""

import random
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np
from faker import Faker

from src.common.schemas import (
    Channel,
    Currency,
    CustomerSegment,
    FraudType,
    MerchantCategory,
    TransactionType,
)

# ── Constantes ─────────────────────────────────────────────────────────────────

BRAZILIAN_BANKS = [
    "Banco do Brasil",
    "Itaú",
    "Bradesco",
    "Caixa Econômica",
    "Santander",
    "Nubank",
    "Inter",
    "C6 Bank",
    "BTG Pactual",
    "Sicredi",
]

BRAZILIAN_CITIES: list[dict[str, Any]] = [
    {"city": "São Paulo", "state": "SP", "lat": -23.5505, "lon": -46.6333},
    {"city": "Rio de Janeiro", "state": "RJ", "lat": -22.9068, "lon": -43.1729},
    {"city": "Brasília", "state": "DF", "lat": -15.7801, "lon": -47.9292},
    {"city": "Salvador", "state": "BA", "lat": -12.9714, "lon": -38.5014},
    {"city": "Fortaleza", "state": "CE", "lat": -3.7319, "lon": -38.5267},
    {"city": "Curitiba", "state": "PR", "lat": -25.4290, "lon": -49.2671},
    {"city": "Manaus", "state": "AM", "lat": -3.1190, "lon": -60.0217},
    {"city": "Recife", "state": "PE", "lat": -8.0476, "lon": -34.8770},
    {"city": "Porto Alegre", "state": "RS", "lat": -30.0346, "lon": -51.2177},
    {"city": "Belo Horizonte", "state": "MG", "lat": -19.9167, "lon": -43.9345},
]

MARKET_SYMBOLS = [
    "PETR4.SA",
    "VALE3.SA",
    "ITUB4.SA",
    "BBDC4.SA",
    "ABEV3.SA",
    "WEGE3.SA",
    "RENT3.SA",
    "BBAS3.SA",
    "MGLU3.SA",
    "LREN3.SA",
]

# Pesos para distribuição de transaction_type
TRANSACTION_TYPE_WEIGHTS = [0.45, 0.20, 0.10, 0.12, 0.08, 0.05]  # PIX dominante

# Pesos para merchant_category
MERCHANT_CATEGORY_WEIGHTS = [0.25, 0.20, 0.10, 0.08, 0.12, 0.10, 0.05, 0.10]

# Pesos para channel
CHANNEL_WEIGHTS = [0.50, 0.25, 0.05, 0.10, 0.10]

# Pesos para currency (80% BRL)
CURRENCY_WEIGHTS = [0.80, 0.12, 0.08]


# ── DataGenerator ──────────────────────────────────────────────────────────────


class DataGenerator:
    """Gera datasets sintéticos realistas de transações financeiras."""

    def __init__(self, seed: int = 42) -> None:
        self.seed = seed
        # Instâncias isoladas de RNG para garantir reprodutibilidade
        self.rng = random.Random(seed)
        self.np_rng = np.random.RandomState(seed)
        self.fake = Faker("pt_BR")
        Faker.seed(seed)

    def _uuid(self) -> str:
        """Gera UUID determinístico usando a instância de RNG."""
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    # ── Customers ──────────────────────────────────────────────────────────────

    def generate_customers(self, n: int = 10_000) -> list[dict[str, Any]]:
        """Gera n registros de clientes (tabela dimensional)."""
        customers = []
        genders = ["M", "F"]
        segments = [s.value for s in CustomerSegment]
        segment_weights = [0.70, 0.25, 0.05]

        for _ in range(n):
            city_info = self.rng.choice(BRAZILIAN_CITIES)
            gender = self.rng.choice(genders)
            birth_date = self.fake.date_of_birth(minimum_age=18, maximum_age=75)
            opening_date = self.fake.date_between(start_date="-10y", end_date="today")

            # CPF mascarado: formato ***.***.***-XX (só os 2 últimos dígitos visíveis)
            cpf_digits = "".join([str(self.rng.randint(0, 9)) for _ in range(11)])
            cpf_masked = f"***.***.***-{cpf_digits[9:11]}"

            customers.append(
                {
                    "customer_id": self._uuid(),
                    "name": self.fake.name_male() if gender == "M" else self.fake.name_female(),
                    "cpf_masked": cpf_masked,
                    "birth_date": birth_date.strftime("%Y-%m-%d"),
                    "gender": gender,
                    "account_opening_date": opening_date.strftime("%Y-%m-%d"),
                    "risk_score": round(self.rng.uniform(0, 100), 2),
                    "segment": self.rng.choices(segments, weights=segment_weights, k=1)[0],
                    "city": city_info["city"],
                    "state": city_info["state"],
                    "country": "BR",
                }
            )

        return customers

    # ── Transactions ───────────────────────────────────────────────────────────

    def generate_transactions(
        self,
        customers: list[dict[str, Any]],
        n: int = 500_000,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Gera n transações financeiras realistas com ~2-3% de fraude."""
        if start_date is None:
            end_date = datetime.now(tz=UTC)
            start_date = end_date - timedelta(days=180)
        elif end_date is None:
            end_date = datetime.now(tz=UTC)

        total_seconds = int((end_date - start_date).total_seconds())
        transactions = []

        transaction_types = [t.value for t in TransactionType]
        merchant_categories = [m.value for m in MerchantCategory]
        channels = [c.value for c in Channel]
        currencies = [c.value for c in Currency]

        # Parâmetros log-normal calibrados para BRL (média ~R$500, mediana ~R$150)
        lognormal_mean = 5.0
        lognormal_sigma = 1.2

        fraud_rate = 0.025  # 2.5%

        customer_ids = [c["customer_id"] for c in customers]
        # Mapa rápido de customer_id → city info (para geoloc impossível)
        customer_cities = {
            c["customer_id"]: next(
                (loc for loc in BRAZILIAN_CITIES if loc["city"] == c["city"]),
                BRAZILIAN_CITIES[0],
            )
            for c in customers
        }

        # Pré-gera valores aleatórios em batch para performance
        amounts_raw = self.np_rng.lognormal(lognormal_mean, lognormal_sigma, n)
        fraud_flags = self.np_rng.random(n) < fraud_rate
        time_offsets = self.np_rng.randint(0, total_seconds, n)

        for i in range(n):
            customer_id = self.rng.choice(customer_ids)
            city_info = customer_cities[customer_id]

            ts = start_date + timedelta(seconds=int(time_offsets[i]))
            is_fraud = bool(fraud_flags[i])

            tx_type = self.rng.choices(transaction_types, weights=TRANSACTION_TYPE_WEIGHTS, k=1)[0]
            merchant_cat = self.rng.choices(
                merchant_categories, weights=MERCHANT_CATEGORY_WEIGHTS, k=1
            )[0]
            channel = self.rng.choices(channels, weights=CHANNEL_WEIGHTS, k=1)[0]
            currency = self.rng.choices(currencies, weights=CURRENCY_WEIGHTS, k=1)[0]

            amount = round(float(amounts_raw[i]), 2)
            amount = max(1.0, min(amount, 500_000.0))

            origin_bank = self.rng.choice(BRAZILIAN_BANKS)
            dest_bank = self.rng.choice(BRAZILIAN_BANKS)

            lat = city_info["lat"] + self.rng.uniform(-0.5, 0.5)
            lon = city_info["lon"] + self.rng.uniform(-0.5, 0.5)
            fraud_type = None

            if is_fraud:
                ts, amount, lat, lon, fraud_type, merchant_cat = self._apply_fraud_pattern(
                    ts, amount, lat, lon, city_info, merchant_categories
                )

            dev_hex = f"{self.rng.getrandbits(48):012x}"
            transactions.append(
                {
                    "transaction_id": self._uuid(),
                    "customer_id": customer_id,
                    "timestamp": ts.isoformat(),
                    "amount": amount,
                    "currency": currency,
                    "transaction_type": tx_type,
                    "merchant_category": merchant_cat,
                    "origin_account": self._gen_account(),
                    "destination_account": self._gen_account(),
                    "origin_bank": origin_bank,
                    "destination_bank": dest_bank,
                    "channel": channel,
                    "device_id": f"dev-{dev_hex}" if self.rng.random() > 0.1 else None,
                    "ip_address": self.fake.ipv4() if self.rng.random() > 0.05 else None,
                    "latitude": round(lat, 6),
                    "longitude": round(lon, 6),
                    "is_fraud": is_fraud,
                    "fraud_type": fraud_type,
                }
            )

        return transactions

    def _apply_fraud_pattern(
        self,
        ts: datetime,
        amount: float,
        lat: float,
        lon: float,
        city_info: dict[str, Any],
        merchant_categories: list[str],
    ) -> tuple[datetime, float, float, float, str, str]:
        """Aplica um padrão de fraude realista à transação."""
        fraud_types = [f.value for f in FraudType]
        fraud_type = self.rng.choice(fraud_types)

        pattern = self.rng.randint(1, 4)

        if pattern == 1:
            # Madrugada (00h-05h) + valor alto
            ts = ts.replace(hour=self.rng.randint(0, 4), minute=self.rng.randint(0, 59))
            amount = round(self.rng.uniform(5_000, 50_000), 2)

        elif pattern == 2:
            # Valor alto e redondo em categoria incomum
            amount = float(self.rng.choice([5000, 10000, 15000, 20000, 25000, 50000]))
            merchant_cat = self.rng.choice(["SAQUE", "TRANSFERENCIA"])
            return ts, amount, lat, lon, fraud_type, merchant_cat

        elif pattern == 3:
            # Geolocalização impossível: outra cidade distante
            other_cities = [c for c in BRAZILIAN_CITIES if c["city"] != city_info["city"]]
            other_city = self.rng.choice(other_cities)
            lat = other_city["lat"] + self.rng.uniform(-0.2, 0.2)
            lon = other_city["lon"] + self.rng.uniform(-0.2, 0.2)
            amount = round(self.rng.uniform(1_000, 20_000), 2)

        else:
            # Valor alto genérico
            amount = round(self.rng.uniform(8_000, 100_000), 2)

        merchant_cat = self.rng.choice(merchant_categories)
        return ts, amount, lat, lon, fraud_type, merchant_cat

    # ── Market Data ────────────────────────────────────────────────────────────

    def generate_market_data(
        self,
        symbols: list[str] | None = None,
        n_days: int = 100,
        end_date: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Gera n_days de dados OHLCV para cada símbolo (total = len(symbols) * n_days)."""
        if symbols is None:
            symbols = MARKET_SYMBOLS
        if end_date is None:
            end_date = datetime.now(tz=UTC)

        records = []

        # Preços iniciais realistas (BRL)
        initial_prices: dict[str, float] = {
            "PETR4.SA": 38.50,
            "VALE3.SA": 68.00,
            "ITUB4.SA": 32.00,
            "BBDC4.SA": 14.50,
            "ABEV3.SA": 12.80,
            "WEGE3.SA": 42.00,
            "RENT3.SA": 58.00,
            "BBAS3.SA": 27.00,
            "MGLU3.SA": 5.50,
            "LREN3.SA": 18.00,
        }

        for symbol in symbols:
            base_price = initial_prices.get(symbol, 30.0)
            price = base_price

            trading_days = self._get_trading_days(end_date, n_days)

            for trade_date in trading_days:
                # Random walk com drift leve
                daily_return = self.np_rng.normal(0.0002, 0.018)
                price = max(0.01, price * (1 + daily_return))

                intraday_vol = price * self.rng.uniform(0.005, 0.03)
                open_p = round(price * (1 + self.np_rng.normal(0, 0.005)), 2)
                close_p = round(price, 2)
                high_p = round(
                    max(open_p, close_p) + abs(self.np_rng.normal(0, intraday_vol)), 2
                )
                low_p = round(
                    min(open_p, close_p) - abs(self.np_rng.normal(0, intraday_vol)), 2
                )
                low_p = max(0.01, low_p)
                volume = int(self.np_rng.lognormal(14, 1))
                adj_close = round(close_p * self.rng.uniform(0.97, 1.0), 2)

                records.append(
                    {
                        "symbol": symbol,
                        "date": trade_date.strftime("%Y-%m-%d"),
                        "open": open_p,
                        "high": high_p,
                        "low": low_p,
                        "close": close_p,
                        "volume": volume,
                        "adjusted_close": adj_close,
                    }
                )

        return records

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _gen_account(self) -> str:
        """Gera número de conta bancária fictício."""
        agency = f"{self.rng.randint(1, 9999):04d}"
        account = f"{self.rng.randint(10000, 999999):06d}"
        digit = self.rng.randint(0, 9)
        return f"{agency}-{account}-{digit}"

    @staticmethod
    def _get_trading_days(end_date: datetime, n_days: int) -> list[datetime]:
        """Retorna lista de dias úteis (seg–sex) terminando em end_date."""
        days: list[datetime] = []
        current = end_date
        while len(days) < n_days:
            if current.weekday() < 5:  # 0=seg … 4=sex
                days.append(current)
            current -= timedelta(days=1)
        return list(reversed(days))
