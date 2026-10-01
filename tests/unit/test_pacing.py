"""Ritmo dos producers (issue #72): N eventos por segundo mesmo com envio lento."""

from __future__ import annotations

import pytest

from src.ingestion.streaming.pacing import Pacer


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class FakeEvent:
    """`wait` avança o relógio falso em vez de dormir; `set` encerra o laço."""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self._set = False

    def wait(self, timeout: float) -> bool:
        self.clock.now += timeout
        return self._set

    def is_set(self) -> bool:
        return self._set

    def set(self) -> None:
        self._set = True


def _ticks(rate: float, send_s: float, seconds: float, **kwargs) -> int:
    """Quantos ticks saem em `seconds` com cada envio custando `send_s`."""
    clock = FakeClock()
    stop = FakeEvent(clock)
    pacer = Pacer(rate, clock=clock, **kwargs)
    sent = 0
    while pacer.wait(stop) and clock.now < seconds:
        sent += 1
        clock.now += send_s  # envio síncrono
    return sent


class TestPacer:
    @pytest.mark.parametrize("rate", [20, 50])
    def test_slow_send_does_not_lower_the_rate(self, rate: int) -> None:
        """O defeito da #72: com envio de 13 ms o laço antigo dava ~15,9 TPS para 20."""
        sent = _ticks(rate, send_s=0.013, seconds=60)
        assert sent == pytest.approx(rate * 60, rel=0.01)

    def test_the_old_loop_would_have_missed_the_target(self) -> None:
        """Não vacuoso: esperar o intervalo inteiro depois de enviar perde ~21% a 20 TPS."""
        clock = FakeClock()
        sent = 0
        while clock.now < 60:
            sent += 1
            clock.now += 0.013 + 1 / 20
        assert sent < 20 * 60 * 0.8

    def test_send_slower_than_the_interval_goes_as_fast_as_possible(self) -> None:
        """Se cada envio leva mais que o intervalo, não há espera: o teto é o envio."""
        sent = _ticks(100, send_s=0.02, seconds=10)
        assert sent == pytest.approx(10 / 0.02, rel=0.02)

    def test_a_long_stall_is_not_followed_by_a_burst(self) -> None:
        clock = FakeClock()
        stop = FakeEvent(clock)
        pacer = Pacer(10, clock=clock)
        assert pacer.wait(stop)
        clock.now += 45  # Kafka fora do ar por 45 s
        sent_right_after = 0
        while pacer.wait(stop) and clock.now < 46:
            sent_right_after += 1
        assert sent_right_after <= 11  # ~10 em 1 s, não os 450 perdidos
        assert pacer.resyncs == 1

    def test_reset_starts_a_fresh_schedule(self) -> None:
        clock = FakeClock()
        stop = FakeEvent(clock)
        pacer = Pacer(10, clock=clock)
        pacer.wait(stop)
        clock.now += 60  # pausa planejada (fora do pregão)
        pacer.reset()
        assert pacer.wait(stop)
        assert pacer.resyncs == 0

    def test_stop_ends_the_loop(self) -> None:
        clock = FakeClock()
        stop = FakeEvent(clock)
        pacer = Pacer(10, clock=clock)
        stop.set()
        assert pacer.wait(stop) is False

    def test_rate_must_be_positive(self) -> None:
        with pytest.raises(ValueError):
            Pacer(0)
