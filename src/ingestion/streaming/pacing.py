"""Ritmo dos producers: N eventos por segundo pelo relógio, sem acumular atraso (issue #72).

Antes, o laço enviava e depois esperava o intervalo inteiro (`1/RATE`). O envio é síncrono
(`future.get()` a cada mensagem, ~13 ms), então o ritmo real era `1/(intervalo + envio)`: ~15 TPS
para um alvo de 20. O `Pacer` marca o tick n em `início + n × intervalo` e espera só o que falta até
lá; se o envio atrasou, o próximo tick sai na hora (ou imediatamente).

Atraso grande (Kafka fora do ar, producer de mercado parado fora do pregão) não vira rajada: passou
de `max_lag_s` atrasado, o relógio é realinhado em vez de emitir todos os ticks perdidos de uma vez.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from threading import Event


class Pacer:
    def __init__(
        self,
        rate: float,
        max_lag_s: float = 1.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if rate <= 0:
            raise ValueError(f"rate precisa ser > 0, veio {rate}")
        self.interval = 1.0 / rate
        self.max_lag_s = max_lag_s
        self._clock = clock
        self._next: float | None = None
        self.resyncs = 0

    def wait(self, stop_event: Event) -> bool:
        """Espera até o próximo tick. Devolve False se `stop_event` foi sinalizado."""
        now = self._clock()
        if self._next is None:
            self._next = now
        elif now - self._next > self.max_lag_s:
            # Muito atrasado: realinha em vez de emitir os ticks perdidos em rajada.
            self._next = now
            self.resyncs += 1
        delay = self._next - now
        self._next += self.interval
        if delay > 0:
            return not stop_event.wait(timeout=delay)
        return not stop_event.is_set()

    def reset(self) -> None:
        """Recomeça a contagem no próximo `wait` (ex.: depois de uma pausa planejada)."""
        self._next = None
