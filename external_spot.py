from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass(frozen=True)
class SpotSnapshot:
    asset: str
    price: float
    timestamp: float
    source: str = "external"

    @property
    def age_ms(self) -> float:
        return max(0.0, (time.time() - self.timestamp) * 1000.0)
