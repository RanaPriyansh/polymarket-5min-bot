from __future__ import annotations

import math


SECONDS_PER_YEAR = 365.0 * 24.0 * 60.0 * 60.0


def normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def clamp_probability(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def gbm_terminal_up_probability(
    *,
    spot: float,
    strike: float,
    seconds_to_expiry: float,
    annualized_vol: float,
    annualized_drift: float = 0.0,
) -> float:
    if spot <= 0 or strike <= 0:
        raise ValueError("spot and strike must be positive")
    if seconds_to_expiry <= 0:
        return 1.0 if spot > strike else 0.0
    if annualized_vol <= 0:
        return 1.0 if spot > strike else 0.0

    tau = seconds_to_expiry / SECONDS_PER_YEAR
    denominator = annualized_vol * math.sqrt(tau)
    if denominator <= 0:
        return 1.0 if spot > strike else 0.0
    numerator = math.log(spot / strike) + (annualized_drift - 0.5 * annualized_vol * annualized_vol) * tau
    return clamp_probability(normal_cdf(numerator / denominator))


def bachelier_terminal_up_probability(
    *,
    spot: float,
    strike: float,
    drift_to_expiry: float = 0.0,
    expected_price_std_to_expiry: float,
) -> float:
    if expected_price_std_to_expiry <= 0:
        return 1.0 if spot + drift_to_expiry > strike else 0.0
    z_score = (spot - strike + drift_to_expiry) / expected_price_std_to_expiry
    return clamp_probability(normal_cdf(z_score))
