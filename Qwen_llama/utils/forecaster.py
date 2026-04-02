"""utils/forecaster.py — Pure-Python time-series forecasting.

Algorithms (no numpy/scipy required):
  - linear_trend   : OLS linear regression, best for consistent growth
  - holt            : Holt double-exponential smoothing, best for trended data
  - sma             : Simple moving average, best for stable/cyclical data

Public API
----------
  forecast(values, periods, method="holt") -> ForecastResult
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Literal


Method = Literal["linear", "holt", "sma"]


@dataclass
class ForecastResult:
    method:        str
    historical:    list[float]
    forecast:      list[float]
    lower_bound:   list[float]
    upper_bound:   list[float]
    confidence_pct: float = 80.0
    rmse:          float  = 0.0
    trend_pct:     float  = 0.0   # overall trend as % change per period


# ── helpers ───────────────────────────────────────────────────────────────────

def _mean(vals: list[float]) -> float:
    return sum(vals) / len(vals) if vals else 0.0


def _variance(vals: list[float]) -> float:
    if len(vals) < 2:
        return 0.0
    m = _mean(vals)
    return sum((v - m) ** 2 for v in vals) / (len(vals) - 1)


def _std(vals: list[float]) -> float:
    return math.sqrt(_variance(vals))


def _rmse(actual: list[float], fitted: list[float]) -> float:
    if not actual or not fitted:
        return 0.0
    n = min(len(actual), len(fitted))
    return math.sqrt(sum((a - f) ** 2 for a, f in zip(actual[:n], fitted[:n])) / n)


# Z-score for confidence level (two-tailed, common approximations)
_Z = {50: 0.674, 60: 0.842, 70: 1.036, 80: 1.282, 90: 1.645, 95: 1.960}


def _confidence_interval(
    forecast: list[float],
    sigma: float,
    confidence_pct: float = 80.0,
) -> tuple[list[float], list[float]]:
    z = _Z.get(int(confidence_pct), 1.282)
    lower, upper = [], []
    for i, f in enumerate(forecast):
        # Uncertainty grows with horizon
        margin = z * sigma * math.sqrt(i + 1)
        lower.append(max(0.0, f - margin))
        upper.append(f + margin)
    return lower, upper


# ── linear trend (OLS) ───────────────────────────────────────────────────────

def _linear_trend(values: list[float], periods: int) -> ForecastResult:
    n = len(values)
    x = list(range(n))
    sx  = sum(x)
    sy  = sum(values)
    sxy = sum(xi * yi for xi, yi in zip(x, values))
    sxx = sum(xi ** 2 for xi in x)

    denom = n * sxx - sx ** 2
    if denom == 0:
        slope = 0.0
    else:
        slope = (n * sxy - sx * sy) / denom
    intercept = (sy - slope * sx) / n

    fitted = [intercept + slope * xi for xi in x]
    residuals = [v - f for v, f in zip(values, fitted)]
    sigma = _std(residuals) if len(residuals) > 1 else abs(_mean(values)) * 0.1

    forecast = [intercept + slope * (n + i) for i in range(periods)]
    lower, upper = _confidence_interval(forecast, sigma)

    trend_pct = (slope / abs(fitted[0]) * 100) if fitted[0] != 0 else 0.0

    return ForecastResult(
        method="linear",
        historical=list(values),
        forecast=forecast,
        lower_bound=lower,
        upper_bound=upper,
        rmse=_rmse(values, fitted),
        trend_pct=round(trend_pct, 2),
    )


# ── Holt double-exponential smoothing ────────────────────────────────────────

def _holt(
    values: list[float],
    periods: int,
    alpha: float = 0.3,
    beta: float  = 0.1,
) -> ForecastResult:
    """Holt's linear exponential smoothing — handles level + trend."""
    if len(values) < 2:
        return _linear_trend(values, periods)

    # Initialise
    level = values[0]
    trend = values[1] - values[0]
    fitted = [level + trend]

    for v in values[1:]:
        prev_level = level
        level = alpha * v + (1 - alpha) * (level + trend)
        trend = beta * (level - prev_level) + (1 - beta) * trend
        fitted.append(level + trend)

    residuals = [v - f for v, f in zip(values, fitted)]
    sigma = _std(residuals) if len(residuals) > 1 else abs(_mean(values)) * 0.1

    forecast = [level + (i + 1) * trend for i in range(periods)]
    lower, upper = _confidence_interval(forecast, sigma)

    trend_pct = (trend / abs(level) * 100) if level != 0 else 0.0

    return ForecastResult(
        method="holt",
        historical=list(values),
        forecast=forecast,
        lower_bound=lower,
        upper_bound=upper,
        rmse=_rmse(values, fitted),
        trend_pct=round(trend_pct, 2),
    )


# ── simple moving average ────────────────────────────────────────────────────

def _sma(values: list[float], periods: int, window: int | None = None) -> ForecastResult:
    n = len(values)
    if window is None:
        window = max(2, min(6, n // 3))
    window = min(window, n)

    fitted: list[float] = []
    for i in range(n):
        if i < window - 1:
            fitted.append(_mean(values[: i + 1]))
        else:
            fitted.append(_mean(values[i - window + 1: i + 1]))

    # Detect recent trend from last window to use for drift
    recent  = values[-window:]
    r_mean  = _mean(recent)
    drift   = 0.0
    if len(recent) >= 2:
        diffs  = [recent[i] - recent[i - 1] for i in range(1, len(recent))]
        drift  = _mean(diffs)

    sigma = _std([v - f for v, f in zip(values, fitted)]) or abs(r_mean) * 0.1

    forecast = [r_mean + drift * (i + 1) for i in range(periods)]
    # Prevent negatives
    forecast = [max(0.0, f) for f in forecast]
    lower, upper = _confidence_interval(forecast, sigma)

    trend_pct = (drift / abs(r_mean) * 100) if r_mean != 0 else 0.0

    return ForecastResult(
        method="sma",
        historical=list(values),
        forecast=forecast,
        lower_bound=lower,
        upper_bound=upper,
        rmse=_rmse(values, fitted),
        trend_pct=round(trend_pct, 2),
    )


# ── public API ────────────────────────────────────────────────────────────────

def forecast(
    values:         list[float],
    periods:        int   = 3,
    method:         Method = "holt",
    confidence_pct: float  = 80.0,
) -> ForecastResult:
    """
    Forecast future values from historical time-series data.

    Parameters
    ----------
    values         : List of historical numeric values (oldest first).
    periods        : Number of future periods to forecast.
    method         : 'holt' | 'linear' | 'sma'
    confidence_pct : Width of confidence band (50–95).

    Returns
    -------
    ForecastResult with historical, forecast, lower_bound, upper_bound lists.
    """
    if not values or all(v == 0 for v in values):
        zeros = [0.0] * periods
        return ForecastResult(
            method=method, historical=list(values),
            forecast=zeros, lower_bound=zeros, upper_bound=zeros,
        )

    # Require at least 3 data points for meaningful forecasting
    if len(values) < 3:
        # Pad with first value to reach minimum
        values = [values[0]] * (3 - len(values)) + list(values)

    periods = max(1, min(periods, 24))  # cap at 24 periods ahead

    if method == "linear":
        result = _linear_trend(values, periods)
    elif method == "sma":
        result = _sma(values, periods)
    else:  # default: holt
        result = _holt(values, periods)

    result.confidence_pct = confidence_pct

    # Clip negatives from lower bound
    result.lower_bound = [max(0.0, v) for v in result.lower_bound]

    return result


def _auto_select_method(values: list[float]) -> Method:
    """Pick the best algorithm based on data characteristics."""
    if len(values) < 4:
        return "linear"

    # Compute autocorrelation lag-1 to detect trend vs cyclical
    m = _mean(values)
    num = sum((values[i] - m) * (values[i - 1] - m) for i in range(1, len(values)))
    den = sum((v - m) ** 2 for v in values)
    autocorr = num / den if den > 0 else 0.0

    # Strong positive autocorrelation → trend → holt
    if autocorr > 0.5:
        return "holt"
    # Low autocorrelation → stable/cyclical → sma
    if autocorr < 0.2:
        return "sma"
    return "linear"


def forecast_auto(values: list[float], periods: int = 3, confidence_pct: float = 80.0) -> ForecastResult:
    """Auto-select the best forecasting method based on data characteristics."""
    method = _auto_select_method(values)
    return forecast(values, periods, method, confidence_pct)