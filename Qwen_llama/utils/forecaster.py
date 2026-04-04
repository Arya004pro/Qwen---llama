"""utils/forecaster.py - Pure-Python time-series forecasting.

Algorithms (no numpy/scipy required):
  - linear          : OLS linear regression
  - holt            : damped Holt double-exponential smoothing
  - sma             : moving average with drift
  - seasonal_naive  : repeats last seasonal cycle

Public API
----------
  forecast(values, periods, method="holt") -> ForecastResult
  forecast_auto(values, periods=3, confidence_pct=80.0, bucket=None) -> ForecastResult
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from statistics import median
from typing import Literal


Method = Literal["linear", "holt", "sma", "seasonal_naive"]


@dataclass
class ForecastResult:
    method: str
    historical: list[float]
    forecast: list[float]
    lower_bound: list[float]
    upper_bound: list[float]
    confidence_pct: float = 80.0
    rmse: float = 0.0
    trend_pct: float = 0.0


# Common z-score approximations (two-tailed)
_Z = {50: 0.674, 60: 0.842, 70: 1.036, 80: 1.282, 90: 1.645, 95: 1.960}


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
    return math.sqrt(sum((a - f) ** 2 for a, f in zip(actual[:n], fitted[:n])) / max(n, 1))


def _mae(actual: list[float], pred: list[float]) -> float:
    if not actual or not pred:
        return float("inf")
    n = min(len(actual), len(pred))
    return sum(abs(a - p) for a, p in zip(actual[:n], pred[:n])) / max(n, 1)


def _stabilize_series(values: list[float]) -> list[float]:
    """Clip extreme spikes via MAD-based winsorization for stabler forecasts."""
    if len(values) < 8:
        return list(values)
    med = median(values)
    abs_dev = [abs(v - med) for v in values]
    mad = median(abs_dev) if abs_dev else 0.0
    if mad <= 0:
        return list(values)
    low = med - 6.0 * mad
    high = med + 6.0 * mad
    return [min(high, max(low, v)) for v in values]


def _confidence_interval(
    forecast: list[float],
    sigma: float,
    confidence_pct: float = 80.0,
) -> tuple[list[float], list[float]]:
    z = _Z.get(int(confidence_pct), 1.282)
    # Keep a small but non-zero floor to avoid unrealistically tight bands.
    sigma_eff = max(0.001, float(sigma or 0.0))
    lower, upper = [], []
    for i, f in enumerate(forecast):
        margin = z * sigma_eff * math.sqrt(i + 1)
        lower.append(max(0.0, f - margin))
        upper.append(f + margin)
    return lower, upper


def _infer_season_length(bucket: str | None, n: int) -> int | None:
    b = (bucket or "").lower().strip()
    if b == "month":
        s = 12
    elif b == "quarter":
        s = 4
    elif b == "week":
        s = 52
    elif b == "day":
        s = 7
    elif b == "year":
        s = 3
    else:
        s = 0

    if s < 2:
        return None
    if n < s * 2:
        return None
    return s


def _linear_trend(values: list[float], periods: int) -> ForecastResult:
    n = len(values)
    x = list(range(n))
    sx = sum(x)
    sy = sum(values)
    sxy = sum(xi * yi for xi, yi in zip(x, values))
    sxx = sum(xi**2 for xi in x)

    denom = n * sxx - sx**2
    slope = 0.0 if denom == 0 else (n * sxy - sx * sy) / denom
    intercept = (sy - slope * sx) / max(n, 1)

    fitted = [intercept + slope * xi for xi in x]
    residuals = [v - f for v, f in zip(values, fitted)]
    sigma = _std(residuals) if len(residuals) > 1 else max(0.1, abs(_mean(values)) * 0.1)

    forecast_vals = [intercept + slope * (n + i) for i in range(periods)]
    lower, upper = _confidence_interval(forecast_vals, sigma)

    base = abs(fitted[0]) if fitted and fitted[0] != 0 else max(1.0, abs(_mean(values)))
    trend_pct = (slope / base) * 100

    return ForecastResult(
        method="linear",
        historical=list(values),
        forecast=forecast_vals,
        lower_bound=lower,
        upper_bound=upper,
        rmse=_rmse(values, fitted),
        trend_pct=round(trend_pct, 2),
    )


def _holt(
    values: list[float],
    periods: int,
    alpha: float = 0.3,
    beta: float = 0.1,
) -> ForecastResult:
    """Damped Holt smoothing to avoid explosive long-horizon trend."""
    if len(values) < 2:
        return _linear_trend(values, periods)

    diffs = [values[i] - values[i - 1] for i in range(1, len(values))]
    vol_ratio = _std(diffs) / max(1.0, abs(_mean(values)))
    if vol_ratio > 1.0:
        phi = 0.85
    elif vol_ratio > 0.5:
        phi = 0.92
    else:
        phi = 0.97

    level = values[0]
    trend = values[1] - values[0]
    fitted = [level + trend]

    for v in values[1:]:
        prev_level = level
        level = alpha * v + (1 - alpha) * (level + phi * trend)
        trend = beta * (level - prev_level) + (1 - beta) * phi * trend
        fitted.append(level + trend)

    residuals = [v - f for v, f in zip(values, fitted)]
    sigma = _std(residuals) if len(residuals) > 1 else max(0.1, abs(_mean(values)) * 0.1)

    forecast_vals: list[float] = []
    if abs(1 - phi) < 1e-6:
        forecast_vals = [level + (i + 1) * trend for i in range(periods)]
    else:
        for i in range(periods):
            k = i + 1
            damp_sum = (1 - (phi**k)) / (1 - phi)
            forecast_vals.append(level + trend * damp_sum)

    lower, upper = _confidence_interval(forecast_vals, sigma)
    base = abs(level) if level != 0 else max(1.0, abs(_mean(values)))
    trend_pct = (trend / base) * 100

    return ForecastResult(
        method="holt",
        historical=list(values),
        forecast=forecast_vals,
        lower_bound=lower,
        upper_bound=upper,
        rmse=_rmse(values, fitted),
        trend_pct=round(trend_pct, 2),
    )


def _sma(values: list[float], periods: int, window: int | None = None) -> ForecastResult:
    n = len(values)
    if window is None:
        window = max(2, min(6, n // 3))
    window = min(window, max(1, n))

    fitted: list[float] = []
    for i in range(n):
        if i < window - 1:
            fitted.append(_mean(values[: i + 1]))
        else:
            fitted.append(_mean(values[i - window + 1: i + 1]))

    recent = values[-window:]
    r_mean = _mean(recent)
    drift = 0.0
    if len(recent) >= 2:
        diffs = [recent[i] - recent[i - 1] for i in range(1, len(recent))]
        drift = _mean(diffs)

    sigma = _std([v - f for v, f in zip(values, fitted)]) or max(0.1, abs(r_mean) * 0.1)

    forecast_vals = [max(0.0, r_mean + drift * (i + 1)) for i in range(periods)]
    lower, upper = _confidence_interval(forecast_vals, sigma)

    base = abs(r_mean) if r_mean != 0 else max(1.0, abs(_mean(values)))
    trend_pct = (drift / base) * 100

    return ForecastResult(
        method="sma",
        historical=list(values),
        forecast=forecast_vals,
        lower_bound=lower,
        upper_bound=upper,
        rmse=_rmse(values, fitted),
        trend_pct=round(trend_pct, 2),
    )


def _seasonal_naive(values: list[float], periods: int, season_length: int) -> ForecastResult:
    n = len(values)
    if season_length < 2 or n < season_length * 2:
        return _sma(values, periods)

    fitted: list[float] = []
    for i in range(n):
        if i < season_length:
            fitted.append(_mean(values[: i + 1]))
        else:
            fitted.append(values[i - season_length])

    forecast_vals: list[float] = []
    tail = values[-season_length:]
    for i in range(periods):
        forecast_vals.append(tail[i % season_length])

    residuals = [v - f for v, f in zip(values[season_length:], fitted[season_length:])]
    sigma = _std(residuals) if len(residuals) > 1 else max(0.1, abs(_mean(values)) * 0.1)
    lower, upper = _confidence_interval(forecast_vals, sigma)

    if n >= season_length * 2:
        recent = _mean(values[-season_length:])
        prev = _mean(values[-2 * season_length: -season_length])
        trend_pct = ((recent - prev) / prev * 100) if prev else 0.0
    else:
        trend_pct = 0.0

    return ForecastResult(
        method="seasonal_naive",
        historical=list(values),
        forecast=forecast_vals,
        lower_bound=lower,
        upper_bound=upper,
        rmse=_rmse(values, fitted),
        trend_pct=round(trend_pct, 2),
    )


def _run_method(values: list[float], periods: int, method: str, bucket: str | None = None) -> ForecastResult:
    m = (method or "").lower().strip()
    if m == "linear":
        return _linear_trend(values, periods)
    if m == "sma":
        return _sma(values, periods)
    if m == "seasonal_naive":
        s = _infer_season_length(bucket, len(values))
        if s:
            return _seasonal_naive(values, periods, s)
        return _sma(values, periods)
    return _holt(values, periods)


def _auto_select_method(values: list[float], periods: int, bucket: str | None = None) -> Method:
    n = len(values)
    if n < 4:
        return "linear"

    candidates: list[Method] = ["holt", "linear", "sma"]
    if _infer_season_length(bucket, n):
        candidates.append("seasonal_naive")

    holdout = min(max(2, periods), max(2, n // 4), 8)
    if n - holdout < 3:
        # Fallback heuristic when not enough samples for holdout scoring.
        m = _mean(values)
        den = sum((v - m) ** 2 for v in values)
        num = sum((values[i] - m) * (values[i - 1] - m) for i in range(1, n))
        autocorr = (num / den) if den > 0 else 0.0
        if autocorr > 0.55:
            return "holt"
        if autocorr < 0.2:
            return "sma"
        return "linear"

    train = values[:-holdout]
    actual = values[-holdout:]

    best_method: Method = "holt"
    best_err = float("inf")

    for method in candidates:
        try:
            pred = _run_method(train, holdout, method, bucket).forecast
            err = _mae(actual, pred)
        except Exception:
            continue
        if err < best_err:
            best_err = err
            best_method = method

    return best_method


def forecast(
    values: list[float],
    periods: int = 3,
    method: Method = "holt",
    confidence_pct: float = 80.0,
    bucket: str | None = None,
) -> ForecastResult:
    """Forecast future values from historical data."""
    raw = [float(v) for v in (values or [])]
    if not raw or all(v == 0 for v in raw):
        zeros = [0.0] * max(1, periods)
        return ForecastResult(
            method=method,
            historical=list(raw),
            forecast=zeros,
            lower_bound=zeros,
            upper_bound=zeros,
        )

    series = _stabilize_series(raw)

    # Require at least 3 points for stable behavior.
    if len(series) < 3:
        series = [series[0]] * (3 - len(series)) + list(series)

    periods = max(1, min(int(periods or 1), 24))

    result = _run_method(series, periods, method, bucket)
    result.confidence_pct = float(confidence_pct)
    result.lower_bound = [max(0.0, v) for v in result.lower_bound]
    return result


def forecast_auto(
    values: list[float],
    periods: int = 3,
    confidence_pct: float = 80.0,
    bucket: str | None = None,
) -> ForecastResult:
    """Auto-select the best method using holdout scoring on recent history."""
    raw = [float(v) for v in (values or [])]
    if not raw:
        return forecast(raw, periods, "holt", confidence_pct, bucket=bucket)

    series = _stabilize_series(raw)
    method = _auto_select_method(series, periods, bucket=bucket)
    return forecast(series, periods, method, confidence_pct, bucket=bucket)
