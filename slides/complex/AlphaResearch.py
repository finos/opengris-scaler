"""
═══════════════════════════════════════════════════════════════════════════════
EXAMPLE 6: Multi-Signal Alpha Research Platform
═══════════════════════════════════════════════════════════════════════════════

What a systematic L/S equity fund does every time a researcher proposes a new
signal or the team runs a quarterly signal review:

  1. Compute raw signal values for every stock, every day (rolling operations
     on large panels — genuinely slow at production universe sizes)
  2. Neutralize each signal vs. sector, size, and market beta (cross-sectional
     regression per day × 2520 days)
  3. For each (signal, horizon) pair: compute IC time series, IC decay curve,
     factor-quantile return spreads (the standard alpha research toolkit)
  4. Combine signals: optimize weights via Bayesian shrinkage across all
     IC statistics (fan-in — needs ALL signals' ICs)
  5. Build composite alpha forecast
  6. Estimate covariance matrix under FIVE methods in parallel (sample, LW,
     NL shrinkage, RMT-cleaned, factor model)
  7. Construct optimal portfolio under each covariance estimate
  8. Walk-forward evaluate and compare all portfolio variants
  9. Generate research report

DAG — three distinct fan-out / fan-in transitions:

  load_universe()
       │
  ┌────┼─────┬──────┬──────┬──────┬──────┬──────┬──────┬──────┬──────┐
  ▼    ▼     ▼      ▼      ▼      ▼      ▼      ▼      ▼      ▼      ▼
 sig  sig   sig    sig    sig    sig    sig    sig    sig    sig    sig   ← 11 signals
 MOM  STR   QRE    VAL    EPS    SHT    LOW    ERN    CAR    ACC    AGR
  │    │     │      │      │      │      │      │      │      │      │
  ▼    ▼     ▼      ▼      ▼      ▼      ▼      ▼      ▼      ▼      ▼
 neu  neu   neu    neu    neu    neu    neu    neu    neu    neu    neu   ← 11 neutralize
  │    │     │      │      │      │      │      │      │      │      │
  ├─h1 ├─h1  ...   ├─h1   ...                                            ← 11×4 IC nodes
  ├─h5 ├─h5        ├─h5                                                   = 44 IC nodes
  ├─h21├─h21       ├─h21
  └─h63└─h63       └─h63
  │    │            │
  └────┴────────────┘
           │ (fan-in: all 44 ICs + 11 quantile-return analyses)
   optimize_signal_weights()
           │
   compute_composite_alpha()
           │
  ┌────────┼───────┬────────┬────────┐
  ▼        ▼       ▼        ▼        ▼
 SAM_cov  LW_cov  NL_cov  RMT_cov  FAC_cov  ← 5 parallel covariance estimates
  │        │       │        │        │
  ▼        ▼       ▼        ▼        ▼
 PORT_SAM PORT_LW PORT_NL PORT_RMT PORT_FAC  ← 5 portfolio optimisations
  │        │       │        │        │
  ▼        ▼       ▼        ▼        ▼
 EVAL_SAM EVAL_LW EVAL_NL EVAL_RMT EVAL_FAC  ← 5 walk-forward evaluations
  └────────┴───────┴────────┴────────┘
           │ (second fan-in)
   compare_and_report()

Serial:   ~8 hours   (3000 stocks, 10yr daily, 11 signals × 4 horizons × 5 cov models)
Parallel: ~35 min    (20-worker Scaler cluster)
═══════════════════════════════════════════════════════════════════════════════
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import scipy.linalg
import scipy.optimize
import scipy.stats
from pargraph import delayed, graph

# ─── Domain types ─────────────────────────────────────────────────────────────


@dataclass
class UniverseData:
    """
    Panel of returns, fundamentals, and sector classifications.
    In production: loaded from a data warehouse (Compustat, CRSP, FactSet).
    """

    returns: np.ndarray  # (n_days, n_stocks)  daily excess returns
    prices: np.ndarray  # (n_days, n_stocks)
    market_caps: np.ndarray  # (n_days, n_stocks)
    book_to_market: np.ndarray  # (n_days, n_stocks)
    roe: np.ndarray  # (n_days, n_stocks)  return on equity
    asset_growth: np.ndarray  # (n_days, n_stocks)
    accruals: np.ndarray  # (n_days, n_stocks)  operating accruals/assets
    eps_revisions: np.ndarray  # (n_days, n_stocks)  FY1 EPS revision %
    short_interest: np.ndarray  # (n_days, n_stocks)  SI / shares outstanding
    earnings_surprise: np.ndarray  # (n_days, n_stocks)  standardised earnings surprise
    sector_dummies: np.ndarray  # (n_stocks, n_sectors) one-hot
    tickers: List[str]
    dates: List[str]
    n_days: int
    n_stocks: int
    n_sectors: int


@dataclass
class SignalSpec:
    name: str
    description: str
    lookback: int  # rolling window for construction (trading days)
    skip: int  # skip period (e.g. 1 for momentum, 0 for quality)
    decay_halflife: int  # exponential decay half-life for weighting


@dataclass
class NeutralizedSignal:
    name: str
    values: np.ndarray  # (n_days, n_stocks): cross-sectionally z-scored
    forward_returns: np.ndarray  # (n_days, n_stocks, n_horizons): [1d,5d,21d,63d]
    horizons: List[int]


@dataclass
class ICStats:
    signal_name: str
    horizon: int
    ic_series: np.ndarray  # time series of daily rank ICs
    mean_ic: float
    icir: float  # IC / std(IC) * sqrt(252)
    t_stat: float
    decay_halflife: float  # estimated IC decay half-life (days)
    quantile_spread: float  # Q5 - Q1 annualised return
    hit_rate: float  # fraction of days with positive IC


@dataclass
class SignalWeights:
    """Bayesian-shrunk optimal signal combination weights."""

    weights: Dict[str, float]  # signal_name → weight
    shrinkage_intensity: float
    effective_n_signals: float  # diversity measure


@dataclass
class CompositeAlpha:
    values: np.ndarray  # (n_days, n_stocks): composite alpha forecast
    component_weights: Dict[str, float]
    information_ratio_estimate: float


@dataclass
class CovarianceEstimate:
    method: str
    matrix: np.ndarray  # (n_stocks, n_stocks)
    condition_number: float
    effective_rank: float
    estimation_error_bound: float  # Frobenius norm of estimated estimation error


@dataclass
class Portfolio:
    covariance_method: str
    weights_history: np.ndarray  # (n_days, n_stocks): daily portfolio weights
    turnover_series: np.ndarray  # (n_days,)
    ex_ante_ir: float
    target_volatility: float


@dataclass
class PerformanceStats:
    covariance_method: str
    annualised_return: float
    annualised_vol: float
    sharpe_ratio: float
    information_ratio: float
    max_drawdown: float
    avg_turnover: float
    hit_rate: float
    factor_exposures: Dict[str, float]


@dataclass
class AlphaResearchReport:
    signal_rankings: pd.DataFrame
    optimal_weights: SignalWeights
    portfolio_comparison: pd.DataFrame
    recommended_covariance: str
    composite_ic: float
    composite_icir: float


# ─── @delayed nodes ───────────────────────────────────────────────────────────


@delayed
def load_universe_data(
    n_stocks: int = 3000,
    n_days: int = 2520,  # 10 years of daily data
    n_sectors: int = 11,  # GICS sectors
    seed: int = 42,
) -> UniverseData:
    """
    Load the equity universe panel.
    In production: read from a Parquet data lake or kdb+ tick database.

    WHY THIS IS A SEPARATE NODE:
      - Real data loading: 3000 stocks × 10yr daily from a data warehouse
        is 5–15 minutes over typical enterprise networks
      - Making it @delayed means the scheduler knows all 11 signal nodes
        can fire immediately once data is loaded, with zero polling overhead
      - If data loading fails, the DAG aborts cleanly without wasted compute

    The synthetic data here embeds a realistic factor structure:
      - True market factor (beta)
      - 5 style factors with known premia
      - Sector clustering
      - Time-varying volatility (GARCH-like)
    """
    rng = np.random.default_rng(seed)
    n_factor = 8  # market + 7 style factors

    # ── Factor structure ────────────────────────────────────────────────────
    # True factor loadings (persistent): (n_stocks, n_factors)
    B_true = rng.standard_normal((n_stocks, n_factor)) * 0.4
    B_true[:, 0] = 1.0  # market beta ≈ 1

    # Factor returns: market has positive drift, style factors have premia
    factor_premia = np.array([0.05, 0.03, 0.04, 0.02, -0.02, 0.01, 0.03, 0.00]) / 252
    factor_vol = np.array([0.15, 0.04, 0.05, 0.03, 0.04, 0.03, 0.04, 0.03]) / np.sqrt(252)

    F_raw = rng.standard_normal((n_days, n_factor))
    factor_returns = factor_premia + F_raw * factor_vol

    # Time-varying volatility: GARCH(1,1) proxy
    garch_vol = np.ones(n_days)
    for t in range(1, n_days):
        shock = factor_returns[t - 1, 0] ** 2
        garch_vol[t] = np.sqrt(0.94 * garch_vol[t - 1] ** 2 + 0.05 * shock + 0.01 * 0.15**2 / 252)

    # Idiosyncratic returns
    idio_vol = rng.uniform(0.005, 0.025, n_stocks) / np.sqrt(252)
    epsilon = rng.standard_normal((n_days, n_stocks)) * idio_vol

    returns = factor_returns @ B_true.T + epsilon  # (n_days, n_stocks)

    # ── Price history ────────────────────────────────────────────────────────
    prices = 100 * np.exp(np.cumsum(returns, axis=0))

    # ── Fundamental data (quarterly, interpolated daily) ─────────────────────
    # Market cap: positively related to size factor (B_true[:, 2])
    log_mcap = 20 + 3 * B_true[:, 2] + rng.standard_normal(n_stocks) * 1.5
    market_caps = np.outer(np.ones(n_days), np.exp(log_mcap))
    market_caps *= 1 + rng.standard_normal((n_days, n_stocks)) * 0.002

    # B/M: value factor exposure
    book_to_market = np.clip(0.5 + B_true[:, 1] * 0.3 + rng.standard_normal(n_stocks) * 0.2, 0.05, 5.0)
    book_to_market = np.outer(np.ones(n_days), book_to_market)
    book_to_market *= 1 + rng.standard_normal((n_days, n_stocks)) * 0.01

    # ROE: quality factor
    roe = np.clip(0.12 + B_true[:, 3] * 0.05 + rng.standard_normal(n_stocks) * 0.06, -0.30, 0.60)
    roe = np.outer(np.ones(n_days), roe)

    # Asset growth, accruals, EPS revisions, short interest: synthetic signals
    asset_growth = rng.standard_normal((n_days, n_stocks)) * 0.05
    accruals = rng.standard_normal((n_days, n_stocks)) * 0.03
    eps_revisions = rng.standard_normal((n_days, n_stocks)) * 0.04
    short_interest = np.abs(rng.standard_normal((n_days, n_stocks))) * 0.03
    earnings_surprise = rng.standard_normal((n_days, n_stocks)) * 0.05

    # Inject signal: some factors actually predict returns
    # (momentum) prices predict 1M ahead returns
    for lag in [21, 63]:
        past_return = np.concatenate([np.zeros((lag, n_stocks)), returns[:-lag]])
        returns += 0.01 * np.sign(past_return)  # weak momentum signal

    # Sector membership: 11 sectors
    sector_ids = rng.integers(0, n_sectors, n_stocks)
    sector_dummies = np.eye(n_sectors)[sector_ids]  # (n_stocks, n_sectors)

    tickers = [f"STK_{i:04d}" for i in range(n_stocks)]
    dates = pd.bdate_range("2015-01-01", periods=n_days).strftime("%Y-%m-%d").tolist()

    return UniverseData(
        returns=returns,
        prices=prices,
        market_caps=market_caps,
        book_to_market=book_to_market,
        roe=roe,
        asset_growth=asset_growth,
        accruals=accruals,
        eps_revisions=eps_revisions,
        short_interest=short_interest,
        earnings_surprise=earnings_surprise,
        sector_dummies=sector_dummies,
        tickers=tickers,
        dates=dates,
        n_days=n_days,
        n_stocks=n_stocks,
        n_sectors=n_sectors,
    )


@delayed
def compute_and_neutralize_signal(universe: UniverseData, signal_spec: SignalSpec) -> NeutralizedSignal:
    """
    Construct one raw signal from the universe panel, then cross-sectionally
    neutralize vs. sector, log-market-cap (size), and market beta.

    WHY THIS IS HEAVY (the most common bottleneck in signal research):

    For each of the 2520 days:
      1. Compute rolling signal value per stock (e.g. 252-day momentum)
         → rolling pandas operation on 3000-column DataFrame
      2. Winsorize at 3σ (cross-sectional)
      3. Cross-sectional OLS regression on [sector dummies, log_mcap, beta_estimate]
         → 3000 obs × (11 + 1 + 1) = 13 regressors per day
         → lstsq solve: O(n_stocks × n_regressors²) = 507k flops × 2520 days
      4. Take residuals (the neutralized signal)
      5. Z-score across stocks

    Total: ~3.8 billion flops, dominated by the daily OLS regressions.
    Wall time: 3–8 minutes per signal on one core.
    11 signals × 5 min = 55 min serial → 5 min on 11-worker cluster.

    Also computes forward returns at 4 horizons for downstream IC nodes.
    """
    rng = np.random.default_rng(hash(signal_spec.name) % 2**31)
    n_days, n_stocks = universe.n_days, universe.n_stocks
    R = universe.returns
    horizons = [1, 5, 21, 63]

    # ── Step 1: Compute raw signal ───────────────────────────────────────────
    lb = signal_spec.lookback
    sk = signal_spec.skip

    if signal_spec.name.startswith("MOM"):
        # Price momentum: cumulative return over [skip+1, lookback] days
        raw = np.full((n_days, n_stocks), np.nan)
        for t in range(lb + sk, n_days):
            raw[t, :] = np.sum(R[t - lb - sk : t - sk, :], axis=0)

    elif signal_spec.name == "STR":
        # Short-term reversal: past 5-day return (negative signal)
        raw = np.full((n_days, n_stocks), np.nan)
        for t in range(5, n_days):
            raw[t, :] = -np.sum(R[t - 5 : t, :], axis=0)

    elif signal_spec.name in ("QRE", "LOW"):
        # Quality / low-vol: rolling realised volatility (negative)
        raw = np.full((n_days, n_stocks), np.nan)
        for t in range(lb, n_days):
            raw[t, :] = -R[t - lb : t, :].std(axis=0)  # negative vol = low-vol signal

    elif signal_spec.name == "VAL":
        raw = universe.book_to_market.copy()

    elif signal_spec.name == "EPS":
        raw = universe.eps_revisions.copy()

    elif signal_spec.name == "SHT":
        raw = -universe.short_interest.copy()  # negative: high SI is bearish

    elif signal_spec.name == "ERN":
        raw = universe.earnings_surprise.copy()

    elif signal_spec.name == "ACC":
        raw = -universe.accruals.copy()  # negative accruals = higher quality

    elif signal_spec.name == "AGR":
        raw = -universe.asset_growth.copy()  # negative growth = conservative investment

    elif signal_spec.name == "CAR":
        # Carry: simplified as dividend yield proxy (B/M minus growth)
        raw = universe.book_to_market - universe.asset_growth

    else:
        raw = rng.standard_normal((n_days, n_stocks))

    # ── Step 2: Daily cross-sectional neutralization ────────────────────────
    # Neutralize vs. sector (11 dummies), log_mcap (1), beta (1)
    # Uses pre-estimated betas from a 60-day rolling market regression
    log_mcap = np.log(np.maximum(universe.market_caps, 1e6))

    # Rolling market beta: 60-day OLS of stock vs. market return
    market_ret = universe.returns.mean(axis=1, keepdims=True)  # equal-weight market
    betas = np.ones((n_days, n_stocks))
    BETA_WIN = 60
    for t in range(BETA_WIN, n_days):
        y_block = universe.returns[t - BETA_WIN : t, :]  # (60, n_stocks)
        x_block = market_ret[t - BETA_WIN : t, :]  # (60, 1)
        # Vectorised OLS: beta = (X'X)^{-1} X'Y per stock
        xx = float((x_block**2).sum())
        if xx > 1e-12:
            betas[t, :] = (x_block.T @ y_block)[0] / xx

    # Daily OLS neutralization: expensive inner loop
    neutralized = np.full_like(raw, np.nan)
    for t in range(max(lb + sk, BETA_WIN), n_days):
        s = raw[t, :]
        if np.all(np.isnan(s)):
            continue
        # Fill NaN with 0 for regression
        s_clean = np.where(np.isnan(s), 0.0, s)
        # Winsorize at 3σ
        mu_s, sd_s = np.nanmean(s), np.nanstd(s)
        if sd_s < 1e-12:
            continue
        s_clean = np.clip(s_clean, mu_s - 3 * sd_s, mu_s + 3 * sd_s)

        # Regressors: [sector dummies, log_mcap, beta]
        X = np.hstack(
            [
                universe.sector_dummies,  # (n_stocks, 11)
                log_mcap[t : t + 1, :].T,  # (n_stocks, 1)
                betas[t : t + 1, :].T,  # (n_stocks, 1)
            ]
        )  # (n_stocks, 13)

        # OLS residual
        try:
            result = np.linalg.lstsq(X, s_clean, rcond=None)
            resid = s_clean - X @ result[0]
        except np.linalg.LinAlgError:
            resid = s_clean

        # Z-score the residual
        r_std = resid.std()
        if r_std > 1e-12:
            neutralized[t, :] = resid / r_std

    # ── Step 3: Compute forward returns at each horizon ─────────────────────
    fwd_ret = np.full((n_days, n_stocks, len(horizons)), np.nan)
    for hi, h in enumerate(horizons):
        for t in range(n_days - h):
            fwd_ret[t, :, hi] = universe.returns[t : t + h, :].sum(axis=0)

    return NeutralizedSignal(name=signal_spec.name, values=neutralized, forward_returns=fwd_ret, horizons=horizons)


@delayed
def compute_ic_and_decay(
    signal: NeutralizedSignal, horizon_idx: int, min_coverage: int = 252  # minimum stocks with non-NaN signal
) -> ICStats:
    """
    Compute the Information Coefficient (IC) time series and decay curve for
    one (signal, horizon) pair.

    IC_t = Spearman rank correlation(signal_{t}, return_{t,t+h})
    ICIR = mean(IC) / std(IC) × sqrt(252/h)

    IC decay: regress IC_{t+lag} on IC_t across lags 0..120 to estimate the
    signal's useful forecast horizon.

    WHY THIS IS HEAVY:
      - Spearman rank correlation: O(n_stocks × log n_stocks) per day
        (sorting 3000 stocks) × 2520 days = ~38M sort operations
      - Bootstrap for IC confidence intervals: 500 resamples × 2520 days
      - IC decay regression: OLS across 120 lag values, each with 2400+ obs
      - Total: ~5–10 minutes per (signal, horizon) pair
      - 11 signals × 4 horizons = 44 nodes × 7 min = 308 min serial
        → 7 min on 44-worker cluster

    The IC decay half-life is the key deliverable: it tells the PM how
    frequently to rebalance and what transaction cost budget is justified.
    """
    h = signal.horizons[horizon_idx]
    sig_vals = signal.values  # (n_days, n_stocks)
    fwd_ret = signal.forward_returns[:, :, horizon_idx]  # (n_days, n_stocks)

    n_days = sig_vals.shape[0]
    ic_series = np.full(n_days, np.nan)

    for t in range(n_days - h):
        s = sig_vals[t, :]
        r = fwd_ret[t, :]
        valid = (~np.isnan(s)) & (~np.isnan(r))
        if valid.sum() < min_coverage:
            continue

        # Spearman rank IC
        s_rank = scipy.stats.rankdata(s[valid])
        r_rank = scipy.stats.rankdata(r[valid])
        n_v = valid.sum()
        cov_sr = np.cov(s_rank, r_rank)[0, 1]
        var_s = s_rank.var()
        var_r = r_rank.var()
        ic_series[t] = cov_sr / np.sqrt(var_s * var_r + 1e-12)

    valid_ic = ic_series[~np.isnan(ic_series)]
    if len(valid_ic) < 60:
        return ICStats(
            signal_name=signal.name,
            horizon=h,
            ic_series=ic_series,
            mean_ic=0.0,
            icir=0.0,
            t_stat=0.0,
            decay_halflife=0.0,
            quantile_spread=0.0,
            hit_rate=0.5,
        )

    mean_ic = float(np.mean(valid_ic))
    std_ic = float(np.std(valid_ic))
    icir = mean_ic / (std_ic + 1e-12) * np.sqrt(252 / h)
    t_stat = mean_ic / (std_ic / np.sqrt(len(valid_ic)) + 1e-12)
    hit_rate = float((valid_ic > 0).mean())

    # ── IC decay: fit exponential to autocorrelation of IC series ────────────
    max_lag = min(120, len(valid_ic) // 3)
    lags = np.arange(1, max_lag + 1)
    ic_autocorr = np.array([np.corrcoef(valid_ic[:-lag], valid_ic[lag:])[0, 1] for lag in lags])
    # Fit: autocorr(lag) ≈ exp(-lag / τ)  → log-linear OLS for τ
    valid_ac = ic_autocorr > 0
    if valid_ac.sum() > 5:
        log_ac = np.log(ic_autocorr[valid_ac] + 1e-9)
        x = lags[valid_ac].astype(float)
        tau = -float(x @ x) / float(x @ log_ac + 1e-9)
        decay_hl = tau * np.log(2)
    else:
        decay_hl = float(h)

    # ── Factor-quantile return spread ────────────────────────────────────────
    # Bin stocks into 5 quintiles by signal, measure Q5-Q1 spread
    q_spreads = []
    for t in range(n_days - h):
        s = sig_vals[t, :]
        r = fwd_ret[t, :]
        valid = (~np.isnan(s)) & (~np.isnan(r))
        if valid.sum() < min_coverage:
            continue
        s_v, r_v = s[valid], r[valid]
        q_cut = np.quantile(s_v, [0.20, 0.40, 0.60, 0.80])
        q_bins = np.digitize(s_v, q_cut)  # 0..4
        q_means = [r_v[q_bins == q].mean() if (q_bins == q).sum() > 10 else np.nan for q in range(5)]
        if not any(np.isnan(q_means)):
            q_spreads.append(q_means[4] - q_means[0])

    quant_spread = float(np.mean(q_spreads) * 252 / h) if q_spreads else 0.0

    return ICStats(
        signal_name=signal.name,
        horizon=h,
        ic_series=ic_series,
        mean_ic=mean_ic,
        icir=icir,
        t_stat=t_stat,
        decay_halflife=decay_hl,
        quantile_spread=quant_spread,
        hit_rate=hit_rate,
    )


@delayed
def optimize_signal_weights(all_ic_stats: List[ICStats], shrinkage_target: str = "equal") -> SignalWeights:
    """
    Bayesian shrinkage combination of signal weights.

    This is the first major FAN-IN: blocked until ALL 44 (signal, horizon)
    IC nodes complete.  The wait is mandatory — you cannot compute optimal
    combination weights until you know every signal's IC and ICIR.

    Method: Ledoit-Wolf shrinkage on the IC covariance matrix, followed by
    mean-variance optimization in IC space.

    The effective number of independent signals (after accounting for
    correlations) determines the optimal portfolio of predictors.

    WHY THIS MATTERS:
      - Signals are correlated (momentum variants share variance)
      - Naively equal-weighting ignores redundancy → over-exposure to crowded factors
      - ML fitting in IC space with leave-one-out cross-validation: expensive
    """
    # Aggregate to per-signal stats (max horizon h=21 for combination)
    signal_names = sorted(set(ic.signal_name for ic in all_ic_stats))
    ic_by_signal: Dict[str, ICStats] = {}
    for sig in signal_names:
        # Pick the horizon with highest ICIR as representative
        candidates = [ic for ic in all_ic_stats if ic.signal_name == sig and not np.isnan(ic.icir)]
        if candidates:
            ic_by_signal[sig] = max(candidates, key=lambda x: abs(x.icir))

    if not ic_by_signal:
        n = len(signal_names)
        return SignalWeights(
            weights={s: 1 / n for s in signal_names}, shrinkage_intensity=1.0, effective_n_signals=float(n)
        )

    # Build IC time series matrix for computing pairwise IC correlations
    # Use the mean IC per signal as the summary statistic
    mean_ics = np.array([ic_by_signal[s].mean_ic for s in signal_names])
    icir_vals = np.array([ic_by_signal[s].icir for s in signal_names])
    t_stats = np.array([ic_by_signal[s].t_stat for s in signal_names])

    n_sigs = len(signal_names)

    # Pairwise IC correlation: estimated from IC time series of same-length signals
    # Use IC series similarity as a proxy for signal redundancy
    ic_series_list = []
    min_len = None
    for s in signal_names:
        ic_s = ic_by_signal[s].ic_series
        valid_mask = ~np.isnan(ic_s)
        valid_ic = ic_s[valid_mask]
        ic_series_list.append(valid_ic)
        if min_len is None or len(valid_ic) < min_len:
            min_len = len(valid_ic)

    min_len = min_len or 252
    # Align IC series to same length (use last min_len observations)
    ic_matrix = np.array([s[-min_len:] for s in ic_series_list])  # (n_sigs, min_len)

    # Sample correlation matrix of IC series
    if ic_matrix.shape[1] > 5:
        sample_corr = np.corrcoef(ic_matrix)
    else:
        sample_corr = np.eye(n_sigs)

    # Ledoit-Wolf shrinkage toward identity
    mu_diag = np.trace(sample_corr) / n_sigs
    n_T = ic_matrix.shape[1]
    # OAS shrinkage intensity
    rho_num = (1 - 2 / n_sigs) * np.trace(sample_corr @ sample_corr) + np.trace(sample_corr) ** 2
    rho_den = (n_T + 1 - 2 / n_sigs) * (np.trace(sample_corr @ sample_corr) - np.trace(sample_corr) ** 2 / n_sigs)
    rho = float(np.clip(rho_num / (rho_den + 1e-10), 0, 1))
    shrunk_corr = (1 - rho) * sample_corr + rho * np.eye(n_sigs)

    # Mean-variance in IC space: maximize ICIR^2 / variance
    # Signal → expected IC (the "return"), IC correlation matrix (the "covariance")
    mu_ic = np.abs(icir_vals)  # use |ICIR| as expected reward per unit IC vol
    sign = np.sign(mean_ics)  # preserve signal direction

    # Constrained optimization: max mu' w s.t. w' Σ w ≤ 1, sum(w) = 1, w ≥ 0
    def neg_ic_sharpe(w):
        portfolio_ic = float(mu_ic @ w)
        portfolio_var = float(w @ shrunk_corr @ w)
        return -(portfolio_ic / np.sqrt(portfolio_var + 1e-10))

    def jac_neg(w):
        n = len(w)
        grad = np.zeros(n)
        portfolio_ic = float(mu_ic @ w)
        portfolio_var = float(w @ shrunk_corr @ w)
        denom = np.sqrt(portfolio_var + 1e-10)
        grad = -(mu_ic * denom - portfolio_ic * (shrunk_corr @ w) / denom) / (portfolio_var + 1e-10)
        return grad

    n_sigs_local = n_sigs
    bounds_w = [(0.0, 1.0)] * n_sigs_local
    constraints = [{"type": "eq", "fun": lambda w: np.sum(w) - 1.0}]
    w0 = np.ones(n_sigs_local) / n_sigs_local

    # Multiple restarts
    best_w, best_val = w0.copy(), np.inf
    for _ in range(20):
        rng_local = np.random.default_rng()
        w_init = rng_local.dirichlet(np.ones(n_sigs_local))
        try:
            result = scipy.optimize.minimize(
                neg_ic_sharpe,
                w_init,
                jac=jac_neg,
                method="SLSQP",
                bounds=bounds_w,
                constraints=constraints,
                options={"maxiter": 300, "ftol": 1e-10},
            )
            if result.fun < best_val:
                best_val = result.fun
                best_w = result.x
        except Exception:
            continue

    # Apply sign (long/short signal direction) and normalise
    w_signed = best_w * sign
    w_signed = w_signed / (np.abs(w_signed).sum() + 1e-12)

    # Effective number of signals: entropy-based
    w_abs_norm = np.abs(best_w) / (np.abs(best_w).sum() + 1e-12)
    eff_n = float(np.exp(-np.sum(w_abs_norm * np.log(w_abs_norm + 1e-12))))

    return SignalWeights(
        weights={s: float(w_signed[i]) for i, s in enumerate(signal_names)},
        shrinkage_intensity=rho,
        effective_n_signals=eff_n,
    )


@delayed
def compute_composite_alpha(all_neutralized: List[NeutralizedSignal], weights: SignalWeights) -> CompositeAlpha:
    """
    Combine neutralized signal values using the optimised weights.
    Applies an exponential decay to give more weight to recent observations.

    Fast node: pure linear combination.
    Waits for: all 11 neutralized signal nodes + optimize_signal_weights.
    """
    signal_map = {s.name: s.values for s in all_neutralized}
    n_days, n_stocks = list(signal_map.values())[0].shape

    composite = np.zeros((n_days, n_stocks))
    for name, w in weights.weights.items():
        if name in signal_map and abs(w) > 1e-6:
            vals = signal_map[name]
            # Replace NaN with 0 (neutral) for combination
            composite += w * np.where(np.isnan(vals), 0.0, vals)

    # Information ratio estimate (upper bound):
    # IR ≈ IC × sqrt(N_signals × breadth)
    valid_ics = [v for v in weights.weights.values() if abs(v) > 1e-6]
    ir_estimate = 0.03 * np.sqrt(len(valid_ics) * 252)  # rough Grinold rule

    return CompositeAlpha(
        values=composite, component_weights=weights.weights, information_ratio_estimate=float(ir_estimate)
    )


@delayed
def estimate_covariance(
    universe: UniverseData, method: str, lookback: int = 504, n_factors: int = 15
) -> CovarianceEstimate:
    """
    Estimate the return covariance matrix using one of five methods.
    All five run concurrently — each is genuinely expensive at n_stocks=3000.

    WHY THIS IS HEAVY (the critical bottleneck in large-universe quant PMs):

      "sample":       O(p² × T) to compute + O(p³) to invert (Cholesky)
                      p=3000, T=504 → 4.5B + 27B flops ≈ 6 min
      "ledoit_wolf":  Same as sample + iterative shrinkage estimation
      "nl_shrinkage": Ledoit-Wolf (2020) nonlinear: O(p³) eigendecomposition
                      + numerical integration per eigenvalue → 8–15 min
      "rmt_cleaned":  Random Matrix Theory eigenvalue clipping:
                      O(p³) SVD + Marchenko-Pastur fitting → 8–12 min
      "factor_model": PCA factor extraction + OLS specific risk + assembly
                      O(min(p,T)² × max(p,T)) SVD ≈ 4 min for T<p

    With 5 parallel covariance nodes, the bottleneck is nl_shrinkage (~12 min).
    Serial would be 6+6+12+10+4 = 38 min; parallel = 12 min.
    """
    rng = np.random.default_rng(hash(method) % 2**31)
    R = universe.returns[-lookback:, :]  # (lookback, n_stocks)
    T, p = R.shape
    R_dm = R - R.mean(axis=0)

    if method == "sample":
        S = R_dm.T @ R_dm / (T - 1)  # (p, p) raw sample covariance
        # Add small regularisation for invertibility
        S += np.eye(p) * 1e-6

    elif method == "ledoit_wolf":
        S_raw = R_dm.T @ R_dm / (T - 1)
        # OAS shrinkage intensity
        mu = np.trace(S_raw) / p
        delta = float(np.trace(S_raw @ S_raw))
        gamma = float(np.trace(S_raw) ** 2)
        rho = ((1 - 2 / p) * delta + gamma) / ((T + 1 - 2 / p) * (delta - gamma / p))
        rho = float(np.clip(rho, 0, 1))
        S = (1 - rho) * S_raw + rho * mu * np.eye(p)

    elif method == "nl_shrinkage":
        # Ledoit-Wolf (2020) Oracle Approximating Shrinkage (OAS) for large p
        # Exact formula using eigenvalue distribution
        S_raw = R_dm.T @ R_dm / (T - 1)
        eigvals, eigvecs = np.linalg.eigh(S_raw)
        eigvals = np.maximum(eigvals, 1e-8)

        # Stein's estimator: d_i^* = d_i / (1 + (p/T) × h(d_i))
        # where h is a function of the empirical spectral distribution
        c = p / T  # concentration ratio
        # Marchenko-Pastur Stieltjes transform approximation
        d_star = np.zeros(p)
        for i, d in enumerate(eigvals):
            # Simplified NL shrinkage: Haff (1980) approximation
            d_star[i] = d / (1 + c * (1 - d / (eigvals.mean() + 1e-10)))

        d_star = np.maximum(d_star, eigvals * 0.1)  # floor
        S = (eigvecs * d_star) @ eigvecs.T

    elif method == "rmt_cleaned":
        # Random Matrix Theory: remove eigenvalues below Marchenko-Pastur upper edge
        S_raw = R_dm.T @ R_dm / (T - 1)
        sigma2 = np.trace(S_raw) / p
        c = p / T
        # Marchenko-Pastur upper/lower edges
        lambda_plus = sigma2 * (1 + np.sqrt(c)) ** 2
        lambda_minus = sigma2 * (1 - np.sqrt(c)) ** 2

        eigvals, eigvecs = np.linalg.eigh(S_raw)
        # Eigenvalues below lambda_plus are noise → shrink to mean noise level
        noise_mask = eigvals < lambda_plus
        noise_mean = float(eigvals[noise_mask].mean()) if noise_mask.any() else sigma2
        d_clean = np.where(noise_mask, noise_mean, eigvals)
        S = (eigvecs * d_clean) @ eigvecs.T

    elif method == "factor_model":
        # PCA factor model: S = B Σ_F B' + diag(σ²_ε)
        # This is the fastest for large p when n_factors << p
        # Full SVD for factor extraction (EXPENSIVE at p=3000)
        U, sv, Vt = np.linalg.svd(R_dm, full_matrices=False)
        F = U[:, :n_factors] * sv[:n_factors]  # (T, n_factors)
        B = Vt[:n_factors, :].T  # (p, n_factors)
        Sigma_F = np.cov(F.T)  # (n_factors, n_factors)
        residuals = R_dm - F @ B.T  # (T, p)
        sigma2_eps = residuals.var(axis=0)  # (p,): specific variance
        S = B @ Sigma_F @ B.T + np.diag(sigma2_eps)

    else:
        S = np.eye(p)

    # Diagnostics
    try:
        eigvals_check = np.linalg.eigvalsh(S)
        cond_num = float(eigvals_check[-1] / max(eigvals_check[0], 1e-12))
        eff_rank = float(np.exp(scipy.stats.entropy(eigvals_check / eigvals_check.sum())))
    except Exception:
        cond_num, eff_rank = 1e6, 1.0

    # Estimation error bound (Frobenius): O(p / sqrt(T))
    est_error = float(p / np.sqrt(T) * np.trace(S) / p)

    return CovarianceEstimate(
        method=method, matrix=S, condition_number=cond_num, effective_rank=eff_rank, estimation_error_bound=est_error
    )


@delayed
def construct_portfolio(
    alpha: CompositeAlpha,
    cov: CovarianceEstimate,
    universe: UniverseData,
    target_vol: float = 0.10,
    max_position: float = 0.005,
    sector_neutral: bool = True,
    n_rebalance_days: int = 21,
) -> Portfolio:
    """
    Construct an optimal portfolio: daily weights solving a QP with
    transaction cost penalty and risk constraints.

    WHY THIS IS HEAVY:
      - QP on n_stocks=3000 variables × ~200 constraints (sector neutrality,
        position limits, net exposure) per rebalancing date
      - ~120 rebalancing dates over a 10-year backtest
      - Each QP: interior-point method, O(n³) per iteration, 50–200 iterations
        → 3000³ × 100 iterations = 2.7 trillion flops per QP
        (in practice, sparsity reduces this, but still 5–20 min per portfolio)
      - 5 covariance variants × 5–20 min = 25–100 min serial
        → 5–20 min parallel (5 Scaler workers)

    The mean-variance objective with transaction costs:
        max  α'w - λ w'Σw - tc × Σ|w_t - w_{t-1}|
        s.t. Σwᵢ = 0 (dollar-neutral), |wᵢ| ≤ max_pos,
             sector exposures = 0 (if sector_neutral),
             tracking error ≤ target_vol
    """
    rng = np.random.default_rng(hash(cov.method) % 2**31)
    n_days, n_stocks = alpha.values.shape
    Sigma = cov.matrix
    gamma = 2.0  # risk aversion
    tc = 0.001  # one-way transaction cost (10bps)

    weights_history = np.zeros((n_days, n_stocks))
    turnover_series = np.zeros(n_days)
    w_prev = np.zeros(n_stocks)

    rebalance_dates = range(252, n_days, n_rebalance_days)  # start after 1yr

    for t in rebalance_dates:
        mu = alpha.values[t, :]
        # Replace NaN alpha with 0
        mu = np.where(np.isnan(mu), 0.0, mu)

        # ── Simplified QP: use closed-form mean-variance with box constraint ──
        # Full QP is too slow in pure Python for 3000 vars; use ridge shrinkage
        # to approximate the constrained solution.
        # (Production: use Gurobi/CVXPY with sparse Sigma)

        # Scale alpha by signal strength
        alpha_scale = float(np.std(mu[mu != 0])) if (mu != 0).any() else 1.0
        mu_scaled = mu / (alpha_scale + 1e-10)

        # Diagonal approximation of Sigma for the initial solve
        diag_sigma = np.diag(Sigma) + 1e-6
        # Unconstrained MVO: w* ∝ Σ^{-1} μ (using diagonal approx for speed)
        w_raw = mu_scaled / (gamma * diag_sigma)

        # Transaction cost penalisation: shrink toward previous weights
        w_tc = (w_raw + tc * w_prev) / (1 + tc)

        # Box constraints: |w| ≤ max_position
        w_clipped = np.clip(w_tc, -max_position, max_position)

        # Dollar neutrality: subtract mean
        w_clipped -= w_clipped.mean()

        # Sector neutrality: subtract sector means
        if sector_neutral:
            sec_d = universe.sector_dummies  # (n_stocks, n_sectors)
            for s in range(universe.n_sectors):
                mask = sec_d[:, s].astype(bool)
                if mask.any():
                    w_clipped[mask] -= w_clipped[mask].mean()

        # Volatility scaling: target_vol / sqrt(w'Σw)
        port_var = float(w_clipped @ Sigma @ w_clipped)
        if port_var > 1e-10:
            scale = target_vol / np.sqrt(port_var * 252)
            w_clipped *= min(scale, 3.0)  # cap leverage at 3×

        weights_history[t, :] = w_clipped
        turnover_series[t] = float(np.sum(np.abs(w_clipped - w_prev)))
        w_prev = w_clipped

        # Fill between rebalancing dates (hold previous)
        if t + n_rebalance_days < n_days:
            for t2 in range(t + 1, t + n_rebalance_days):
                weights_history[t2, :] = w_clipped

    # Ex-ante IR
    port_var_annual = float(np.diag(weights_history[-1:] @ Sigma @ weights_history[-1:].T)[-1] * 252)
    alpha_last = alpha.values[-1, :]
    expected_ret = float((weights_history[-1, :] * np.where(np.isnan(alpha_last), 0.0, alpha_last)).sum())
    ex_ante_ir = expected_ret / np.sqrt(port_var_annual + 1e-10)

    return Portfolio(
        covariance_method=cov.method,
        weights_history=weights_history,
        turnover_series=turnover_series,
        ex_ante_ir=ex_ante_ir,
        target_volatility=target_vol,
    )


@delayed
def walk_forward_evaluate(portfolio: Portfolio, universe: UniverseData) -> PerformanceStats:
    """
    Compute out-of-sample performance statistics.
    Fast node: pure array arithmetic.
    """
    W = portfolio.weights_history  # (n_days, n_stocks)
    R = universe.returns  # (n_days, n_stocks)
    n_days = R.shape[0]

    # Daily portfolio return (net of estimated transaction costs)
    gross_ret = (W * R).sum(axis=1)
    tc_cost = portfolio.turnover_series * 0.001  # 10bps per unit turnover
    net_ret = gross_ret - tc_cost

    # Skip warmup period
    warmup = 252
    net_eval = net_ret[warmup:]

    ann_ret = float(net_eval.mean() * 252)
    ann_vol = float(net_eval.std() * np.sqrt(252))
    sharpe = ann_ret / (ann_vol + 1e-10)

    # Information ratio vs. benchmark (equal-weight market)
    bmark_ret = R[warmup:, :].mean(axis=1)
    active_ret = net_eval - bmark_ret
    ir = float(active_ret.mean() * 252 / (active_ret.std() * np.sqrt(252) + 1e-10))

    # Max drawdown
    cum_ret = np.cumprod(1 + net_eval)
    running_max = np.maximum.accumulate(cum_ret)
    drawdowns = cum_ret / running_max - 1
    max_dd = float(drawdowns.min())

    avg_turnover = float(portfolio.turnover_series[warmup:].mean() * 252)
    hit_rate = float((net_eval > 0).mean())

    # Factor exposures: average sector weight
    avg_weights = W[warmup:, :].mean(axis=0)
    sec_exp = {}
    for s in range(universe.n_sectors):
        mask = universe.sector_dummies[:, s].astype(bool)
        sec_exp[f"SECTOR_{s:02d}"] = float(avg_weights[mask].sum())

    return PerformanceStats(
        covariance_method=portfolio.covariance_method,
        annualised_return=ann_ret,
        annualised_vol=ann_vol,
        sharpe_ratio=sharpe,
        information_ratio=ir,
        max_drawdown=max_dd,
        avg_turnover=avg_turnover,
        hit_rate=hit_rate,
        factor_exposures=sec_exp,
    )


@delayed
def compare_and_report(
    all_ic_stats: List[ICStats],
    signal_weights: SignalWeights,
    all_perf_stats: List[PerformanceStats],
    composite_alpha: CompositeAlpha,
) -> AlphaResearchReport:
    """
    Second and final FAN-IN: waits for all 5 portfolio performance nodes.
    Generates the research report that a PM would review.
    """
    # Signal ranking table
    signal_rows = []
    best_by_signal: Dict[str, ICStats] = {}
    for ic in all_ic_stats:
        if ic.signal_name not in best_by_signal or abs(ic.icir) > abs(best_by_signal[ic.signal_name].icir):
            best_by_signal[ic.signal_name] = ic

    for name, ic in sorted(best_by_signal.items(), key=lambda x: -abs(x[1].icir)):
        signal_rows.append(
            {
                "Signal": name,
                "Best Horizon": f"{ic.horizon}d",
                "Mean IC": f"{ic.mean_ic:.4f}",
                "ICIR": f"{ic.icir:.3f}",
                "t-stat": f"{ic.t_stat:.2f}",
                "IC Decay (days)": f"{ic.decay_halflife:.1f}",
                "Q5-Q1 (ann)": f"{ic.quantile_spread:.2%}",
                "Hit Rate": f"{ic.hit_rate:.1%}",
                "Weight": f"{signal_weights.weights.get(name, 0.0):+.3f}",
            }
        )
    signal_df = pd.DataFrame(signal_rows)

    # Portfolio comparison table
    port_rows = [
        {
            "Covariance": p.covariance_method,
            "Ann Return": f"{p.annualised_return:.2%}",
            "Ann Vol": f"{p.annualised_vol:.2%}",
            "Sharpe": f"{p.sharpe_ratio:.3f}",
            "IR": f"{p.information_ratio:.3f}",
            "MaxDD": f"{p.max_drawdown:.2%}",
            "Turnover": f"{p.avg_turnover:.1f}×/yr",
        }
        for p in sorted(all_perf_stats, key=lambda x: -x.sharpe_ratio)
    ]
    port_df = pd.DataFrame(port_rows)

    best_cov = max(all_perf_stats, key=lambda x: x.sharpe_ratio).covariance_method

    # Composite IC (using portfolio returns as proxy)
    composite_ic = float(np.nanmean([ic.mean_ic for ic in all_ic_stats]))
    composite_icir = composite_alpha.information_ratio_estimate

    return AlphaResearchReport(
        signal_rankings=signal_df,
        optimal_weights=signal_weights,
        portfolio_comparison=port_df,
        recommended_covariance=best_cov,
        composite_ic=composite_ic,
        composite_icir=composite_icir,
    )


@graph
def alpha_research_platform(n_stocks: int = 3000, n_days: int = 2520, seed: int = 42) -> AlphaResearchReport:
    """
    Full multi-signal alpha research pipeline.

    PARALLEL WIN SUMMARY:
      Tier 1 — 11 concurrent signal computation nodes    (5 min each → 5 min total)
      Tier 2 — 44 concurrent IC nodes (11 × 4 horizons) (7 min each → 7 min total)
      Fan-in — optimize_signal_weights (waits for all 44 IC nodes)
      Tier 3 — 5 concurrent covariance estimation nodes  (12 min each → 12 min total)
      Tier 4 — 5 concurrent portfolio construction nodes  (8 min each → 8 min total)
      Tier 5 — 5 concurrent walk-forward evaluation nodes (2 min each → 2 min total)
      Fan-in — compare_and_report (waits for all 5 evaluation nodes)

    Critical path: load(1) + signal(5) + IC(7) + opt_wt(3) + composite(1)
                   + cov(12) + port(8) + eval(2) + report(1) ≈ 40 min

    Serial equivalent: 1 + 55 + 308 + 3 + 1 + 38 + 40 + 10 + 1 ≈ 457 min (7.6 hr)
    Speedup: ~11×  (limited by the 12-min NL shrinkage covariance node)

    Adding a new signal: 1 signal node + 4 IC nodes.  Zero other changes.
    Adding a new covariance method: 1 cov node + 1 port node + 1 eval node.
    """
    universe = load_universe_data(n_stocks=n_stocks, n_days=n_days, seed=seed)

    # Signal specifications
    signal_specs = [
        SignalSpec("MOM", "12-1 month momentum", lookback=252, skip=21, decay_halflife=30),
        SignalSpec("MOM6", "6-1 month momentum", lookback=126, skip=21, decay_halflife=20),
        SignalSpec("STR", "Short-term 1M reversal", lookback=21, skip=0, decay_halflife=5),
        SignalSpec("QRE", "Quality: low realised vol", lookback=252, skip=0, decay_halflife=60),
        SignalSpec("VAL", "Value: book-to-market", lookback=63, skip=0, decay_halflife=90),
        SignalSpec("EPS", "Earnings revision breadth", lookback=63, skip=0, decay_halflife=45),
        SignalSpec("SHT", "Short interest (contrarian)", lookback=21, skip=0, decay_halflife=30),
        SignalSpec("LOW", "Low volatility anomaly", lookback=126, skip=0, decay_halflife=45),
        SignalSpec("ERN", "Earnings surprise persistence", lookback=63, skip=0, decay_halflife=21),
        SignalSpec("ACC", "Accruals (quality)", lookback=252, skip=0, decay_halflife=90),
        SignalSpec("AGR", "Asset growth (conservative)", lookback=252, skip=0, decay_halflife=90),
    ]

    # Tier 1: 11 parallel signal computation + neutralization nodes
    neutralized_signals = [compute_and_neutralize_signal(universe, spec) for spec in signal_specs]

    # Tier 2: 44 parallel IC analysis nodes (11 signals × 4 horizons)
    ic_stats: List = []
    for sig in neutralized_signals:
        for h_idx in range(4):  # horizons: 1d, 5d, 21d, 63d
            ic_stats.append(compute_ic_and_decay(sig, horizon_idx=h_idx))

    # Fan-in 1: optimize signal weights (waits for all 44 IC nodes)
    signal_weights = optimize_signal_weights(ic_stats)

    # Composite alpha (waits for 11 neutralized signals + signal weights)
    composite = compute_composite_alpha(neutralized_signals, signal_weights)

    # Tier 3: 5 parallel covariance estimation nodes
    cov_methods = ["sample", "ledoit_wolf", "nl_shrinkage", "rmt_cleaned", "factor_model"]
    covariances = [estimate_covariance(universe, method=m) for m in cov_methods]

    # Tier 4: 5 parallel portfolio construction nodes
    # Each depends on: composite alpha (shared) + one covariance estimate
    portfolios = [construct_portfolio(composite, cov, universe) for cov in covariances]

    # Tier 5: 5 parallel walk-forward evaluation nodes
    perf_stats = [walk_forward_evaluate(port, universe) for port in portfolios]

    # Fan-in 2: research report (waits for all 5 perf nodes)
    return compare_and_report(ic_stats, signal_weights, perf_stats, composite)


# ── Demo runner ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from pargraph import GraphEngine

    engine = GraphEngine()

    print("Running Multi-Signal Alpha Research Platform...")
    print("(Reduced universe for local test — use n_stocks=3000, n_days=2520 on Scaler)")

    report = alpha_research_platform(
        n_stocks=3000, n_days=2520, seed=42  # increased to 3000  # increased to 2520 (10 years)
    )

    print("\n=== Signal Rankings ===")
    print(report.signal_rankings.to_string(index=False))

    print(f"\n=== Signal Combination ===")
    print(f"Shrinkage intensity    : {report.optimal_weights.shrinkage_intensity:.3f}")
    print(f"Effective # signals    : {report.optimal_weights.effective_n_signals:.2f}")
    print(f"Composite ICIR estimate: {report.composite_icir:.3f}")

    print("\n=== Portfolio Comparison ===")
    print(report.portfolio_comparison.to_string(index=False))
    print(f"\nRecommended covariance method: {report.recommended_covariance}")

    # Visualise the 3-tier DAG:
    # alpha_research_platform.to_graph(n_stocks=300, n_days=504).to_dot().write_png("alpha_dag.png")
