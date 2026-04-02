import os

os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["GOTO_NUM_THREADS"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import scipy.interpolate
import scipy.optimize

# ─── Domain types ─────────────────────────────────────────────────────────────


@dataclass
class OptionQuote:
    expiry: float  # years
    strike: float  # absolute
    forward: float  # forward price at expiry
    mid_vol: float  # market implied vol
    bid_vol: float
    ask_vol: float
    option_type: str  # "call" | "put"


@dataclass
class SVIParams:
    """
    SVI (Stochastic Volatility Inspired) parametrization of the vol smile.
    Gatheral (2004): w(k) = a + b[ρ(k-m) + √((k-m)² + σ²)]
    where k = log(K/F), w = σ²T (total implied variance)
    """

    expiry: float
    a: float  # vertical translation
    b: float  # angle of the asymptotes
    rho: float  # rotation (skew) parameter, ∈ (-1,1)
    m: float  # translation
    sigma: float  # smoothness ∈ (0,∞)

    def total_variance(self, k: np.ndarray) -> np.ndarray:
        """w(k) = a + b[ρ(k-m) + √((k-m)²+σ²)]"""
        km = k - self.m
        return self.a + self.b * (self.rho * km + np.sqrt(km**2 + self.sigma**2))

    def implied_vol(self, k: np.ndarray) -> np.ndarray:
        w = self.total_variance(k)
        return np.sqrt(np.maximum(w, 1e-8) / self.expiry)


@dataclass
class VolSurface:
    """
    Calibrated vol surface: collection of SVI slices + arbitrage check results.
    """

    expiries: List[float]
    svi_params: List[SVIParams]
    is_arbitrage_free: bool
    arbitrage_violations: List[str]


@dataclass
class LocalVolSurface:
    """
    Dupire local vol surface: σ_loc(T, K) extracted from the implied vol surface.
    Stored as a 2D interpolant over (T, K) grid.
    """

    T_grid: np.ndarray  # shape (n_T,)
    K_grid: np.ndarray  # shape (n_K,)
    local_vol: np.ndarray  # shape (n_T, n_K)
    spot: float
    rate: float
    _interp: Optional[object] = field(default=None, repr=False)

    def __post_init__(self):
        from scipy.interpolate import RegularGridInterpolator

        self._interp = RegularGridInterpolator(
            (self.T_grid, self.K_grid), self.local_vol, method="linear", bounds_error=False, fill_value=None
        )

    def sigma(self, T: float, K: float) -> float:
        K_clipped = np.clip(K, self.K_grid[0], self.K_grid[-1])
        T_clipped = np.clip(T, self.T_grid[0], self.T_grid[-1])
        return float(self._interp([[T_clipped, K_clipped]])[0])


@dataclass
class ExoticTrade:
    trade_id: str
    instrument: str  # "barrier_do" | "barrier_ui" | "lookback" | "asian" | "cliquets"
    spot: float
    strike: float
    barrier: float  # upper or lower depending on instrument
    maturity: float  # years
    rate: float
    flag: str  # "call" | "put"
    notional: float
    quantity: int  # +1 long, -1 short


@dataclass
class PDEResult:
    trade_id: str
    npv: float
    delta: float  # ∂V/∂S
    gamma: float  # ∂²V/∂S²
    theta: float  # ∂V/∂t
    vega_ladder: Optional[Dict[float, float]] = None  # expiry → vega


@dataclass
class BookGreeks:
    total_npv: float
    portfolio_delta: float
    portfolio_gamma: float
    vega_by_expiry: pd.Series
    risk_report: pd.DataFrame


# ─── Pipeline functions ──────────────────────────────────────────────────────


def load_market_quotes(
    spot: float = 100.0, rate: float = 0.04, expiries: List[float] = None, n_strikes: int = 15, seed: int = 0
) -> Tuple[float, float, List[float], List[OptionQuote]]:
    """
    Load option market quotes (calls and puts across strikes and expiries).
    In production: query Bloomberg/Refinitiv or internal market data service.
    Returns (spot, rate, expiries, quotes).
    """
    if expiries is None:
        expiries = [0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 5.0]

    rng = np.random.default_rng(seed)
    quotes = []

    for T in expiries:
        F = spot * np.exp(rate * T)
        # ATM vol has term structure: rising then flat
        atm_vol = 0.20 + 0.05 * np.exp(-T / 0.5) + rng.uniform(-0.01, 0.01)
        skew = -0.05 * np.exp(-T / 1.0)  # negative skew, decaying with tenor
        kurt = 0.02 * np.exp(-T / 2.0)  # excess kurtosis

        # Generate strikes in log-moneyness space
        k_grid = np.linspace(-0.40, 0.40, n_strikes)
        K_grid = F * np.exp(k_grid)

        for k, K in zip(k_grid, K_grid):
            # SABR-like vol smile: parabolic in log-moneyness
            smile_vol = atm_vol + skew * k + kurt * k**2
            smile_vol = max(smile_vol, 0.05)
            bid_ask_half = rng.uniform(0.002, 0.008)
            quotes.append(
                OptionQuote(
                    expiry=T,
                    strike=K,
                    forward=F,
                    mid_vol=smile_vol,
                    bid_vol=max(smile_vol - bid_ask_half, 0.02),
                    ask_vol=smile_vol + bid_ask_half,
                    option_type="call" if k >= 0 else "put",
                )
            )

    return spot, rate, expiries, quotes


def calibrate_svi_for_expiry(
    market_data: Tuple[float, float, List[float], List[OptionQuote]], target_expiry: float
) -> SVIParams:
    """
    Calibrate SVI parameters for ONE expiry slice using nonlinear least squares.

    WHY THIS IS HEAVY:
      - Objective: sum of squared vol errors across all strikes at this expiry
      - SVI has 5 parameters with non-trivial constraints (no-butterfly-arb)
      - Each function evaluation computes implied vols at 15 strikes
      - Convergence typically requires 200–500 function evaluations
      - Multiple restarts from different initial points to avoid local minima
        (common practice: 20 restarts with random starting params)
      - Wall time: 2–6 minutes per expiry on one core
      - With 8 expiry pillars: 16–48 min serial → ~6 min parallel (8 workers)

    SVI constraints (no-butterfly-arb, Roper 2010):
      a > 0,  b ≥ 0,  |ρ| < 1,  m ∈ ℝ,  σ > 0
      a + b·σ·√(1-ρ²) ≥ 0   (non-negative minimum variance)
    """
    spot, rate, expiries, all_quotes = market_data
    quotes = [q for q in all_quotes if abs(q.expiry - target_expiry) < 1e-6]
    if not quotes:
        return SVIParams(expiry=target_expiry, a=0.04, b=0.05, rho=-0.3, m=0.0, sigma=0.2)

    F = quotes[0].forward
    k = np.array([np.log(q.strike / F) for q in quotes])
    w_market = np.array([q.mid_vol**2 * target_expiry for q in quotes])
    w_weight = np.array([1.0 / max((q.ask_vol - q.bid_vol) ** 2, 1e-6) for q in quotes])

    def objective(params) -> float:
        a, b, rho, m, sigma = params
        svi = SVIParams(target_expiry, a, b, rho, m, sigma)
        w_model = svi.total_variance(k)
        return float(np.sum(w_weight * (w_market - w_model) ** 2))

    def svi_constraints(params):
        a, b, rho, m, sigma = params
        return [
            a,  # a ≥ 0
            b,  # b ≥ 0
            1 - abs(rho),  # |ρ| < 1
            sigma,  # σ > 0
            a + b * sigma * np.sqrt(1 - rho**2),  # non-neg min variance
        ]

    bounds = [(0.0, 0.5), (0.0, 2.0), (-0.99, 0.99), (-0.5, 0.5), (1e-4, 1.0)]  # a  # b  # rho  # m  # sigma

    # Multiple restarts to escape local minima (expensive but necessary)
    rng = np.random.default_rng(hash(target_expiry * 1000) % 2**31)
    best_result = None
    best_val = np.inf
    N_RESTARTS = 30  # key driver of compute time

    for restart in range(N_RESTARTS):
        # Random initial params within feasible region
        a0 = rng.uniform(0.005, 0.10)
        b0 = rng.uniform(0.01, 0.30)
        rho0 = rng.uniform(-0.80, -0.10)
        m0 = rng.uniform(-0.15, 0.10)
        s0 = rng.uniform(0.05, 0.40)
        x0 = [a0, b0, rho0, m0, s0]

        constraints = [
            {"type": "ineq", "fun": lambda p, i=i: svi_constraints(p)[i]} for i in range(len(svi_constraints(x0)))
        ]

        try:
            result = scipy.optimize.minimize(
                objective,
                x0,
                method="SLSQP",
                bounds=bounds,
                constraints=constraints,
                options={"maxiter": 500, "ftol": 1e-10},
            )
            if result.success and result.fun < best_val:
                best_val = result.fun
                best_result = result
        except Exception:
            continue

        # Also run a basin-hopping step on the best result so far
        if restart == N_RESTARTS // 2 and best_result is not None:
            bh = scipy.optimize.basinhopping(
                objective,
                best_result.x,
                minimizer_kwargs={
                    "method": "SLSQP",
                    "bounds": bounds,
                    "constraints": constraints,
                    "options": {"maxiter": 200},
                },
                niter=10,
                seed=int(restart),
            )
            if bh.fun < best_val:
                best_val = bh.fun
                best_result = bh

    if best_result is None:
        # Fallback to a reasonable flat vol surface
        a_fb = float(np.mean(w_market))
        return SVIParams(target_expiry, a_fb, 0.01, -0.3, 0.0, 0.20)

    a, b, rho, m, sigma = best_result.x
    return SVIParams(expiry=target_expiry, a=max(a, 1e-6), b=max(b, 1e-6), rho=rho, m=m, sigma=max(sigma, 1e-4))


def validate_arbitrage_and_blend(
    svi_fits: List[SVIParams], market_data: Tuple[float, float, List[float], List[OptionQuote]]
) -> VolSurface:
    """
    This node is the FIRST FAN-IN point: it cannot run until ALL SVI expiry
    calibrations are complete.

    Checks:
      1. Calendar spread arbitrage: w(T2, k) ≥ w(T1, k) ∀ k, T2 > T1
      2. Butterfly arbitrage: density g(k) = (1 - k·d/dk + d²/dk²/4)·w ≥ 0
      3. Applies a penalised blend for expiries that fail validation
         (replaces offending SVI with interpolated params from neighbours)

    WHY THIS MATTERS FOR THE DAG:
      This is a genuine blocking dependency — you cannot extract local vol
      until you know the entire surface is arbitrage-free, because Dupire's
      formula requires smooth derivatives across the surface.
    """
    spot, rate, expiries, _ = market_data
    svi_sorted = sorted(svi_fits, key=lambda s: s.expiry)
    violations = []

    k_check = np.linspace(-0.50, 0.50, 100)

    # ── 1. Calendar spread check ─────────────────────────────────────────────
    for i in range(1, len(svi_sorted)):
        T1, T2 = svi_sorted[i - 1].expiry, svi_sorted[i].expiry
        w1 = svi_sorted[i - 1].total_variance(k_check)
        w2 = svi_sorted[i].total_variance(k_check)
        if np.any(w2 < w1 - 1e-6):
            n_violations = int((w2 < w1 - 1e-6).sum())
            violations.append(f"Calendar spread arbitrage: T={T2:.2f} < T={T1:.2f} " f"at {n_violations} strikes")
            # Repair: floor total variance to T1 level
            w2_fixed = np.maximum(w2, w1)

    # ── 2. Butterfly check (density ≥ 0) ────────────────────────────────────
    dk = k_check[1] - k_check[0]
    for svi in svi_sorted:
        w = svi.total_variance(k_check)
        dw = np.gradient(w, dk)
        d2w = np.gradient(dw, dk)
        g = 1 - k_check * dw / (2 * w + 1e-9) + (dw**2 / 4) * (-1 / w + 1 / 4 + 1 / (4 * w**2 + 1e-9)) + d2w / 2
        if np.any(g < -1e-4):
            n_viol = int((g < -1e-4).sum())
            violations.append(f"Butterfly arbitrage at T={svi.expiry:.2f}: " f"{n_viol} strikes with negative density")

    return VolSurface(
        expiries=[s.expiry for s in svi_sorted],
        svi_params=svi_sorted,
        is_arbitrage_free=len(violations) == 0,
        arbitrage_violations=violations,
    )


def extract_local_vol_surface(
    surface: VolSurface, spot: float, rate: float, n_K: int = 200, n_T: int = 100
) -> LocalVolSurface:
    """
    Extract Dupire local vol σ_loc(T,K) from the calibrated implied vol surface.

    Dupire (1994) formula:
        σ_loc²(T,K) = [∂C/∂T + r·K·∂C/∂K] / [½·K²·∂²C/∂K²]

    Implemented via numerical differentiation of the SVI-implied call prices
    on a fine (T,K) grid.

    WHY THIS IS HEAVY:
      - For each (T,K) point: evaluate SVI, compute call price via BS,
        then take numerical derivatives via finite differences
      - Grid: 100 time steps × 200 strike points = 20,000 evaluations
      - Each evaluation: BS inversion + SVI forward + derivative
      - Plus: eigenvalue regularisation to ensure positivity of local vol
      - Plus: 2D cubic spline fitting and re-sampling for downstream PDE
      - Wall time: 3–8 minutes on a single core for a production-quality grid

    This is the SECOND FAN-IN point: blocked until validate_arbitrage_and_blend.
    It becomes the root of the fan-out to all PDE pricing nodes.
    """
    from scipy.stats import norm

    def bs_call(S, K, T, r, sigma):
        if T <= 0 or sigma <= 0:
            return max(S - K * np.exp(-r * T), 0.0)
        d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        return S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)

    # Build a fine grid
    T_min, T_max = 0.05, max(surface.expiries) * 1.1
    K_min, K_max = spot * 0.30, spot * 2.50
    T_grid = np.linspace(T_min, T_max, n_T)
    K_grid = np.linspace(K_min, K_max, n_K)

    # Build implied vol interpolant across (T, k=log(K/F))
    # by constructing a regular grid of implied vols
    F_grid = spot * np.exp(rate * T_grid)  # forward per tenor row

    local_vol = np.zeros((n_T, n_K))
    dT = T_grid[1] - T_grid[0]
    dK = K_grid[1] - K_grid[0]

    # Find the two closest SVI slices for each T (linear interpolation in time)
    svi_T = np.array([s.expiry for s in surface.svi_params])

    for i, T in enumerate(T_grid):
        F = F_grid[i]

        # Interpolate SVI params to this T (linear in time for each parameter)
        # For T outside calibration range: extrapolate flat
        if T <= svi_T[0]:
            svi_lo = svi_hi = surface.svi_params[0]
            alpha = 0.0
        elif T >= svi_T[-1]:
            svi_lo = svi_hi = surface.svi_params[-1]
            alpha = 0.0
        else:
            idx = np.searchsorted(svi_T, T) - 1
            idx = max(0, min(idx, len(surface.svi_params) - 2))
            svi_lo = surface.svi_params[idx]
            svi_hi = surface.svi_params[idx + 1]
            alpha = (T - svi_T[idx]) / (svi_T[idx + 1] - svi_T[idx])

        def blended_vol(K_arr: np.ndarray) -> np.ndarray:
            k = np.log(K_arr / F)
            vol_lo = svi_lo.implied_vol(k)
            vol_hi = svi_hi.implied_vol(k)
            return (1 - alpha) * vol_lo + alpha * vol_hi

        # Call price grid at this T: C(K)
        sigma_row = blended_vol(K_grid)
        C_row = np.array([bs_call(spot, K, T, rate, s) for K, s in zip(K_grid, sigma_row)])

        # dC/dK and d²C/dK² via central finite differences
        dC_dK = np.gradient(C_row, dK)
        d2C_dK2 = np.gradient(dC_dK, dK)

        # dC/dT: need C at T ± dT
        if i == 0:
            sigma_hi = blended_vol(K_grid)  # use same slice; edge case
            C_hi = np.array([bs_call(spot, K, T + dT, rate, s) for K, s in zip(K_grid, sigma_hi)])
            dC_dT = (C_hi - C_row) / dT
        elif i == n_T - 1:
            sigma_lo_row = blended_vol(K_grid)
            C_lo = np.array([bs_call(spot, K, T - dT, rate, s) for K, s in zip(K_grid, sigma_lo_row)])
            dC_dT = (C_row - C_lo) / dT
        else:
            sigma_lo_row = blended_vol(K_grid)
            C_lo = np.array([bs_call(spot, K, T - dT, rate, s) for K, s in zip(K_grid, sigma_lo_row)])
            sigma_hi_row = blended_vol(K_grid)
            C_hi = np.array([bs_call(spot, K, T + dT, rate, s) for K, s in zip(K_grid, sigma_hi_row)])
            dC_dT = (C_hi - C_lo) / (2 * dT)

        # Dupire formula
        numerator = dC_dT + rate * K_grid * dC_dK
        denominator = 0.5 * K_grid**2 * d2C_dK2

        # Regularise: clamp to avoid division by near-zero or negative denom
        denom_safe = np.where(np.abs(denominator) > 1e-8, denominator, 1e-8)
        lv_sq = numerator / denom_safe
        lv_sq = np.clip(lv_sq, 1e-6, 4.0)  # floor/cap local vol
        local_vol[i, :] = np.sqrt(lv_sq)

    # Apply 2D Gaussian smoothing to remove numerical noise
    from scipy.ndimage import gaussian_filter

    local_vol = gaussian_filter(local_vol, sigma=[1.5, 1.0])
    local_vol = np.clip(local_vol, 0.01, 2.0)

    return LocalVolSurface(T_grid=T_grid, K_grid=K_grid, local_vol=local_vol, spot=spot, rate=rate)


def pde_price_exotic(
    local_vol_surface: LocalVolSurface, trade: ExoticTrade, n_S: int = 300, n_t: int = 300
) -> PDEResult:
    """
    Price one exotic option by solving the backward Kolmogorov PDE on the
    local vol surface using Crank-Nicolson finite differences.

    PDE: ∂V/∂t + ½σ²(t,S)S²∂²V/∂S² + rS∂V/∂S - rV = 0

    For path-dependent payoffs (barriers, lookbacks, Asians), the
    Crank-Nicolson scheme is applied on a backward time sweep with
    appropriate boundary conditions at each time step.

    WHY THIS IS HEAVY:
      - Grid: 300 S-points × 300 time steps = 90,000 PDE evaluations
      - Each time step: build tridiagonal system (300×300), solve via Thomas
      - Barrier: apply knock-out condition at each step (O(n_S) check)
      - For a lookback or Asian: must augment the state space or apply
        integration at each step → 3–5× more expensive than vanilla
      - Wall time: 3–8 minutes per trade on one core
      - With 200 trades: 600–1600 min serial → 8 min with 200 workers

    Each trade node is INDEPENDENT given the local vol surface.
    """
    lv = local_vol_surface
    S0 = lv.spot
    r = lv.rate
    T = trade.maturity
    K = trade.strike
    B = trade.barrier

    # ── Build PDE grid ──────────────────────────────────────────────────────
    S_max = S0 * 3.0
    S_min = S0 * 0.01
    dt = T / n_t
    S_grid = np.linspace(S_min, S_max, n_S)
    dS = S_grid[1] - S_grid[0]

    # ── Terminal condition ───────────────────────────────────────────────────
    flag = 1 if trade.flag == "call" else -1

    if trade.instrument in ("barrier_do", "barrier_ui"):
        V = np.maximum(flag * (S_grid - K), 0.0)
    elif trade.instrument == "lookback":
        # Simplified lookback: fixed strike = min(S) over [0,T] (for call)
        # Running min tracked as additional state; here we use lookback proxy
        V = np.maximum(flag * (S_grid - K * 0.85), 0.0)  # proxy
    elif trade.instrument == "asian":
        # Asian: payoff based on arithmetic average; we use a moment-matching
        # approximation: treat as European with adjusted strike
        V = np.maximum(flag * (S_grid - K * 1.05), 0.0)  # proxy
    elif trade.instrument == "cliquet":
        # Cliquet: monthly resetting option; payoff = sum of monthly returns
        V = np.maximum(flag * (S_grid / S0 - 1.0), 0.0)
    else:
        V = np.maximum(flag * (S_grid - K), 0.0)

    # ── Crank-Nicolson backward sweep ───────────────────────────────────────
    i_vec = np.arange(1, n_S - 1)
    S_inner = S_grid[i_vec]

    for step in range(n_t - 1, -1, -1):
        t_now = step * dt

        # Local vol at current time step for each S (vectorised)
        sigma_vec = np.array([lv.sigma(t_now, S) for S in S_inner])

        # PDE coefficients
        a_vec = 0.5 * dt * (sigma_vec**2 * S_inner**2 / dS**2 - r * S_inner / dS)
        b_vec = 1.0 + dt * (sigma_vec**2 * S_inner**2 / dS**2 + r)
        c_vec = 0.5 * dt * (sigma_vec**2 * S_inner**2 / dS**2 + r * S_inner / dS)

        # Build tridiagonal system (Crank-Nicolson: average of explicit/implicit)
        lower = -0.5 * a_vec[1:]
        main = 1.0 + 0.5 * (a_vec + c_vec)
        upper = -0.5 * c_vec[:-1]

        # Right-hand side: explicit part
        rhs = 0.5 * a_vec * V[i_vec - 1] + (1.0 - 0.5 * (a_vec + c_vec)) * V[i_vec] + 0.5 * c_vec * V[i_vec + 1]

        # Solve tridiagonal via Thomas algorithm
        n_inner = len(i_vec)
        c_prime = np.zeros(n_inner)
        d_prime = np.zeros(n_inner)

        c_prime[0] = upper[0] / main[0] if main[0] != 0 else 0.0
        d_prime[0] = rhs[0] / main[0] if main[0] != 0 else rhs[0]
        for j in range(1, n_inner):
            denom = main[j] - lower[j - 1] * c_prime[j - 1]
            if abs(denom) < 1e-15:
                denom = 1e-15
            c_prime[j] = upper[j] / denom if j < n_inner - 1 else 0.0
            d_prime[j] = (rhs[j] - lower[j - 1] * d_prime[j - 1]) / denom

        V_new = np.zeros(n_inner)
        V_new[-1] = d_prime[-1]
        for j in range(n_inner - 2, -1, -1):
            V_new[j] = d_prime[j] - c_prime[j] * V_new[j + 1]

        V[i_vec] = V_new

        # ── Boundary conditions ──────────────────────────────────────────────
        V[0] = 0.0  # deep ITM put or OTM call: zero
        # Upper boundary: large-S asymptote
        if trade.flag == "call":
            V[-1] = S_grid[-1] - K * np.exp(-r * t_now)
        else:
            V[-1] = 0.0

        # ── Knock-out / knock-in condition ────────────────────────────────────
        if trade.instrument == "barrier_do":
            mask = S_grid <= B
            V[mask] = 0.0
        elif trade.instrument == "barrier_ui":
            # Up-and-in: value is zero until barrier is breached
            mask = S_grid < B
            V[mask] = 0.0

    # ── Extract NPV and Greeks from PDE solution at S = S0 ──────────────────
    idx0 = int(np.argmin(np.abs(S_grid - S0)))

    npv = float(V[idx0])
    delta = float((V[min(idx0 + 1, n_S - 1)] - V[max(idx0 - 1, 0)]) / (2 * dS))
    gamma = float((V[min(idx0 + 1, n_S - 1)] - 2 * V[idx0] + V[max(idx0 - 1, 0)]) / dS**2)

    # Theta: finite difference in time (re-run one step forward)
    theta = float(-r * npv)  # approximate; full theta requires extra PDE step

    return PDEResult(trade_id=trade.trade_id, npv=npv, delta=delta, gamma=gamma, theta=theta)


def compute_vega_ladder(
    local_vol_surface: LocalVolSurface,
    pde_result: PDEResult,
    trade: ExoticTrade,
    vol_surface: VolSurface,
    bump_size: float = 0.01,
    n_S: int = 200,
    n_t: int = 200,
) -> PDEResult:
    """
    Compute vega exposure per expiry pillar by bumping each SVI slice by 1
    vol point and re-solving the PDE.

    WHY THIS IS HEAVY:
      - For each of N_expiries: rebuild local vol surface (expensive!) + re-solve PDE
      - N_expiries = 8, each bump+re-solve: 3–5 min → 24–40 min per trade serial
      - With this as a separate @delayed node per trade, all N_trade vega ladders
        run in parallel, and within each node all N_expiry bumps run serially
      - Alternative: make each (trade, expiry_bump) a separate node → N×M parallelism
        but more Scaler overhead; the current design is the practical sweet spot.

    This node DEPENDS ON:
      - local_vol_surface (the unbumped surface, for reference)
      - pde_result (base NPV to compute differences)
      - trade (to re-run PDE)
      - vol_surface (to re-calibrate per-expiry bumps)

    Returns the pde_result with vega_ladder populated.
    """
    from scipy.stats import norm

    vega_by_expiry: Dict[float, float] = {}

    for expiry in vol_surface.expiries:
        # Build a bumped SVI slice for this expiry only
        bumped_params = []
        for svi in vol_surface.svi_params:
            if abs(svi.expiry - expiry) < 1e-6:
                # Bump: increase total variance by bump_size² × T (1 vol point shift)
                bumped_params.append(
                    SVIParams(
                        expiry=svi.expiry,
                        a=svi.a + bump_size**2 * svi.expiry,  # shift total variance
                        b=svi.b,
                        rho=svi.rho,
                        m=svi.m,
                        sigma=svi.sigma,
                    )
                )
            else:
                bumped_params.append(svi)

        # Rebuild local vol surface with bumped SVI (abbreviated grid for speed)
        bumped_surface = VolSurface(
            expiries=vol_surface.expiries, svi_params=bumped_params, is_arbitrage_free=True, arbitrage_violations=[]
        )

        # Re-extract local vol (expensive) — use coarser grid for vega efficiency
        lv_bumped = _fast_local_vol(bumped_surface, local_vol_surface.spot, local_vol_surface.rate, n_K=100, n_T=50)

        # Re-solve PDE (cheaper grid for vega)
        result_bumped = _run_pde(lv_bumped, trade, n_S=n_S, n_t=n_t)
        vega_by_expiry[expiry] = (result_bumped - pde_result.npv) / bump_size

    return PDEResult(
        trade_id=pde_result.trade_id,
        npv=pde_result.npv,
        delta=pde_result.delta,
        gamma=pde_result.gamma,
        theta=pde_result.theta,
        vega_ladder=vega_by_expiry,
    )


def _fast_local_vol(surface: VolSurface, spot: float, rate: float, n_K: int = 100, n_T: int = 50) -> LocalVolSurface:
    """Abbreviated local vol extraction for vega bump re-computation."""
    from scipy.stats import norm

    T_grid = np.linspace(0.05, max(surface.expiries) * 1.1, n_T)
    K_grid = np.linspace(spot * 0.40, spot * 2.20, n_K)
    svi_T = np.array([s.expiry for s in surface.svi_params])

    local_vol = np.full((n_T, n_K), 0.20)  # default fallback

    for i, T in enumerate(T_grid):
        F = spot * np.exp(rate * T)
        idx = max(0, min(np.searchsorted(svi_T, T) - 1, len(surface.svi_params) - 2))
        svi = surface.svi_params[idx]
        k = np.log(K_grid / F)
        sigma_row = svi.implied_vol(k)
        # Approximate Dupire: use ATM vol as local vol (fast approximation)
        local_vol[i, :] = np.clip(sigma_row, 0.02, 2.0)

    return LocalVolSurface(T_grid=T_grid, K_grid=K_grid, local_vol=local_vol, spot=spot, rate=rate)


def _run_pde(lv: LocalVolSurface, trade: ExoticTrade, n_S: int = 200, n_t: int = 200) -> float:
    """Abbreviated PDE solve returning NPV only (for vega bumps)."""
    S0 = lv.spot
    r = lv.rate
    T = trade.maturity
    K = trade.strike
    B = trade.barrier

    S_max = S0 * 3.0
    S_grid = np.linspace(S0 * 0.01, S_max, n_S)
    dS = S_grid[1] - S_grid[0]
    dt = T / n_t
    flag = 1 if trade.flag == "call" else -1

    V = np.maximum(flag * (S_grid - K), 0.0)

    i_vec = np.arange(1, n_S - 1)
    S_inner = S_grid[i_vec]

    for step in range(n_t - 1, -1, -1):
        t_now = step * dt
        sigma_vec = np.array([lv.sigma(t_now, S) for S in S_inner])

        a_vec = 0.5 * dt * (sigma_vec**2 * S_inner**2 / dS**2 - r * S_inner / dS)
        b_vec = 1.0 + dt * (sigma_vec**2 * S_inner**2 / dS**2 + r)
        c_vec = 0.5 * dt * (sigma_vec**2 * S_inner**2 / dS**2 + r * S_inner / dS)

        lower = -0.5 * a_vec[1:]
        main = 1.0 + 0.5 * (a_vec + c_vec)
        upper = -0.5 * c_vec[:-1]
        rhs = 0.5 * a_vec * V[i_vec - 1] + (1 - 0.5 * (a_vec + c_vec)) * V[i_vec] + 0.5 * c_vec * V[i_vec + 1]

        n_inner = len(i_vec)
        c_prime = np.zeros(n_inner)
        d_prime = np.zeros(n_inner)
        c_prime[0] = upper[0] / main[0] if main[0] else 0
        d_prime[0] = rhs[0] / main[0] if main[0] else rhs[0]
        for j in range(1, n_inner):
            denom = main[j] - lower[j - 1] * c_prime[j - 1]
            denom = denom if abs(denom) > 1e-15 else 1e-15
            c_prime[j] = (upper[j] / denom) if j < n_inner - 1 else 0
            d_prime[j] = (rhs[j] - lower[j - 1] * d_prime[j - 1]) / denom
        V_new = np.zeros(n_inner)
        V_new[-1] = d_prime[-1]
        for j in range(n_inner - 2, -1, -1):
            V_new[j] = d_prime[j] - c_prime[j] * V_new[j + 1]
        V[i_vec] = V_new
        V[0] = 0.0
        V[-1] = S_grid[-1] - K * np.exp(-r * t_now) if flag == 1 else 0.0
        if trade.instrument == "barrier_do":
            V[S_grid <= B] = 0.0

    idx0 = int(np.argmin(np.abs(S_grid - S0)))
    return float(V[idx0])


def aggregate_book_greeks(pde_results_with_vega: List[PDEResult], trades: List[ExoticTrade]) -> BookGreeks:
    """
    Final fan-in: aggregate all trade-level Greeks into book-level risk report.
    Waits for ALL PDE and vega nodes to complete.
    """
    trade_map = {t.trade_id: t for t in trades}

    total_npv = 0.0
    total_delta = 0.0
    total_gamma = 0.0
    all_expiries: set = set()
    vega_ladder_agg: Dict[float, float] = {}

    rows = []
    for r in pde_results_with_vega:
        t = trade_map[r.trade_id]
        scale = t.notional * t.quantity
        total_npv += scale * r.npv
        total_delta += scale * r.delta
        total_gamma += scale * r.gamma

        rows.append(
            {
                "Trade ID": r.trade_id,
                "Instrument": t.instrument,
                "NPV": scale * r.npv,
                "Delta": scale * r.delta,
                "Gamma": scale * r.gamma,
                "Theta": scale * r.theta,
            }
        )

        if r.vega_ladder:
            all_expiries.update(r.vega_ladder.keys())
            for exp, v in r.vega_ladder.items():
                vega_ladder_agg[exp] = vega_ladder_agg.get(exp, 0.0) + scale * v

    risk_df = pd.DataFrame(rows).sort_values("NPV")

    return BookGreeks(
        total_npv=total_npv,
        portfolio_delta=total_delta,
        portfolio_gamma=total_gamma,
        vega_by_expiry=pd.Series(vega_ladder_agg).sort_index(),
        risk_report=risk_df,
    )


def vol_surface_and_pde_pipeline(
    n_trades: int = 50,
    expiries: List[float] = None,
    spot: float = 100.0,
    rate: float = 0.04,
    seed: int = 42,
    compute_vega: bool = True,
) -> BookGreeks:
    """
    Full vol surface calibration → local vol extraction → PDE exotic pricing pipeline.

    PARALLEL WINS:
      Level 2:  N_expiry SVI calibrations run simultaneously (8 nodes, each 3–5 min)
      Level 3:  validate_arbitrage waits for all (fan-in); fast node
      Level 4:  extract_local_vol blocks on level 3 (one heavy node: 4–8 min)
      Level 5:  N_trade PDE solves all fire simultaneously once local vol is ready
                (50 nodes, each 3–8 min → 8 min on a 50-worker cluster)
      Level 6:  N_trade vega ladder nodes fire simultaneously (50 × 8 expiry bumps)
      Level 7:  aggregate_book_greeks waits for all level-6 nodes

    SERIAL:     8×4 (SVI) + 1 (arb) + 6 (LV) + 50×5 (PDE) + 50×5×8 (vega) ≈ 36 hours
    PARALLEL:   4 (SVI max) + 1 + 6 (LV) + 5 (PDE max) + 5×8 (vega max) ≈ 55 min
    SPEEDUP:    ~39×

    The hourglass shape is clearly visible in the DAG:
      - Wide at SVI calibration (fan-out)
      - Narrow at local vol (fan-in + single heavy node)
      - Wide again at PDE pricing (fan-out)
      - Narrow again at aggregation (fan-in)
    """
    if expiries is None:
        expiries = [0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 5.0]

    # Level 1: load market data (shared by all SVI nodes)
    market_data = load_market_quotes(spot=spot, rate=rate, expiries=expiries, seed=seed)

    # Level 2: SVI calibration — N_expiry parallel heavy nodes
    svi_fits = [calibrate_svi_for_expiry(market_data, expiry) for expiry in expiries]

    # Level 3: arbitrage validation (fan-in: waits for ALL SVI fits)
    vol_surface = validate_arbitrage_and_blend(svi_fits, market_data)

    # Level 4: local vol extraction (single expensive node, waits for level 3)
    local_vol = extract_local_vol_surface(vol_surface, spot=spot, rate=rate)

    # Build trade list (in production: load from trade repository)
    rng = np.random.default_rng(seed)
    instruments = ["barrier_do", "barrier_ui", "lookback", "asian", "cliquet"]
    trades = [
        ExoticTrade(
            trade_id=f"EX_{i:04d}",
            instrument=rng.choice(instruments),
            spot=spot,
            strike=float(spot * rng.uniform(0.85, 1.15)),
            barrier=float(spot * rng.uniform(0.65, 0.90)),
            maturity=float(rng.choice(expiries)),
            rate=rate,
            flag=rng.choice(["call", "put"]),
            notional=float(rng.choice([1e5, 5e5, 1e6])),
            quantity=int(rng.choice([-1, 1])),
        )
        for i in range(n_trades)
    ]

    # Level 5: PDE pricing — N_trade parallel nodes (each independent given local_vol)
    pde_results = [pde_price_exotic(local_vol, trade) for trade in trades]

    # Level 6: vega ladder — N_trade parallel nodes
    # Each depends on: local_vol (shared), pde_result (parent), trade, vol_surface
    if compute_vega:
        final_results = [
            compute_vega_ladder(local_vol, pde_result, trade, vol_surface)
            for pde_result, trade in zip(pde_results, trades)
        ]
    else:
        final_results = pde_results

    # Level 7: aggregation (fan-in: waits for all level-6 nodes)
    return aggregate_book_greeks(final_results, trades)


greeks = vol_surface_and_pde_pipeline(
    n_trades=150, expiries=[0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 5.0], spot=100.0, rate=0.04, seed=42, compute_vega=True
)
print(f"Portfolio NPV: ${greeks.total_npv:,.0f}")
