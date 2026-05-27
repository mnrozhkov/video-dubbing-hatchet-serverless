"""Nebius preset → USD/hour price table + cost-estimation helpers.

Rates match the published prices at https://nebius.com/prices (checked 2026-05-27).
GPU prices are per GPU-hour; CPU prices are per instance-hour.

Lookup key: ``(platform, preset, preemptible)``. Preemptible prices apply only
when the job actually ran on preemptible capacity. On-demand cost for the same
wall time is computed via ``on_demand_estimate()`` (used in savings rollups).
"""

from __future__ import annotations

# Rates in USD/hour, with 450 GiB boot disk.  Source: https://nebius.com/prices, checked 2026-05-27.
PRICE_PER_MIN_USD: dict[tuple[str, str, bool], float] = {
    # CPU
    ("cpu-e2", "4vcpu-16gb",  False): 0.16,
    ("cpu-e2", "8vcpu-32gb",  False): 0.26,
    # GPU L40S with Intel CPU: from $0.90 preemptible / $1.82 on-demand per GPU-hour
    ("gpu-l40s-d", "1gpu-16vcpu-96gb", True):  0.94,
    ("gpu-l40s-d", "1gpu-16vcpu-96gb", False): 1.87,
    # GPU H100 HGX SXM: $1.25 preemptible / $2.95 on-demand per GPU-hour
    ("gpu-h100-sxm", "1gpu-16vcpu-200gb", True):  1.3,
    ("gpu-h100-sxm", "1gpu-16vcpu-200gb", False): 3.00,
    # GPU H200 HGX SXM: $1.45 preemptible / $3.50 on-demand per GPU-hour
    ("gpu-h200-sxm", "1gpu-16vcpu-200gb", True):  1.50,
    ("gpu-h200-sxm", "1gpu-16vcpu-200gb", False): 3.55,
}


def _lookup(platform: str, preset: str, preemptible: bool) -> float:
    rate = PRICE_PER_MIN_USD.get((platform, preset, preemptible))
    if rate is None:
        # Unknown combo → return 0 rather than blow up; surfaces as "n/a" in summary.
        return 0.0
    return rate


def estimate_cost(platform: str, preset: str, preemptible: bool, run_s: float) -> float:
    """USD estimate for a job that ran ``run_s`` seconds on the given machine."""
    if run_s <= 0:
        return 0.0
    return _lookup(platform, preset, preemptible) * (run_s / 3600.0)


def on_demand_estimate(platform: str, preset: str, run_s: float) -> float:
    """What the same wall time would have cost at on-demand rates.

    Used by ``write_run_summary`` to quantify the savings from running on
    preemptible capacity.
    """
    if run_s <= 0:
        return 0.0
    return _lookup(platform, preset, preemptible=False) * (run_s / 3600.0)
