"""
Same-burst criteria for notices from *different* facility families.

Two events are the same burst when either rule holds:

A. **Time** — trigger times within ``time_window_sec`` and positions that do not
   contradict:  ``sep <= pos_sigma * sqrt(s1² + s2²)``.
B. **Position** — both localized to ``<= precise_deg`` and agreeing the same way,
   within ``pos_window_hours`` (an X-ray imager such as EP can trigger on the
   same source well after the gamma-ray trigger).

``s_i = sqrt(max(err_i, err_floor_deg)² + sys_deg[family]²)`` — GBM notice errors
are "statistical only", so the family's systematic is added.

Defaults were fixed on the bot's own history (gcn_notices.csv, 2026-09-04 →
11-08, 86 events / 83 GRB-family): all 8 genuine cross-facility pairs found,
0 false links.

* time window 300 s — genuine pairs had |Δt| ≤ 143 s (6 of 8 ≤ 7 s), unrelated
  pairs ≥ 398 s. P(unrelated trigger within ±300 s) ≈ 0.9 % per event
  (1.3 events/day).
* GBM systematic 5° — offsets of GBM GND/FIN positions from SVOM/Swift
  positions imply a systematic of 2.8° (median) / 5.1° (68 %).
* pos_sigma 3 — the hardest genuine pair (GRB 261108A, GBM ground position
  27.4° off with err 12.5°) needs 2.03; 8 % of unrelated pairs pass the check,
  so expected false links ≈ 0.9 % × 8 % ≈ 0.3 per year.
* precise 0.5° / 24 h — no false link even with a 30-day window; 0.5° admits
  ECLAIRs/EP/MXT/XRT and excludes GBM.
* circular links ≤ 6 h — genuine pairs ≤ 1.15 h apart; circulars that cite a
  *different* burst did so 35.5 h, 39.2 h and 94.7 h later.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, Mapping, Optional

import pandas as pd

GRB_FAMILIES = ("Fermi", "SVOM", "Swift", "EinsteinProbe", "CALET")


@dataclass(frozen=True)
class LinkParams:
    time_window_sec: float = 300.0
    pos_sigma: float = 3.0
    sys_deg: Mapping[str, float] = field(default_factory=lambda: {"Fermi": 5.0})
    err_floor_deg: float = 0.05
    precise_deg: float = 0.5
    pos_window_hours: float = 24.0
    circular_max_dt_hours: float = 6.0
    families: FrozenSet[str] = frozenset(GRB_FAMILIES)

    @classmethod
    def from_config(cls, config: Any) -> "LinkParams":
        return cls(
            time_window_sec=config.same_event_time_window_sec,
            pos_sigma=config.same_event_pos_sigma,
            sys_deg=dict(config.same_event_sys_deg),
            err_floor_deg=config.same_event_err_floor_deg,
            precise_deg=config.same_event_precise_deg,
            pos_window_hours=config.same_event_pos_window_hours,
            circular_max_dt_hours=config.same_event_circular_max_dt_hours,
            families=frozenset(config.same_event_families),
        )


def to_utc(value: Any) -> Optional[pd.Timestamp]:
    """Parse a naive-UTC / aware datetime or string; ``None`` if unusable."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    t = pd.to_datetime(str(value).strip().strip('"'), errors="coerce", utc=True)
    return None if pd.isna(t) else t


def to_float(value: Any) -> Optional[float]:
    try:
        f = float(str(value).strip().strip('"'))
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else f


def angular_sep(ra1: float, dec1: float, ra2: float, dec2: float) -> float:
    """Great-circle separation in degrees (haversine)."""
    r1, d1, r2, d2 = map(math.radians, [ra1, dec1, ra2, dec2])
    a = (math.sin((d2 - d1) / 2) ** 2
         + math.cos(d1) * math.cos(d2) * math.sin((r2 - r1) / 2) ** 2)
    return math.degrees(2 * math.asin(math.sqrt(min(a, 1.0))))


def sigma(err: Optional[float], family: str, p: LinkParams) -> float:
    """Localization sigma used for linking: error floor + family systematic."""
    e = max(err or 0.0, p.err_floor_deg)
    return math.sqrt(e ** 2 + float(p.sys_deg.get(family, 0.0)) ** 2)


def position_check(a: Dict[str, Any], b: Dict[str, Any], p: LinkParams) -> Optional[tuple]:
    """``(consistent, sep, tol)`` for two ``{family, ra, dec, err}`` dicts, or
    ``None`` when either position is unknown."""
    if None in (a.get("ra"), a.get("dec"), b.get("ra"), b.get("dec")):
        return None
    sep = angular_sep(a["ra"], a["dec"], b["ra"], b["dec"])
    tol = p.pos_sigma * math.hypot(sigma(a.get("err"), a["family"], p),
                                   sigma(b.get("err"), b["family"], p))
    return sep <= tol, sep, tol


def evaluate(new: Dict[str, Any], cand: Dict[str, Any], p: LinkParams) -> Optional[Dict[str, Any]]:
    """Apply rules A/B to two events ``{family, t, ra, dec, err}``.

    Returns ``{rule, dt_s, sep_deg, tol_deg}`` when they are the same burst, a
    dict with ``rule=None`` for a near miss (trigger times coincide but the
    positions contradict — logged, never linked), else ``None``.
    """
    if new["family"] == cand["family"]:
        return None
    if new["family"] not in p.families or cand["family"] not in p.families:
        return None
    if new.get("t") is None or cand.get("t") is None:
        return None
    pos = position_check(new, cand, p)
    if pos is None:
        return None
    ok, sep, tol = pos
    dt = (new["t"] - cand["t"]).total_seconds()
    out = {"dt_s": dt, "sep_deg": sep, "tol_deg": tol}
    if abs(dt) <= p.time_window_sec:
        return {**out, "rule": "time" if ok else None}
    precise = max(new.get("err") or 0.0, cand.get("err") or 0.0) <= p.precise_deg
    if ok and precise and abs(dt) <= p.pos_window_hours * 3600:
        return {**out, "rule": "position"}
    return None


def family_of(facility: Any) -> str:
    """Collapse an instrument name to its mission family ("SVOM-MXT" → "SVOM")."""
    fac = str(facility or "").strip()
    if not fac or fac.lower() in ("nan", "none"):
        return ""
    if "swift" in fac.lower():
        return "Swift"
    if any(x in fac for x in ("Fermi", "GBM", "LAT")):
        return "Fermi"
    if "GECAM" in fac:
        return "GECAM"
    if "SVOM" in fac:
        return "SVOM"
    if "Einstein" in fac or "EP" in fac:
        return "EinsteinProbe"
    if "IceCube" in fac or "ICECUBE" in fac:
        return "IceCube"
    return fac


def name_prefix(name: Any) -> str:
    """``"GRB"`` / ``"EP"`` / ``"IceCube"`` for a bot or GCN event name."""
    s = str(name or "").strip().upper()
    for prefix in ("GRB", "EP", "ICECUBE"):
        if s.startswith(prefix):
            return {"ICECUBE": "IceCube"}.get(prefix, prefix)
    return ""


def facility_prefix(family: str) -> str:
    """Name prefix the bot generates for a facility family (see _generate_name)."""
    if family == "EinsteinProbe":
        return "EP"
    if family in ("IceCube", "AMON"):
        return "IceCube"
    return "GRB"
