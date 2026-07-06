"""
location_item_filter.py

Part 7 of the location solver:
Item-level residual filtering.

Purpose
-------
After a first joint solve, inspect residuals item by item:

    distance item
    angle item
    yaw item

Then decide for each item:

    USED_NORMAL
    USED_LOW_WEIGHT
    DISABLED

Important design rule
---------------------
Do not reject the whole tag just because one measurement item is bad.

Example:
    distance good
    angle good
    yaw bad

Action:
    keep distance
    keep angle
    disable yaw

This module only performs residual classification/filter decision.
It does not solve pose and does not read/write files.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Iterable


@dataclass(frozen=True)
class ItemFilterConfig:
    # Residual classification thresholds.
    # Distance units: meters.
    distance_good_m: float = 0.08
    distance_weak_m: float = 0.20
    distance_severe_m: float = 0.35

    # Angle/yaw units: degrees.
    angle_good_deg: float = 5.0
    angle_weak_deg: float = 12.0
    angle_severe_deg: float = 20.0

    yaw_good_deg: float = 10.0
    yaw_weak_deg: float = 25.0
    yaw_severe_deg: float = 45.0

    # Minimum diversity/information to preserve.
    min_distance_items_to_keep: int = 3
    min_angle_items_to_keep: int = 2

    # Weight multipliers.
    weak_multiplier: float = 0.50
    bad_but_needed_multiplier: float = 0.25
    yaw_weak_multiplier: float = 0.25


@dataclass
class ItemFilterState:
    tag_id: int
    camera_role: str
    kind: str

    residual: float
    abs_residual: float
    classification: str

    enabled: bool
    weight_multiplier: float
    state: str
    reason: str

    def key(self) -> tuple[int, str, str]:
        return (int(self.tag_id), str(self.camera_role), str(self.kind))

    def to_dict(self) -> dict:
        return asdict(self)


def _classify_distance(abs_residual_m: float, cfg: ItemFilterConfig) -> str:
    if abs_residual_m <= cfg.distance_good_m:
        return "GOOD"
    if abs_residual_m <= cfg.distance_weak_m:
        return "WEAK"
    if abs_residual_m <= cfg.distance_severe_m:
        return "BAD"
    return "SEVERE"


def _classify_angle(abs_residual_deg: float, cfg: ItemFilterConfig) -> str:
    if abs_residual_deg <= cfg.angle_good_deg:
        return "GOOD"
    if abs_residual_deg <= cfg.angle_weak_deg:
        return "WEAK"
    if abs_residual_deg <= cfg.angle_severe_deg:
        return "BAD"
    return "SEVERE"


def _classify_yaw(abs_residual_deg: float, cfg: ItemFilterConfig) -> str:
    if abs_residual_deg <= cfg.yaw_good_deg:
        return "GOOD"
    if abs_residual_deg <= cfg.yaw_weak_deg:
        return "WEAK"
    if abs_residual_deg <= cfg.yaw_severe_deg:
        return "BAD"
    return "SEVERE"


def _make_state(
    tag_id: int,
    camera_role: str,
    kind: str,
    residual: float,
    classification: str,
    enabled: bool,
    weight_multiplier: float,
    state: str,
    reason: str,
) -> ItemFilterState:
    return ItemFilterState(
        tag_id=int(tag_id),
        camera_role=str(camera_role).strip().lower(),
        kind=str(kind),
        residual=float(residual),
        abs_residual=abs(float(residual)),
        classification=classification,
        enabled=enabled,
        weight_multiplier=float(weight_multiplier),
        state=state,
        reason=reason,
    )


def build_filter_states_from_joint_yaw_residuals(
    residual_records,
    cfg: ItemFilterConfig | None = None,
) -> dict[tuple[int, str, str], ItemFilterState]:
    """
    Build item filter states from Part 6 residual records.

    residual_records are expected to have fields:
        tag_id, camera_role
        distance_residual_m
        angle_residual_deg
        yaw_residual_deg
    """
    cfg = cfg or ItemFilterConfig()

    # First classify every candidate item.
    candidates: list[ItemFilterState] = []

    for r in residual_records:
        # Distance item.
        d_abs = abs(float(r.distance_residual_m))
        d_cls = _classify_distance(d_abs, cfg)
        candidates.append(
            _make_state(
                tag_id=r.tag_id,
                camera_role=r.camera_role,
                kind="distance",
                residual=float(r.distance_residual_m),
                classification=d_cls,
                enabled=True,
                weight_multiplier=1.0,
                state="USED_NORMAL",
                reason="initial",
            )
        )

        # Angle item.
        a_abs = abs(float(r.angle_residual_deg))
        a_cls = _classify_angle(a_abs, cfg)
        candidates.append(
            _make_state(
                tag_id=r.tag_id,
                camera_role=r.camera_role,
                kind="angle",
                residual=float(r.angle_residual_deg),
                classification=a_cls,
                enabled=True,
                weight_multiplier=1.0,
                state="USED_NORMAL",
                reason="initial",
            )
        )

        # Yaw item.
        y_abs = abs(float(r.yaw_residual_deg))
        y_cls = _classify_yaw(y_abs, cfg)
        candidates.append(
            _make_state(
                tag_id=r.tag_id,
                camera_role=r.camera_role,
                kind="yaw",
                residual=float(r.yaw_residual_deg),
                classification=y_cls,
                enabled=True,
                weight_multiplier=1.0,
                state="USED_NORMAL",
                reason="initial",
            )
        )

    # Decide distance/angle with minimum item preservation.
    states: list[ItemFilterState] = []

    for kind, min_keep in [
        ("distance", cfg.min_distance_items_to_keep),
        ("angle", cfg.min_angle_items_to_keep),
    ]:
        kind_items = [c for c in candidates if c.kind == kind]

        # Severe items are disabled first.
        non_severe = [c for c in kind_items if c.classification != "SEVERE"]
        severe = [c for c in kind_items if c.classification == "SEVERE"]

        for c in severe:
            states.append(
                _make_state(
                    c.tag_id, c.camera_role, c.kind, c.residual, c.classification,
                    enabled=False,
                    weight_multiplier=0.0,
                    state="DISABLED",
                    reason=f"severe_{kind}_outlier",
                )
            )

        # Among non-severe, GOOD normal, WEAK low, BAD either disabled or low
        # depending on whether we need it to preserve enough items.
        good_or_weak = [c for c in non_severe if c.classification in {"GOOD", "WEAK"}]
        bad = [c for c in non_severe if c.classification == "BAD"]

        keep_count_without_bad = len(good_or_weak)
        bad_needed_count = max(0, min_keep - keep_count_without_bad)

        # Keep the least-bad BAD items if needed.
        bad_sorted = sorted(bad, key=lambda c: c.abs_residual)
        bad_needed = set(c.key() for c in bad_sorted[:bad_needed_count])

        for c in good_or_weak:
            if c.classification == "GOOD":
                states.append(
                    _make_state(
                        c.tag_id, c.camera_role, c.kind, c.residual, c.classification,
                        enabled=True,
                        weight_multiplier=1.0,
                        state="USED_NORMAL",
                        reason=f"good_{kind}",
                    )
                )
            else:
                states.append(
                    _make_state(
                        c.tag_id, c.camera_role, c.kind, c.residual, c.classification,
                        enabled=True,
                        weight_multiplier=cfg.weak_multiplier,
                        state="USED_LOW_WEIGHT",
                        reason=f"weak_{kind}",
                    )
                )

        for c in bad_sorted:
            if c.key() in bad_needed:
                states.append(
                    _make_state(
                        c.tag_id, c.camera_role, c.kind, c.residual, c.classification,
                        enabled=True,
                        weight_multiplier=cfg.bad_but_needed_multiplier,
                        state="USED_LOW_WEIGHT",
                        reason=f"bad_{kind}_kept_for_diversity",
                    )
                )
            else:
                states.append(
                    _make_state(
                        c.tag_id, c.camera_role, c.kind, c.residual, c.classification,
                        enabled=False,
                        weight_multiplier=0.0,
                        state="DISABLED",
                        reason=f"bad_{kind}_outlier",
                    )
                )

    # Yaw is not needed for diversity. It is kept only if GOOD, weak if WEAK,
    # and disabled if BAD/SEVERE.
    for c in [x for x in candidates if x.kind == "yaw"]:
        if c.classification == "GOOD":
            states.append(
                _make_state(
                    c.tag_id, c.camera_role, c.kind, c.residual, c.classification,
                    enabled=True,
                    weight_multiplier=1.0,
                    state="USED_NORMAL",
                    reason="good_yaw",
                )
            )
        elif c.classification == "WEAK":
            states.append(
                _make_state(
                    c.tag_id, c.camera_role, c.kind, c.residual, c.classification,
                    enabled=True,
                    weight_multiplier=cfg.yaw_weak_multiplier,
                    state="USED_LOW_WEIGHT",
                    reason="weak_yaw",
                )
            )
        else:
            states.append(
                _make_state(
                    c.tag_id, c.camera_role, c.kind, c.residual, c.classification,
                    enabled=False,
                    weight_multiplier=0.0,
                    state="DISABLED",
                    reason=f"{c.classification.lower()}_yaw_outlier",
                )
            )

    return {s.key(): s for s in states}


def count_filter_states(states: Iterable[ItemFilterState]) -> dict:
    out = {
        "total": 0,
        "enabled": 0,
        "disabled": 0,
        "used_normal": 0,
        "used_low_weight": 0,
        "distance_enabled": 0,
        "angle_enabled": 0,
        "yaw_enabled": 0,
        "distance_disabled": 0,
        "angle_disabled": 0,
        "yaw_disabled": 0,
    }

    for s in states:
        out["total"] += 1
        if s.enabled:
            out["enabled"] += 1
            out[f"{s.kind}_enabled"] += 1
            if s.state == "USED_NORMAL":
                out["used_normal"] += 1
            elif s.state == "USED_LOW_WEIGHT":
                out["used_low_weight"] += 1
        else:
            out["disabled"] += 1
            out[f"{s.kind}_disabled"] += 1

    return out
