"""
location_items.py

Part 3 of the location solver:
Convert each tag observation into independent measurement items.

Design goal
-----------
A single tag observation can produce up to three independent items:

    distance item
    angle item
    yaw item

This allows item-level filtering later. For example:

    distance: keep
    angle: keep
    yaw: disable

without rejecting the whole tag.

This module contains reusable production-evolving logic only.
It does not read CSV files, does not write output files, and does not use pandas.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Mapping, Any, Iterable, List, Optional


@dataclass(frozen=True)
class TagObservation:
    """
    One observed tag from one camera.

    distance_m:
        measured camera-to-tag distance

    angle_deg:
        measured view angle using the project convention:
            angle_deg = camera_heading_deg - bearing_camera_to_tag_deg

    yaw_deg:
        measured yaw value from AprilTag observation / probe code
    """
    tag_id: int
    camera_role: str
    distance_m: float
    angle_deg: float
    yaw_deg: float
    source: str = ""


@dataclass
class MeasurementItem:
    """
    One independent measurement item used by the future joint solver.

    kind:
        "distance", "angle", or "yaw"

    value:
        measured value. Unit depends on kind:
            distance: meters
            angle: degrees
            yaw: degrees

    sigma:
        residual normalization scale for future solver:
            distance: meters
            angle/yaw: degrees

    base_weight:
        camera/type-dependent base weight

    enabled:
        whether this item participates in solving

    weight_multiplier:
        later filtering may reduce this, e.g. 1.0 -> 0.25

    state:
        "USED_NORMAL", "USED_LOW_WEIGHT", or "DISABLED"

    reason:
        human-readable reason for current state
    """
    tag_id: int
    camera_role: str
    kind: str
    value: float
    sigma: float
    base_weight: float
    enabled: bool = True
    weight_multiplier: float = 1.0
    state: str = "USED_NORMAL"
    reason: str = ""

    def effective_weight(self) -> float:
        if not self.enabled:
            return 0.0
        return float(self.base_weight) * float(self.weight_multiplier)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["effective_weight"] = self.effective_weight()
        return d


DEFAULT_ITEM_SIGMA = {
    "distance": 0.05,  # meters
    "angle": 3.0,     # degrees
    "yaw": 10.0,      # degrees
}


DEFAULT_MEASUREMENT_WEIGHTS = {
    "front": {
        "distance": 1.0,
        "angle": 0.5,
        "yaw": 0.05,
    },
    "rear": {
        "distance": 0.5,
        "angle": 0.2,
        "yaw": 0.01,
    },
}


def normalize_camera_role(camera_role: str) -> str:
    return str(camera_role).strip().lower()


def make_tag_observation(
    tag_id: int,
    camera_role: str,
    distance_m: float,
    angle_deg: float,
    yaw_deg: float,
    source: str = "",
) -> TagObservation:
    """
    Small helper to create a normalized TagObservation.
    """
    return TagObservation(
        tag_id=int(tag_id),
        camera_role=normalize_camera_role(camera_role),
        distance_m=float(distance_m),
        angle_deg=float(angle_deg),
        yaw_deg=float(yaw_deg),
        source=str(source),
    )


def build_measurement_items_for_observation(
    obs: TagObservation,
    measurement_weights: Mapping[str, Mapping[str, float]] | None = None,
    item_sigma: Mapping[str, float] | None = None,
    use_distance: bool = True,
    use_angle: bool = True,
    use_yaw: bool = True,
) -> List[MeasurementItem]:
    """
    Convert one tag observation into independent measurement items.

    This function does not decide whether the observation is geometrically good.
    It only creates items with configured base weights and sigmas.
    """
    measurement_weights = measurement_weights or DEFAULT_MEASUREMENT_WEIGHTS
    item_sigma = item_sigma or DEFAULT_ITEM_SIGMA

    camera_role = normalize_camera_role(obs.camera_role)

    if camera_role not in measurement_weights:
        raise ValueError(f"Unknown camera_role={camera_role!r}")

    cam_w = measurement_weights[camera_role]
    items: List[MeasurementItem] = []

    if use_distance and cam_w.get("distance", 0.0) > 0.0:
        items.append(
            MeasurementItem(
                tag_id=obs.tag_id,
                camera_role=camera_role,
                kind="distance",
                value=float(obs.distance_m),
                sigma=float(item_sigma["distance"]),
                base_weight=float(cam_w["distance"]),
            )
        )

    if use_angle and cam_w.get("angle", 0.0) > 0.0:
        items.append(
            MeasurementItem(
                tag_id=obs.tag_id,
                camera_role=camera_role,
                kind="angle",
                value=float(obs.angle_deg),
                sigma=float(item_sigma["angle"]),
                base_weight=float(cam_w["angle"]),
            )
        )

    if use_yaw and cam_w.get("yaw", 0.0) > 0.0:
        items.append(
            MeasurementItem(
                tag_id=obs.tag_id,
                camera_role=camera_role,
                kind="yaw",
                value=float(obs.yaw_deg),
                sigma=float(item_sigma["yaw"]),
                base_weight=float(cam_w["yaw"]),
            )
        )

    return items


def build_measurement_items(
    observations: Iterable[TagObservation],
    measurement_weights: Mapping[str, Mapping[str, float]] | None = None,
    item_sigma: Mapping[str, float] | None = None,
    mode: str = "distance_angle_yaw",
) -> List[MeasurementItem]:
    """
    Convert many tag observations into measurement items.

    Supported modes:
        "distance_only"
        "distance_angle"
        "distance_angle_yaw"
    """
    if mode not in {"distance_only", "distance_angle", "distance_angle_yaw"}:
        raise ValueError(f"Unsupported mode={mode!r}")

    use_distance = True
    use_angle = mode in {"distance_angle", "distance_angle_yaw"}
    use_yaw = mode == "distance_angle_yaw"

    all_items: List[MeasurementItem] = []
    for obs in observations:
        all_items.extend(
            build_measurement_items_for_observation(
                obs=obs,
                measurement_weights=measurement_weights,
                item_sigma=item_sigma,
                use_distance=use_distance,
                use_angle=use_angle,
                use_yaw=use_yaw,
            )
        )
    return all_items


def count_items_by_kind(items: Iterable[MeasurementItem]) -> dict:
    """
    Count enabled items by kind.
    """
    counts = {
        "distance": 0,
        "angle": 0,
        "yaw": 0,
        "total": 0,
    }
    for item in items:
        if not item.enabled:
            continue
        if item.kind not in counts:
            counts[item.kind] = 0
        counts[item.kind] += 1
        counts["total"] += 1
    return counts


def count_items_by_camera_and_kind(items: Iterable[MeasurementItem]) -> dict:
    """
    Count enabled items by camera and kind.
    """
    counts: dict[str, dict[str, int]] = {}
    for item in items:
        if not item.enabled:
            continue
        cam = item.camera_role
        if cam not in counts:
            counts[cam] = {"distance": 0, "angle": 0, "yaw": 0, "total": 0}
        counts[cam][item.kind] += 1
        counts[cam]["total"] += 1
    return counts


def disable_item(item: MeasurementItem, reason: str) -> None:
    """
    Disable one item in place.

    This will be used later by item-level residual filtering.
    """
    item.enabled = False
    item.weight_multiplier = 0.0
    item.state = "DISABLED"
    item.reason = reason


def set_item_low_weight(item: MeasurementItem, multiplier: float, reason: str) -> None:
    """
    Keep one item enabled but reduce its weight.

    This will be used later when an item is noisy but useful for diversity.
    """
    item.enabled = True
    item.weight_multiplier = float(multiplier)
    item.state = "USED_LOW_WEIGHT"
    item.reason = reason
