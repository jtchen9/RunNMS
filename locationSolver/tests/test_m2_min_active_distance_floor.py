from __future__ import annotations

from src.common.component_preparation import (
    M2DistanceScreenConfig,
    decide_distance_use_from_m2,
)


def _rows(values):
    return [
        {
            "observation_uid": f"o{i}",
            "distance_m2_signed": value,
        }
        for i, value in enumerate(values, start=1)
    ]


def test_all_six_candidates_keep_three():
    rows = _rows([1.31, -1.04, -1.56, -1.44, 1.39, 1.04])

    decisions = decide_distance_use_from_m2(
        component_rows=rows,
        config=M2DistanceScreenConfig(
            abs_m2_threshold=0.95,
            min_active_distances_after_m2=3,
        ),
    )

    active = sum(
        1
        for d in decisions.values()
        if d["distance_use"]
    )
    rejected = sum(
        1
        for d in decisions.values()
        if not d["distance_use"]
    )

    assert active == 3
    assert rejected == 3


def test_normal_case_unchanged_when_floor_not_binding():
    rows = _rows([0.20, 1.20, -0.30, 0.40, -1.10, 0.10])

    decisions = decide_distance_use_from_m2(
        component_rows=rows,
        config=M2DistanceScreenConfig(
            abs_m2_threshold=0.95,
            min_active_distances_after_m2=3,
        ),
    )

    rejected = [
        uid
        for uid, d in decisions.items()
        if not d["distance_use"]
    ]

    assert len(rejected) == 2


if __name__ == "__main__":
    test_all_six_candidates_keep_three()
    test_normal_case_unchanged_when_floor_not_binding()
    print("PASS: M2 minimum-active-distance safeguard")
