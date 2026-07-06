# Interactive validation collection with frozen Stage-5

Place:

`t11_collect_validation_stage5.py`

beside:

`t11_collect_observation_sample.py`

under `testLocation\`.

The new script imports and reuses the proven collector. It does not rewrite:
- Redis command sending
- report waiting
- AprilTag extraction
- measurement logging
- existing per-tag console table

After collection it runs the exact frozen Stage-5 configuration:

Measurement scales:
- distance sigma = 0.05 m
- angle sigma = 2.5 deg
- yaw sigma = 3.0 deg

Relative weights:
- distance = 1.0
- angle = 1.0
- yaw = 0.75

D/A outlier rule:
- threshold = 0.80
- min score gap = 0.50
- max rejections = 1
- min observations after rejection = 3

GT-free yaw rule:
- min abs predicted yaw = 8 deg
- best branch error <= 10 deg
- keep/flip branch separation >= 15 deg

Important:
- Stage-5 reads raw yaw only
- Stage-5 does not use offline yaw labels
- automatic KEEP/RETAKE uses observable acquisition quality only
- GT-vs-Stage5 difference is printed for human inspection only
- GT-vs-Stage5 difference is not a KEEP/RETAKE gate

The included `validation_points_v1.csv` is exactly the same point plan as the
previous validation package.
