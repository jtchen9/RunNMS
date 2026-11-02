# Concurrent movement validation

The workbook launcher and NMS registration both call checker_runner.validate_script.
Deploy the same CommonCheckers directory to both environments; restart the NMS
process after updating (Python caches imported modules). No VBA, workbook, robot,
or m4Commands.py change is required.

Rules:
- Commands for the same robot require 180 seconds between scheduled starts.
- Every pair of different robots' moving commands less than 180 seconds apart
  requires at least 1.0 m between their entire planned center paths, endpoints
  included. Exact 1.0 m passes; exact 180 seconds permits sequential scheduling.
- The checker makes no assumptions about relative speeds or start order.
- Straight mobility.move paths and the site's straight ramp macro paths are
  supported, after reserved launch constants are resolved by the existing runner.
- Unknown/missing/non-finite path geometry does not grant a concurrency exemption.
- Existing stationary robot, map, charging-zone, macro, and first-report checks
  remain active. Waiting 180 seconds does not permit collision with a parked robot.

The existing global_min_seconds_between_moving_mobility_commands policy key now
means the conservative overlap window and same-robot gap. Its value remains 180.
concurrent_movement_min_path_separation_m is 1.0. Distances are center-to-center,
not wheel-edge clearance. All pairs in the window are tested, not only adjacent
CSV rows. No rows are automatically rescheduled.

This is planned-script validation, not a runtime movement reservation system.
It cannot guarantee separation if a robot deviates from its planned path or takes
longer than 180 seconds. Existing runtime safety/stopping rules remain necessary.

Validation commands (from the NMS repository root):

    python -m unittest discover -s sitemap/CommonCheckers/tests -p test_concurrent_movement_rules.py -v
    python -m unittest discover -s sitemap/CommonCheckers/tests -v

Patch verification: 9 new concurrency tests passed. The uploaded baseline's 45
existing tests have two pre-existing ramp-related failures, reproduced unchanged:
- test_preflight_requires_exact_launch_but_runtime_uses_axis_tolerances
- test_valid_constant_is_resolved_then_full_validation_passes

The supplied workbook launcher and the registration validation adapter were also
executed against identical synthetic inputs: both accepted parallel paths at 1 m,
both rejected paths at 0.9 m with simultaneous starts, and both accepted those
0.9 m paths with starts 180 seconds apart. Furniture was excluded in these
synthetic adapter tests; they do not validate a particular DemoRoom route.
