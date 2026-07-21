# AutoLab Script Authoring Checker

This package contains the common offline checker shared by all AutoLab sites.

## VS Code workflow

Open:

```text
sitemap/CommonCheckers/checker/checker_runner.py
```

At the bottom, edit the paths inside:

```python
def run_from_vscode() -> Dict[str, Any]:
```

For example:

```python
SCRIPT_CSV = Path(r"sitemap\DemoRoom\script_authoring\examples\demo_safe_script.csv")
INITIAL_POSES_CSV = Path(r"sitemap\DemoRoom\script_authoring\examples\demo_initial_poses.csv")
SITE_DIR = Path(r"sitemap\DemoRoom")
COMMON_DIR = Path(r"sitemap\CommonCheckers")
REPORT_JSON = Path(r"validation_report.json")
```

Then press the VS Code **Run** button.

No PowerShell command-line arguments are required.

## Current checker scope

The checker currently validates:

- command vocabulary
- blocked low-level mobility actions
- each participating robot's first mobility command must be `mobility.report.location`
- intended initial pose table exists for each mobility robot
- first movement for each robot must be at least 60 seconds after its `mobility.report.location`
- no two moving mobility commands may occur within 180 seconds globally, across all robots
- each script-level `mobility.move` must be no longer than 3.0 m
- macro/bump guard rules
- planned static path rules, including workspace bounds, charging/rest zones, restriction_map.npy, and planned robot clearance

## Script-level mobility commands

Script writers should use semantic commands only:

```text
mobility.report.location
mobility.move
mobility.in2out
mobility.out2in
```

Do not put low-level robot commands in a script:

```text
mobility.turn
mobility.turn_move_turn.forward
mobility.turn_move_turn.backward
```

The NMS mobility policy turns semantic movement into the final robot command.

## Timing rules

During bring-up, timing is strict:

```text
first movement after mobility.report.location: at least 60 seconds
between any two moving mobility commands globally: at least 180 seconds
```

The global 180-second rule applies across all robots, not only within one robot's rows.

## Movement distance rule

Each script-level `mobility.move` is limited to 3.0 m.

For a longer route, split the route into multiple shorter `mobility.move` rows and keep at least 180 seconds between movement rows.


## Robot safety radius synchronization

DemoRoom robot-to-robot clearance is configured in:

```text
sitemap/DemoRoom/script_authoring/config/safety_policy.json
```

The field is:

```json
"robot_safety_radius_m": 0.60
```

CommonCheckers uses this value for offline planned robot-to-robot clearance.

Runtime S5 dynamic obstacle checking uses the matching runtime copy in `config.py`:

```python
MOBILITY_ROBOT_RESTRICT_RADIUS_M = 0.60
```

For DemoRoom, keep both values synchronized. If one changes, update the other in the same commit.

## Initial pose file reminder

`SCRIPT_CSV` and `INITIAL_POSES_CSV` are two different files.

Use:

```python
SCRIPT_CSV = SITEMAP_DIR / "DemoRoom" / "script_authoring" / "examples" / "demo_safe_script.csv"
INITIAL_POSES_CSV = SITEMAP_DIR / "DemoRoom" / "script_authoring" / "examples" / "demo_initial_poses.csv"
```

Do not point `INITIAL_POSES_CSV` to a command script such as `demo_safe_script.csv` or `demo_invalid_timing.csv`.
