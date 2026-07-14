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

- P1 command vocabulary
- P2 first mobility command must be `mobility.report.location`
- intended initial pose table exists for each mobility robot
- P3 no two moving mobility commands within 180 seconds globally

Later versions will add macro/bump guard zones, path simulation, and robot-to-robot checks.
