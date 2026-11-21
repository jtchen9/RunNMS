"""Bridge for the CommandSheet preview button; never registers an experiment."""
import argparse
import html
import json
from pathlib import Path
import sys
import webbrowser
import tempfile


def main():
    parser = argparse.ArgumentParser()
    for name in ("root", "workbook", "job"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--no-open", action="store_true")
    args = parser.parse_args()
    root, workbook, job = map(Path, (args.root, args.workbook, args.job))
    job = job.resolve()
    sys.path.insert(0, str(root.resolve() / "sitemap" / "CommonCheckers"))
    try:
        from route_reconstruct import run
        from checker.registration_csv import parse_registration_csv
        from single_csv_authoring import planned_poses_bytes
        manifest = parse_registration_csv((job / "experiment_script.csv").read_bytes())
        with tempfile.TemporaryDirectory(prefix="autolab_preview_") as folder:
            poses = Path(folder) / "renderer_poses.csv"
            poses.write_bytes(planned_poses_bytes(manifest))
            result = run(
                commands=job / "experiment_script.csv",
                initial_poses=poses,
                policy=None,
                output_dir=job / "images",
                macro_policy=root / "sitemap/DemoRoom/script_authoring/config/macro_policy.json",
                map_workbook=workbook,
                per_robot=True,
            )["integration_result"]
    except Exception as exc:
        result = {"status": "error", "png_files": [],
                  "diagnostics": [{"message": str(exc)}]}
    (job / "bridge_result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    esc = html.escape
    sections = []
    for record in sorted(result["png_files"], key=lambda r: (r["name"] != "routes.png", r["name"])):
        uri = esc(Path(record["path"]).resolve().as_uri(), quote=True)
        name = esc(record["name"])
        sections.append(f'<p><a href="{uri}" download>{name}</a></p>')
        if record["name"] == "routes.png":
            sections.append(f'<img style="max-width:100%" src="{uri}" alt="All robots planned routes">')
    diagnostics = esc(json.dumps(result.get("diagnostics", []), ensure_ascii=False, indent=2))
    page = job / "preview.html"
    page.write_text(
        '<!doctype html><meta charset="utf-8"><title>Planned route preview</title>'
        '<body style="font:16px sans-serif;margin:24px"><h1>Planned route preview</h1>'
        f'<p>Status: <strong>{esc(result["status"])}</strong></p>'
        '<p>Planned movement only. CommonCheckers safety validation remains separate.</p>'
        + "".join(sections) + f'<details><summary>Diagnostics</summary><pre>{diagnostics}</pre></details>',
        encoding="utf-8")
    if not args.no_open:
        webbrowser.open(page.as_uri())
    return 0 if result["status"] == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
