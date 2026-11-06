# Route preview 0.3.0

Apply this incremental patch after RoutePreview_CommonCheckers.patch, from the RunNMS root:

    git apply RoutePreview_Step3_PNG.patch

Install the rendering dependency using the Python interpreter selected in VS Code:

    python -m pip install -r sitemap/CommonCheckers/requirements_route_preview.txt

Open sitemap/CommonCheckers/route_reconstruct.py. Edit the parameters under
`if __name__ == "__main__":` and click Run Python File. No CLI parameters are needed.
Check COMMANDS_CSV, INITIAL_POSES_CSV and MAP_WORKBOOK against your actual filenames.
All default paths resolve relative to this Python file, independently of working directory.

Outputs in route_preview:
- reconstruction.json, diagnostics.json, route_points.csv: existing parser outputs.
- routes.png: all robots; labels every fifth movement by default.
- route_01_*.png etc.: one zoomed view per robot, all destinations labelled.
- render_manifest.json: input hashes, calibration, output names, completion status.
- render_diagnostics.json: map loading / fill interpretation issues.

Labels show command-issue offsets, not measured arrival times. Stars show intended
initial positions, open squares the last known destination, arrows commanded direction.
Repeated destinations can have overlapping annotations; route_points.csv retains every row.
Unknown incoming segments are omitted, with diagnostics retained. Missing map data produces
an explicitly incomplete preview. Inputs are never corrected or modified.

StaticMap defaults: C4:DL117, 0.1 m cells; C117 covers x=[0,0.1], y=[0,0.1].
Stored white/red/yellow fills mean free/restricted/bump; unknown fills are magenta and reported.
This renderer does not evaluate clearance, collisions, timing policy, or actual robot motion.
Manual exclusions not stored in StaticMap are not added. Safety remains a separate checker.

Importable API (Python 3.10+):

    from sitemap.CommonCheckers.route_reconstruct import run
    result = run(commands, initial_poses, policy, output_dir,
                 map_workbook=workbook, dpi=160, per_robot=True,
                 overview_label_every=5)

Omit map_workbook for the original parse-only behavior. Rendering can also be called through
sitemap.CommonCheckers.route_render.render_routes(result, workbook, output_dir).
