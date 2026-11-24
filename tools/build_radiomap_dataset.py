"""NMS offline radiomap exporter. Edit __main__, then Run in VS Code.

Inputs: session-source ZIPs in one folder, a standard SVG, a documented
StaticMap XLSM and an x,y CSV. Python 3.10+ standard library only.
Does not connect to Redis, start NMS, execute VBA, or control robots.
"""
from __future__ import annotations

import csv
import io
import json
import math
import posixpath
import re
import shutil
import tempfile
import uuid
import zipfile
import hashlib
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

import radiomap_export_core as core

NS = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
SOURCE_SCHEMA = "datalake-radiomap-source-v1"
MAP_SCHEMA = "autolab-static-map-v1"
REQUIRED_SOURCE = {"CommandSheet.csv", "registration_meta.json", "session_meta.json",
                   "archive_meta.json", "preparation_events.jsonl", "frames.json",
                   "events.json", "measurements.json", "summary.json"}


@dataclass
class Settings:
    workspace: Path
    layout_svg: Path
    static_map_xlsm: Path
    grid_csv: Path
    output_zip: Path
    site_id: str
    survey_id: str
    expected_site: str
    expected_experiment: str
    survey_title: str
    layout_assertion: str
    survey_revision: int = 1
    revision_reason: str = "Initial export"
    ap_ids: tuple = ("AP1", "AP4", "AP5", "AP6")
    timezone_name: str = "Asia/Taipei"
    utc_offset_hours: float = 8
    settling_seconds: float = 45
    max_frame_age_seconds: float = 30
    max_robot_age_seconds: float = 30
    max_ap_age_seconds: float = 60
    max_target_error_m: float = 0.3
    tx_power_mode: str = "unknown"
    macro_endpoints: dict = field(default_factory=dict)


def safe_zip(z):
    names = z.namelist()
    if len(names) != len(set(names)):
        raise ValueError("ZIP contains duplicate member names")
    for name in names:
        p = PurePosixPath(name)
        if p.is_absolute() or ".." in p.parts or "\\" in name or ":" in name:
            raise ValueError(f"Unsafe ZIP member: {name}")


def source_bundle(path, cfg):
    """Validate the downloaded bundle before using any of its records."""
    with zipfile.ZipFile(path) as z:
        safe_zip(z)
        if "source_manifest.json" not in z.namelist():
            raise ValueError(f"{path.name}: use DataLake's Download dataset source button, not Event/Report download")
        manifest = json.loads(z.read("source_manifest.json"))
        if manifest.get("schema_version") != SOURCE_SCHEMA:
            raise ValueError(f"{path.name}: unsupported dataset-source schema")
        if manifest.get("site_id") != cfg.expected_site or manifest.get("experiment_id") != cfg.expected_experiment:
            raise ValueError(f"{path.name}: site/experiment differs from the configured selection")
        if not manifest.get("session_id") or not manifest.get("session_uid"):
            raise ValueError(f"{path.name}: missing session identity")
        listed = manifest.get("files", [])
        paths = [r["path"] for r in listed]
        if len(paths) != len(set(paths)) or not REQUIRED_SOURCE <= set(paths):
            raise ValueError(f"{path.name}: incomplete or duplicate manifest file list")
        if set(z.namelist()) != set(paths) | {"source_manifest.json"}:
            raise ValueError(f"{path.name}: ZIP members differ from its manifest")
        for r in listed:
            h, size = hashlib.sha256(), 0
            with z.open(r["path"]) as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    h.update(chunk); size += len(chunk)
            if size != r["bytes"] or h.hexdigest() != r["sha256"]:
                raise ValueError(f"{path.name}: hash/size mismatch for {r['path']}")
        if manifest.get("command_csv_path") != "CommandSheet.csv":
            raise ValueError("Unsupported embedded command CSV path")
        command = z.read("CommandSheet.csv")
        if hashlib.sha256(command).hexdigest() != manifest.get("command_csv_sha256"):
            raise ValueError("Embedded command CSV hash mismatch")
        summary = json.loads(z.read("summary.json"))
        archive = json.loads(z.read("archive_meta.json"))
        session = json.loads(z.read("session_meta.json"))
        registration = json.loads(z.read("registration_meta.json"))
        for label, data in [("summary", summary), ("archive", archive), ("session", session), ("registration", registration)]:
            for key in ("site_id", "experiment_id", "session_id", "session_uid"):
                if str(data.get(key)) != str(manifest.get(key)):
                    raise ValueError(f"{path.name}: conflicting {label} {key}")
        if manifest.get("archive_snapshot") != archive:
            raise ValueError("Source archive snapshot mismatch")
        for data in (session, registration):
            if data.get("registration_uid") != manifest.get("registration_uid"):
                raise ValueError("Registration UID mismatch")
            recorded_hash = data.get("public_validation", {}).get("script_hash")
            if recorded_hash and recorded_hash.removeprefix("sha256:") != manifest["command_csv_sha256"]:
                raise ValueError("Registration script hash differs from embedded CSV")
        for kind in ("frames", "events", "measurements"):
            records = json.loads(z.read(kind + ".json"))
            if not isinstance(records, list) or len(records) != manifest.get("counts", {}).get(kind):
                raise ValueError(f"{path.name}: {kind} count mismatch")
            for record in records:
                for key in ("site_id", "experiment_id", "session_id", "session_uid"):
                    if record.get(key) is not None and str(record[key]) != str(manifest[key]):
                        raise ValueError(f"{path.name}: conflicting {kind} record {key}")
        for line in z.read("preparation_events.jsonl").decode("utf-8-sig").splitlines():
            if not line.strip(): continue
            record = json.loads(line)
            for key in ("experiment_id", "session_id", "session_uid", "registration_uid"):
                if record.get(key) is not None and str(record[key]) != str(manifest[key]):
                    raise ValueError(f"{path.name}: conflicting preparation record {key}")
        return manifest, session, command


def cell_ref(ref):
    m = re.fullmatch(r"([A-Z]+)([1-9][0-9]*)", ref)
    if not m:
        raise ValueError(f"Invalid cell reference: {ref}")
    col = 0
    for c in m[1]: col = col * 26 + ord(c) - 64
    return col, int(m[2])


def read_static_map(path, site_id, expected_site):
    """Read the stored map and definition cells; never open Excel or run macros."""
    with zipfile.ZipFile(path) as z:
        safe_zip(z)
        book = ET.fromstring(z.read("xl/workbook.xml"))
        sheets = [s for s in book.find("s:sheets", NS) if s.get("name") == "StaticMap"]
        if len(sheets) != 1:
            raise ValueError("Workbook must contain a StaticMap sheet")
        rid = sheets[0].get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
        rels = {r.get("Id"): r.get("Target") for r in ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))}
        target = rels[rid]
        target = target.lstrip("/") if target.startswith("/") else posixpath.normpath("xl/" + target)
        tree = ET.fromstring(z.read(target))
        strings = []
        if "xl/sharedStrings.xml" in z.namelist():
            strings = ["".join(t.text or "" for t in si.findall(".//s:t", NS)) for si in ET.fromstring(z.read("xl/sharedStrings.xml"))]
        cells = {c.get("r"): c for c in tree.findall("s:sheetData/s:row/s:c", NS)}
        def value(c):
            if c is None: return ""
            if c.find("s:f", NS) is not None:
                raise ValueError(f"Use literal map definitions, not formulas, at {c.get('r')}")
            if c.get("t") == "inlineStr":
                return "".join(t.text or "" for t in c.findall(".//s:t", NS))
            v = c.findtext("s:v", "", NS)
            return strings[int(v)] if c.get("t") == "s" else v
        fields = "schema site_id map_range cell_size_m origin_cell origin_x_m origin_y_m x_direction y_direction free_fill_rgb restricted_fill_rgb bump_fill_rgb overlap_marker unknown_fill_policy layout_file grid_file svg_world_to_root map_cell_reference".split()
        definition = {}
        for ref, c in cells.items():
            if re.fullmatch(r"C[0-9]+", ref):
                key = value(c).strip()
                if key in fields:
                    if key in definition: raise ValueError(f"Duplicate StaticMap definition: {key}")
                    definition[key] = value(cells.get("S" + ref[1:])).strip()
        if set(definition) != set(fields) or definition.get("schema") != MAP_SCHEMA:
            raise ValueError("Use the revised StaticMap with the definition table (keys in C, values in S)")
        if definition["site_id"] != expected_site:
            raise ValueError("StaticMap site_id differs from the selected DataLake site")
        if (definition["x_direction"], definition["y_direction"], definition["map_cell_reference"], definition["unknown_fill_policy"]) != ("right", "up", "lower_left", "error"):
            raise ValueError("This version supports right/up axes, lower-left cell coordinates and error on unknown fills")
        first, last = definition["map_range"].split(":")
        c0, r0 = cell_ref(first); c1, r1 = cell_ref(last)
        if c1 < c0 or r1 < r0 or cell_ref(definition["origin_cell"]) != (c0, r1):
            raise ValueError("Invalid map range or origin_cell; origin must be the bottom-left cell")
        step, ox, oy = (core.numeric(definition[k]) for k in ("cell_size_m", "origin_x_m", "origin_y_m"))
        if step is None or step <= 0 or ox is None or oy is None:
            raise ValueError("Invalid StaticMap metre coordinates or cell size")
        colours = [definition[k].upper() for k in ("free_fill_rgb", "restricted_fill_rgb", "bump_fill_rgb")]
        if len(set(colours)) != 3 or not all(re.fullmatch(r"[0-9A-F]{6}", v) for v in colours):
            raise ValueError("StaticMap requires three distinct six-digit RGB colour codes")
        styles = ET.fromstring(z.read("xl/styles.xml"))
        fills, xfs = list(styles.find("s:fills", NS)), list(styles.find("s:cellXfs", NS))
        # Conditional colours can differ from stored fills; do not silently read the wrong layer.
        for cf in tree.findall("s:conditionalFormatting", NS):
            for area in cf.get("sqref", "").split():
                a, b = area.split(":") if ":" in area else (area, area)
                ca, ra = cell_ref(a); cb, rb = cell_ref(b)
                if ca <= c1 and cb >= c0 and ra <= r1 and rb >= r0:
                    raise ValueError("Conditional formatting overlaps the map; use stored solid RGB fills")
        by_position = {cell_ref(ref): cell for ref, cell in cells.items()}
        values = []
        for r in range(r0, r1 + 1):
            row = []
            for c in range(c0, c1 + 1):
                cell = by_position.get((c, r))
                fill = None if cell is None else fills[int(xfs[int(cell.get("s", "0"))].get("fillId", "0"))]
                pattern = None if fill is None else fill.find("s:patternFill", NS)
                fg = None if pattern is None else pattern.find("s:fgColor", NS)
                rgb = fg.get("rgb", "")[-6:].upper() if fg is not None and pattern.get("patternType") == "solid" else ""
                if rgb not in colours:
                    raise ValueError(f"Unknown StaticMap fill at column {c}, row {r}; review the workbook")
                category = colours.index(rgb)
                if category == 1 and value(cell).strip() == definition["overlap_marker"]:
                    category = 3
                row.append(category)
            values.append(row)
    frame = site_id + ":local-metres-v1"
    restriction = {"coordinate_frame_id": frame, "source": "sources/site/StaticMap.xlsm", "source_sha256": core.digest(path),
        "sheet": "StaticMap", "cell_range": definition["map_range"], "role": "operator-supplied planning restrictions, not physical walls",
        "origin_m": [ox, oy], "cell_size_m": [step, step], "rows": r1-r0+1, "columns": c1-c0+1,
        "cell_anchor": "lower_left", "array_order": "row_major", "row_direction": "decreasing_y", "column_direction": "increasing_x",
        "cell_coordinate_rule": f"x={ox}+column_index*{step}; y={oy}+({r1-r0}-row_index)*{step}; zero-based indices",
        "category_definitions": {"0": "free_by_map_not_a_safety_guarantee", "1": "static_restricted", "2": "bump_guard", "3": "static_restricted_and_bump_guard"},
        "color_encoding": {colours[0]: 0, colours[1]: 1, colours[2]: 2, colours[1]+"_with_plus_cell_text": 3},
        "values": values, "dynamic_overlays": [], "dynamic_overlay_status": "not_recorded"}
    return definition, restriction


def build_site(cfg, destination):
    uuid.UUID(cfg.site_id)
    definition, restriction = read_static_map(cfg.static_map_xlsm, cfg.site_id, cfg.expected_site)
    svg = ET.parse(cfg.layout_svg).getroot()
    if svg.tag != "{http://www.w3.org/2000/svg}svg":
        raise ValueError("Layout must be a standard SVG with the SVG namespace")
    transform = [core.numeric(v.strip()) for v in definition["svg_world_to_root"].split(",")]
    if len(transform) != 6 or any(v is None for v in transform) or abs(transform[0]*transform[3]-transform[1]*transform[2]) < 1e-12:
        raise ValueError("Invalid world-to-SVG matrix in StaticMap")
    if definition["layout_file"] != Path(cfg.layout_svg).name or definition["grid_file"] != Path(cfg.grid_csv).name:
        raise ValueError("StaticMap layout_file/grid_file must match the selected raw filenames")
    ox, oy = restriction["origin_m"]; step = restriction["cell_size_m"][0]
    xmax, ymax = ox+restriction["columns"]*step, oy+restriction["rows"]*step
    with Path(cfg.grid_csv).open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != ["x", "y"]:
            raise ValueError("Grid CSV must have exactly two columns with headers x,y")
        grid, coordinates = [], set()
        for number, row in enumerate(reader, 2):
            if None in row: raise ValueError(f"Extra grid CSV field at row {number}")
            point = core.xy([row["x"], row["y"]]); key = core.coord_key(point)
            if key in coordinates: raise ValueError(f"Duplicate grid coordinate at row {number}: {point}")
            if not (ox <= point[0] < xmax and oy <= point[1] < ymax):
                raise ValueError(f"Grid point outside map extent at row {number}: {point}")
            coordinates.add(key)
            grid.append({"grid_id": core.identity("grid_", [cfg.site_id, key]), "x_m": point[0], "y_m": point[1],
                         "scan_target": 1, "room_id": "", "phase": "", "metadata_json": core.canonical({"source_row": number})})
    if not grid: raise ValueError("Grid CSV contains no points")
    aligned = all(abs((v-o)/step-round((v-o)/step)) < 1e-6 for p in coordinates for v, o in zip(p, (ox, oy)))
    restriction["restriction_version"] = core.identity("restriction_", restriction)
    destination.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(cfg.layout_svg, destination / "layout.svg")
    with (destination / "grid.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(grid[0])); writer.writeheader(); writer.writerows(grid)
    core.write_json(destination / "restrictions.json", restriction)
    a,b,c,d,_,_ = transform
    site = {"site_id": cfg.site_id, "source_site_id": cfg.expected_site, "public_display_name": cfg.expected_site,
        "coordinate_frame_id": restriction["coordinate_frame_id"], "units": "m", "timezone": cfg.timezone_name,
        "origin_description": f"Lower-left corner of StaticMap {definition['origin_cell']} at ({ox}, {oy}) m",
        "x_axis_direction": "increasing right", "y_axis_direction": "increasing up",
        "bounds": {"xmin": ox, "ymin": oy, "xmax": xmax, "ymax": ymax}, "bounds_meaning": "restriction-grid extent, not physical-wall bounding box",
        "address": None, "latitude": None, "longitude": None, "floor": None, "building_name": None, "geographical_north_direction": None,
        "layout": {"path": "site/layout.svg", "world_to_svg": transform, "semantic_geometry_status": "not_recorded", "semantic_geometry_path": None,
                   "transform_note": "Preserve internal SVG transforms; apply this matrix only to world-coordinate overlays.",
                   "equal_physical_axis_scale": abs(a*a+b*b-c*c-d*d)<1e-9 and abs(a*c+b*d)<1e-9},
        "restrictions": {"path": "site/restrictions.json", "restriction_version": restriction["restriction_version"]},
        "grid": {"path": "site/grid.csv", "spacing_m": step if aligned else None, "origin_m": [ox, oy], "cell_anchor": "lower_left" if aligned else None,
                 "coordinate_note": "Exact survey point coordinates. No snapping or half-cell shift. Phase/room not supplied in the two-column input."},
        "raw_input_definition": definition}
    core.write_json(destination / "site.json", site)
    return len(grid)


def build_dataset(cfg: Settings):
    workspace = Path(cfg.workspace).resolve()
    if not workspace.is_dir(): raise ValueError(f"Create the workspace and put downloaded session ZIPs in it: {workspace}")
    if not cfg.layout_assertion.strip(): raise ValueError("Confirm the site assets apply to the selected sessions in layout_assertion")
    uuid.UUID(cfg.survey_id)
    output = Path(cfg.output_zip).resolve()
    if output.exists(): raise FileExistsError(f"Output already exists: {output}. Use a new survey revision/output filename.")
    if output.parent != workspace / "output": raise ValueError("Output ZIP must be directly inside workspace/output")
    for path in (cfg.layout_svg, cfg.static_map_xlsm, cfg.grid_csv):
        if not Path(path).is_file(): raise FileNotFoundError(path)
    paths = sorted(p for p in workspace.iterdir() if p.is_file() and p.suffix.lower() == ".zip")
    if not paths: raise ValueError("No session-source ZIPs in the workspace root")
    selected, sessions = set(), []
    for path in paths:
        manifest, meta, command = source_bundle(path, cfg)
        key = str(manifest["session_id"])
        if key in selected:
            raise ValueError(f"Select exactly one ZIP per session. Session {key} appears more than once; move the unwanted version out of the workspace.")
        selected.add(key); sessions.append((path, manifest, meta, command))
        print(f"Session {key}: {manifest['session_uid']}; state={meta.get('state', 'unknown')}; result={meta.get('final_result', 'unknown')}")
    work = workspace / "work"; work.mkdir(exist_ok=True)
    # Retain this isolated run for inspection. Never delete the workspace or inputs.
    run = Path(tempfile.mkdtemp(prefix="conversion_", dir=work))
    print(f"Working files: {run}")
    site = run / "site"
    count = build_site(cfg, site)
    specs = []
    for index, (path, manifest, meta, command) in enumerate(sessions, 1):
        folder = run / f"session_{index:03d}"; folder.mkdir()
        csv_path = folder / "CommandSheet.csv"; csv_path.write_bytes(command)
        specs.append(core.SessionInput(path, csv_path, manifest, meta))
    core_cfg = core.Settings(sessions=specs, site_directory=site, output_zip=output,
        survey_id=cfg.survey_id, survey_title=cfg.survey_title, survey_revision=cfg.survey_revision, revision_reason=cfg.revision_reason,
        expected_site=cfg.expected_site, expected_experiment=cfg.expected_experiment, ap_ids=cfg.ap_ids,
        timezone_name=cfg.timezone_name, utc_offset_hours=cfg.utc_offset_hours, settling_seconds=cfg.settling_seconds,
        max_frame_age_seconds=cfg.max_frame_age_seconds, max_robot_age_seconds=cfg.max_robot_age_seconds,
        max_ap_age_seconds=cfg.max_ap_age_seconds, max_target_error_m=cfg.max_target_error_m,
        tx_power_mode=cfg.tx_power_mode, layout_assertion=cfg.layout_assertion, macro_endpoints=cfg.macro_endpoints,
        raw_assets=((cfg.static_map_xlsm, "sources/site/StaticMap.xlsm"), (cfg.grid_csv, "sources/site/Grid.csv")))
    print(f"Site {cfg.expected_site}: {count} grid points; {len(specs)} selected sessions")
    result = core.export_dataset(core_cfg)
    core.write_json(run / "validation_report.json", result)
    print("Finished. Import the output ZIP with the dataset manager; this tool makes no Redis writes.")
    return result


if __name__ == "__main__":
    # Edit these paths and values, then click Run in VS Code. No argparse.
    WORKSPACE = Path(r"D:\_Download\DataLakeWS")
    SITE_FOLDER = Path(r"D:\Data\_Action\_RunNMS\sitemap\DemoRoom\script_authoring")
    CONFIG = Settings(
        workspace=WORKSPACE,
        layout_svg=SITE_FOLDER / "DemoRoom.svg",
        static_map_xlsm=SITE_FOLDER / "AutoLab_DemoRoom_Template.xlsm",
        grid_csv=SITE_FOLDER / "Grid_DemoRoom.csv",
        output_zip=WORKSPACE / "output" / "RadioMapExportTest_r1.zip",
        # Keep the site UUID for all DemoRoom surveys.
        site_id="7b05bca7-3285-508f-82c5-cb7c69765417",
        # This UUID identifies the test survey only. For a NEW survey, generate
        # str(uuid.uuid4()) once and paste the result here. Retain it for revisions.
        survey_id="ed510faa-3e8b-4c95-9e2e-0ca89ef36ed8",
        expected_site="DemoRoom",
        expected_experiment="RadioMapExportTest",
        survey_title="DemoRoom RadioMapExportTest: main and makeup sessions",
        survey_revision=1,
        revision_reason="Initial dataset from the two verified DataLake session-source downloads",
        layout_assertion="The supplied DemoRoom layout, restrictions and grid apply to both selected test sessions.",
        ap_ids=("AP1", "AP4", "AP5", "AP6"),
        timezone_name="Asia/Taipei", utc_offset_hours=8,
        settling_seconds=45,
        max_frame_age_seconds=30, max_robot_age_seconds=30, max_ap_age_seconds=60,
        max_target_error_m=0.3,
        tx_power_mode="unknown",  # Use "default" only when confirmed. Numeric dBm is never guessed.
        macro_endpoints={},  # Optional known endpoints: {"mobility.in2out": (x, y), ...}.
    )
    build_dataset(CONFIG)
