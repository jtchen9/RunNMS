"""Build the companion LUT against the INSTALLED primary headings; never overwrite the primary.

Run from VS Code. Placement: tools/, beside build_preferred_direction_lut.py.
The scalar visibility/scoring routine is shared with v2. The optional occlusions
are operator observations in world coordinates; this code does not infer them.
"""
from __future__ import annotations
import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
import numpy as np
import build_preferred_direction_lut as v2

NAME = "alternative_heading_v1.npz"
MIN_TURN = 30.0
MIN_NEW_IDS = 2
SHARED_CREDIT = 0.25
CAMERAS = [
    v2.CameraCfg("front", 0., v2.FRONT_FORWARD_OFFSET_M, v2.FRONT_HALF_FOV_DEG, v2.FRONT_CONFIDENCE),
    v2.CameraCfg("rear", 180., v2.REAR_FORWARD_OFFSET_M, v2.REAR_HALF_FOV_DEG, v2.REAR_CONFIDENCE),
]

def signature(path):
    return v2.sha256_file(Path(path))

def new_view(x,y,h,tags):
    visible={"front":{}, "rear":{}}
    metrics=v2.eval_pose_heading(x,y,h,tags,CAMERAS,visibility=visible)
    # Prefer the better camera if a future configuration has overlapping FOVs.
    weights=dict(visible["front"])
    for tag,w in visible["rear"].items(): weights[tag]=max(w,weights.get(tag,0.))
    return metrics,(tuple(sorted(visible["front"])),tuple(sorted(visible["rear"]))),weights

def alternate(x,y,h,tags):
    _,pv,pw=new_view(x,y,h,tags)
    best_key=None; best=None
    for angle in v2.heading_values(v2.HEADING_STEP_DEG):
        a=float(angle); turn=v2.angle_diff_abs(a,h)
        if turn < MIN_TURN: continue
        m,view,weights=new_view(x,y,a,tags)
        novel=weights.keys()-pw.keys()
        if (len(novel)<MIN_NEW_IDS or m['status'] not in ('GOOD','MARGINAL')
                or m['geometry_span_deg'] < v2.MIN_GOOD_GEOMETRY_SPAN_DEG): continue
        # Only geometrical support differs. RSSI, AP placement and robot identity are not inputs.
        new_support=sum(weights[t] for t in sorted(novel))
        shared=sum(weights[t] for t in sorted(weights.keys() & pw.keys()))
        key=(round(new_support+SHARED_CREDIT*shared,8),len(novel),
             m['status']=='GOOD',bool(view[0] and view[1]),m['geometry_span_deg'],-turn,-a)
        if best_key is None or key>best_key:
            best_key=key;best=(a,view)
    return pv,best

def check_zones(zones,ids):
    for z in zones:
        if set(z)!= {'x','y','tags'}:raise ValueError('Each occlusion needs x, y, tags')
        for key in ('x','y'):
            v=z[key]
            if len(v)!=2 or not np.isfinite(v).all() or v[1]<v[0]:raise ValueError(f'Invalid {key} range: {v}')
        if any(t not in ids for t in z['tags']):raise ValueError(f'Unknown excluded tag in {z}')

def excluded(zones,x,y):
    return {t for z in zones if z['x'][0]-1e-6<=x<=z['x'][1]+1e-6 and z['y'][0]-1e-6<=y<=z['y'][1]+1e-6 for t in z['tags']}

def check_inputs(meta, tag_file, folder, reviewed_tag_sha256=None):
    raw = tag_file.read_bytes()
    actual = hashlib.sha256(raw).hexdigest()
    recorded = meta.get("tag_map_sha256")
    version = meta.get("table_version")
    convention = meta.get("heading_convention")
    # Match only exact bytes or a proved LF/CRLF conversion. A different hash
    # by itself cannot tell us whether tag positions or even the site changed.
    lf = raw.replace(b"\r\n", b"\n")
    equivalent = {hashlib.sha256(b).hexdigest() for b in (lf, lf.replace(b"\n", b"\r\n"))}
    match = "exact" if recorded == actual else "line endings only" if recorded in equivalent else "unverified"
    if reviewed_tag_sha256 is not None and reviewed_tag_sha256 != actual:
        raise ValueError(f"The reviewed tag-map SHA-256 is not the current digest: {actual}. No LUT files were written.")
    if match == "unverified" and reviewed_tag_sha256 == actual:
        match = "operator-selected current map; historical map differs or is unknown"
    print(f"Primary LUT: {folder.resolve()}\nInstalled heading rule: {version!r}; "
          f"view scoring rule: {v2.RULE_ID}\nTag map: {tag_file.resolve()}\nTag map match: {match}")
    problems = []
    if version not in ("preferred_direction_v1", v2.RULE_ID):
        problems.append(f"Unsupported primary rule {version!r}; expected preferred_direction_v1 or {v2.RULE_ID}.")
    if convention != "0=+x,90=+y,deg_norm_360":
        problems.append(f"Unrecognized primary heading convention: {convention!r}.")
    if match == "unverified":
        problems.append(f"Tag map SHA-256 in primary metadata: {recorded}\n"
                        f"Tag map SHA-256 of current file:     {actual}\n"
                        "Verify which tag file represents the current DemoRoom. If intentionally using this map with historical primary headings, pin its current digest in REVIEWED_TAG_SHA256.")
    if problems:
        raise ValueError("\n".join(problems) + "\nNo LUT files were written.")
    return actual, match


def build(tag_file,folder,zones=(), *, reviewed_tag_sha256=None):
    tag_file=Path(tag_file);folder=Path(folder)
    names=['preferred_heading_deg.npy','axis_x_m.npy','axis_y_m.npy','preferred_direction_meta.json']
    hashes={n:signature(folder/n) for n in names}
    meta=json.loads((folder/names[3]).read_text(encoding='utf-8-sig'))
    tag_hash, match = check_inputs(meta, tag_file, folder, reviewed_tag_sha256)
    h,xs,ys=[np.load(folder/n,allow_pickle=False) for n in names[:3]]
    if (h.shape!=(len(ys),len(xs)) or not np.array_equal(xs,v2.grid_values(v2.X_MIN_M,v2.X_MAX_M,v2.GRID_STEP_M))
        or not np.array_equal(ys,v2.grid_values(v2.Y_MIN_M,v2.Y_MAX_M,v2.GRID_STEP_M))):
        raise ValueError("Installed primary grid differs from v2 builder axes.")
    if np.isinf(h).any() or np.any((h[np.isfinite(h)] < 0) | (h[np.isfinite(h)] >= 360)):
        raise ValueError("Primary headings must be degrees in [0, 360), or NaN.")
    tags=v2.parse_tag_location(tag_file)
    ids={t.tag_id for t in tags}
    if len(ids)!=len(tags):raise ValueError("Duplicate tag IDs")
    # Keep exactly v2's parsed tag set and disclose map lines it does not recognise.
    listed={int(i) for i in re.findall(r'^\s*Tag#\s*(\d+)',tag_file.read_text(encoding='utf-8-sig'),re.M)}
    skipped=sorted(listed-ids)
    if skipped:print("Tag IDs skipped by the installed primary parser:",skipped)
    check_zones(zones,ids)
    rooms={r:v2.eligible_tags_for_room(tags,r) for r in ('inner','outer','blocked')}
    alts=np.full(h.shape,np.nan,dtype=np.float32)
    pv=np.zeros(h.shape,np.int32);av=pv.copy();views=[((),())];codes={views[0]:0}
    def encode(view):
        if view not in codes:
            codes[view]=len(views);views.append(view)
        return codes[view]
    for iy,y0 in enumerate(ys):
        y=float(y0)
        if iy%10==0: print(f'Alternative LUT: row {iy+1}/{len(ys)}',flush=True)
        for ix,x0 in enumerate(xs):
            heading=float(h[iy,ix]);x=float(x0)
            if not np.isfinite(heading):continue
            rr=rooms[v2.classify_room(x,y)]; blocked=excluded(zones,x,y)
            active=[t for t in rr if t.tag_id not in blocked] if blocked else rr
            view,best=alternate(x,y,heading,active);pv[iy,ix]=encode(view)
            if best is not None: alts[iy,ix]=best[0];av[iy,ix]=encode(best[1])
    meta2=dict(version=1,primary_files=hashes,tag_map_sha256=tag_hash,
               primary_tag_map_sha256=meta.get('tag_map_sha256'),primary_rule=meta['table_version'],view_scoring_rule=v2.RULE_ID,tag_map_match=match,
               skipped_tag_ids=skipped,zones=list(zones),min_turn_deg=MIN_TURN,
               min_new_ids=MIN_NEW_IDS,shared_credit=SHARED_CREDIT)
    if hashes!={n:signature(folder/n) for n in names} or signature(tag_file)!=tag_hash:
        raise RuntimeError("Inputs changed during build; existing companion untouched.")
    dest=folder/NAME
    with tempfile.NamedTemporaryFile(dir=folder,suffix='.npz',delete=False) as tmp:name=tmp.name
    try:
        np.savez_compressed(name,heading=alts,primary_view=pv,alternative_view=av,
                            views=json.dumps(views),metadata=json.dumps(meta2))
        os.replace(name,dest)
    finally:
        Path(name).unlink(missing_ok=True)
    print(f"Wrote {dest}: {np.isfinite(alts).sum()} alternate headings. Primary files unchanged.")
    return dest

if __name__=="__main__":
    TAG_FILE=v2.DEFAULT_TAG_FILE
    LUT_DIR=v2.DEFAULT_OUTPUT_DIR
    # Optional operator-observed occlusions: one dictionary per affected ROBOT area,
    # e.g. {"x":[xmin,xmax], "y":[ymin,ymax], "tags":[id1,id2]}. Applies to both views.
    OCCLUSIONS=[]
    # Leave unset unless you have confirmed this is the current site map and
    # want both views scored on it, relative to the installed primary angles.
    REVIEWED_TAG_SHA256=None
    build(TAG_FILE,LUT_DIR,OCCLUSIONS,reviewed_tag_sha256=REVIEWED_TAG_SHA256)
