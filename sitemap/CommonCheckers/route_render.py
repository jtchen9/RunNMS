"""Read-only StaticMap PNG renderer. Requires matplotlib; no safety checking."""
from pathlib import Path
import hashlib,json,math,posixpath,re,zipfile
import xml.etree.ElementTree as ET
NS={'s':'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
VERSION='0.4.3'

def read_static_map(workbook,sheet='StaticMap',cell_range='C4:DL117',resolution_m=.1):
    """Bottom-left cell covers [0,resolution] on both axes; never execute VBA."""
    if not math.isfinite(resolution_m) or resolution_m<=0:raise ValueError('resolution_m must be positive and finite')
    def coord(ref):
        m=re.fullmatch(r'([A-Z]+)([1-9][0-9]*)',ref)
        if not m:raise ValueError('Invalid cell reference: '+ref)
        c=0
        for ch in m[1]:c=c*26+ord(ch)-64
        return c,int(m[2])
    a,b=cell_range.split(':');c0,r0=coord(a);c1,r1=coord(b)
    if c1<c0 or r1<r0:raise ValueError('Reversed cell range')
    raw=Path(workbook).read_bytes();cells={};messages=[];overlap=[]
    with zipfile.ZipFile(Path(workbook)) as z:
        wb=ET.fromstring(z.read('xl/workbook.xml'))
        sheets=[s for s in wb.find('s:sheets',NS) if s.get('name')==sheet]
        if not sheets:raise ValueError('Missing sheet '+sheet)
        rid=sheets[0].get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id')
        rels={r.get('Id'):r.get('Target') for r in ET.fromstring(z.read('xl/_rels/workbook.xml.rels'))}
        target=rels[rid];target=target.lstrip('/') if target.startswith('/') else posixpath.normpath('xl/'+target)
        tree=ET.fromstring(z.read(target));styles=ET.fromstring(z.read('xl/styles.xml'))
        fills=list(styles.find('s:fills',NS));xfs=list(styles.find('s:cellXfs',NS))
        if tree.findall('s:conditionalFormatting',NS):messages.append('Conditional formatting exists; only stored cell fills are rendered.')
        shared = []
        if 'xl/sharedStrings.xml' in z.namelist():
            shared = [''.join(t.text or '' for t in si.findall('.//s:t', NS))
                      for si in ET.fromstring(z.read('xl/sharedStrings.xml'))]
        for cell in tree.findall('.//s:sheetData/s:row/s:c',NS):
            c,r=coord(cell.get('r'))
            if not (c0<=c<=c1 and r0<=r<=r1):continue
            color=None
            try:
                fill=fills[int(xfs[int(cell.get('s','0'))].get('fillId','0'))]
                pattern=fill.find('s:patternFill',NS);fg=pattern.find('s:fgColor',NS) if pattern is not None else None
                if pattern is not None and pattern.get('patternType')=='solid' and fg is not None:
                    rgb=fg.get('rgb','')
                    if re.fullmatch(r'(?:[0-9A-Fa-f]{2})?[0-9A-Fa-f]{6}',rgb):color=rgb[-6:].upper()
            except (IndexError,ValueError):pass
            if color == 'FF0000':
                value = cell.findtext('s:v', '', NS)
                if cell.get('t') == 's':
                    value = shared[int(value)]
                elif cell.get('t') == 'inlineStr':
                    value = ''.join(t.text or '' for t in cell.findall('.//s:is//s:t', NS))
                if value.strip() == '+':
                    overlap.append(((c-c0)*resolution_m, (r1-r)*resolution_m))
            cells[(c,r)]=color
    grid=[];unknown=[]
    for r in range(r0,r1+1):
        for c in range(c0,c1+1):
            color=cells.get((c,r));kind={'FFFFFF':'free','FF0000':'restricted','FFD966':'bump'}.get(color,'unknown')
            if kind=='unknown':unknown.append((c,r))
            grid.append(((c-c0)*resolution_m,(r1-r)*resolution_m,kind))
    if unknown:messages.append(f'{len(unknown)} cells have missing/unsupported fills; shown magenta, not free.')
    return dict(cells=grid,overlap=overlap,width=(c1-c0+1)*resolution_m,height=(r1-r0+1)*resolution_m,resolution=resolution_m,messages=messages,sha256=hashlib.sha256(raw).hexdigest(),sheet=sheet,cell_range=cell_range)

def compact_label(prefix, event):
    # Separate numeric fallback prefixes (R1, R2, ...) from the row.
    sep = ':' if prefix[-1:].isdigit() else ''
    return f"{prefix}{sep}{event['source_line']}"



def _place_row_labels(ax, pending, nodes, segments):
    """Place horizontal row labels in points, after the axes have their final limits."""
    if not pending:
        return
    import numpy as np  # Matplotlib's existing dependency
    from matplotlib.colors import to_rgb
    from matplotlib.font_manager import FontProperties

    ax.apply_aspect()
    renderer = ax.figure.canvas.get_renderer()
    px = renderer.points_to_pixels(1)
    font = FontProperties(family='DejaVu Sans', size=4.1, weight='medium')
    # Work in typographic points so offsets do not grow with the PNG resolution.
    def project(points):
        return ax.transData.transform(points) / px

    dots = project(nodes)
    ends = project(np.asarray(segments).reshape(-1, 2)).reshape(-1, 2, 2)
    midpoint = ends.mean(axis=1)
    half = (ends[:, 1] - ends[:, 0]) / 2
    boxes = np.empty((len(pending), 4))
    directions = [(1,1), (1,-1), (-1,1), (-1,-1), (0,1), (0,-1), (1,0), (-1,0)]
    # Use the same text artist to measure and draw; includes the actual font descent.
    for i, (label, xy, color, gap) in enumerate(pending):
        p = project([xy])[0]
        ink = tuple(.54 * v for v in to_rgb(color))
        text = ax.annotate(label, xy, xytext=(0,0), textcoords='offset points',
                           fontproperties=font, color=ink, zorder=5)
        extent = text.get_window_extent(renderer)
        size = np.array([extent.width, extent.height]) / px
        delta = np.abs(dots - p)
        nearby = dots[(delta.max(axis=1) < size.sum() + 9) & (delta.max(axis=1) > 1e-6)]
        best = None
        for order, (dx, dy) in enumerate(directions):
            direction = np.array([dx, dy])
            lo = p + gap * direction - size * (1 - direction) / 2
            hi = lo + size
            center = (lo + hi) / 2
            occupied = np.count_nonzero(np.all(boxes[:i,:2] < hi + .5, axis=1) &
                                        np.all(boxes[:i,2:] > lo - .5, axis=1))
            distance = np.linalg.norm(np.maximum(np.maximum(lo-nearby, nearby-hi), 0), axis=1)
            ownership = max(0., np.linalg.norm(center-p) + .5 -
                            np.min(np.linalg.norm(center-nearby, axis=1), initial=np.inf))
            # Segment/rectangle intersection via the two box axes and segment normal.
            extent = size / 2 + .35
            v = midpoint - center
            hits = (np.all(np.abs(v) <= extent + np.abs(half), axis=1) &
                    (np.abs(v[:,0]*half[:,1] - v[:,1]*half[:,0]) <=
                     extent[0]*np.abs(half[:,1]) + extent[1]*np.abs(half[:,0])))
            score = (occupied, np.maximum(0, 2-distance).sum(), ownership,
                     np.count_nonzero(hits), order)
            if best is None or score < best[0]:
                best = score, dx, dy, lo, hi
        _, dx, dy, lo, hi = best
        boxes[i] = [*lo, *hi]
        text.set_position((gap * dx, gap * dy))
        text.set_ha({1:'left', -1:'right', 0:'center'}[dx])
        text.set_va({1:'bottom', -1:'top', 0:'center'}[dy])


def render_routes(result,workbook,output_dir,*,sheet='StaticMap',cell_range='C4:DL117',resolution_m=.1,dpi=320,per_robot=True,overview_label_every=5):
    """Return manifest; retain interpretation gaps and issue-time semantics."""
    if not isinstance(dpi,int) or dpi<50 or dpi>600:raise ValueError('dpi must be an integer from 50 to 600')
    if not isinstance(overview_label_every,int) or overview_label_every<1:raise ValueError('overview_label_every must be a positive integer')
    from matplotlib.figure import Figure
    from matplotlib.backends.backend_agg import FigureCanvasAgg
    from matplotlib.collections import PatchCollection
    from matplotlib.patches import Rectangle,Patch
    from matplotlib.lines import Line2D
    out=Path(output_dir);names=sorted(result['robots']);outputs=['routes.png']
    robot_files={n:f'route_{i+1:02d}_{re.sub("[^A-Za-z0-9_-]","_",n)}.png' for i,n in enumerate(names)}
    if per_robot:outputs+=list(robot_files.values())
    protected={Path(i['path']).resolve() for i in result['inputs']}|{Path(workbook).resolve()}
    if any((out/f).resolve() in protected for f in outputs+['render_manifest.json','render_diagnostics.json']):raise ValueError('Render output would overwrite an input')
    out.mkdir(parents=True,exist_ok=True);messages=[]
    try:
        data=read_static_map(workbook,sheet,cell_range,resolution_m);messages+=data['messages']
    except (OSError,ValueError,KeyError,IndexError,zipfile.BadZipFile,ET.ParseError) as exc:
        data=None;messages.append('StaticMap unavailable: '+str(exc))
    palette=['#0072B2','#D55E00','#009E73','#CC79A7','#E69F00','#56B4E9','#6f4c9b','#555555']
    colors={n:palette[i%len(palette)] for i,n in enumerate(names)}
    initials = {n:n.removeprefix('twin-scout-')[:1].upper() for n in names}
    prefixes = {n:initials[n] if list(initials.values()).count(initials[n]) == 1 else f'R{i+1}' for i,n in enumerate(names)}
    bounds=[(0,0),(data['width'],data['height'])] if data else []
    for robot in result['robots'].values():
        if robot['initial_pose']:bounds.append((robot['initial_pose']['x_m'],robot['initial_pose']['y_m']))
        bounds.extend((e['x_m'],e['y_m']) for e in robot['points'] if e.get('x_m') is not None)
    if not bounds:bounds=[(0,0),(1,1)]
    def draw(selected,filename,detail):
        pending, nodes, segments = [], [], []
        fig=Figure(figsize=(17,14), dpi=dpi);FigureCanvasAgg(fig);ax=fig.add_subplot(111);fig.subplots_adjust(left=.07,right=.81,bottom=.12,top=.9)
        if data:
            for kind,color in [('restricted','#dddddd'),('bump','#f6d66b'),('unknown','#e154d4')]:
                patches=[Rectangle((x,y),data['resolution'],data['resolution']) for x,y,k in data['cells'] if k==kind]
                if patches:ax.add_collection(PatchCollection(patches,facecolor=color,edgecolor='none',zorder=0))
        handles=[Patch(facecolor='#dddddd',label='Static restriction'),Patch(facecolor='#f6d66b',label='Bump guard')]
        if data and data['overlap']:
            patches = [Rectangle(xy,data['resolution'],data['resolution']) for xy in data['overlap']]
            ax.add_collection(PatchCollection(patches, facecolor='none', edgecolor='#705600',
                                              hatch='////', linewidths=0, zorder=1))
            handles.append(Patch(facecolor='#dddddd', edgecolor='#705600', hatch='////',
                                 label='Static + bump'))
        for name in selected:
            robot=result['robots'][name];color=colors[name];short=name.removeprefix('twin-scout-');initial=robot['initial_pose']
            handles.append(Line2D([0],[0],color=color,marker='o',label=prefixes[name]+' = '+name))
            if initial:
                x,y=initial['x_m'],initial['y_m'];ax.scatter(x,y,marker='*',s=62,color=color,edgecolor='black',zorder=6)
                nodes.append((x,y))
                if not detail:
                    pending.append((short+' initial', (x,y), color, 4.5))
            points=robot['points'];known=[e for e in points if e.get('x_m') is not None]
            for seq,e in enumerate(points,1):
                if e.get('x_m') is None:continue
                x,y=e['x_m'],e['y_m']
                if e['segment_known']:
                    fx,fy=e['from_x_m'],e['from_y_m'];ax.plot([fx,x],[fy,y],color=color,lw=.8,linestyle='--' if e['status']=='expected_macro_segment' else '-',zorder=2)
                    segments.append(((fx,fy), (x,y)))
                    if fx!=x or fy!=y:ax.annotate('',xy=(fx+.75*(x-fx),fy+.75*(y-fy)),xytext=(fx+.4*(x-fx),fy+.4*(y-fy)),arrowprops=dict(arrowstyle='->',color=color,lw=.6,mutation_scale=6),zorder=3)
                nodes.append((x,y))
                ax.scatter(x,y,s=6,marker='o' if e['segment_known'] else 'x',color=color,zorder=4)
                if detail or (seq-1)%overview_label_every==0 or e is known[-1]:
                    suffix=' ?' if not e['segment_known'] else ''
                    gap = 2.8 if e is known[-1] else 1.65
                    if initial and (x,y) == (initial['x_m'],initial['y_m']):
                        gap = 3.8  # The destination coincides with the start star.
                    pending.append((compact_label(prefixes[name], e)+suffix, (x,y), color, gap))
            if known:ax.scatter(known[-1]['x_m'],known[-1]['y_m'],marker='s',s=25,facecolors='none',edgecolors=color,zorder=6)
        view_bounds = bounds
        if detail:
            robot = result['robots'][selected[0]]
            view_bounds = [(e['x_m'], e['y_m']) for e in robot['points'] if e.get('x_m') is not None]
            if robot['initial_pose']:
                view_bounds.append((robot['initial_pose']['x_m'], robot['initial_pose']['y_m']))
            view_bounds = view_bounds or bounds
        xs,ys=zip(*view_bounds);ax.set(xlim=(min(xs)-.2,max(xs)+.2),ylim=(min(ys)-.2,max(ys)+.2),xlabel='x (m)',ylabel='y (m)');ax.set_aspect('equal');ax.grid(alpha=.15)
        ax.legend(handles=handles,loc='upper left',bbox_to_anchor=(1.01,1),fontsize=8)
        title=selected[0] if detail else 'All robots'
        fig.suptitle('Commanded routes — '+title,fontsize=17)
        issues=len(result['diagnostics'])+len(messages)
        fig.text(.07,.93,f'{len(selected)} robot(s) | planned command sequence | '+('INCOMPLETE / DIAGNOSTICS: '+str(issues) if issues else 'Reconstruction has no interpretation errors'),color='#b02020' if issues else '#333333',fontsize=10)
        note='A134 = Alpha, command CSV row 134 (header = row 1). Dashed: expected macro crossing.\nStars: intended initial positions. Open squares: last known destinations. Arrows: commanded direction.\nNo safety checking or actual arrival-time inference. Unknown incoming segments are not connected.\n'+('Every known destination is labelled.' if detail else f'Overview labels every {overview_label_every}th movement; per-robot views label every known destination.')
        fig.text(.07,.04,note,fontsize=9)
        if issues:
            text='\n'.join(messages+[f"{d['code']} row {d['source_line'] or '-'}: {d['message']}" for d in result['diagnostics']][:4])
            import textwrap
            ax.text(1.01,.6,'Diagnostics (first four):\n'+ '\n'.join(textwrap.fill(line,32) for line in text.splitlines())+'\n\nSee diagnostics JSON files.',transform=ax.transAxes,fontsize=8,color='#b02020',va='top')
        if not selected:ax.text(.5,.5,'No interpretable robot routes',transform=ax.transAxes,ha='center')
        _place_row_labels(ax, pending, nodes, segments)
        fig.savefig(out/filename,dpi=dpi);fig.clear()
    draw(names,'routes.png',False)
    if per_robot:
        for name in names:draw([name],robot_files[name],True)
    manifest=dict(renderer_version=VERSION,outputs=outputs,robot_files=robot_files if per_robot else {},reconstruction_inputs=result['inputs'],map=dict(path=str(workbook),sha256=data['sha256'] if data else None,sheet=sheet,cell_range=cell_range,resolution_m=resolution_m,origin_m=[0,0]),settings=dict(dpi=dpi,per_robot=per_robot,overview_label_every=overview_label_every),complete=result['route_complete'] and not messages,safety_checked=False,render_diagnostics=messages)
    (out/'render_diagnostics.json').write_text(json.dumps(messages,indent=2)+'\n',encoding='utf-8')
    (out/'render_manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')
    return manifest
