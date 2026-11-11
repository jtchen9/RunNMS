"""Read-only StaticMap PNG renderer. Requires matplotlib; no safety checking."""
from pathlib import Path
import hashlib,json,math,posixpath,re,zipfile
import xml.etree.ElementTree as ET
NS={'s':'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
VERSION='0.4.2'

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
    seconds = event['t_offset_sec']
    seconds = '?' if seconds is None else str(int(seconds)) if seconds == int(seconds) else str(seconds)
    return f"{prefix}#{event['source_line']},{seconds}"

def render_routes(result,workbook,output_dir,*,sheet='StaticMap',cell_range='C4:DL117',resolution_m=.1,dpi=160,per_robot=True,overview_label_every=5):
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
        fig=Figure(figsize=(17,14));FigureCanvasAgg(fig);ax=fig.add_subplot(111);fig.subplots_adjust(left=.07,right=.81,bottom=.12,top=.9)
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
                x,y=initial['x_m'],initial['y_m'];ax.scatter(x,y,marker='*',s=160,color=color,edgecolor='black',zorder=6)
                ax.annotate(short+' initial',(x,y),xytext=(7,-15),textcoords='offset points',fontsize=8,color=color)
            points=robot['points'];known=[e for e in points if e.get('x_m') is not None]
            for seq,e in enumerate(points,1):
                if e.get('x_m') is None:continue
                x,y=e['x_m'],e['y_m']
                if e['segment_known']:
                    fx,fy=e['from_x_m'],e['from_y_m'];ax.plot([fx,x],[fy,y],color=color,lw=1.2,linestyle='--' if e['status']=='expected_macro_segment' else '-',zorder=2)
                    if fx!=x or fy!=y:ax.annotate('',xy=(fx+.75*(x-fx),fy+.75*(y-fy)),xytext=(fx+.4*(x-fx),fy+.4*(y-fy)),arrowprops=dict(arrowstyle='->',color=color,lw=.9),zorder=3)
                ax.scatter(x,y,s=17,marker='o' if e['segment_known'] else 'x',color=color,zorder=4)
                if detail or (seq-1)%overview_label_every==0 or e is known[-1]:
                    suffix=' ?' if not e['segment_known'] else ''
                    horizontal = e['segment_known'] and abs(e['dx_m']) > 2 * abs(e['dy_m'])
                    ax.annotate(compact_label(prefixes[name],e)+suffix,(x,y),xytext=(0,8) if horizontal else (8,0),textcoords='offset points',ha='left',va='bottom' if horizontal else 'center',rotation=60 if horizontal else 0,rotation_mode='anchor',fontsize=9 if detail else 8,color=color,zorder=5)
            if known:ax.scatter(known[-1]['x_m'],known[-1]['y_m'],marker='s',s=65,facecolors='none',edgecolors=color,zorder=6)
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
        fig.text(.07,.93,f'{len(selected)} robot(s) | command-issue times | '+('INCOMPLETE / DIAGNOSTICS: '+str(issues) if issues else 'Reconstruction has no interpretation errors'),color='#b02020' if issues else '#333333',fontsize=10)
        note='A#134,303 = Alpha, CSV line 134 (header=1), offset 303 seconds. Dashed: expected macro crossing.\nStars: intended initial positions. Open squares: last known destinations. Arrows: commanded direction.\nNo safety checking or actual arrival-time inference. Unknown incoming segments are not connected.\n'+('Every known destination is labelled.' if detail else f'Overview labels every {overview_label_every}th movement; per-robot views label every known destination.')
        fig.text(.07,.04,note,fontsize=9)
        if issues:
            text='\n'.join(messages+[f"{d['code']} row {d['source_line'] or '-'}: {d['message']}" for d in result['diagnostics']][:4])
            import textwrap
            ax.text(1.01,.6,'Diagnostics (first four):\n'+ '\n'.join(textwrap.fill(line,32) for line in text.splitlines())+'\n\nSee diagnostics JSON files.',transform=ax.transAxes,fontsize=8,color='#b02020',va='top')
        if not selected:ax.text(.5,.5,'No interpretable robot routes',transform=ax.transAxes,ha='center')
        fig.savefig(out/filename,dpi=dpi);fig.clear()
    draw(names,'routes.png',False)
    if per_robot:
        for name in names:draw([name],robot_files[name],True)
    manifest=dict(renderer_version=VERSION,outputs=outputs,robot_files=robot_files if per_robot else {},reconstruction_inputs=result['inputs'],map=dict(path=str(workbook),sha256=data['sha256'] if data else None,sheet=sheet,cell_range=cell_range,resolution_m=resolution_m,origin_m=[0,0]),settings=dict(dpi=dpi,per_robot=per_robot,overview_label_every=overview_label_every),complete=result['route_complete'] and not messages,safety_checked=False,render_diagnostics=messages)
    (out/'render_diagnostics.json').write_text(json.dumps(messages,indent=2)+'\n',encoding='utf-8')
    (out/'render_manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')
    return manifest
