"""DemoRoom ideal-position CHECK ONLY, not for real-time mobility.
Detect contact with the superseded entrance strip except its approved room-side edge.
Coordinates in metres, rounded to nanometres for reproducible boundary comparisons.
"""
from fractions import Fraction as F
def vec(p): return tuple(round(float(x)*1_000_000_000) for x in p)
def sub(a,b):return a[0]-b[0],a[1]-b[1]
def cross(u,v):return u[0]*v[1]-u[1]*v[0]
def on(p,a,b):
    return cross(sub(p,a),sub(b,a))==0 and all(min(x,y)<=t<=max(x,y) for t,x,y in zip(p,a,b))
def pairs(v):return list(zip(v,v[1:]))
U,A,B,V,B1,A1=map(vec,[(2.0091,9.8534),(1.75,9.85),(1.75,9.65),(1.9683,9.6433),(1.85,9.65),(1.95,9.85)])
approved=pairs([U,A1,B1,V]); outline=pairs([U,A,B,V,B1,A1,U])
def _blocked(p):
    if any(on(p,a,b) for a,b in approved):return False
    if any(on(p,a,b) for a,b in outline):return True
    x,y=p
    return sum((a[1]>y)!=(b[1]>y) and x < a[0]+F((y-a[1])*(b[0]-a[0]),b[1]-a[1]) for a,b in outline)%2==1
def entrance_blocked(start,end=None):
    """True if any part of this point or straight segment touches the excluded strip."""
    p=vec(start);q=p if end is None else vec(end)
    if any(max(p[i],q[i])<min(v[i] for v in (U,A,B,V,B1,A1)) or min(p[i],q[i])>max(v[i] for v in (U,A,B,V,B1,A1)) for i in (0,1)):return False
    if p==q:return _blocked(p)
    d=sub(q,p); times={F(0),F(1)};axis=0 if d[0] else 1
    for a,b in outline:
        v=sub(b,a);w=sub(a,p);den=cross(d,v)
        if den:
            t=F(cross(w,v),den);u=F(cross(w,d),den)
            if 0<=t<=1 and 0<=u<=1:times.add(t)
        elif cross(w,d)==0:
            for k in (a,b):
                t=F(k[axis]-p[axis],d[axis])
                if 0<=t<=1:times.add(t)
    ts=sorted(times)
    return any(_blocked((p[0]+t*d[0],p[1]+t*d[1])) for t in ts+[(a+b)/2 for a,b in pairs(ts)])
