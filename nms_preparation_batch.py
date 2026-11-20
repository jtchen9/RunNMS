"""Concurrent preparation with an all-robot path barrier before any move."""
import asyncio
import math
from itertools import combinations
from nms_preparation_positioning import PositioningError


class BatchFailure(RuntimeError):
    def __init__(self, scanner, reason):
        super().__init__(reason)
        self.scanner = scanner


async def run_batch(targets, job):
    """First failure cancels and joins sibling tasks; no background tasks leak."""
    tasks = [asyncio.create_task(job(target)) for target in targets]
    try:
        return await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done(): task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def _point_segment(p, a, b):
    dx,dy=b[0]-a[0],b[1]-a[1]
    norm=dx*dx+dy*dy
    t=max(0.,min(1.,((p[0]-a[0])*dx+(p[1]-a[1])*dy)/norm)) if norm else 0.
    return math.hypot(p[0]-a[0]-t*dx,p[1]-a[1]-t*dy)


def segment_distance(a,b,c,d):
    def cross(p,q,r):return (q[0]-p[0])*(r[1]-p[1])-(q[1]-p[1])*(r[0]-p[0])
    if cross(a,b,c)*cross(a,b,d)<0 and cross(c,d,a)*cross(c,d,b)<0:return 0.
    return min(_point_segment(a,c,d),_point_segment(b,c,d),_point_segment(c,a,b),_point_segment(d,a,b))


class PathBarrier:
    """Check whole nominal paths, independent of robot timing/poll order.

    Ordinary runtime collision/static checks still apply to motion/corrections.
    This is not a path planner: conflicting starting placements fail preparation.
    """
    def __init__(self, scanners, port, clearance_m):
        self.scanners=set(scanners);self.port=port;self.clearance=float(clearance_m)
        if not math.isfinite(self.clearance) or self.clearance<=0:
            raise ValueError("Invalid preparation robot clearance")
        self.paths={};self.ready=asyncio.Event();self.error=None

    async def wait(self, scanner, start, target):
        if scanner not in self.scanners or scanner in self.paths:
            raise PositioningError("Unexpected or duplicate preparation path")
        self.paths[scanner]=(dict(start),dict(target))
        if len(self.paths)==len(self.scanners):
            try:
                for name,(a,b) in self.paths.items():
                    if not all(math.isfinite(float(p[k])) for p in (a,b) for k in ('x_m','y_m')):
                        raise PositioningError("Invalid preparation path coordinates")
                    self.port.check_path(name,a,b)
                for left,right in combinations(sorted(self.paths),2):
                    a,b=self.paths[left];c,d=self.paths[right]
                    xy=lambda p:(p['x_m'],p['y_m'])
                    distance=segment_distance(xy(a),xy(b),xy(c),xy(d))
                    if distance<=self.clearance:
                        raise PositioningError(f"Preparation paths for {left} and {right} are too close: {distance:.3f}m; required >{self.clearance:.3f}m")
            except Exception as exc:
                self.error=exc
            finally:
                self.ready.set()
        await self.ready.wait()
        if self.error is not None:raise self.error
