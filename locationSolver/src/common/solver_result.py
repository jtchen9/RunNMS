from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

@dataclass
class SolverResult:
    """Common output contract for every solver."""
    sample_uid: str
    success: bool
    estimated_x_m: Optional[float] = None
    estimated_y_m: Optional[float] = None
    estimated_heading_deg: Optional[float] = None
    failure_reason: str = ""
    iterations: Optional[int] = None
    runtime_ms: Optional[float] = None
    objective_value: Optional[float] = None
    tags_input: List[int] = field(default_factory=list)
    tags_used: List[int] = field(default_factory=list)
    tags_rejected: List[int] = field(default_factory=list)
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
