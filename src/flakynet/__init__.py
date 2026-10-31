"""Seeded, reproducible network faults for testing real clients over real sockets."""

from ._proxy import Proxy
from .api import proxy
from .events import CanonicalEvent, Event
from .faults import (
    Bandwidth,
    Blackhole,
    CloseAfter,
    Fragment,
    Latency,
    RefuseConnect,
    Reset,
)
from .matcher import Matcher
from .plan import ALL, Plan, between, every, nth, saw
from .report import (
    ENDED_EARLY,
    FIRED,
    MISSED,
    PENDING,
    PREEMPTED,
    UNMATCHED,
    FaultStatus,
)
from .resolve import ArmedFault, ConnectionPlan, StaticFault, resolve

__version__ = "0.1.0"

__all__ = [
    "ALL",
    "ENDED_EARLY",
    "FIRED",
    "MISSED",
    "PENDING",
    "PREEMPTED",
    "UNMATCHED",
    "ArmedFault",
    "Bandwidth",
    "Blackhole",
    "CanonicalEvent",
    "CloseAfter",
    "ConnectionPlan",
    "Event",
    "FaultStatus",
    "Fragment",
    "Latency",
    "Matcher",
    "Plan",
    "Proxy",
    "RefuseConnect",
    "Reset",
    "StaticFault",
    "between",
    "every",
    "nth",
    "proxy",
    "resolve",
    "saw",
]
