"""Seeded, reproducible network faults for testing real clients over real sockets."""

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
from .proxy import Proxy
from .resolve import ArmedFault, ConnectionPlan, StaticFault, resolve

__version__ = "0.1.0"

__all__ = [
    "ALL",
    "ArmedFault",
    "Bandwidth",
    "Blackhole",
    "CloseAfter",
    "ConnectionPlan",
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
    "resolve",
    "saw",
]
