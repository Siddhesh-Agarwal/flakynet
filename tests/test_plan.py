import random
from typing import Any, cast

import pytest

from flakynet.faults import Reset
from flakynet.plan import ALL, Plan, between, every, nth, saw


def test_between_is_inclusive_and_uniform():
    rng = random.Random("test")
    draws = {between(2, 4).draw(rng) for _ in range(200)}
    assert draws == {2, 3, 4}


def test_between_rejects_negative_or_reversed_ranges():
    with pytest.raises(ValueError):
        between(-1, 3)
    with pytest.raises(ValueError):
        between(4, 2)


def test_nth_is_zero_based():
    assert nth(0).matches(0)
    assert not nth(0).matches(1)
    assert nth(2).matches(2)


def test_nth_rejects_negative_index():
    with pytest.raises(ValueError):
        nth(-1)


def test_every_matches_every_connection():
    assert every().matches(0)
    assert every().matches(99)
    assert ALL.matches(1)


def test_saw_requires_explicit_stream_side():
    with pytest.raises(TypeError):
        cast("Any", saw)(b"x")
    with pytest.raises(ValueError):
        saw(b"", side="client")
    with pytest.raises(ValueError):
        saw(b"x", side=cast("Any", "both"))


def test_plan_is_iterable_in_order():
    first = Reset(after_bytes=1)
    second = Reset(after_bytes=2)
    assert list(Plan(first, second)) == [first, second]


def test_plan_repr_is_stable_and_address_free():
    plan = Plan(Reset(after_bytes=between(1, 2)), Reset(after_bytes=3))
    same = Plan(Reset(after_bytes=between(1, 2)), Reset(after_bytes=3))
    assert repr(plan) == repr(same)
    assert repr(plan).startswith("Plan(")
    assert "0x" not in repr(plan)


def test_between_rejects_non_integer_bounds():
    with pytest.raises(TypeError):
        between(cast("Any", 0.5), cast("Any", 2.5))
    with pytest.raises(TypeError):
        between(True, 2)  # noqa: FBT003 - bool is an int subclass and must be rejected


def test_nth_rejects_non_integer_index():
    with pytest.raises(TypeError):
        nth(cast("Any", 0.5))
    with pytest.raises(TypeError):
        nth(True)  # noqa: FBT003 - bool is an int subclass and must be rejected


def test_connection_selectors_reject_invalid_connection_indexes():
    with pytest.raises(ValueError):
        every().matches(-1)
    with pytest.raises(TypeError):
        every().matches(cast("Any", 0.5))
    with pytest.raises(ValueError):
        nth(0).matches(-1)
    with pytest.raises(TypeError):
        nth(0).matches(cast("Any", 0.5))
