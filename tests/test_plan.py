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
        cast(Any, saw)(b"x")
    with pytest.raises(ValueError):
        saw(b"", side="client")
    with pytest.raises(ValueError):
        saw(b"x", side=cast(Any, "both"))


def test_plan_is_iterable_in_order():
    first = Reset(after_bytes=1)
    second = Reset(after_bytes=2)
    assert list(Plan(first, second)) == [first, second]
