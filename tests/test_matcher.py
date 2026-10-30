import itertools

import pytest
from hypothesis import given
from hypothesis import strategies as st

from flakynet.matcher import Matcher


def test_match_in_single_chunk():
    m = Matcher(b"COMMIT")
    assert m.feed(b"BEGIN;COMMIT;") == 12


def test_match_split_across_chunks():
    m = Matcher(b"COMMIT")
    assert m.feed(b"BEG") is None
    assert m.feed(b"IN;COM") is None
    assert m.feed(b"MIT;") == 12


def test_match_at_stream_start_and_end():
    m = Matcher(b"abc")
    assert m.feed(b"abc") == 3
    m2 = Matcher(b"abc")
    assert m2.feed(b"xxabc") == 5


def test_fires_once():
    m = Matcher(b"abc")
    assert m.feed(b"abcabc") == 3
    assert m.feed(b"abc") is None


def test_empty_pattern_rejected():
    with pytest.raises(ValueError):
        Matcher(b"")


def _feed_all(pattern: bytes, chunks: list[bytes]) -> int | None:
    m = Matcher(pattern)
    for chunk in chunks:
        result = m.feed(chunk)
        if result is not None:
            return result
    return None


@given(
    pattern=st.binary(min_size=1, max_size=8),
    stream=st.binary(min_size=0, max_size=64),
    cuts=st.lists(st.integers(min_value=0, max_value=64), max_size=12),
)
def test_match_end_is_chunk_invariant(pattern, stream, cuts):
    boundaries = sorted({0, len(stream), *[min(c, len(stream)) for c in cuts]})
    chunks = [stream[a:b] for a, b in itertools.pairwise(boundaries)]
    assert _feed_all(pattern, chunks) == _feed_all(pattern, [stream])


def test_match_spanning_one_byte_chunks():
    matcher = Matcher(b"COMMIT")
    result = None
    for byte in b"BEGIN;COMMIT;":
        result = matcher.feed(bytes([byte]))
        if result is not None:
            break
    assert result == 12


@given(
    pattern=st.binary(min_size=1, max_size=8),
    stream=st.binary(min_size=0, max_size=64),
)
def test_match_end_is_chunk_invariant_with_one_byte_chunks(pattern, stream):
    chunks = [stream[index : index + 1] for index in range(len(stream))]
    assert _feed_all(pattern, chunks) == _feed_all(pattern, [stream])
