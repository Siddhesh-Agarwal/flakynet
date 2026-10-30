from __future__ import annotations


class Matcher:
    """Find the first occurrence of a byte pattern across chunk boundaries.

    A match is reported by its exclusive end offset. Only the last
    ``len(pattern) - 1`` bytes are retained between chunks, which is enough to
    detect any match spanning a boundary. Each matcher reports its first match
    and then stays matched.
    """

    def __init__(self, pattern: bytes) -> None:
        if not isinstance(pattern, bytes) or not pattern:
            raise ValueError("pattern must be non-empty bytes")
        self._pattern = pattern
        self._seen = 0
        self._tail = b""
        self._matched = False

    def feed(self, chunk: bytes) -> int | None:
        """Feed one chunk and return the exclusive end offset of the first match."""
        if self._matched or not chunk:
            return None

        base = self._seen - len(self._tail)
        data = self._tail + chunk
        index = data.find(self._pattern)

        self._seen += len(chunk)
        keep = len(self._pattern) - 1
        self._tail = data[-keep:] if keep else b""

        if index >= 0:
            self._matched = True
            return base + index + len(self._pattern)
        return None
