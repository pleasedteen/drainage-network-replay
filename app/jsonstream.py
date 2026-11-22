"""Incremental (chunked) JSON reader for log segments.

A segment is one JSON object.  Two of its sections (``ids``, ``taps``,
``lays``, ``retires``, ``redirects``) are arrays that may be far larger than
the synchronisation process' memory, so they are never materialised: the
reader walks the file with a fixed-size buffer and hands the caller one
element at a time.

Fields that are not arrays must stay small (a few KiB); a hard per-value cap
protects against malformed or hostile input.

The reader is deliberately narrow: one top-level object, scalar/array/object
values, no duplicate-key checks beyond the one in ``wire``.  Parsing errors
carry the byte offset at which the parser gave up.
"""

from __future__ import annotations

import codecs
import io
import json


def _is_text(fp) -> bool:
    return isinstance(fp, io.TextIOBase)


_NUM_CHARS = set("0123456789+-.eE")


def _may_continue(buf: str, end: int, val) -> bool:
    if end == len(buf):
        return True
    # a JSON number may be cut by the chunk boundary even when the character
    # after it is invalid on its own ("1" followed by "." in the next chunk)
    return isinstance(val, (int, float)) and buf[end] in _NUM_CHARS


class JsonStreamError(ValueError):
    """Malformed or over-long JSON input."""


class _Reader:
    def __init__(self, fp, chunk_size: int = 65536, max_value_bytes: int = 1 << 20):
        self.fp = fp
        self.chunk_size = chunk_size
        self.max_value_bytes = max_value_bytes
        self.buf = ""
        self.pos = 0            # index into self.buf
        self.base = 0           # number of chars discarded from the front
        self.byte_base = 0      # file byte offset of buf[0] (binary streams)
        self.eof = False
        self.decoder = json.JSONDecoder()
        # byte streams are decoded incrementally, so a multi-byte sequence
        # split across two reads is still handled correctly
        self.text = _is_text(fp)
        self._decoder = None if self.text else codecs.getincrementaldecoder(
            "utf-8")()

    # -- buffer plumbing ---------------------------------------------------
    def _fill(self, at_least: int = 1) -> bool:
        """Read more input.  Returns False at EOF."""
        if self.eof:
            return False
        raw = self.fp.read(self.chunk_size)
        if self.text:
            data = raw
        else:
            try:
                data = self._decoder.decode(raw, final=False)
            except UnicodeDecodeError as exc:
                raise self._error(f"invalid UTF-8: {exc}") from exc
        if not raw and not data:
            # the stream is exhausted; flush any buffered partial characters
            if not self.text:
                try:
                    data = self._decoder.decode(b"", final=True)
                except UnicodeDecodeError as exc:
                    raise self._error(f"invalid UTF-8: {exc}") from exc
            if not data:
                self.eof = True
                return False
        if self.pos:
            self._advance(self.pos)
        self.buf += data
        return True

    def _advance(self, n: int):
        """Drop the first ``n`` chars from the buffer, tracking offsets."""
        self.base += n
        if not self.text:
            # the decoder only ever emits whole characters, so re-encoding
            # the consumed prefix reproduces exactly its source bytes
            self.byte_base += len(self.buf[:n].encode("utf-8"))
        self.buf = self.buf[n:]
        self.pos = 0

    def _compact(self):
        if self.pos > self.chunk_size:
            self._advance(self.pos)

    def byte_offset(self) -> int:
        """File byte offset of the character at ``self.pos``.

        Only meaningful on binary streams; used to remember where a section
        array starts so it can be re-read later without re-scanning the file.
        """
        if self.text:
            raise JsonStreamError("byte offsets require a binary stream")
        return self.byte_base + len(self.buf[:self.pos].encode("utf-8"))

    def _error(self, msg: str) -> JsonStreamError:
        return JsonStreamError(f"{msg} (near byte offset {self.base + self.pos})")

    def _skip_ws(self):
        while True:
            while self.pos < len(self.buf) and self.buf[self.pos] in " \t\r\n":
                self.pos += 1
            if self.pos < len(self.buf) or not self._fill():
                return

    def _peek(self) -> str:
        self._skip_ws()
        if self.pos >= len(self.buf) and not self._fill():
            raise self._error("unexpected end of input")
        return self.buf[self.pos]

    def _expect(self, ch: str):
        if self._peek() != ch:
            raise self._error(f"expected {ch!r}, found {self.buf[self.pos]!r}")
        self.pos += 1

    # -- values ------------------------------------------------------------
    def value(self):
        """Decode one complete JSON value, refilling until it is unambiguous."""
        self._skip_ws()
        buf = self.buf
        while True:
            if self.pos >= len(buf):
                if not self._fill():
                    raise self._error("unexpected end of input")
                buf = self.buf
                continue
            try:
                val, end = self.decoder.raw_decode(buf, self.pos)
            except json.JSONDecodeError as exc:
                # A truncated value and an invalid value look alike; only trust
                # the error once we know we have hit EOF.
                if self.eof or len(buf) - self.pos > self.max_value_bytes:
                    raise self._error(f"invalid JSON: {exc.msg}") from exc
                if not self._fill():
                    raise self._error(f"invalid JSON: {exc.msg}") from exc
                buf = self.buf
                continue
            if not self.eof and _may_continue(buf, end, val):
                # The buffer edge may have cut a number short ("1" from
                # "1.5e-9"): read on before trusting the value.
                if len(buf) - self.pos > self.max_value_bytes:
                    raise self._error("value exceeds max_value_bytes")
                if self._fill():
                    buf = self.buf
                    continue
            self.pos = end
            return val

    def string(self) -> str:
        val = self.value()
        if not isinstance(val, str):
            raise self._error("expected a JSON string")
        return val

    def key(self) -> str:
        return self.string()

    def array_items(self):
        """Iterate over the elements of the array that starts at self.pos."""
        self._expect("[")
        first = True
        while True:
            if self._peek() == "]":
                self.pos += 1
                return
            if not first:
                self._expect(",")
                if self._peek() == "]":        # tolerate / reject trailing comma
                    raise self._error("trailing comma in array")
            first = False
            item = self.value()
            self._compact()
            yield item

    def object_items(self):
        self._expect("{")
        first = True
        while True:
            if self._peek() == "}":
                self.pos += 1
                return
            if not first:
                self._expect(",")
            first = False
            yield self.key()


def iter_segment(fp, *, chunk_size: int = 65536, max_value_bytes: int = 1 << 20,
                 on_field=None, on_array_item=None, on_array_start=None):
    """Stream one segment object.

    ``on_field(key, value)`` is called for scalar/object fields and
    ``on_array_item(key, item)`` for every element of an array field, in file
    order.  ``on_array_start(key, byte_offset)`` fires when an array field
    opens; the offset lets a later pass seek straight back to that array (see
    ``iter_array_at``) instead of re-scanning the whole file.  The function
    returns once the closing brace has been consumed.
    """
    reader = _Reader(fp, chunk_size=chunk_size, max_value_bytes=max_value_bytes)
    reader._expect("{")
    first = True
    while True:
        if reader._peek() == "}":
            reader.pos += 1
            break
        if not first:
            reader._expect(",")
        first = False
        key = reader.key()
        reader._expect(":")
        if reader._peek() == "[":
            if on_array_start is not None:
                on_array_start(key, reader.byte_offset())
            for item in reader.array_items():
                if on_array_item is not None:
                    on_array_item(key, item)
        else:
            value = reader.value()
            if on_field is not None:
                on_field(key, value)
    reader._skip_ws()
    if not reader.eof:
        rest = reader.buf[reader.pos:]
        if rest.strip():
            raise reader._error("trailing data after segment object")


def iter_array_at(fp, offset: int, *, chunk_size: int = 65536,
                  max_value_bytes: int = 1 << 20):
    """Yield the items of the array whose ``[`` sits at byte ``offset``.

    ``fp`` must be a binary stream positioned anywhere; it is seeked to the
    offset first.  Iteration stops at the matching ``]`` -- the rest of the
    file is left unread, so re-reading one section of a large segment costs
    only that section's bytes.
    """
    fp.seek(offset)
    reader = _Reader(fp, chunk_size=chunk_size, max_value_bytes=max_value_bytes)
    yield from reader.array_items()



def load_small(fp, **kwargs):
    """Read a whole (small) segment into memory -- for tests and fixtures."""
    header: dict = {}
    arrays: dict = {}

    def field(k, v):
        header[k] = v

    def item(k, v):
        arrays.setdefault(k, []).append(v)

    iter_segment(fp, on_field=field, on_array_item=item, **kwargs)
    header.update(arrays)
    return header
