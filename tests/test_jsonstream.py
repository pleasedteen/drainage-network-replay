"""Streaming JSON reader: chunk boundaries, sizes, malformed input."""

from __future__ import annotations

import io
import json
import unittest

from support import ROOT  # noqa: F401  (adds the project root to sys.path)

from app import jsonstream


def load(text, **kw):
    return jsonstream.load_small(io.BytesIO(text.encode()), **kw)


class StreamTest(unittest.TestCase):
    def test_round_trip_at_tiny_chunk_sizes(self):
        doc = {"segment": "s", "batch": "B-1", "seq": 1, "total": 1, "part": 0,
               "parts": 1, "upgrade": False,
               "taps": [{"ref": f"J-{i:06d}", "attrs": {"k": "值" * 3}}
                        for i in range(50)],
               "lays": [{"code": f"P-{i:06d}", "from": "J-000001",
                         "to": "J-000002"} for i in range(50)]}
        text = json.dumps(doc, ensure_ascii=False)
        for chunk in (1, 2, 3, 7, 64, 8192):
            got = load(text, chunk_size=chunk)
            self.assertEqual(got, doc, chunk)

    def test_numbers_split_across_buffers(self):
        doc = {"a": 1234567890, "b": [1.5e-9, -2, True, None, "x"]}
        text = json.dumps(doc)
        for chunk in (1, 2, 3):
            self.assertEqual(load(text, chunk_size=chunk), doc)

    def test_value_larger_than_one_chunk(self):
        doc = {"blob": "y" * 100000, "n": 1}
        self.assertEqual(load(json.dumps(doc), chunk_size=1024), doc)

    def test_callback_order_and_item_streaming(self):
        doc = {"a": 1, "xs": [1, 2, 3], "b": {"c": 2}, "ys": []}
        events = []
        jsonstream.iter_segment(
            io.BytesIO(json.dumps(doc).encode()), chunk_size=3,
            on_field=lambda k, v: events.append(("field", k, v)),
            on_array_item=lambda k, v: events.append(("item", k, v)))
        self.assertEqual(events, [
            ("field", "a", 1), ("item", "xs", 1), ("item", "xs", 2),
            ("item", "xs", 3), ("field", "b", {"c": 2})])

    def test_truncated_and_malformed(self):
        good = json.dumps({"a": "x" * 100, "b": [1, 2, 3]})
        for cut in (5, 20, len(good) - 1):
            with self.assertRaises(jsonstream.JsonStreamError):
                load(good[:cut])
        with self.assertRaises(jsonstream.JsonStreamError):
            load('{"a": 1, }')
        with self.assertRaises(jsonstream.JsonStreamError):
            load('{"a": [1, 2,]}')
        with self.assertRaises(jsonstream.JsonStreamError):
            load('{"a": 1} trailing')

    def test_unread_remainder_is_detected_only_after_full_read(self):
        doc = json.dumps({"a": 1})
        p = io.BytesIO((doc + "   \n").encode())
        self.assertEqual(jsonstream.load_small(p), {"a": 1})


if __name__ == "__main__":
    unittest.main()
