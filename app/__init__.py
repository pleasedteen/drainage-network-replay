"""Incremental municipal drainage network synchronisation.

Layering
-------
* ``wire``      -- validation / coercion of the external log-segment format.
* ``archive``   -- append-only landing store for accepted segments (never
                   memory-bound: one line at a time).
* ``staging``   -- SQLite scratch tables for archive -> staging ingest and for
                   the deterministic batch-replay (``BatchReplayer``).
* ``snapshot``  -- the published network plus per-batch language/identifier
                   bookkeeping, swapped in only when a batch is complete.
* ``tracing``   -- upstream reachability over the published snapshot.
* ``server``    -- the local HTTP query API.
* ``sample_store`` -- versioned sample sets: atomic regeneration, user files
                   preserved, one version selected per replay, stale sets
                   reaped while in-flight replays stay readable.
* ``replay``    -- offline source-log replay used by the demo and tests.
"""

__all__ = ["__version__"]
__version__ = "1.0.0"
