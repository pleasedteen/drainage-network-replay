"""Incremental municipal drainage network synchronisation.

Storage is split into two independent SQLite databases: the **inbox**
(接收库) where log segments are acknowledged and staged, and the **publish**
database (发布库) that serves the queryable network.

Layering
--------
* ``wire``      -- validation / coercion of the external log-segment format.
* ``jsonstream``-- chunked JSON reading: one item at a time, never the whole
                   segment in memory.
* ``archive``   -- append-only landing store for accepted segments plus the
                   inbox staging tables (one commit per segment).
* ``snapshot``  -- applies complete batches from the inbox to the publish
                   database, one transaction per batch; reconciles the two
                   sides after a crash so effects apply exactly once.
* ``migrate``   -- splits a pre-split single database into the pair and
                   verifies snapshot, id mapping, topology and batch basis.
* ``tracing``   -- upstream reachability over the published snapshot.
* ``server``    -- the local HTTP query API (publish database only).
* ``service``   -- in-process orchestration: receive -> publish -> status.
* ``replay``    -- offline source-log replay used by the demo and tests.
"""

__all__ = ["__version__"]
__version__ = "2.0.0"
