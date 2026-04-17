# pydcache
Collections of dCache python utilities

## scripts/cta_nanny.py

Long-running “nanny” that consumes CTA taped/ingest logs from Kafka, detects specific duplicate-key failures, and enqueues PNFSIDs for worker processes that repair/update Chimera metadata and trigger dCache admin actions.

### Flow (overview)

![Flow (overview)](docs/diagrams/cta_nanny_flow_overview.svg)

### Flow (worker per PNFSID)

![Flow (worker per PNFSID)](docs/diagrams/cta_nanny_flow_worker.svg)

