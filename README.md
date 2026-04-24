# pydcache
Collections of dCache python utilities

## scripts/cta_nanny.py

Long-running “nanny” that consumes CTA taped/ingest logs from Kafka, detects specific duplicate-key failures, and enqueues PNFSIDs for worker processes that repair/update Chimera metadata and trigger dCache admin actions.

### Flow (overview)

```mermaid
flowchart TD
  A([Start]) --> B[Parse CLI args<br/>--cpu-count, --instance, --verbose]
  B --> C[Load config from dcache.yaml]
  C --> D{Config file perms OK?<br/>st_mode == 33152}
  D -- No --> E[Log error<br/>Exit 1]
  D -- Yes --> F[Start Kerberos KinitWorker]
  F --> G[Create multiprocessing Queue]
  G --> H[Spawn N Worker processes]
  H --> I[Create KafkaConsumer<br/>topic/group/bootstrap]
  I --> J{{Consume Kafka messages forever}}
  J --> K[Deserialize JSON<br/>safe_json_deserializer]
  K --> L{message exists?}
  L -- No --> J
  L -- Yes --> M[Extract payload = message['cta']]
  M --> N{payload has 'vo' and 'instance'?}
  N -- No --> J
  N -- Yes --> O[Get exceptionMessageValue]
  O --> P{exceptionMessageValue exists?}
  P -- No --> J
  P -- Yes --> Q{Contains 'duplicate key value violates unique constraint'?}
  Q -- No --> J
  Q -- Yes --> R[Parse (disk_instance, pnfsid)<br/>from exception text]
  R --> S{disk_instance == args.instance?}
  S -- No --> J
  S -- Yes --> T[queue.put(pnfsid)]
  T --> J

  J -. Ctrl+C .-> U[KeyboardInterrupt]
  U --> V[Send N sentinels: queue.put(None)]
  V --> W[Stop/terminate KinitWorker]
  W --> X([Exit])
```

### Flow (worker per PNFSID)

```mermaid
flowchart TD
  A([Worker process start]) --> B[Connect to CTA DB]
  B --> C[Connect to Chimera DB]
  C --> D[Open SSH shell to admin host]
  D --> E{{Loop: pnfsid = queue.get()}}
  E --> F{pnfsid is None?}
  F -- Yes --> Z[Close SSH/DB connections<br/>Exit worker]
  F -- No --> G[Query CTA DB for<br/>disk_instance_name, storage_class_name, location]
  G --> H{CTA rows found?}
  H -- No --> E1[Log error: no CTA location]
  E1 --> E

  H -- Yes --> I[Derive storage_group/file_family<br/>from storage_class]
  I --> J[Query Chimera: count existing t_locationinfo rows]
  J --> K{count != 0?}
  K -- Yes --> L[Log: already has location<br/>Skip]
  L --> E

  K -- No --> M[Insert into t_storageinfo<br/>(HSM=cta, group/subgroup)]
  M --> N[Insert into t_locationinfo<br/>(itype=0, priority=10, state=1, ilocation=cta URL)]
  N --> O[SSH admin: \sl pnfsid rep set cached pnfsid]
  O --> P[SSH admin: \sl pnfsid st kill pnfsid]
  P --> E
```
If you want me to tailor the README section name/wording (e.g., keep it generic like “Utilities” and list multiple scripts), tell me what other scripts you want documented.
Important update
On April 24 we'll start using GitHub Copilot interaction data for AI model training unless you opt out. Review this update and manage your preferences in your GitHub account settings.

