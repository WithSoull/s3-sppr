# CLAUDE.md — Metadata Service

## What this is

gRPC metadata service. Stores object/bucket metadata and HOT/COLD placements of versions in PostgreSQL. Publishes events to Kafka when objects/buckets are deleted. Full runbook: [README](README.md).

Port: `:50052`

## Architecture

```
cmd/server/main.go → (standard app/service_provider pattern)

internal/handler/metadata/  — MetadataService (Gateway)
internal/handler/placement/ — PlacementService (DSS planner, migration executor)
```

Both gRPC services share one process and port.

## Placement invariants

- Every committed blob version has exactly one `version_placements` row; reads use
  its `blob_id`, never `versions.blob_id` (that is the upload blob).
- `ListPlacements` is the Planner catalogue: current versions of active objects,
  plus their non-current HOT versions when `include_noncurrent_hot=true`, all
  ordered by `version_id`. `PlacementInfo.version_created_at` comes from
  `versions.created_at` (Unix ms), independently of placement updates.
- `ReserveCopy` and `SwitchPlacement` require an active object. Current versions
  may move in either direction; non-current versions may only be evicted HOT →
  COLD. A newer current version does not invalidate that eviction. Other
  non-current moves and inactive objects still return `ErrVersionNotCurrent`
  (`ABORTED`); an absent placement in a fresh reserve remains `NOT_FOUND`.
- A placement changes only in `SwitchPlacement`: CAS on the generation and class
  captured by `ReserveCopy`, under row locks on the object and the placement.
  Switch reads the target, locks the object/placement, then reloads the target
  under lock. This avoids copy-row → object lock inversion with delete's FK
  cleanup; aborts and concurrent switches are checked against the locked target.
- Delete and switch serialize on the same object row, including non-current
  evictions. Delete-first orphans the HOT source and invalidates the reserved
  target; switch-first leaves a HOT source copy and orphans the COLD placement.
  All bytes stay charged until cleanup, and deletion never promotes a target.
- `ReserveCopy` locks the `storage_classes` row before computing usage; removing
  that lock lets concurrent reservations overfill HOT.
- Copies in `reserved`, `cleanup_pending`, `cleanup_failed` count against class
  capacity. Copy rows outlive deleted versions (`version_id` → NULL).
- `DeleteObjectMeta` locks the object row, turns every placement of the object
  into an `orphan` copy (`migration_id` NULL, `cleanup_pending`, due after
  `METADATA_DELETE_CLEANUP_DELAY`) and only then deletes the object, all in one
  transaction. The delay must exceed `GATEWAY_MAX_READ_DURATION`; config refuses
  to start otherwise.
- `ChooseWriteClass` returns HOT only for an overwrite of a HOT key with known size
  that fits; new keys go to COLD. It does not reserve space, and
  `CreateObjectVersion` trusts the class Gateway wrote to.

## Service boundaries

| Does | Does NOT |
|------|----------|
| Store object/bucket metadata (PostgreSQL) | Store bytes (that's Storage) |
| Emit Kafka events on mutations | Check permissions (that's AuthZ) |
| Map object keys to blob_ids and placements | Know about S3 API directly |
| Switch placements after a verified copy | Copy blobs or verify MD5 (migration executor) |

## Commands

```bash
cd services/metadata && go test ./...
make test-metadata-integration-local   # from repo root, needs Docker
```
