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
- A placement changes only in `SwitchPlacement`: CAS on the generation and class
  captured by `ReserveCopy`, under row locks on the object and the placement.
- `ReserveCopy` locks the `storage_classes` row before computing usage; removing
  that lock lets concurrent reservations overfill HOT.
- Copies in `reserved`, `cleanup_pending`, `cleanup_failed` count against class
  capacity. Copy rows outlive deleted versions (`version_id` → NULL).

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
