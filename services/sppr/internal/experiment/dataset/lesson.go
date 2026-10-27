package dataset

import (
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

// Lesson keeps the hand calculation small enough to inspect every event.
func Lesson() model.FeatureRequest {
	at := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	deleted := at.Add(10 * time.Minute)
	catalog := []model.Version{
		{VersionRef: model.VersionRef{Bucket: "lesson", ObjectKey: "a", VersionID: "a1"}, SizeBytes: 100, CreatedAt: at.Add(-24 * time.Hour)},
		{VersionRef: model.VersionRef{Bucket: "lesson", ObjectKey: "b", VersionID: "b1"}, SizeBytes: 100, CreatedAt: at.Add(-24 * time.Hour)},
		{VersionRef: model.VersionRef{Bucket: "lesson", ObjectKey: "c", VersionID: "c1"}, SizeBytes: 100, CreatedAt: at.Add(-15 * time.Minute), DeletedAt: &deleted},
		{VersionRef: model.VersionRef{Bucket: "lesson", ObjectKey: "future", VersionID: "f1"}, SizeBytes: 100, CreatedAt: at.Add(2 * time.Hour)},
	}
	var events []model.AccessEvent
	add := func(v model.Version, offset time.Duration, bytes int64, origin string) {
		events = append(events, model.AccessEvent{GatewayID: "lesson", StreamID: "reads", Sequence: int64(len(events) + 1), OccurredAt: at.Add(offset), Bucket: v.Bucket, ObjectKey: v.ObjectKey, VersionID: v.VersionID, Operation: model.OperationRead, Origin: origin, Result: model.ResultSuccess, Bytes: bytes})
	}
	add(catalog[0], -time.Hour, 100, model.OriginClient)
	add(catalog[0], -30*time.Minute, 50, model.OriginClient)
	add(catalog[0], 0, 25, model.OriginClient)
	add(catalog[0], 59*time.Minute, 100, model.OriginClient)
	add(catalog[0], time.Hour, 100, model.OriginClient)
	add(catalog[0], -time.Minute, 100, model.OriginMigration)
	add(catalog[2], -5*time.Minute, 100, model.OriginClient)
	add(catalog[2], 5*time.Minute, 100, model.OriginClient)
	return model.FeatureRequest{At: at, Catalog: catalog, Events: events, Coverage: model.Coverage{Known: true, Start: at.Add(-24 * time.Hour), Watermark: at}}
}
