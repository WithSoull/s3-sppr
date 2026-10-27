package admin

import (
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/stretchr/testify/require"
)

func TestFormatting(t *testing.T) {
	timestamp := time.Date(2026, 10, 28, 1, 27, 49, 0, time.FixedZone("MSK", 3*3600))
	var absent *time.Time
	var noRead *float64
	cases := []struct {
		name   string
		format func() string
		want   string
	}{
		{"milliseconds rounded and grouped", func() string { return formatMilliseconds(1491.26) }, "1 491,3 мс"},
		{"milliseconds whole", func() string { return formatMilliseconds(1000) }, "1 000 мс"},
		{"small negative rounds to zero", func() string { return formatMilliseconds(-0.01) }, "0 мс"},
		{"expected reads fixed decimal", func() string { return formatDecimal(12) }, "12,0"},
		{"fraction as percent", func() string { return formatPercent(1) }, "100 %"},
		{"fraction rounded", func() string { return formatPercent(0.66666) }, "66,7 %"},
		{"percentile", func() string { return formatNumber(66.6666) }, "66,7"},
		{"integer grouping", func() string { return formatInteger(int64(1234567)) }, "1 234 567"},
		{"bytes", func() string { return formatBytes(512) }, "512 Б"},
		{"kibibytes", func() string { return formatBytes(65536) }, "64 КиБ"},
		{"mebibytes", func() string { return formatBytes(1 << 20) }, "1,0 МиБ"},
		{"gibibytes", func() string { return formatBytes(1 << 30) }, "1,0 ГиБ"},
		{"time converted to UTC", func() string { return formatTime(timestamp) }, "2026-10-27 22:27:49 UTC"},
		{"time pointer", func() string { return formatTime(&timestamp) }, "2026-10-27 22:27:49 UTC"},
		{"zero time", func() string { return formatTime(time.Time{}) }, "—"},
		{"nil time", func() string { return formatTime(absent) }, "—"},
		{"hours", func() string { return formatDuration(time.Hour) }, "1 ч"},
		{"seconds", func() string { return formatDuration(300 * time.Second) }, "300 с"},
		{"absent last read", func() string { return formatElapsed(noRead) }, "—"},
		{"true", func() string { return formatBool(true) }, "да"},
		{"false", func() string { return formatBool(false) }, "нет"},
		{"empty text", func() string { return formatText("") }, "—"},
		{"plan status", func() string { return enumLabel(model.PlanConfirmed) }, "подтверждён"},
		{"trigger", func() string { return enumLabel(model.TriggerTimer) }, "по таймеру"},
		{"kind", func() string { return enumLabel(model.KindPromote) }, "в HOT"},
		{"reject reason", func() string { return enumLabel(model.RejectBudget) }, "недостаточно бюджета"},
		{"action status", func() string { return enumLabel(model.ActionSucceeded) }, "успешно завершено"},
		{"action step", func() string { return enumLabel(model.StepCopying) }, "копирование"},
		{"action reason", func() string { return enumLabel(model.ReasonSourceMissing) }, "источник не найден"},
		{"command type", func() string { return enumLabel(model.CommandRecompute) }, "расчёт плана"},
		{"command status", func() string { return enumLabel(model.CommandRejected) }, "отклонена"},
		{"empty enum", func() string { return enumLabel(model.RejectNone) }, "—"},
		{"unknown enum retained", func() string { return enumLabel(model.ActionStep("future_step")) }, "future_step"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) { require.Equal(t, tt.want, tt.format()) })
	}
}
