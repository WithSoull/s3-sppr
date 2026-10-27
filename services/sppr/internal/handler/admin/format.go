package admin

import (
	"fmt"
	"html/template"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

func templateFuncs() template.FuncMap {
	return template.FuncMap{
		"id": formID, "seconds": seconds, "urlquery": url.QueryEscape, "help": Help,
		"ms": formatMilliseconds, "decimal": formatDecimal, "number": formatNumber,
		"integer": formatInteger, "bytes": formatBytes, "percent": formatPercent,
		"datetime": formatTime, "duration": formatDuration, "durationSeconds": func(n int64) string { return formatSeconds(float64(n)) },
		"settingHint": formatSettingHint, "elapsed": formatElapsed, "yesno": formatBool, "text": formatText, "label": enumLabel,
	}
}

func groupDigits(s string) string {
	sign := ""
	if strings.HasPrefix(s, "-") {
		sign, s = "-", s[1:]
	}
	var out strings.Builder
	out.WriteString(sign)
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			out.WriteByte(' ')
		}
		out.WriteRune(c)
	}
	return out.String()
}
func decimal(n float64, trim bool) string {
	if math.IsNaN(n) || math.IsInf(n, 0) {
		return "—"
	}
	s := strconv.FormatFloat(n, 'f', 1, 64)
	if s == "-0.0" {
		s = "0.0"
	}
	whole, fraction, _ := strings.Cut(s, ".")
	s = groupDigits(whole)
	if !trim || fraction != "0" {
		s += "," + fraction
	}
	return s
}
func formatNumber(n float64) string  { return decimal(n, true) }
func formatDecimal(n float64) string { return decimal(n, false) }
func formatInteger(n any) string     { return groupDigits(fmt.Sprint(n)) }
func formatMilliseconds(n float64) string {
	if math.IsNaN(n) || math.IsInf(n, 0) {
		return "—"
	}
	return formatNumber(n) + " мс"
}
func formatPercent(n float64) string {
	if math.IsNaN(n) || math.IsInf(n, 0) {
		return "—"
	}
	return formatNumber(n*100) + " %"
}
func formatBytes(n int64) string {
	value := float64(n)
	units := []string{"Б", "КиБ", "МиБ", "ГиБ", "ТиБ", "ПиБ", "ЭиБ"}
	unit := 0
	for math.Abs(value) >= 1024 && unit < len(units)-1 {
		value /= 1024
		unit++
	}
	if unit == 0 {
		return groupDigits(strconv.FormatInt(n, 10)) + " Б"
	}
	return decimal(value, unit == 1) + " " + units[unit]
}
func formatTime(value any) string {
	var t time.Time
	switch v := value.(type) {
	case time.Time:
		t = v
	case *time.Time:
		if v == nil {
			return "—"
		}
		t = *v
	default:
		return "—"
	}
	if t.IsZero() {
		return "—"
	}
	return t.UTC().Format("2006-01-02 15:04:05 UTC")
}
func formatDuration(d time.Duration) string { return formatSeconds(d.Seconds()) }
func formatSeconds(n float64) string {
	if math.IsNaN(n) || math.IsInf(n, 0) {
		return "—"
	}
	if n != 0 && math.Mod(n, 3600) == 0 {
		return formatNumber(n/3600) + " ч"
	}
	return formatNumber(n) + " с"
}
func formatElapsed(n *float64) string {
	if n == nil {
		return "—"
	}
	return formatSeconds(*n)
}
func formatBool(b bool) string {
	if b {
		return "да"
	}
	return "нет"
}
func formatText(value any) string {
	if value == nil {
		return "—"
	}
	s := fmt.Sprint(value)
	if s == "" {
		return "—"
	}
	return s
}

func formatSettingHint(f field) string {
	switch f.Name {
	case "horizon_seconds":
		return formatSeconds(float64(f.Value.(int64)))
	case "hot_utilization", "cold_utilization":
		return formatPercent(f.Value.(float64))
	case "hot_fixed_ms", "cold_fixed_ms", "min_gain_ms":
		return formatMilliseconds(f.Value.(float64))
	default:
		return ""
	}
}

var enumLabels = map[any]string{
	model.PlanComputing: "рассчитывается", model.PlanReady: "готов", model.PlanHistoryIncomplete: "история неполна",
	model.PlanFailed: "ошибка расчёта", model.PlanSuperseded: "заменён новой ревизией", model.PlanConfirmed: "подтверждён", model.PlanRejected: "отклонён",
	model.TriggerTimer: "по таймеру", model.TriggerManual: "вручную",
	model.KindPromote: "в HOT", model.KindEvict: "вытеснение в COLD", model.KindRebalance: "разгрузка HOT", model.KindEvictNoncurrent: "старая версия в COLD",
	model.RejectPinned: "объект закреплён", model.RejectMinInterval: "минимальный интервал не истёк", model.RejectInProgress: "версия переносится",
	model.RejectBudget: "недостаточно бюджета", model.RejectColdCapacity: "недостаточно места в COLD", model.RejectHotCapacity: "недостаточно места в HOT",
	model.RejectGainThreshold: "выигрыш ниже порога", model.RejectNoForecast: "нет прогноза спроса",
	model.ActionPending: "ожидает", model.ActionRunning: "выполняется", model.ActionSucceeded: "успешно завершено", model.ActionFailed: "ошибка выполнения", model.ActionCancelled: "отменено",
	model.StepReserving: "резервирование", model.StepCopying: "копирование", model.StepSwitching: "переключение размещения", model.StepAborting: "отмена копирования",
	model.ReasonStale: "размещение изменилось", model.ReasonNoCapacity: "недостаточно места", model.ReasonVerifyMismatch: "данные не прошли сверку",
	model.ReasonSourceMissing: "источник не найден", model.ReasonUpstreamUnavailable: "зависимый сервис недоступен", model.ReasonInternal: "внутренняя ошибка",
	model.ReasonUser: "по команде администратора", model.ReasonPinned: "объект закреплён", model.ReasonMinInterval: "минимальный интервал не истёк", model.ReasonDependencyFailed: "предшественник не выполнен",
	model.BlockedByDependencies: "ожидание предшественников", model.BlockedByBudget: "ожидание бюджета", model.BlockedByPaused: "исполнитель приостановлен",
	model.CommandConfirm: "подтверждение плана", model.CommandReject: "отклонение плана", model.CommandCancelPending: "отмена ожидающих действий",
	model.CommandPause: "пауза исполнителя", model.CommandResume: "возобновление исполнителя", model.CommandSettings: "настройки Planner", model.CommandRecompute: "расчёт плана",
	model.CommandType("budget"): "изменение бюджета", model.CommandType("interval"): "изменение интервала", model.CommandType("pin-add"): "закрепление объекта", model.CommandType("pin-delete"): "снятие закрепления",
	model.CommandAccepted: "принята", model.CommandRejected: "отклонена",
	"noncurrent": "старая версия", "unavailable": "прогноз недоступен", "unobserved": "период не наблюдался",
	"complete": "полная история", "warmup": "накопление истории", "no_history": "нет истории", "gap": "пропуск событий",
	"unknown_completeness": "полнота неизвестна", "watermark_behind": "история отстаёт", "no_forecast": "нет прогноза спроса",
}

func enumLabel(value any) string {
	if label, ok := enumLabels[value]; ok {
		return label
	}
	return formatText(value)
}
