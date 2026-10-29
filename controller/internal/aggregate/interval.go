package aggregate

import (
	"fmt"
	"strings"
	"time"

	"github.com/bharathappali/kubegenx/internal/planner"
)

// ParseInterval accepts a whole number of minutes, hours, or days ("15m", "1h", "2d").
// It must be longer than step and an exact multiple of step.
func ParseInterval(s string, step time.Duration) (time.Duration, error) {
	if s == "" {
		return 0, fmt.Errorf("aggregation interval is required")
	}
	unit := s[len(s)-1]
	num := s[:len(s)-1]
	if num == "" || strings.Contains(num, ".") {
		return 0, fmt.Errorf("aggregation interval must be a whole number of minutes, hours, or days")
	}
	n := 0
	for _, c := range num {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("aggregation interval must be a whole number of minutes, hours, or days")
		}
		n = n*10 + int(c-'0')
	}
	if n < 1 {
		return 0, fmt.Errorf("aggregation interval must be a whole number of minutes, hours, or days")
	}
	var d time.Duration
	switch unit {
	case 'm':
		d = time.Duration(n) * time.Minute
	case 'h':
		d = time.Duration(n) * time.Hour
	case 'd':
		d = time.Duration(n) * 24 * time.Hour
	default:
		return 0, fmt.Errorf("aggregation interval must be minutes, hours, or days")
	}
	if step <= 0 {
		return 0, fmt.Errorf("dataset has no data interval")
	}
	if d <= step {
		return 0, fmt.Errorf("aggregation interval must be longer than the data interval")
	}
	if d%step != 0 {
		return 0, fmt.Errorf("aggregation interval must be a multiple of the data interval")
	}
	return d, nil
}

// Window places the dataset duration on the clock using a start or an end anchor.
func Window(plan *planner.Plan, anchor, at string) (start, end time.Time, err error) {
	when, err := time.Parse(time.RFC3339, at)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("time must be RFC3339")
	}
	when = when.UTC()
	dur := time.Duration(plan.Steps*plan.StepSeconds) * time.Second
	if dur <= 0 {
		return time.Time{}, time.Time{}, fmt.Errorf("dataset has no duration")
	}
	switch anchor {
	case "end":
		return when.Add(-dur), when, nil
	case "start", "":
		return when, when.Add(dur), nil
	default:
		return time.Time{}, time.Time{}, fmt.Errorf("anchor must be start or end")
	}
}
