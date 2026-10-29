package app

import (
	"time"

	"github.com/bharathappali/kubegenx/internal/spec"
)

// defaultEpoch is the next UTC midnight, minus the dataset duration.
// A 15 day dataset created on 29 October therefore starts at 15 October 00:00 UTC
// and the last step falls before 30 October 00:00 UTC, so the 29th is complete.
// The remainder of today is ahead of the clock. Thanos store serves those blocks.
func defaultEpoch(now time.Time, tpl spec.Template) string {
	dur, err := spec.ParseDuration(tpl.Time.Duration)
	if err != nil || dur <= 0 {
		dur = 24 * time.Hour
	}
	end := now.UTC().Truncate(24 * time.Hour).Add(24 * time.Hour)
	return end.Add(-dur).Format(time.RFC3339)
}
