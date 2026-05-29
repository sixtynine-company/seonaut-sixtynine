package services

// These tests focus on the pure guardrail computations (effectiveCap,
// effectiveTimeout) and the constructor's per-field defaulting. They use the
// internal package so they can reach the unexported effectiveCap/effectiveTimeout
// methods and the sem field. The queue/timeout/dedup/partial-results behaviors
// are covered by the end-to-end checklist (they require a live MySQL + crawler),
// so they are intentionally out of scope here.

import (
	"errors"
	"testing"
	"time"

	"github.com/stjudewashere/seonaut/internal/config"
)

// fullAPIConfig returns a fully-populated APIConfig so the defaulting logic is
// not exercised and each computation can be asserted against explicit values.
func fullAPIConfig() *config.APIConfig {
	return &config.APIConfig{
		Key:                   "test",
		MaxConcurrentCrawls:   3,
		HomepageCap:           1,
		KeyCap:                10,
		FullCap:               200,
		DefaultTimeoutSeconds: 90,
		MaxTimeoutSeconds:     300,
	}
}

// TestEffectiveCap verifies depth-to-cap resolution, the maxPages override, the
// FullCap hard ceiling (guardrail 5.3) and the unknown-depth error.
func TestEffectiveCap(t *testing.T) {
	svc := NewAPICrawlService(nil, nil, nil, nil, fullAPIConfig(), nil)

	tests := []struct {
		name     string
		depth    string
		maxPages int
		want     int
		wantErr  error
	}{
		{name: "homepage default", depth: "homepage", maxPages: 0, want: 1},
		{name: "key default", depth: "key", maxPages: 0, want: 10},
		{name: "full default", depth: "full", maxPages: 0, want: 200},
		{name: "full override below ceiling", depth: "full", maxPages: 50, want: 50},
		{name: "full override clamped to FullCap", depth: "full", maxPages: 5000, want: 200},
		{name: "homepage override raises small cap", depth: "homepage", maxPages: 25, want: 25},
		{name: "key non-positive override ignored", depth: "key", maxPages: -5, want: 10},
		{name: "unknown empty depth", depth: "", maxPages: 0, want: 0, wantErr: ErrUnknownDepth},
		{name: "unknown deep depth", depth: "deep", maxPages: 0, want: 0, wantErr: ErrUnknownDepth},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := svc.effectiveCap(tt.depth, tt.maxPages)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("effectiveCap(%q, %d) error = %v, want %v", tt.depth, tt.maxPages, err, tt.wantErr)
				}
				if got != tt.want {
					t.Errorf("effectiveCap(%q, %d) = %d, want %d on error", tt.depth, tt.maxPages, got, tt.want)
				}
				return
			}
			if err != nil {
				t.Errorf("effectiveCap(%q, %d) unexpected error: %v", tt.depth, tt.maxPages, err)
			}
			if got != tt.want {
				t.Errorf("effectiveCap(%q, %d) = %d, want %d", tt.depth, tt.maxPages, got, tt.want)
			}
		})
	}
}

// TestEffectiveCapClampUsesConfiguredFullCap confirms the ceiling tracks the
// configured FullCap rather than a hardcoded 200.
func TestEffectiveCapClampUsesConfiguredFullCap(t *testing.T) {
	cfg := fullAPIConfig()
	cfg.FullCap = 50
	svc := NewAPICrawlService(nil, nil, nil, nil, cfg, nil)

	tests := []struct {
		name     string
		depth    string
		maxPages int
		want     int
	}{
		{name: "override clamped to configured FullCap", depth: "full", maxPages: 5000, want: 50},
		{name: "full default equals configured FullCap", depth: "full", maxPages: 0, want: 50},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := svc.effectiveCap(tt.depth, tt.maxPages)
			if err != nil {
				t.Errorf("effectiveCap(%q, %d) unexpected error: %v", tt.depth, tt.maxPages, err)
			}
			if got != tt.want {
				t.Errorf("effectiveCap(%q, %d) = %d, want %d", tt.depth, tt.maxPages, got, tt.want)
			}
		})
	}
}

// TestEffectiveTimeout verifies the default fallback, the override, and clamping
// to [1s, MaxTimeoutSeconds].
func TestEffectiveTimeout(t *testing.T) {
	svc := NewAPICrawlService(nil, nil, nil, nil, fullAPIConfig(), nil)

	tests := []struct {
		name           string
		timeoutSeconds int
		want           time.Duration
	}{
		{name: "zero uses default", timeoutSeconds: 0, want: 90 * time.Second},
		{name: "negative uses default", timeoutSeconds: -3, want: 90 * time.Second},
		{name: "override below ceiling", timeoutSeconds: 60, want: 60 * time.Second},
		{name: "override clamped to max", timeoutSeconds: 9999, want: 300 * time.Second},
		{name: "minimum override", timeoutSeconds: 1, want: 1 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := svc.effectiveTimeout(tt.timeoutSeconds)
			if got != tt.want {
				t.Errorf("effectiveTimeout(%d) = %v, want %v", tt.timeoutSeconds, got, tt.want)
			}
		})
	}
}

// TestNewAPICrawlServiceDefaults verifies the constructor applies per-field
// defaults for nil and partial configs, and builds sem with the right capacity.
func TestNewAPICrawlServiceDefaults(t *testing.T) {
	t.Run("nil config applies all defaults", func(t *testing.T) {
		svc := NewAPICrawlService(nil, nil, nil, nil, nil, nil)

		if got, err := svc.effectiveCap("homepage", 0); err != nil || got != 1 {
			t.Errorf("effectiveCap(homepage,0) = %d, %v; want 1, nil", got, err)
		}
		if got, err := svc.effectiveCap("full", 0); err != nil || got != 200 {
			t.Errorf("effectiveCap(full,0) = %d, %v; want 200, nil", got, err)
		}
		if got := svc.effectiveTimeout(0); got != 90*time.Second {
			t.Errorf("effectiveTimeout(0) = %v, want %v", got, 90*time.Second)
		}
		if got := cap(svc.sem); got != 3 {
			t.Errorf("cap(sem) = %d, want 3", got)
		}
	})

	t.Run("partial config defaults remaining fields", func(t *testing.T) {
		svc := NewAPICrawlService(nil, nil, nil, nil, &config.APIConfig{KeyCap: 25}, nil)

		if got, err := svc.effectiveCap("key", 0); err != nil || got != 25 {
			t.Errorf("effectiveCap(key,0) = %d, %v; want 25, nil", got, err)
		}
		if got, err := svc.effectiveCap("full", 0); err != nil || got != 200 {
			t.Errorf("effectiveCap(full,0) = %d, %v; want 200, nil", got, err)
		}
		if got := cap(svc.sem); got != 3 {
			t.Errorf("cap(sem) = %d, want 3", got)
		}
	})
}

// TestEffectiveCapDefaultedMaxConcurrencyChannel verifies a configured
// MaxConcurrentCrawls sizes the concurrency semaphore channel.
func TestEffectiveCapDefaultedMaxConcurrencyChannel(t *testing.T) {
	svc := NewAPICrawlService(nil, nil, nil, nil, &config.APIConfig{MaxConcurrentCrawls: 7}, nil)

	if got := cap(svc.sem); got != 7 {
		t.Errorf("cap(sem) = %d, want 7", got)
	}
}
