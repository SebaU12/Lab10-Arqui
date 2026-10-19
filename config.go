package main

import (
	"fmt"
	"log"
	"math"
	"time"
)

const ServiceCount = 10

// Config contains every parameter that can change the simulated workload or
// the capacity available to the two architectures.
type Config struct {
	RequestCount                int
	MaxClientConcurrency        int
	SlowProbability             float64
	NormalLatencyMin            time.Duration
	NormalLatencyMax            time.Duration
	SlowLatencyMin              time.Duration
	SlowLatencyMax              time.Duration
	HedgeThreshold              time.Duration
	RescueWorkers               int
	CapacityPerInstance         int
	RescueBudgetPerRequest      int
	MaxRescueAttemptsPerService int
	Timeout                     time.Duration
	Seed                        int64
	Verbose                     bool
	LogRequestID                int
}

// DefaultConfig returns the initial values defined for the PoC. RequestCount
// and MaxClientConcurrency correspond to the functional scenario; later
// experiment phases will override them for the main and load scenarios.
func DefaultConfig() Config {
	return Config{
		RequestCount:                100,
		MaxClientConcurrency:        5,
		SlowProbability:             0.03,
		NormalLatencyMin:            10 * time.Millisecond,
		NormalLatencyMax:            30 * time.Millisecond,
		SlowLatencyMin:              400 * time.Millisecond,
		SlowLatencyMax:              1500 * time.Millisecond,
		HedgeThreshold:              150 * time.Millisecond,
		RescueWorkers:               2,
		CapacityPerInstance:         5,
		RescueBudgetPerRequest:      3,
		MaxRescueAttemptsPerService: 1,
		Timeout:                     5 * time.Second,
		Seed:                        2026,
	}
}

// Validate rejects configurations that cannot define a finite and coherent
// experiment. Zero rescue workers or a zero rescue budget are valid and allow
// experiments in which hedging is disabled explicitly.
func (c Config) Validate() error {
	if c.RequestCount <= 0 {
		return fmt.Errorf("request count must be greater than zero: %d", c.RequestCount)
	}
	if c.MaxClientConcurrency <= 0 {
		return fmt.Errorf("max client concurrency must be greater than zero: %d", c.MaxClientConcurrency)
	}
	if math.IsNaN(c.SlowProbability) || c.SlowProbability < 0 || c.SlowProbability > 1 {
		return fmt.Errorf("slow probability must be between 0 and 1: %g", c.SlowProbability)
	}
	if err := validateLatencyRange("normal", c.NormalLatencyMin, c.NormalLatencyMax); err != nil {
		return err
	}
	if err := validateLatencyRange("slow", c.SlowLatencyMin, c.SlowLatencyMax); err != nil {
		return err
	}
	if c.HedgeThreshold <= 0 {
		return fmt.Errorf("hedge threshold must be greater than zero: %s", c.HedgeThreshold)
	}
	if c.RescueWorkers < 0 {
		return fmt.Errorf("rescue workers cannot be negative: %d", c.RescueWorkers)
	}
	if c.CapacityPerInstance <= 0 {
		return fmt.Errorf("capacity per instance must be greater than zero: %d", c.CapacityPerInstance)
	}
	if c.RescueBudgetPerRequest < 0 {
		return fmt.Errorf("rescue budget per request cannot be negative: %d", c.RescueBudgetPerRequest)
	}
	if c.MaxRescueAttemptsPerService < 0 {
		return fmt.Errorf("max rescue attempts per service cannot be negative: %d", c.MaxRescueAttemptsPerService)
	}
	if c.MaxRescueAttemptsPerService > 1 {
		return fmt.Errorf("max rescue attempts per service cannot exceed one: %d", c.MaxRescueAttemptsPerService)
	}
	if c.Timeout <= 0 {
		return fmt.Errorf("timeout must be greater than zero: %s", c.Timeout)
	}
	if c.LogRequestID < 0 || c.LogRequestID > c.RequestCount {
		return fmt.Errorf(
			"log request ID must be zero or within the configured request range [1, %d]: %d",
			c.RequestCount,
			c.LogRequestID,
		)
	}

	return nil
}

// logEvent emits a concurrency-safe, human-readable trace when verbose mode
// is enabled. LogRequestID zero means that every request is included.
func (c Config) logEvent(
	architecture Architecture,
	requestID int,
	event string,
	format string,
	args ...any,
) {
	if !c.Verbose || (c.LogRequestID != 0 && c.LogRequestID != requestID) {
		return
	}
	details := fmt.Sprintf(format, args...)
	log.Printf(
		"[POC] architecture=%s request=%d event=%s %s",
		architecture,
		requestID,
		event,
		details,
	)
}

func validateLatencyRange(name string, minLatency, maxLatency time.Duration) error {
	if minLatency < 0 {
		return fmt.Errorf("%s minimum latency cannot be negative: %s", name, minLatency)
	}
	if maxLatency < minLatency {
		return fmt.Errorf(
			"%s maximum latency must be greater than or equal to its minimum: %s < %s",
			name,
			maxLatency,
			minLatency,
		)
	}

	return nil
}
