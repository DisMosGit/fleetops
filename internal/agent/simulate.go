package agent

import (
	"math/rand"
	"sync"
)

// Status names the simulated lifecycle condition a device reports in its heartbeats. Firmware
// stages extend this set (updating, rolled back) rather than replacing it.
const (
	// StatusOnline reports a device running normally, without an active degradation.
	StatusOnline = "online"
	// StatusDegraded reports a device inside a simulated degradation episode.
	StatusDegraded = "degraded"
)

// Sample is one reading of a simulated device's condition. CPU and Mem are utilisation values
// in [0.0, 1.0] and Health is a score in [0.0, 1.0] where 1.0 is fully healthy.
type Sample struct {
	// CPU is the simulated CPU utilisation.
	CPU float64
	// Mem is the simulated memory utilisation.
	Mem float64
	// Health is the simulated health score.
	Health float64
	// Status is StatusOnline or StatusDegraded, consistent with the sample.
	Status string
}

// Source produces the next condition sample for a simulated device. Implementations must be
// safe for concurrent use: one fleet shares its source across device goroutines.
type Source interface {
	// Next returns the next condition sample for the named device.
	Next(deviceID string) Sample
}

// Simulation is the default Source: bounded random metric variation with occasional
// degradation episodes that lower health and switch the reported status to StatusDegraded for a
// few samples. Randomness comes from the injected rand.Source, never a package-level generator,
// so a seeded or scripted source makes whole fleets reproducible.
type Simulation struct {
	mu sync.Mutex
	r  *rand.Rand
	// episodes holds the remaining degraded samples per device; a device absent from the map
	// is healthy.
	episodes map[string]int
}

// episodeChance is the per-sample probability that a healthy device starts degrading.
const episodeChance = 0.02

// episodeMin and episodeMax bound a degradation episode length, in samples.
const (
	episodeMin = 2
	episodeMax = 4
)

// healthy and degraded bound the metric ranges of each condition.
const (
	healthyCPULo, healthyCPUHi         = 0.1, 0.6
	healthyMemLo, healthyMemHi         = 0.2, 0.7
	healthyHealthLo, healthyHealthHi   = 0.85, 1.0
	degradedCPULo, degradedCPUHi       = 0.6, 0.95
	degradedMemLo, degradedMemHi       = 0.6, 0.95
	degradedHealthLo, degradedHealthHi = 0.2, 0.5
)

// NewSimulation returns a Simulation drawing its randomness from src.
func NewSimulation(src rand.Source) *Simulation {
	return &Simulation{r: rand.New(src), episodes: make(map[string]int)}
}

// Next returns the next simulated sample for deviceID, advancing its degradation episode
// state. A degraded sample always carries StatusDegraded and a health score below every healthy
// score; the reverse holds for StatusOnline.
func (s *Simulation) Next(deviceID string) Sample {
	s.mu.Lock()
	defer s.mu.Unlock()

	if remaining, ok := s.episodes[deviceID]; ok {
		if remaining <= 1 {
			delete(s.episodes, deviceID)
		} else {
			s.episodes[deviceID] = remaining - 1
		}
		return Sample{
			CPU:    lerp(s.r.Float64(), degradedCPULo, degradedCPUHi),
			Mem:    lerp(s.r.Float64(), degradedMemLo, degradedMemHi),
			Health: lerp(s.r.Float64(), degradedHealthLo, degradedHealthHi),
			Status: StatusDegraded,
		}
	}

	if s.r.Float64() < episodeChance {
		length := episodeMin + int(s.r.Float64()*float64(episodeMax-episodeMin+1))
		s.episodes[deviceID] = length
	}
	return Sample{
		CPU:    lerp(s.r.Float64(), healthyCPULo, healthyCPUHi),
		Mem:    lerp(s.r.Float64(), healthyMemLo, healthyMemHi),
		Health: lerp(s.r.Float64(), healthyHealthLo, healthyHealthHi),
		Status: StatusOnline,
	}
}

// lerp maps a unit random value into the [lo, hi] metric range.
func lerp(unit, lo, hi float64) float64 {
	return lo + unit*(hi-lo)
}
