package storage

import (
	"sync"
)

// Stats is a stats type for S3 storage.
type Stats struct {
	mu   sync.Mutex
	keys map[string]StatsResponse
}

// StatsResponse is a container for response of stats events.git
type StatsResponse struct {
	Message string
	Success bool
}

// SuccessCount returns the total success count for stats.
func (s *Stats) Keys() map[string]StatsResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.keys
}

// put adds response to stats map.
func (s *Stats) put(key string, value StatsResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.keys == nil {
		s.keys = make(map[string]StatsResponse)
	}
	s.keys[key] = value
}
