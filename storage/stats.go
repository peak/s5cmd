package storage

import (
	"sync"
	"sync/atomic"
)

// Stats is a stats type for S3 storage.
type Stats struct {
	failCount    uint32
	successCount uint32
	content      statsMap
}

// StatsResponse is a container for response of stats events.git
type StatsResponse struct {
	Message string
	Success bool
}

// StatsMap is a atomic container for stats responses.
type statsMap struct {
	mu   sync.Mutex
	keys map[string]StatsResponse
}

// Get returns the stats response.
func (m *statsMap) Get() map[string]StatsResponse {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.keys
}

// Put adds stats response to atomic map.
func (m *statsMap) Put(key string, value StatsResponse) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.keys == nil {
		m.keys = make(map[string]StatsResponse)
	}
	m.keys[key] = value
}

// FailCount returns the total fail count for stats.
func (s Stats) FailCount() uint32 {
	return atomic.LoadUint32(&s.failCount)
}

// SuccessCount returns the total success count for stats.
func (s Stats) SuccessCount() uint32 {
	return atomic.LoadUint32(&s.successCount)
}

// put adds response to stats map.
func (s *Stats) put(key string, value StatsResponse) {
	s.content.Put(key, value)
}

// incrementFailCount increments atomic counter for failed operations.
func (s *Stats) incrementFailCount() {
	atomic.AddUint32(&s.failCount, 1)
}

// incrementSuccessCount increments atomic counter for successful operations.
func (s *Stats) incrementSuccessCount() {
	atomic.AddUint32(&s.successCount, 1)
}
