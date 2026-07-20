package pool

import "sync"

// Set holds one Pool per enabled chain, keyed by canonical chain key.
type Set struct {
	mu sync.RWMutex
	m  map[string]*Pool
}

func NewSet() *Set {
	return &Set{m: map[string]*Pool{}}
}

// Ensure returns the pool for key, creating it with the given lag limit if needed.
func (s *Set) Ensure(key string, lag uint64) *Pool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p, ok := s.m[key]; ok {
		return p
	}
	p := New(key, lag)
	s.m[key] = p
	return p
}

func (s *Set) Get(key string) (*Pool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.m[key]
	return p, ok
}
