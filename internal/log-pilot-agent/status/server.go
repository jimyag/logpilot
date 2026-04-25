package status

import (
	"encoding/json"
	"net/http"
	"sync"
)

// RunnerStatus holds the current state of a single runner.
type RunnerStatus struct {
	Name string `json:"name"`
	Lag  int64  `json:"lag"`
	Sent int64  `json:"sent"`
}

// Response is the JSON body returned by /status.
type Response struct {
	Runners []RunnerStatus `json:"runners"`
}

// Server exposes runner status over HTTP for IsDoneCollected queries.
type Server struct {
	mu      sync.RWMutex
	runners map[string]RunnerStatus
}

// New creates a new status Server.
func New() *Server {
	return &Server{runners: make(map[string]RunnerStatus)}
}

// UpdateRunner registers or updates a runner's status.
func (s *Server) UpdateRunner(name string, lag, sent int64) {
	s.mu.Lock()
	s.runners[name] = RunnerStatus{Name: name, Lag: lag, Sent: sent}
	s.mu.Unlock()
}

// RemoveRunner removes a runner from the status map.
func (s *Server) RemoveRunner(name string) {
	s.mu.Lock()
	delete(s.runners, name)
	s.mu.Unlock()
}

// IsDone returns true if all tracked runners have lag == 0.
func (s *Server) IsDone() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, r := range s.runners {
		if r.Lag > 0 {
			return false
		}
	}
	return true
}

// ServeHTTP implements http.Handler for the /status endpoint.
func (s *Server) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	runners := make([]RunnerStatus, 0, len(s.runners))
	for _, rs := range s.runners {
		runners = append(runners, rs)
	}
	s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(Response{Runners: runners})
}

// ListenAndServe starts the HTTP server on the given address.
func (s *Server) ListenAndServe(addr string) error {
	return http.ListenAndServe(addr, s)
}
