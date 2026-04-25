package status

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
)

func TestStatusServerEmpty(t *testing.T) {
	srv := New()
	req := httptest.NewRequest("GET", "/status", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	var resp Response
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Runners) != 0 {
		t.Fatalf("expected 0 runners, got %d", len(resp.Runners))
	}
}

func TestStatusServerUpdate(t *testing.T) {
	srv := New()
	srv.UpdateRunner("runner-1", 42, 100)
	srv.UpdateRunner("runner-2", 0, 200)

	req := httptest.NewRequest("GET", "/status", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	var resp Response
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Runners) != 2 {
		t.Fatalf("expected 2 runners, got %d", len(resp.Runners))
	}

	runnerMap := make(map[string]RunnerStatus)
	for _, r := range resp.Runners {
		runnerMap[r.Name] = r
	}
	if runnerMap["runner-1"].Lag != 42 {
		t.Errorf("expected runner-1 lag=42, got %d", runnerMap["runner-1"].Lag)
	}
	if runnerMap["runner-2"].Sent != 200 {
		t.Errorf("expected runner-2 sent=200, got %d", runnerMap["runner-2"].Sent)
	}
}

func TestStatusServerRemove(t *testing.T) {
	srv := New()
	srv.UpdateRunner("runner-1", 10, 50)
	srv.RemoveRunner("runner-1")

	req := httptest.NewRequest("GET", "/status", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	var resp Response
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Runners) != 0 {
		t.Fatalf("expected 0 runners after remove, got %d", len(resp.Runners))
	}
}

func TestIsDone(t *testing.T) {
	srv := New()
	if !srv.IsDone() {
		t.Fatal("empty server should be done")
	}

	srv.UpdateRunner("r1", 5, 0)
	if srv.IsDone() {
		t.Fatal("server with lag>0 should not be done")
	}

	srv.UpdateRunner("r1", 0, 5)
	if !srv.IsDone() {
		t.Fatal("server with all lag==0 should be done")
	}
}
