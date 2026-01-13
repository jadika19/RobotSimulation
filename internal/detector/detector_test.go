package detector_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/detector"
)

func TestHTTPRegistration(t *testing.T) {
	// Mock Coordinator HTTP Server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := detector.RegResp{
			ID:     1,
			Width:  10,
			Height: 10,
		}
		resp.Start.X = 5
		resp.Start.Y = 5
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	out, err := detector.SendHTTPRegistrationRequest(server.URL)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if out.ID != 1 || out.Start.X != 5 || out.Start.Y != 5 {
		t.Errorf("unexpected register output: %+v", out)
	}
}

func TestCheckForProblem(t *testing.T) {
	// Mock World HTTP Server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && r.URL.Path == "/problem-at" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"present": true,
				"type":    "dirt",
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	problemType, found := detector.CheckForProblem(server.URL, 5, 5)
	if !found {
		t.Error("expected to find a problem")
	}
	if problemType != "dirt" {
		t.Errorf("expected problem type 'dirt', got '%s'", problemType)
	}
}

func TestCheckForProblemNotFound(t *testing.T) {
	// Mock World HTTP Server - no problem at location
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && r.URL.Path == "/problem-at" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"present": false,
				"type":    "",
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	_, found := detector.CheckForProblem(server.URL, 5, 5)
	if found {
		t.Error("expected not to find a problem")
	}
}
