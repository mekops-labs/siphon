package editor

import (
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

//go:embed index.html
var ui embed.FS

func newMux(configPath string, reload chan<- struct{}, status *Status) *http.ServeMux {
	mux := http.NewServeMux()

	mux.Handle("/", http.FileServer(http.FS(ui)))

	mux.HandleFunc("/api/config", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {

		case http.MethodGet:
			data, err := os.ReadFile(configPath)
			if err != nil {
				http.Error(w, "Failed to read config", http.StatusInternalServerError)
				log.Printf("Editor: Failed to read config (%s): %v", configPath, err)
				return
			}
			w.Header().Set("Content-Type", "text/yaml")
			w.Write(data)

		case http.MethodPost:
			body, err := io.ReadAll(r.Body)
			if err != nil || len(body) == 0 {
				http.Error(w, "Invalid request body", http.StatusBadRequest)
				log.Printf("Editor: Failed to read request body: %v", err)
				return
			}

			if err := os.WriteFile(configPath, body, 0644); err != nil {
				log.Printf("Editor: Failed to write config (%s): %v", configPath, err)
				http.Error(w, "Failed to save file", http.StatusInternalServerError)
				return
			}

			log.Println("Editor: config.yaml updated via Web UI — signalling reload")
			select {
			case reload <- struct{}{}:
			default:
			}

			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Saved"))

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		ok, message := status.Get()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"ok":      ok,
			"message": message,
		})
	})

	return mux
}

// Start launches the config editor web server. It runs for the lifetime of the
// process and is intentionally independent of the engine lifecycle, so it
// remains reachable even when the engine fails to start due to config errors.
func Start(port int, configPath string, reload chan<- struct{}, status *Status) {
	mux := newMux(configPath, reload, status)

	addr := fmt.Sprintf("0.0.0.0:%d", port)
	log.Printf("Starting embedded config editor on http://%s", addr)

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Editor server crashed: %v", err)
	}
}
