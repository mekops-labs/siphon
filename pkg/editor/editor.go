package editor

import (
	"embed"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

//go:embed index.html
var ui embed.FS

// Start launches a standalone web server serving the Ace.js editor.
func Start(port int, configPath string) {
	mux := http.NewServeMux()

	// 1. Serve the embedded HTML UI
	mux.Handle("/", http.FileServer(http.FS(ui)))

	// 2. API Endpoint to Read/Write the YAML file
	mux.HandleFunc("/api/config", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {

		case http.MethodGet:
			data, err := os.ReadFile(configPath)
			if err != nil {
				http.Error(w, "Failed to read config", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/yaml")
			w.Write(data)

		case http.MethodPost:
			body, err := io.ReadAll(r.Body)
			if err != nil || len(body) == 0 {
				http.Error(w, "Invalid request body", http.StatusBadRequest)
				return
			}

			// Save the file
			if err := os.WriteFile(configPath, body, 0644); err != nil {
				log.Printf("Editor: Failed to write config: %v", err)
				http.Error(w, "Failed to save file", http.StatusInternalServerError)
				return
			}

			log.Println("Editor: config.yaml updated via Web UI")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Saved"))

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	addr := fmt.Sprintf("0.0.0.0:%d", port)
	log.Printf("Starting Embedded Editor on http://%s", addr)

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Editor server crashed: %v", err)
	}
}
