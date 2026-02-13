package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"GoSearch/internal/server"
)

// Version is set at build time via -ldflags.
var Version = "dev"

func main() {
	configPath := flag.String("config", "", "path to config file")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(getEnv("GOTEXTSEARCH_LOG_LEVEL", "info")),
	}))
	slog.SetDefault(logger)

	port := getEnv("GOTEXTSEARCH_PORT", "8080")
	dataDir := getEnv("GOTEXTSEARCH_DATA_DIR", "data")

	logger.Info("starting GoSearch",
		"version", Version,
		"port", port,
		"data_dir", dataDir,
		"config", *configPath,
	)

	// Initialize index manager (loads existing indexes, runs recovery).
	mgr, err := server.NewIndexManager(dataDir, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize index manager: %v\n", err)
		os.Exit(1)
	}

	// Create HTTP handler and register API routes.
	handler := server.NewHandler(mgr, logger)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	// Health check endpoint.
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":  "healthy",
			"version": Version,
		})
	})

	// Readiness probe.
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": "ready",
		})
	})

	// Root info endpoint.
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"name":    "GoSearch",
			"version": Version,
		})
	})

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	logger.Info("listening", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseLogLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
