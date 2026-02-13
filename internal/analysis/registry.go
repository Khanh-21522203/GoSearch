package analysis

import (
	"fmt"
	"sync"
)

// Registry manages analyzer instances by name.
// Analyzer instances are reused via sync.Pool to avoid allocations.
type Registry struct {
	analyzers map[string]Analyzer
	mu        sync.RWMutex
}

// NewRegistry creates a Registry with the built-in analyzers registered.
func NewRegistry() *Registry {
	r := &Registry{
		analyzers: make(map[string]Analyzer),
	}
	r.analyzers["standard"] = NewStandardAnalyzer()
	r.analyzers["whitespace"] = NewWhitespaceAnalyzer()
	r.analyzers["keyword"] = NewKeywordAnalyzer()
	return r
}

// Get returns the analyzer registered under the given name.
func (r *Registry) Get(name string) (Analyzer, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	a, ok := r.analyzers[name]
	if !ok {
		return nil, fmt.Errorf("unknown analyzer: %q", name)
	}
	return a, nil
}

// Register adds a custom analyzer to the registry.
func (r *Registry) Register(name string, a Analyzer) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.analyzers[name]; exists {
		return fmt.Errorf("analyzer already registered: %q", name)
	}
	r.analyzers[name] = a
	return nil
}

// Names returns the names of all registered analyzers.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.analyzers))
	for name := range r.analyzers {
		names = append(names, name)
	}
	return names
}
