package benchmark

import (
	"testing"

	"GoSearch/internal/automaton"
)

func BenchmarkAutomaton_Prefix_Short(b *testing.B) {
	prefix := []byte("hel")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		automaton.NewPrefixAutomaton(prefix)
	}
}

func BenchmarkAutomaton_Prefix_Long(b *testing.B) {
	prefix := []byte("internationalization")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		automaton.NewPrefixAutomaton(prefix)
	}
}

func BenchmarkAutomaton_Prefix_Run(b *testing.B) {
	a := automaton.NewPrefixAutomaton([]byte("hel"))
	input := []byte("hello")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state := a.Start()
		for _, ch := range input {
			state = a.Step(state, ch)
		}
		_ = a.IsAccept(state)
	}
}

func BenchmarkAutomaton_Wildcard_Simple(b *testing.B) {
	pattern := []byte("hel*")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		automaton.NewWildcardAutomaton(pattern)
	}
}

func BenchmarkAutomaton_Wildcard_Complex(b *testing.B) {
	pattern := []byte("*ell*")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		automaton.NewWildcardAutomaton(pattern)
	}
}

func BenchmarkAutomaton_Wildcard_Run(b *testing.B) {
	a, _ := automaton.NewWildcardAutomaton([]byte("h*o"))
	input := []byte("hello")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state := a.Start()
		for _, ch := range input {
			state = a.Step(state, ch)
		}
		_ = a.IsAccept(state)
	}
}

func BenchmarkAutomaton_Levenshtein_Dist1(b *testing.B) {
	target := []byte("hello")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		automaton.NewLevenshteinAutomaton(target, 1)
	}
}

func BenchmarkAutomaton_Levenshtein_Dist2(b *testing.B) {
	target := []byte("hello")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		automaton.NewLevenshteinAutomaton(target, 2)
	}
}

func BenchmarkAutomaton_Levenshtein_Run(b *testing.B) {
	a, _ := automaton.NewLevenshteinAutomaton([]byte("hello"), 1)
	input := []byte("hallo")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state := a.Start()
		for _, ch := range input {
			state = a.Step(state, ch)
		}
		_ = a.IsAccept(state)
	}
}
