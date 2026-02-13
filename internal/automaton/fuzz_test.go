package automaton

import (
	"testing"
)

func FuzzWildcardAutomaton(f *testing.F) {
	f.Add("hel*", "hello")
	f.Add("*orld", "world")
	f.Add("h?llo", "hello")
	f.Add("*", "anything")
	f.Add("", "")
	f.Add("a*b*c", "abc")
	f.Add("???", "abc")

	f.Fuzz(func(t *testing.T, pattern, input string) {
		if len(pattern) > MaxWildcardPatternLength {
			return
		}

		auto, err := NewWildcardAutomaton([]byte(pattern))
		if err != nil {
			return // Invalid pattern is acceptable.
		}

		// Run should not panic.
		state := auto.Start()
		for i := 0; i < len(input); i++ {
			state = auto.Step(state, input[i])
			if state == DeadState {
				break
			}
		}
		_ = auto.IsAccept(state)
		_ = auto.CanMatch(state)
	})
}

func FuzzLevenshteinAutomaton(f *testing.F) {
	f.Add("hello", 1, "hallo")
	f.Add("cat", 0, "cat")
	f.Add("test", 2, "tset")
	f.Add("", 1, "a")

	f.Fuzz(func(t *testing.T, target string, maxDist int, input string) {
		if maxDist < 0 || maxDist > MaxEditDistance {
			return
		}
		if len(target) > 100 {
			return
		}

		auto, err := NewLevenshteinAutomaton([]byte(target), maxDist)
		if err != nil {
			return
		}

		// Run should not panic.
		state := auto.Start()
		for i := 0; i < len(input); i++ {
			state = auto.Step(state, input[i])
			if state == DeadState {
				break
			}
		}
		_ = auto.IsAccept(state)
		_ = auto.CanMatch(state)
	})
}

func FuzzPrefixAutomaton(f *testing.F) {
	f.Add("hel", "hello")
	f.Add("", "anything")
	f.Add("abc", "ab")

	f.Fuzz(func(t *testing.T, prefix, input string) {
		if len(prefix) > 1000 {
			return
		}

		auto := NewPrefixAutomaton([]byte(prefix))

		state := auto.Start()
		for i := 0; i < len(input); i++ {
			state = auto.Step(state, input[i])
			if state == DeadState {
				break
			}
		}
		_ = auto.IsAccept(state)
		_ = auto.CanMatch(state)
	})
}
