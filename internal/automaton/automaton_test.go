package automaton

import (
	"testing"
)

// runAutomaton feeds a string through an automaton byte-by-byte and returns whether it accepts.
func runAutomaton(a Automaton, input string) bool {
	state := a.Start()
	for i := 0; i < len(input); i++ {
		state = a.Step(state, input[i])
		if state == DeadState {
			return false
		}
	}
	return a.IsAccept(state)
}

// --- Prefix Automaton Tests ---

func TestPrefixAutomaton_Accepts(t *testing.T) {
	a := NewPrefixAutomaton([]byte("hel"))

	accepts := []string{"hel", "hell", "hello", "help", "helmet"}
	for _, s := range accepts {
		if !runAutomaton(a, s) {
			t.Errorf("PrefixAutomaton(hel) should accept %q", s)
		}
	}
}

func TestPrefixAutomaton_Rejects(t *testing.T) {
	a := NewPrefixAutomaton([]byte("hel"))

	rejects := []string{"he", "h", "world", "", "HEL"}
	for _, s := range rejects {
		if runAutomaton(a, s) {
			t.Errorf("PrefixAutomaton(hel) should reject %q", s)
		}
	}
}

func TestPrefixAutomaton_EmptyPrefix(t *testing.T) {
	a := NewPrefixAutomaton([]byte(""))

	// Empty prefix accepts everything.
	for _, s := range []string{"", "a", "hello", "anything"} {
		if !runAutomaton(a, s) {
			t.Errorf("PrefixAutomaton('') should accept %q", s)
		}
	}
}

func TestPrefixAutomaton_CanMatch(t *testing.T) {
	a := NewPrefixAutomaton([]byte("ab"))

	state := a.Start()
	if !a.CanMatch(state) {
		t.Error("start state should CanMatch")
	}

	state = a.Step(state, 'a')
	if !a.CanMatch(state) {
		t.Error("after 'a' should CanMatch")
	}

	dead := a.Step(a.Start(), 'x')
	if a.CanMatch(dead) {
		t.Error("dead state should not CanMatch")
	}
}

// --- Wildcard Automaton Tests ---

func TestWildcardAutomaton_Star(t *testing.T) {
	a, err := NewWildcardAutomaton([]byte("h*o"))
	if err != nil {
		t.Fatal(err)
	}

	accepts := []string{"ho", "heo", "hello", "hallo"}
	for _, s := range accepts {
		if !runAutomaton(a, s) {
			t.Errorf("Wildcard(h*o) should accept %q", s)
		}
	}

	rejects := []string{"h", "hello!", "world", "o"}
	for _, s := range rejects {
		if runAutomaton(a, s) {
			t.Errorf("Wildcard(h*o) should reject %q", s)
		}
	}
}

func TestWildcardAutomaton_Question(t *testing.T) {
	a, err := NewWildcardAutomaton([]byte("h?llo"))
	if err != nil {
		t.Fatal(err)
	}

	accepts := []string{"hallo", "hello", "hxllo"}
	for _, s := range accepts {
		if !runAutomaton(a, s) {
			t.Errorf("Wildcard(h?llo) should accept %q", s)
		}
	}

	rejects := []string{"hllo", "heello", "llo"}
	for _, s := range rejects {
		if runAutomaton(a, s) {
			t.Errorf("Wildcard(h?llo) should reject %q", s)
		}
	}
}

func TestWildcardAutomaton_LeadingStar(t *testing.T) {
	a, err := NewWildcardAutomaton([]byte("*tion"))
	if err != nil {
		t.Fatal(err)
	}

	accepts := []string{"tion", "action", "section", "mention"}
	for _, s := range accepts {
		if !runAutomaton(a, s) {
			t.Errorf("Wildcard(*tion) should accept %q", s)
		}
	}

	rejects := []string{"tio", "actions", ""}
	for _, s := range rejects {
		if runAutomaton(a, s) {
			t.Errorf("Wildcard(*tion) should reject %q", s)
		}
	}
}

func TestWildcardAutomaton_AllStar(t *testing.T) {
	a, err := NewWildcardAutomaton([]byte("*"))
	if err != nil {
		t.Fatal(err)
	}

	for _, s := range []string{"", "a", "hello", "anything"} {
		if !runAutomaton(a, s) {
			t.Errorf("Wildcard(*) should accept %q", s)
		}
	}
}

func TestWildcardAutomaton_ExactMatch(t *testing.T) {
	a, err := NewWildcardAutomaton([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}

	if !runAutomaton(a, "hello") {
		t.Error("should accept exact match")
	}
	if runAutomaton(a, "hell") {
		t.Error("should reject partial match")
	}
	if runAutomaton(a, "helloo") {
		t.Error("should reject longer string")
	}
}

func TestWildcardAutomaton_TooLong(t *testing.T) {
	pattern := make([]byte, MaxWildcardPatternLength+1)
	for i := range pattern {
		pattern[i] = 'a'
	}
	_, err := NewWildcardAutomaton(pattern)
	if err == nil {
		t.Error("expected error for pattern exceeding max length")
	}
}

// --- Levenshtein Automaton Tests ---

func TestLevenshteinAutomaton_ExactMatch(t *testing.T) {
	a, err := NewLevenshteinAutomaton([]byte("hello"), 1)
	if err != nil {
		t.Fatal(err)
	}

	if !runAutomaton(a, "hello") {
		t.Error("should accept exact match (0 edits)")
	}
}

func TestLevenshteinAutomaton_Substitution(t *testing.T) {
	a, err := NewLevenshteinAutomaton([]byte("hello"), 1)
	if err != nil {
		t.Fatal(err)
	}

	if !runAutomaton(a, "hallo") {
		t.Error("should accept 1 substitution")
	}
}

func TestLevenshteinAutomaton_Insertion(t *testing.T) {
	a, err := NewLevenshteinAutomaton([]byte("hello"), 1)
	if err != nil {
		t.Fatal(err)
	}

	if !runAutomaton(a, "helloo") {
		t.Error("should accept 1 insertion at end")
	}
}

func TestLevenshteinAutomaton_Rejects(t *testing.T) {
	a, err := NewLevenshteinAutomaton([]byte("hello"), 1)
	if err != nil {
		t.Fatal(err)
	}

	if runAutomaton(a, "world") {
		t.Error("should reject 'world' (5 edits)")
	}
}

func TestLevenshteinAutomaton_Distance0(t *testing.T) {
	a, err := NewLevenshteinAutomaton([]byte("cat"), 0)
	if err != nil {
		t.Fatal(err)
	}

	if !runAutomaton(a, "cat") {
		t.Error("should accept exact match with distance 0")
	}
	if runAutomaton(a, "bat") {
		t.Error("should reject 1 edit with distance 0")
	}
}

func TestLevenshteinAutomaton_MaxDistanceExceeded(t *testing.T) {
	_, err := NewLevenshteinAutomaton([]byte("hello"), 3)
	if err == nil {
		t.Error("expected error for distance > 2")
	}
}

func TestLevenshteinAutomaton_CanMatch(t *testing.T) {
	a, err := NewLevenshteinAutomaton([]byte("ab"), 1)
	if err != nil {
		t.Fatal(err)
	}

	if !a.CanMatch(a.Start()) {
		t.Error("start state should CanMatch")
	}
	if a.CanMatch(DeadState) {
		t.Error("dead state should not CanMatch")
	}
}
