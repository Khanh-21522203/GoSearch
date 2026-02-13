package analysis

import (
	"testing"
)

func TestStandardAnalyzer(t *testing.T) {
	a := NewStandardAnalyzer()

	tests := []struct {
		name   string
		input  string
		want   []string
	}{
		{"basic", "The Quick Brown Fox", []string{"the", "quick", "brown", "fox"}},
		{"empty", "", nil},
		{"punctuation", "hello, world! foo-bar", []string{"hello", "world", "foo", "bar"}},
		{"numbers", "test123 456abc", []string{"test123", "456abc"}},
		{"unicode", "café résumé", []string{"café", "résumé"}},
		{"mixed whitespace", "  hello   world  ", []string{"hello", "world"}},
		{"single word", "hello", []string{"hello"}},
		{"uppercase", "HELLO WORLD", []string{"hello", "world"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens := a.Analyze("field", tt.input)
			got := tokenTerms(tokens)
			if !stringSliceEqual(got, tt.want) {
				t.Errorf("Analyze(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestStandardAnalyzer_Positions(t *testing.T) {
	a := NewStandardAnalyzer()
	tokens := a.Analyze("field", "The Quick Brown Fox")

	for i, tok := range tokens {
		if tok.Position != i {
			t.Errorf("token %q position = %d, want %d", tok.Term, tok.Position, i)
		}
	}
}

func TestStandardAnalyzer_ByteOffsets(t *testing.T) {
	a := NewStandardAnalyzer()
	input := "hello world"
	tokens := a.Analyze("field", input)

	if len(tokens) != 2 {
		t.Fatalf("expected 2 tokens, got %d", len(tokens))
	}
	if tokens[0].StartByte != 0 || tokens[0].EndByte != 5 {
		t.Errorf("token 0 offsets = (%d, %d), want (0, 5)", tokens[0].StartByte, tokens[0].EndByte)
	}
	if tokens[1].StartByte != 6 || tokens[1].EndByte != 11 {
		t.Errorf("token 1 offsets = (%d, %d), want (6, 11)", tokens[1].StartByte, tokens[1].EndByte)
	}
}

func TestWhitespaceAnalyzer(t *testing.T) {
	a := NewWhitespaceAnalyzer()

	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{"basic", "The Quick Brown Fox", []string{"The", "Quick", "Brown", "Fox"}},
		{"empty", "", nil},
		{"preserves case", "Hello WORLD", []string{"Hello", "WORLD"}},
		{"preserves punctuation", "hello, world!", []string{"hello,", "world!"}},
		{"multiple spaces", "  hello   world  ", []string{"hello", "world"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens := a.Analyze("field", tt.input)
			got := tokenTerms(tokens)
			if !stringSliceEqual(got, tt.want) {
				t.Errorf("Analyze(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestWhitespaceAnalyzer_Positions(t *testing.T) {
	a := NewWhitespaceAnalyzer()
	tokens := a.Analyze("field", "The Quick Brown")

	for i, tok := range tokens {
		if tok.Position != i {
			t.Errorf("token %q position = %d, want %d", tok.Term, tok.Position, i)
		}
	}
}

func TestKeywordAnalyzer(t *testing.T) {
	a := NewKeywordAnalyzer()

	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{"basic", "The Quick Brown Fox", []string{"The Quick Brown Fox"}},
		{"empty", "", nil},
		{"single word", "hello", []string{"hello"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens := a.Analyze("field", tt.input)
			got := tokenTerms(tokens)
			if !stringSliceEqual(got, tt.want) {
				t.Errorf("Analyze(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestKeywordAnalyzer_SingleToken(t *testing.T) {
	a := NewKeywordAnalyzer()
	tokens := a.Analyze("field", "hello world")

	if len(tokens) != 1 {
		t.Fatalf("expected 1 token, got %d", len(tokens))
	}
	if tokens[0].Position != 0 {
		t.Errorf("position = %d, want 0", tokens[0].Position)
	}
	if tokens[0].StartByte != 0 || tokens[0].EndByte != 11 {
		t.Errorf("offsets = (%d, %d), want (0, 11)", tokens[0].StartByte, tokens[0].EndByte)
	}
}

func TestRegistry_BuiltinAnalyzers(t *testing.T) {
	r := NewRegistry()

	for _, name := range []string{"standard", "whitespace", "keyword"} {
		a, err := r.Get(name)
		if err != nil {
			t.Errorf("Get(%q) error: %v", name, err)
		}
		if a == nil {
			t.Errorf("Get(%q) returned nil", name)
		}
	}
}

func TestRegistry_UnknownAnalyzer(t *testing.T) {
	r := NewRegistry()
	_, err := r.Get("nonexistent")
	if err == nil {
		t.Error("expected error for unknown analyzer")
	}
}

func TestRegistry_Register(t *testing.T) {
	r := NewRegistry()
	custom := NewKeywordAnalyzer()

	if err := r.Register("custom", custom); err != nil {
		t.Fatal(err)
	}

	a, err := r.Get("custom")
	if err != nil {
		t.Fatal(err)
	}
	if a == nil {
		t.Error("custom analyzer should not be nil")
	}
}

func TestRegistry_RegisterDuplicate(t *testing.T) {
	r := NewRegistry()
	err := r.Register("standard", NewStandardAnalyzer())
	if err == nil {
		t.Error("expected error for duplicate registration")
	}
}

func TestRegistry_Names(t *testing.T) {
	r := NewRegistry()
	names := r.Names()
	if len(names) != 3 {
		t.Errorf("expected 3 names, got %d", len(names))
	}
}

func tokenTerms(tokens []Token) []string {
	if len(tokens) == 0 {
		return nil
	}
	terms := make([]string, len(tokens))
	for i, t := range tokens {
		terms[i] = t.Term
	}
	return terms
}

func stringSliceEqual(a, b []string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
