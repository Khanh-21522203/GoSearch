package analysis

import (
	"testing"
)

func FuzzStandardAnalyzer(f *testing.F) {
	f.Add("Hello World")
	f.Add("")
	f.Add("  spaces  everywhere  ")
	f.Add("café résumé naïve")
	f.Add("hello-world foo_bar")
	f.Add("123 456 789")

	f.Fuzz(func(t *testing.T, input string) {
		a := NewStandardAnalyzer()
		// Should not panic.
		tokens := a.Analyze("field", input)

		for i, tok := range tokens {
			if tok.Position != i {
				t.Errorf("token %d position = %d, want %d", i, tok.Position, i)
			}
			if tok.StartByte < 0 || tok.EndByte > len(input) || tok.StartByte > tok.EndByte {
				t.Errorf("invalid byte offsets: start=%d end=%d input_len=%d", tok.StartByte, tok.EndByte, len(input))
			}
			if tok.Term == "" {
				t.Error("empty term produced")
			}
		}
	})
}

func FuzzWhitespaceAnalyzer(f *testing.F) {
	f.Add("Hello World")
	f.Add("")
	f.Add("\t\n\r mixed whitespace")

	f.Fuzz(func(t *testing.T, input string) {
		a := NewWhitespaceAnalyzer()
		tokens := a.Analyze("field", input)

		for i, tok := range tokens {
			if tok.Position != i {
				t.Errorf("token %d position = %d, want %d", i, tok.Position, i)
			}
			if tok.Term == "" {
				t.Error("empty term produced")
			}
		}
	})
}
