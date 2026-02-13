package scoring

import (
	"math"
	"testing"
)

func TestBM25Scorer_IDF(t *testing.T) {
	s := NewBM25Scorer(10000, 25.0)

	tests := []struct {
		name    string
		docFreq int64
		wantPos bool // IDF should be positive
	}{
		{"rare term", 10, true},
		{"common term", 5000, true},
		{"very common", 9999, true},
		{"single doc", 1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idf := s.IDF(tt.docFreq)
			if tt.wantPos && idf <= 0 {
				t.Errorf("IDF(%d) = %f, want > 0", tt.docFreq, idf)
			}
		})
	}

	// Rare terms should have higher IDF than common terms.
	rareIDF := s.IDF(10)
	commonIDF := s.IDF(5000)
	if rareIDF <= commonIDF {
		t.Errorf("rare IDF (%f) should be > common IDF (%f)", rareIDF, commonIDF)
	}
}

func TestBM25Scorer_Score(t *testing.T) {
	s := NewBM25Scorer(10000, 25.0)
	idf := s.IDF(100)

	// Basic score should be positive.
	score := s.Score(3, 25, idf)
	if score <= 0 {
		t.Errorf("Score = %f, want > 0", score)
	}

	// Higher term frequency should give higher score (with saturation).
	scoreLow := s.Score(1, 25, idf)
	scoreHigh := s.Score(10, 25, idf)
	if scoreHigh <= scoreLow {
		t.Errorf("higher tf should give higher score: tf=1 → %f, tf=10 → %f", scoreLow, scoreHigh)
	}

	// Shorter documents should score higher (length normalization).
	scoreShort := s.Score(3, 10, idf)
	scoreLong := s.Score(3, 100, idf)
	if scoreShort <= scoreLong {
		t.Errorf("shorter doc should score higher: dl=10 → %f, dl=100 → %f", scoreShort, scoreLong)
	}
}

func TestBM25Scorer_ScoreZeroFreq(t *testing.T) {
	s := NewBM25Scorer(10000, 25.0)
	idf := s.IDF(100)

	score := s.Score(0, 25, idf)
	if score != 0 {
		t.Errorf("Score with tf=0 = %f, want 0", score)
	}
}

func TestBM25Scorer_DefaultParams(t *testing.T) {
	s := NewBM25Scorer(1000, 20.0)
	if s.K1 != DefaultK1 {
		t.Errorf("K1 = %f, want %f", s.K1, DefaultK1)
	}
	if s.B != DefaultB {
		t.Errorf("B = %f, want %f", s.B, DefaultB)
	}
}

func TestBM25Scorer_ScoreMultiTerm(t *testing.T) {
	s := NewBM25Scorer(10000, 25.0)

	terms := []QueryTerm{
		{Term: "hello", TermFreq: 3, DocFreq: 100, Boost: 1.0},
		{Term: "world", TermFreq: 1, DocFreq: 500, Boost: 1.0},
	}

	score := s.ScoreMultiTerm(terms, 25)
	if score <= 0 {
		t.Errorf("multi-term score = %f, want > 0", score)
	}

	// Score with boost should be higher.
	boostedTerms := []QueryTerm{
		{Term: "hello", TermFreq: 3, DocFreq: 100, Boost: 2.0},
		{Term: "world", TermFreq: 1, DocFreq: 500, Boost: 1.0},
	}
	boostedScore := s.ScoreMultiTerm(boostedTerms, 25)
	if boostedScore <= score {
		t.Errorf("boosted score (%f) should be > unboosted (%f)", boostedScore, score)
	}
}

func TestBM25Scorer_ScoreMultiTerm_ZeroFreq(t *testing.T) {
	s := NewBM25Scorer(10000, 25.0)

	terms := []QueryTerm{
		{Term: "hello", TermFreq: 0, DocFreq: 100, Boost: 1.0},
	}

	score := s.ScoreMultiTerm(terms, 25)
	if score != 0 {
		t.Errorf("score with zero freq = %f, want 0", score)
	}
}

func TestBM25Scorer_Explain(t *testing.T) {
	s := NewBM25Scorer(10000, 25.0)

	exp := s.Explain("title", "search", 3, 15, 500)

	if exp.Value <= 0 {
		t.Errorf("explanation value = %f, want > 0", exp.Value)
	}
	if len(exp.Details) != 3 {
		t.Errorf("expected 3 detail entries, got %d", len(exp.Details))
	}
	if exp.Description == "" {
		t.Error("description should not be empty")
	}
}

func TestBM25Scorer_IDFFormula(t *testing.T) {
	// Verify the exact IDF formula: ln(1 + (N - n + 0.5) / (n + 0.5))
	s := NewBM25Scorer(100, 10.0)
	idf := s.IDF(10)

	expected := float32(math.Log(1 + (100.0 - 10.0 + 0.5) / (10.0 + 0.5)))
	if math.Abs(float64(idf-expected)) > 0.001 {
		t.Errorf("IDF = %f, want %f", idf, expected)
	}
}

func TestBM25Scorer_ParameterEffects(t *testing.T) {
	// Low k1: quick saturation.
	sLowK1 := &BM25Scorer{K1: 0.1, B: 0.75, DocCount: 1000, AvgDocLen: 20}
	// High k1: more weight to repeated terms.
	sHighK1 := &BM25Scorer{K1: 3.0, B: 0.75, DocCount: 1000, AvgDocLen: 20}

	idf := sLowK1.IDF(100)

	// With low k1, tf=1 and tf=10 should give similar scores.
	diffLow := sLowK1.Score(10, 20, idf) - sLowK1.Score(1, 20, idf)
	diffHigh := sHighK1.Score(10, 20, idf) - sHighK1.Score(1, 20, idf)

	if diffHigh <= diffLow {
		t.Errorf("high k1 should amplify tf difference more: low=%f, high=%f", diffLow, diffHigh)
	}
}
