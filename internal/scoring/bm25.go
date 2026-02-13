package scoring

import (
	"fmt"
	"math"
)

// Default BM25 parameters.
const (
	DefaultK1 = 1.2
	DefaultB  = 0.75
)

// BM25Scorer computes BM25 relevance scores.
// Scoring is per-segment using segment-local statistics (MVP).
type BM25Scorer struct {
	K1 float32
	B  float32

	// Per-segment statistics.
	DocCount  int64
	AvgDocLen float32
}

// NewBM25Scorer creates a scorer with default parameters and the given segment stats.
func NewBM25Scorer(docCount int64, avgDocLen float32) *BM25Scorer {
	return &BM25Scorer{
		K1:        DefaultK1,
		B:         DefaultB,
		DocCount:  docCount,
		AvgDocLen: avgDocLen,
	}
}

// IDF computes the Inverse Document Frequency for a term.
//
//	IDF(qi) = ln(1 + (N - n(qi) + 0.5) / (n(qi) + 0.5))
func (s *BM25Scorer) IDF(docFreq int64) float32 {
	n := float64(docFreq)
	N := float64(s.DocCount)
	return float32(math.Log(1 + (N - n + 0.5) / (n + 0.5)))
}

// Score computes the BM25 score for a single term in a document.
//
//	score = IDF × (tf × (k1 + 1)) / (tf + k1 × (1 - b + b × dl / avgdl))
func (s *BM25Scorer) Score(termFreq uint32, docLen uint32, idf float32) float32 {
	tf := float32(termFreq)
	dl := float32(docLen)

	numerator := tf * (s.K1 + 1)
	denominator := tf + s.K1*(1-s.B+s.B*dl/s.AvgDocLen)

	if denominator == 0 {
		return 0
	}
	return idf * numerator / denominator
}

// ScoreMultiTerm computes the total BM25 score for multiple query terms.
func (s *BM25Scorer) ScoreMultiTerm(terms []QueryTerm, docLen uint32) float32 {
	var total float32
	for _, qt := range terms {
		if qt.TermFreq == 0 {
			continue
		}
		idf := s.IDF(qt.DocFreq)
		termScore := s.Score(qt.TermFreq, docLen, idf)
		termScore *= qt.Boost
		total += termScore
	}
	return total
}

// QueryTerm holds per-term scoring inputs.
type QueryTerm struct {
	Term     string
	TermFreq uint32
	DocFreq  int64
	Boost    float32
}

// Explanation provides a human-readable breakdown of a score.
type Explanation struct {
	Description string        `json:"description"`
	Value       float32       `json:"value"`
	Details     []Explanation `json:"details,omitempty"`
}

// Explain returns a detailed breakdown of the BM25 score for a single term.
func (s *BM25Scorer) Explain(field, term string, termFreq uint32, docLen uint32, docFreq int64) Explanation {
	idf := s.IDF(docFreq)
	score := s.Score(termFreq, docLen, idf)

	tf := float32(termFreq)
	dl := float32(docLen)
	tfNorm := tf * (s.K1 + 1) / (tf + s.K1*(1-s.B+s.B*dl/s.AvgDocLen))

	return Explanation{
		Description: fmt.Sprintf("weight(%s:%s) [BM25]", field, term),
		Value:       score,
		Details: []Explanation{
			{
				Description: fmt.Sprintf("idf(docFreq=%d, N=%d)", docFreq, s.DocCount),
				Value:       idf,
			},
			{
				Description: fmt.Sprintf("tf(freq=%d, norm=%.4f)", termFreq, tfNorm),
				Value:       tfNorm,
			},
			{
				Description: fmt.Sprintf("dl=%d, avgdl=%.1f", docLen, s.AvgDocLen),
				Value:       s.B * dl / s.AvgDocLen,
			},
		},
	}
}
