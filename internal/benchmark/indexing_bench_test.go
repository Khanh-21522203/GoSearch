package benchmark

import (
	"fmt"
	"testing"

	"GoSearch/internal/analysis"
	"GoSearch/internal/index"
	"GoSearch/internal/indexing"
)

func benchSchema() *index.Schema {
	return &index.Schema{
		Version:         1,
		DefaultAnalyzer: "standard",
		Fields: []index.FieldDef{
			{Name: "id", Type: index.FieldTypeKeyword, Stored: true, Indexed: true},
			{Name: "title", Type: index.FieldTypeText, Analyzer: "standard", Stored: true, Indexed: true, Positions: true},
			{Name: "body", Type: index.FieldTypeText, Analyzer: "standard", Stored: false, Indexed: true, Positions: true},
			{Name: "tags", Type: index.FieldTypeKeyword, Stored: true, Indexed: true, MultiValued: true},
		},
	}
}

func smallDoc(i int) indexing.Document {
	return indexing.Document{Fields: map[string]interface{}{
		"id":    fmt.Sprintf("doc-%d", i),
		"title": "Introduction to Search Engines",
		"body":  "Full-text search is a technique for searching documents.",
		"tags":  []interface{}{"search", "tutorial"},
	}}
}

func largeDoc(i int) indexing.Document {
	body := "Full-text search is a technique for searching documents stored in a database. " +
		"It involves indexing the content of documents and building inverted indexes that map " +
		"terms to the documents containing them. Modern search engines use sophisticated ranking " +
		"algorithms like BM25 to estimate the relevance of documents to a given query. " +
		"The query processing pipeline includes parsing, analysis, rewriting, planning, and execution. " +
		"Boolean queries combine multiple terms using AND, OR, and NOT operators. " +
		"Phrase queries require position information to verify that terms appear in sequence. " +
		"Fuzzy queries use Levenshtein automata to find terms within an edit distance. " +
		"Wildcard queries compile patterns into deterministic finite automata for efficient matching. " +
		"Segment merging reduces the number of segments to improve query performance over time."

	return indexing.Document{Fields: map[string]interface{}{
		"id":    fmt.Sprintf("doc-%d", i),
		"title": "Comprehensive Guide to Building Search Engines from Scratch",
		"body":  body,
		"tags":  []interface{}{"search", "tutorial", "advanced", "indexing"},
	}}
}

func BenchmarkIndexing_SmallDocs(b *testing.B) {
	schema := benchSchema()
	registry := analysis.NewRegistry()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := indexing.NewWriter(schema, registry)
		for j := 0; j < 100; j++ {
			w.AddDocument(smallDoc(j))
		}
	}
}

func BenchmarkIndexing_LargeDocs(b *testing.B) {
	schema := benchSchema()
	registry := analysis.NewRegistry()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := indexing.NewWriter(schema, registry)
		for j := 0; j < 100; j++ {
			w.AddDocument(largeDoc(j))
		}
	}
}

func BenchmarkIndexing_SingleDoc(b *testing.B) {
	schema := benchSchema()
	registry := analysis.NewRegistry()
	w := indexing.NewWriter(schema, registry)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.AddDocument(indexing.Document{Fields: map[string]interface{}{
			"id":    fmt.Sprintf("doc-%d", i),
			"title": "Quick brown fox jumps over the lazy dog",
			"body":  "The five boxing wizards jump quickly at dawn.",
		}})
	}
}
