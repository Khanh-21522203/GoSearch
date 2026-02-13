package index

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func testSchema() *Schema {
	return &Schema{
		Version:   1,
		CreatedAt: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
		Fields: []FieldDef{
			{Name: "id", Type: FieldTypeKeyword, Stored: true, Indexed: true},
			{Name: "title", Type: FieldTypeText, Analyzer: AnalyzerStandard, Stored: true, Indexed: true, Positions: true},
			{Name: "body", Type: FieldTypeText, Analyzer: AnalyzerStandard, Stored: false, Indexed: true, Positions: true},
			{Name: "tags", Type: FieldTypeKeyword, Stored: true, Indexed: true, MultiValued: true},
			{Name: "metadata", Type: FieldTypeStoredOnly, Stored: true, Indexed: false},
		},
		DefaultAnalyzer: AnalyzerStandard,
	}
}

func TestSchema_Validate_Valid(t *testing.T) {
	s := testSchema()
	if err := s.Validate(); err != nil {
		t.Fatalf("valid schema should pass validation: %v", err)
	}
}

func TestSchema_Validate_TooManyFields(t *testing.T) {
	s := &Schema{Version: 1}
	for i := 0; i <= MaxFieldsPerSchema; i++ {
		s.Fields = append(s.Fields, FieldDef{
			Name: fmt.Sprintf("field_%d", i), Type: FieldTypeKeyword, Indexed: true,
		})
	}
	err := s.Validate()
	if !errors.Is(err, ErrSchemaFieldLimit) {
		t.Errorf("expected ErrSchemaFieldLimit, got: %v", err)
	}
}

func TestSchema_Validate_ReservedField(t *testing.T) {
	for _, name := range []string{"_id", "_score", "_source"} {
		s := &Schema{
			Version: 1,
			Fields:  []FieldDef{{Name: name, Type: FieldTypeKeyword, Indexed: true}},
		}
		err := s.Validate()
		if !errors.Is(err, ErrSchemaReservedField) {
			t.Errorf("field %q: expected ErrSchemaReservedField, got: %v", name, err)
		}
	}
}

func TestSchema_Validate_DuplicateField(t *testing.T) {
	s := &Schema{
		Version: 1,
		Fields: []FieldDef{
			{Name: "title", Type: FieldTypeText, Analyzer: "standard", Indexed: true},
			{Name: "title", Type: FieldTypeKeyword, Indexed: true},
		},
	}
	err := s.Validate()
	if !errors.Is(err, ErrSchemaDuplicateField) {
		t.Errorf("expected ErrSchemaDuplicateField, got: %v", err)
	}
}

func TestSchema_Validate_InvalidType(t *testing.T) {
	s := &Schema{
		Version: 1,
		Fields:  []FieldDef{{Name: "f", Type: "invalid_type", Indexed: true}},
	}
	err := s.Validate()
	if !errors.Is(err, ErrSchemaInvalidType) {
		t.Errorf("expected ErrSchemaInvalidType, got: %v", err)
	}
}

func TestSchema_Validate_InvalidAnalyzer(t *testing.T) {
	s := &Schema{
		Version: 1,
		Fields:  []FieldDef{{Name: "f", Type: FieldTypeText, Analyzer: "bad_analyzer", Indexed: true}},
	}
	err := s.Validate()
	if !errors.Is(err, ErrSchemaInvalidAnalyzer) {
		t.Errorf("expected ErrSchemaInvalidAnalyzer, got: %v", err)
	}
}

func TestSchema_Validate_PositionsOnNonText(t *testing.T) {
	s := &Schema{
		Version: 1,
		Fields:  []FieldDef{{Name: "f", Type: FieldTypeKeyword, Indexed: true, Positions: true}},
	}
	err := s.Validate()
	if err == nil {
		t.Error("expected error for positions on keyword field")
	}
}

func TestSchema_Validate_StoredOnlyIndexed(t *testing.T) {
	s := &Schema{
		Version: 1,
		Fields:  []FieldDef{{Name: "f", Type: FieldTypeStoredOnly, Stored: true, Indexed: true}},
	}
	err := s.Validate()
	if err == nil {
		t.Error("expected error for stored_only field with indexed=true")
	}
}

func TestSchema_Validate_StoredOnlyNotStored(t *testing.T) {
	s := &Schema{
		Version: 1,
		Fields:  []FieldDef{{Name: "f", Type: FieldTypeStoredOnly, Stored: false, Indexed: false}},
	}
	err := s.Validate()
	if err == nil {
		t.Error("expected error for stored_only field with stored=false")
	}
}

func TestSchema_Validate_InvalidDefaultAnalyzer(t *testing.T) {
	s := &Schema{
		Version:         1,
		Fields:          []FieldDef{{Name: "f", Type: FieldTypeKeyword, Indexed: true}},
		DefaultAnalyzer: "nonexistent",
	}
	err := s.Validate()
	if !errors.Is(err, ErrSchemaInvalidAnalyzer) {
		t.Errorf("expected ErrSchemaInvalidAnalyzer, got: %v", err)
	}
}

func TestSchema_FieldID(t *testing.T) {
	s := testSchema()
	if id := s.FieldID("id"); id != 0 {
		t.Errorf("FieldID(id) = %d, want 0", id)
	}
	if id := s.FieldID("title"); id != 1 {
		t.Errorf("FieldID(title) = %d, want 1", id)
	}
	if id := s.FieldID("nonexistent"); id != -1 {
		t.Errorf("FieldID(nonexistent) = %d, want -1", id)
	}
}

func TestMarshalUnmarshalSchema_RoundTrip(t *testing.T) {
	s := testSchema()

	data, err := MarshalSchema(s)
	if err != nil {
		t.Fatal(err)
	}

	if s.Checksum == "" {
		t.Error("checksum should be populated after marshal")
	}

	got, err := UnmarshalSchema(data)
	if err != nil {
		t.Fatal(err)
	}

	if got.Version != s.Version {
		t.Errorf("Version = %d, want %d", got.Version, s.Version)
	}
	if len(got.Fields) != len(s.Fields) {
		t.Fatalf("Fields length = %d, want %d", len(got.Fields), len(s.Fields))
	}
	for i, f := range got.Fields {
		if f.Name != s.Fields[i].Name {
			t.Errorf("Field[%d].Name = %s, want %s", i, f.Name, s.Fields[i].Name)
		}
		if f.Type != s.Fields[i].Type {
			t.Errorf("Field[%d].Type = %s, want %s", i, f.Type, s.Fields[i].Type)
		}
	}
}

func TestUnmarshalSchema_TamperedData(t *testing.T) {
	s := testSchema()
	data, err := MarshalSchema(s)
	if err != nil {
		t.Fatal(err)
	}

	// Tamper with data.
	tampered := make([]byte, len(data))
	copy(tampered, data)
	for i := range tampered {
		if tampered[i] == '1' {
			tampered[i] = '2'
			break
		}
	}

	_, err = UnmarshalSchema(tampered)
	if err == nil {
		t.Error("expected error for tampered schema")
	}
	if !errors.Is(err, ErrSchemaCorrupt) {
		t.Errorf("expected ErrSchemaCorrupt, got: %v", err)
	}
}

func TestWriteLoadSchema_RoundTrip(t *testing.T) {
	dir := newTestDir(t)
	s := testSchema()

	if err := WriteSchema(dir, s); err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadSchema(dir)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Version != s.Version {
		t.Errorf("Version = %d, want %d", loaded.Version, s.Version)
	}
	if len(loaded.Fields) != len(s.Fields) {
		t.Errorf("Fields length = %d, want %d", len(loaded.Fields), len(s.Fields))
	}
	if loaded.DefaultAnalyzer != s.DefaultAnalyzer {
		t.Errorf("DefaultAnalyzer = %s, want %s", loaded.DefaultAnalyzer, s.DefaultAnalyzer)
	}
}
