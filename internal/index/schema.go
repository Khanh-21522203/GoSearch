package index

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"GoSearch/internal/storage"
)

// Field type constants.
const (
	FieldTypeText       = "text"
	FieldTypeKeyword    = "keyword"
	FieldTypeStoredOnly = "stored_only"
)

// Analyzer constants.
const (
	AnalyzerStandard   = "standard"
	AnalyzerWhitespace = "whitespace"
	AnalyzerKeyword    = "keyword"
)

// Schema limits.
const (
	MaxFieldsPerSchema  = 256
	MaxFieldNameLength  = 255
	MaxAnalyzerCount    = 64
)

// Reserved field names that cannot be used in user schemas.
var reservedFieldNames = map[string]bool{
	"_id":     true,
	"_score":  true,
	"_source": true,
}

var (
	ErrSchemaCorrupt       = errors.New("schema checksum verification failed")
	ErrSchemaFieldLimit    = errors.New("schema exceeds maximum field count")
	ErrSchemaReservedField = errors.New("field name is reserved")
	ErrSchemaDuplicateField = errors.New("duplicate field name")
	ErrSchemaInvalidType   = errors.New("invalid field type")
	ErrSchemaInvalidAnalyzer  = errors.New("invalid analyzer")
	ErrSchemaFieldNameTooLong = errors.New("field name exceeds maximum length")
	ErrSchemaMissingAnalyzer  = errors.New("text field requires an analyzer")
)

// Schema represents the immutable schema definition for an index.
type Schema struct {
	Version         uint32           `json:"version"`
	CreatedAt       time.Time        `json:"created_at"`
	Fields          []FieldDef       `json:"fields"`
	DefaultAnalyzer string           `json:"default_analyzer"`
	Checksum        storage.Checksum `json:"checksum"`
}

// FieldDef defines a single field in the schema.
type FieldDef struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Analyzer    string `json:"analyzer,omitempty"`
	Stored      bool   `json:"stored"`
	Indexed     bool   `json:"indexed"`
	Positions   bool   `json:"positions,omitempty"`
	MultiValued bool   `json:"multi_valued,omitempty"`
}

// FieldID returns the uint8 field ID for the given field name.
// Returns -1 if not found.
func (s *Schema) FieldID(name string) int {
	for i, f := range s.Fields {
		if f.Name == name {
			return i
		}
	}
	return -1
}

// Validate checks the schema for correctness.
func (s *Schema) Validate() error {
	if len(s.Fields) > MaxFieldsPerSchema {
		return fmt.Errorf("%w: %d fields (max %d)", ErrSchemaFieldLimit, len(s.Fields), MaxFieldsPerSchema)
	}

	seen := make(map[string]bool, len(s.Fields))
	for _, f := range s.Fields {
		if reservedFieldNames[f.Name] {
			return fmt.Errorf("%w: %q", ErrSchemaReservedField, f.Name)
		}
		if seen[f.Name] {
			return fmt.Errorf("%w: %q", ErrSchemaDuplicateField, f.Name)
		}
		seen[f.Name] = true

		if len(f.Name) > MaxFieldNameLength {
			return fmt.Errorf("%w: %q (%d bytes, max %d)", ErrSchemaFieldNameTooLong, f.Name, len(f.Name), MaxFieldNameLength)
		}
		if err := validateFieldType(f.Type); err != nil {
			return fmt.Errorf("field %q: %w", f.Name, err)
		}
		if f.Analyzer != "" {
			if err := validateAnalyzer(f.Analyzer); err != nil {
				return fmt.Errorf("field %q: %w", f.Name, err)
			}
		}
		if f.Type == FieldTypeText && f.Analyzer == "" {
			return fmt.Errorf("field %q: %w", f.Name, ErrSchemaMissingAnalyzer)
		}
		if f.Positions && f.Type != FieldTypeText {
			return fmt.Errorf("field %q: positions only allowed on text fields", f.Name)
		}
		if f.Type == FieldTypeStoredOnly {
			if f.Indexed {
				return fmt.Errorf("field %q: stored_only fields cannot be indexed", f.Name)
			}
			if !f.Stored {
				return fmt.Errorf("field %q: stored_only fields must be stored", f.Name)
			}
		}
	}

	if s.DefaultAnalyzer != "" {
		if err := validateAnalyzer(s.DefaultAnalyzer); err != nil {
			return fmt.Errorf("default_analyzer: %w", err)
		}
	}

	return nil
}

// MarshalSchema serializes a schema to JSON and computes its checksum.
func MarshalSchema(s *Schema) ([]byte, error) {
	checksum, err := computeSchemaChecksum(s)
	if err != nil {
		return nil, fmt.Errorf("compute schema checksum: %w", err)
	}
	s.Checksum = checksum

	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal schema: %w", err)
	}
	return data, nil
}

// UnmarshalSchema deserializes a schema from JSON and verifies its checksum.
func UnmarshalSchema(data []byte) (*Schema, error) {
	var s Schema
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("unmarshal schema: %w", err)
	}

	savedChecksum := s.Checksum
	computed, err := computeSchemaChecksum(&s)
	if err != nil {
		return nil, fmt.Errorf("compute schema checksum for verification: %w", err)
	}
	if computed != savedChecksum {
		return nil, fmt.Errorf("%w: expected %s, got %s", ErrSchemaCorrupt, savedChecksum, computed)
	}

	return &s, nil
}

// WriteSchema atomically writes a schema file to the index directory.
// The schema is immutable after creation.
func WriteSchema(dir *IndexDir, s *Schema) error {
	data, err := MarshalSchema(s)
	if err != nil {
		return fmt.Errorf("marshal schema: %w", err)
	}

	tmpPath := fmt.Sprintf("%s.tmp", dir.SchemaPath())
	if err := storage.WriteFileSync(tmpPath, data, storage.FilePerm); err != nil {
		return fmt.Errorf("write tmp schema: %w", err)
	}

	if err := os.Rename(tmpPath, dir.SchemaPath()); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename schema: %w", err)
	}

	if err := storage.FsyncDir(dir.Root); err != nil {
		return fmt.Errorf("fsync index root after schema write: %w", err)
	}

	return nil
}

// LoadSchema reads and verifies a schema file from the index directory.
func LoadSchema(dir *IndexDir) (*Schema, error) {
	data, err := os.ReadFile(dir.SchemaPath())
	if err != nil {
		return nil, fmt.Errorf("read schema: %w", err)
	}
	return UnmarshalSchema(data)
}

func computeSchemaChecksum(s *Schema) (storage.Checksum, error) {
	saved := s.Checksum
	s.Checksum = ""
	defer func() { s.Checksum = saved }()

	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal for checksum: %w", err)
	}
	return storage.ComputeChecksum(data), nil
}

func validateFieldType(t string) error {
	switch t {
	case FieldTypeText, FieldTypeKeyword, FieldTypeStoredOnly:
		return nil
	default:
		return fmt.Errorf("%w: %q", ErrSchemaInvalidType, t)
	}
}

func validateAnalyzer(a string) error {
	switch a {
	case AnalyzerStandard, AnalyzerWhitespace, AnalyzerKeyword:
		return nil
	default:
		return fmt.Errorf("%w: %q", ErrSchemaInvalidAnalyzer, a)
	}
}
