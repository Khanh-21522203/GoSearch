package server

import "encoding/json"

// encodeJSON marshals v to indented JSON bytes.
func encodeJSON(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}
