package store

import (
	"strconv"
	"testing"
)

func TestNextStringID(t *testing.T) {
	// Generate a new ID
	id := NextStringID()

	// Check that the ID is not empty
	if id == "" {
		t.Error("NextStringID should return a non-empty string")
	}

	// Check that the ID can be parsed to an uint64
	if _, err := strconv.ParseUint(id, 10, 64); err != nil {
		t.Errorf("NextStringID should return a string that can be parsed to an uint64: %v", err)
	}
}
