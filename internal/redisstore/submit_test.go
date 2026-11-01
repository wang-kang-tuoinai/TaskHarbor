package redisstore

import (
	"errors"
	"testing"
)

func TestParseSubmitResponse(t *testing.T) {
	for _, status := range []string{"created", "duplicate"} {
		result, err := parseSubmitResponse([]any{status, "id"}, "id")
		if err != nil || result == nil || result.TaskID != "id" || result.Created != (status == "created") {
			t.Fatalf("%s: result=%+v err=%v", status, result, err)
		}
	}
	for _, tc := range []struct {
		name  string
		value any
		want  error
	}{
		{"conflict", []any{"conflict", "id"}, ErrConflict},
		{"incomplete", []any{"incomplete", "id"}, ErrIncomplete},
		{"nil", nil, ErrInvalidResponse},
		{"scalar", "created", ErrInvalidResponse},
		{"short", []any{"created"}, ErrInvalidResponse},
		{"long", []any{"created", "id", "extra"}, ErrInvalidResponse},
		{"nil-status", []any{nil, "id"}, ErrInvalidResponse},
		{"integer-status", []any{int64(1), "id"}, ErrInvalidResponse},
		{"nil-id", []any{"created", nil}, ErrInvalidResponse},
		{"integer-id", []any{"created", int64(1)}, ErrInvalidResponse},
		{"wrong-id", []any{"created", "other"}, ErrInvalidResponse},
		{"unknown", []any{"not_found", "id"}, ErrInvalidResponse},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := parseSubmitResponse(tc.value, "id")
			if result != nil || !errors.Is(err, tc.want) {
				t.Fatalf("result=%+v err=%v, want %v", result, err, tc.want)
			}
		})
	}
}
