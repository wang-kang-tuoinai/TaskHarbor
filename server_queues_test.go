package taskq

import (
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestNewServerRejectsInvalidConsumptionQueues(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{MaxRetries: -1, ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = rdb.Close() })
	for _, tc := range []struct {
		name   string
		queues []string
	}{
		{"empty list", nil},
		{"empty name", []string{""}},
		{"reserved", []string{"meta"}},
		{"invalid name", []string{"mail", "sms:urgent"}},
		{"oversize name", []string{strings.Repeat("a", 65)}},
		{"duplicate", []string{"mail", "sms", "mail"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if s, err := NewServer(rdb, WithQueues(tc.queues...)); err == nil || s != nil {
				t.Fatalf("NewServer with queues %q = %v, %v", tc.queues, s, err)
			}
		})
	}
}
