package redisstore

import (
	_ "embed"

	"github.com/redis/go-redis/v9"
)

//go:embed scripts/submit_task.lua
var submitSource string

var submitScript = redis.NewScript(submitSource)
