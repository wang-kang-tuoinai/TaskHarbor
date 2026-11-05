package redisstore

import (
	_ "embed"

	"github.com/redis/go-redis/v9"
)

//go:embed scripts/submit_task.lua
var submitSource string

var submitScript = redis.NewScript(submitSource)

//go:embed scripts/claim_task.lua
var claimSource string

var claimScript = redis.NewScript(claimSource)

//go:embed scripts/renew_task.lua
var renewSource string

var renewScript = redis.NewScript(renewSource)
