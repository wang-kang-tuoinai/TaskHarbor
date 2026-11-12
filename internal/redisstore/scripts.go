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

//go:embed scripts/report_authority.lua
var reportAuthoritySource string

//go:embed scripts/complete_task.lua
var completeSource string

var completeScript = redis.NewScript(reportAuthoritySource + "\n" + completeSource)

//go:embed scripts/report_failure_task.lua
var failureSource string

var failureScript = redis.NewScript(reportAuthoritySource + "\n" + failureSource)

//go:embed scripts/reclaim_candidates.lua
var reclaimCandidatesSource string

//go:embed scripts/reclaim_leases.lua
var reclaimLeasesSource string

var reclaimLeasesScript = redis.NewScript(reclaimCandidatesSource + "\n" + reclaimLeasesSource)

//go:embed scripts/schedule_retries.lua
var scheduleRetriesSource string

var scheduleRetriesScript = redis.NewScript(reclaimCandidatesSource + "\n" + scheduleRetriesSource)
