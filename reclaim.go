package taskq

import (
	"context"
	"math"
	"math/rand/v2"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

const reclaimRequestTimeout = 3 * time.Second

func (s *server) reclaimLoop() {
	defer close(s.reclaimDone)
	phases := [...]func(context.Context, *redis.Client) error{
		redisstore.ReclaimLeases,
		redisstore.ScheduleRetries,
	}
	for waitContext(s.reclaimCtx, s.reclaimDelay()) {
		for _, phase := range phases {
			ctx, cancel := context.WithTimeout(s.reclaimCtx, reclaimRequestTimeout)
			err := phase(ctx, s.reclaimClient)
			cancel()
			if s.reclaimCtx.Err() != nil {
				return
			}
			if err == nil {
				continue
			}
			if transientRedisError(err) {
				// 结果可能已生效，下一轮从当前索引重新选择候选。
				break
			}
			s.reportFatal(err)
			return
		}
	}
}

func (s *server) reclaimDelay() time.Duration {
	spread := s.reclaimInterval / 5
	lower := s.reclaimInterval - spread
	// 配置允许任意 >=2s 的 Duration，限制可采样上界以免乘加溢出。
	width := min(2*spread, time.Duration(math.MaxInt64)-lower)
	return lower + rand.N(width+1)
}
