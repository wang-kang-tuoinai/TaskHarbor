package taskq

import (
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

var (
	// ErrInvalidArgument 表示 Client 构造或操作参数非法。
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrStorageData 表示存储数据不一致或响应不符合协议，不会自动修复。
	ErrStorageData = errors.New("invalid storage data")
)

// Client 提交、查询和取消任务，持有调用方的 Redis 客户端，可并发使用。
// 必须通过 NewClient 构造；调用方负责配置和关闭 Redis 客户端。
type Client struct {
	rdb *redis.Client
}

// NewClient 只保存 rdb，不发起请求或启动 goroutine。
func NewClient(rdb *redis.Client) (*Client, error) {
	if rdb == nil {
		return nil, fmt.Errorf("%w: rdb is required", ErrInvalidArgument)
	}
	return &Client{rdb: rdb}, nil
}
