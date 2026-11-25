package taskq

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

// MaxResultSize 是任务结果 JSON 编码后的字节数上限。
const MaxResultSize = redisstore.MaxResultSize

// Task 是传递给 handler 的业务视图，执行身份单独存储。
type Task struct {
	ID       string
	Type     string
	Attempts int
	Payload  []byte
	result   []byte
}

// HandlerFunc 处理一个任务。所有结果写入必须在返回前完成。
// handler 返回后不得再修改 task，也不得并发调用 SetResult。
type HandlerFunc func(ctx context.Context, task *Task) error

// SetResult 将 v JSON 编码到独立的 result 缓冲区。Server 只在
// HandlerFunc 成功返回后读取结果。
func (t *Task) SetResult(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal task result: %w", err)
	}
	if len(b) > MaxResultSize {
		return fmt.Errorf("task result too large: %d bytes (max %d)", len(b), MaxResultSize)
	}
	t.result = append([]byte(nil), b...)
	return nil
}
