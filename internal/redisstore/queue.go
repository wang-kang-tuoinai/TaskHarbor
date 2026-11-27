package redisstore

import "fmt"

// ValidateQueueName 检查提交参数和已保存任务详情使用的队列名。
func ValidateQueueName(queue string) error {
	if len(queue) == 0 || len(queue) > 64 {
		return fmt.Errorf("queue name must contain 1..64 ASCII bytes")
	}
	if queue == "meta" {
		return fmt.Errorf("queue name %q is reserved", queue)
	}
	for i := 0; i < len(queue); i++ {
		b := queue[i]
		if !(b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_' || b == '-') {
			return fmt.Errorf("invalid queue name byte at %d", i)
		}
	}
	return nil
}
