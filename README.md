# xPool

A controllable goroutine pool for Go, supporting dynamic scaling and task overflow strategies.

一个可控的 Go 协程池，支持动态伸缩和任务溢出策略。

## Features / 特性

- **Core-Max model**: Maintains `core` goroutines at idle, scales up to `max` when busy
- **TTL auto-scaling**: Extra goroutines expire after idle timeout, returning to core size
- **3 overflow strategies**: `Reject`, `CallerRun`, `Block`
- **Graceful shutdown**: `Stop()` returns unprocessed tasks
- **Generic**: Works with any type via `pool[T]`

- **核心-最大模型**：空闲时维持 `core` 个协程，繁忙时自动扩容至 `max`
- **TTL 自动缩容**：额外协程空闲超时后自动退出，恢复至核心数量
- **3 种溢出策略**：`Reject`（拒绝）、`CallerRun`（调用者执行）、`Block`（阻塞等待）
- **优雅关闭**：`Stop()` 返回未处理的任务
- **泛型支持**：通过 `pool[T]` 支持任意类型

## Install / 安装

```bash
go get github.com/Nonlone/xPool
```

## Quick Start / 快速开始

### Chinese / 中文

```go
package main

import (
	"fmt"
	"time"

	"github.com/Nonlone/xPool"
)

func main() {
	// 创建协程池: core=2, max=4, channel容量=10, TTL=5秒, 满时拒绝
	p, err := xPool.NewPool[string](
		2,              // core: 核心协程数
		4,              // max: 最大协程数
		10,             // capacity: 任务队列容量
		5*time.Second,  // ttl: 额外协程空闲超时
		xPool.Reject,   // reject: 溢出策略
		func(s string) { // Future: 任务处理函数
			fmt.Println("处理任务:", s)
			time.Sleep(100 * time.Millisecond)
		},
	)
	if err != nil {
		panic(err)
	}

	// 提交任务
	for i := 0; i < 20; i++ {
		if err := p.Submit(fmt.Sprintf("task-%d", i)); err != nil {
			fmt.Println("提交失败:", err)
		}
	}

	// 停止协程池，获取未处理的任务
	remaining := p.Stop()
	fmt.Printf("未处理任务数: %d\n", len(remaining))
}
```

### English

```go
package main

import (
	"fmt"
	"time"

	"github.com/Nonlone/xPool"
)

func main() {
	// Create pool: core=2, max=4, channel capacity=10, TTL=5s, reject when full
	p, err := xPool.NewPool[string](
		2,              // core: number of core goroutines
		4,              // max: maximum goroutines
		10,             // capacity: task queue size
		5*time.Second,  // ttl: idle timeout for extra goroutines
		xPool.Reject,   // reject: overflow strategy
		func(s string) { // Future: task handler
			fmt.Println("processing:", s)
			time.Sleep(100 * time.Millisecond)
		},
	)
	if err != nil {
		panic(err)
	}

	// Submit tasks
	for i := 0; i < 20; i++ {
		if err := p.Submit(fmt.Sprintf("task-%d", i)); err != nil {
			fmt.Println("submit failed:", err)
		}
	}

	// Stop pool and get unprocessed tasks
	remaining := p.Stop()
	fmt.Printf("remaining tasks: %d\n", len(remaining))
}
```

## API / 接口

### `NewPool[T]`

```go
func NewPool[T any](core, max, capacity int, ttl time.Duration, reject strategy, f Future[T]) (*pool[T], error)
```

| Parameter / 参数 | Description / 说明 |
|---|---|
| `core` | Core goroutine count, always alive / 核心协程数，始终存活 |
| `max` | Max goroutine count, auto-scaled / 最大协程数，按需自动创建 |
| `capacity` | Task channel buffer size / 任务队列缓冲区大小 |
| `ttl` | Idle timeout for extra goroutines / 额外协程空闲超时时间 |
| `reject` | Overflow strategy / 溢出策略 |
| `f` | Task handler function / 任务处理函数 |

### `Submit`

```go
func (p *pool[T]) Submit(t T) error
```

Submits a task to the pool. Returns error when pool is full (with `Reject` strategy) or pool is stopped.

提交任务到协程池。池满时返回错误（`Reject` 策略下），或池已停止时返回错误。

### `Stop`

```go
func (p *pool[T]) Stop() []T
```

Stops all goroutines and returns unprocessed tasks in the channel.

停止所有协程，返回通道中未处理的任务。

### Overflow Strategies / 溢出策略

| Strategy / 策略 | Behavior / 行为 |
|---|---|
| `Reject` | Returns `ErrRejectByPoolIsFull` / 返回错误 |
| `CallerRun` | Executes task in the calling goroutine / 在调用者协程中执行 |
| `Block` | Blocks until a slot is available / 阻塞等待直到有空位 |

## Errors / 错误

| Error / 错误 | Meaning / 含义 |
|---|---|
| `ErrInvalidParams` | core/max/capacity <= 0 / 参数非法 |
| `ErrCoreGreaterThanMax` | core > max / 核心数大于最大数 |
| `ErrRejectByPoolIsFull` | Pool full with Reject strategy / 池满被拒绝 |
| `ErrPoolStopped` | Pool already stopped / 协程池已停止 |

## How It Works / 工作原理

1. `NewPool` creates `core` goroutines, each linked in a doubly-linked list
2. `Submit` sends tasks to a buffered channel; goroutines consume from it
3. After each task, `monitor()` checks if the channel is full and `cur < max` — if so, a new goroutine is created with TTL
4. Idle extra goroutines exit after TTL expires (checked in `default` branch of select)
5. `Stop()` closes a `done` channel, all goroutines exit, then remaining channel items are drained and returned

1. `NewPool` 创建 `core` 个协程，通过双向链表管理
2. `Submit` 将任务发送到带缓冲的 channel，协程从中消费
3. 每次处理任务后，`monitor()` 检查 channel 是否已满且 `cur < max`，是则创建带 TTL 的新协程
4. 额外协程空闲超时后自动退出（在 select 的 `default` 分支中检查）
5. `Stop()` 关闭 `done` channel，所有协程退出，然后排空并返回 channel 中剩余的任务

## License / 许可

MIT
