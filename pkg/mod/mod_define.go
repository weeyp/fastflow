package mod

import (
	"context"
	"time"

	"github.com/weeyp/fastflow/pkg/entity"
	"github.com/weeyp/fastflow/pkg/entity/run"
)

type CtxKey string

const (
	CtxKeyRunningTaskIns CtxKey = "running-task"
)

func CtxWithRunningTaskIns(ctx context.Context, task *entity.TaskInstance) context.Context {
	return context.WithValue(ctx, CtxKeyRunningTaskIns, task)
}

func CtxRunningTaskIns(ctx context.Context) (*entity.TaskInstance, bool) {
	ins, ok := ctx.Value(CtxKeyRunningTaskIns).(*entity.TaskInstance)
	return ins, ok
}

var (
	ActionMap = map[string]run.Action{}

	defExc       Executor
	defStore     Store
	defParser    Parser
	defCommander Commander
)

// Commander used to execute command
type Commander interface {
	RunDag(dagId string, specVar map[string]string) (*entity.DagInstance, error)
	RetryDagIns(dagInsId string, ops ...CommandOptSetter) error
	RetryTask(taskInsIds []string, ops ...CommandOptSetter) error
	CancelTask(taskInsIds []string, ops ...CommandOptSetter) error
}

// CommandOption is used to set command option
type CommandOption struct {
	// isSync means commander will watch dag instance's cmd executing situation until it's command is executed
	// usually command executing time is very short, so async mode is enough,
	// but if you want a sync call, you set it to true
	isSync bool
	// syncTimeout is just work at sync mode, it is the timeout of watch dag instance
	// default is 5s
	syncTimeout time.Duration
	// syncInterval is just work at sync mode, it is the interval of watch dag instance
	// default is 500ms
	syncInterval time.Duration
}
type CommandOptSetter func(opt *CommandOption)

var (
	// CommSync means commander will watch dag instance's cmd executing situation until it's command is executed
	// usually command executing time is very short, so async mode is enough,
	// but if you want a sync call, you set it to true
	CommSync = func() CommandOptSetter {
		return func(opt *CommandOption) {
			opt.isSync = true
		}
	}
	// CommSyncTimeout means commander will watch dag instance's cmd executing situation until it's command is executed
	// usually command executing time is very short, so async mode is enough,
	// but if you want a sync call, you set it to true
	CommSyncTimeout = func(duration time.Duration) CommandOptSetter {
		return func(opt *CommandOption) {
			if duration > 0 {
				opt.syncTimeout = duration
			}
		}
	}
	// CommSyncInterval is just work at sync mode, it is the interval of watch dag instance
	// default is 500ms
	CommSyncInterval = func(duration time.Duration) CommandOptSetter {
		return func(opt *CommandOption) {
			if duration > 0 {
				opt.syncInterval = duration
			}
		}
	}
)

// SetCommander set commander
func SetCommander(c Commander) {
	defCommander = c
}

// GetCommander get commander
func GetCommander() Commander {
	return defCommander
}

// Executor is used to execute task
type Executor interface {
	// Push ExecuteTaskIns execute task instance
	Push(dagIns *entity.DagInstance, taskIns *entity.TaskInstance)
	// CancelTaskIns cancel task instance
	CancelTaskIns(taskInsIds []string) error
}

// SetExecutor set executor
func SetExecutor(e Executor) {
	defExc = e
}

// GetExecutor get executor
func GetExecutor() Executor {
	return defExc
}

// Closer means the component need be closeFunc
type Closer interface {
	Close()
}

// Store used to persist obj
type Store interface {
	Closer
	CreateDag(dag *entity.Dag) error
	CreateDagIns(dagIns *entity.DagInstance) error
	BatchCreatTaskIns(taskIns []*entity.TaskInstance) error
	PatchTaskIns(taskIns *entity.TaskInstance) error
	PatchDagIns(dagIns *entity.DagInstance, mustsPatchFields ...string) error
	UpdateDag(dagIns *entity.Dag) error
	UpdateDagIns(dagIns *entity.DagInstance) error
	UpdateTaskIns(taskIns *entity.TaskInstance) error
	BatchUpdateDagIns(dagIns []*entity.DagInstance) error
	BatchUpdateTaskIns(taskIns []*entity.TaskInstance) error
	GetTaskIns(taskIns string) (*entity.TaskInstance, error)
	GetDag(dagId string) (*entity.Dag, error)
	GetDagInstance(dagInsId string) (*entity.DagInstance, error)
	ListDagInstance(input *ListDagInstanceInput) ([]*entity.DagInstance, error)
	ListTaskInstance(input *ListTaskInstanceInput) ([]*entity.TaskInstance, error)
	Marshal(obj interface{}) ([]byte, error)
	Unmarshal(bytes []byte, ptr interface{}) error
}

// ListDagInstanceInput list dag instance input
type ListDagInstanceInput struct {
	DagID      string
	UpdatedEnd int64
	Status     []entity.DagInstanceStatus
	HasCmd     bool
}

// ListTaskInstanceInput list task instance input
type ListTaskInstanceInput struct {
	IDs      []string
	DagInsID string
	Status   []entity.TaskInstanceStatus
}

// SetStore set store
func SetStore(e Store) {
	defStore = e
}

// GetStore get store
func GetStore() Store {
	return defStore
}

// Parser used to execute command, init dag instance and push task instance
type Parser interface {
	InitialDagIns(dagIns *entity.DagInstance)
	EntryTaskIns(taskIns *entity.TaskInstance)
}

// SetParser set parser
func SetParser(e Parser) {
	defParser = e
}

// GetParser get parser
func GetParser() Parser {
	return defParser
}
