package store

import (
	"github.com/weeyp/fastflow/pkg/entity"
	"github.com/weeyp/fastflow/pkg/mod"
	"time"
)

type StoreOption struct {
	// mongo connection string
	ConnStr  string
	Database string
	// Timeout access mongo timeout.default 5s
	Timeout time.Duration
	// the prefix will append to the database
	Prefix string
}

type Store struct {
	opt            *StoreOption
	dagClsName     string
	dagInsClsName  string
	taskInsClsName string
}

type Storage interface {
	// Init init store
	Init() error
	// Close store
	Close() error
	// CreateDag create dag
	CreateDag(dag *entity.Dag) error
	// CreateDagIns create dag instance
	CreateDagIns(dagIns *entity.DagInstance) error
	// CreateTaskIns create task instance
	CreateTaskIns(taskIns *entity.TaskInstance) error
	// BatchCreatTaskIns batch create task instance
	BatchCreatTaskIns(taskIns []*entity.TaskInstance) error
	// PatchTaskIns patch task instance
	PatchTaskIns(taskIns *entity.TaskInstance) error
	// PatchDagIns patch dag instance
	PatchDagIns(dagIns *entity.DagInstance, mustsPatchFields ...string) error
	// UpdateDag update dag
	UpdateDag(dag *entity.Dag) error
	// UpdateDagIns update dag instance
	UpdateDagIns(dagIns *entity.DagInstance) error
	// UpdateTaskIns update task instance
	UpdateTaskIns(taskIns *entity.TaskInstance) error
	// BatchUpdateDagIns batch update dag instance
	BatchUpdateDagIns(dagIns []*entity.DagInstance) error
	// BatchUpdateTaskIns batch update task instance
	BatchUpdateTaskIns(taskIns []*entity.TaskInstance) error
	// GetTaskIns get task instance
	GetTaskIns(taskInsId string) (*entity.TaskInstance, error)
	// GetDag get dag
	GetDag(dagId string) (*entity.Dag, error)
	// GetDagInstance get dag instance
	GetDagInstance(dagInsId string) (*entity.DagInstance, error)
	// ListDag list dag
	ListDag(input *mod.ListDagInput) ([]*entity.Dag, error)
	// ListDagInstance list dag instance
	ListDagInstance(input *mod.ListDagInstanceInput) ([]*entity.DagInstance, error)
	// ListTaskInstance list task instance
	ListTaskInstance(input *mod.ListTaskInstanceInput) ([]*entity.TaskInstance, error)
	// BatchDeleteDag batch delete dag
	BatchDeleteDag(ids []string) error
	// BatchDeleteDagIns batch delete dag instance
	BatchDeleteDagIns(ids []string) error
	// BatchDeleteTaskIns batch delete task instance
	BatchDeleteTaskIns(ids []string) error
}
