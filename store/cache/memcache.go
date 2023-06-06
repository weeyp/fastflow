package cache

import (
	"encoding/json"
	"fmt"
	"github.com/patrickmn/go-cache"
	"github.com/weeyp/fastflow/pkg/entity"
	"github.com/weeyp/fastflow/pkg/mod"
	"github.com/weeyp/fastflow/pkg/utils/data"
	"reflect"
)

type MemCache struct {
	dags    *cache.Cache
	dagIns  *cache.Cache
	taskIns *cache.Cache
}

func NewMemCache() *MemCache {
	return &MemCache{
		dags:    cache.New(cache.NoExpiration, cache.NoExpiration),
		dagIns:  cache.New(cache.NoExpiration, cache.NoExpiration),
		taskIns: cache.New(cache.NoExpiration, cache.NoExpiration),
	}
}

func (m *MemCache) Close() {
}

func (m *MemCache) createItem(id string, item interface{}, c *cache.Cache) error {
	// Check if the item already exists in the specified cache.
	if _, found := c.Get(id); found {
		return data.ErrDataConflicted
	}
	// If not, add the item to the cache.
	c.Set(id, item, cache.NoExpiration)
	return nil
}

func (m *MemCache) updateItem(id string, item interface{}, c *cache.Cache) error {
	// Check if the item already exists in the specified cache.
	if _, found := c.Get(id); !found {
		return data.ErrDataNotFound
	}

	// If it exists, update the item in the cache.
	c.Set(id, item, cache.NoExpiration)
	return nil
}

func (m *MemCache) CreateDag(dag *entity.Dag) error {
	return m.createItem(dag.ID, dag, m.dags)
}

func (m *MemCache) UpdateDag(dag *entity.Dag) error {
	return m.updateItem(dag.ID, dag, m.dags)
}

func (m *MemCache) GetDag(dagId string) (*entity.Dag, error) {
	// Attempt to get the dag from the cache.
	if dag, found := m.dags.Get(dagId); found {
		// We need to type assert because the cache stores interface{} values.
		return dag.(*entity.Dag), nil
	}
	return nil, data.ErrDataNotFound
}

func (m *MemCache) CreateDagIns(dagIns *entity.DagInstance) error {
	return m.createItem(dagIns.ID, dagIns, m.dagIns)
}

func (m *MemCache) PatchDagIns(dagIns *entity.DagInstance, mustsPatchFields ...string) error {
	// Get the existing DagInstance
	oldDagInsInterface, found := m.dagIns.Get(dagIns.ID)
	if !found {
		return data.ErrDataNotFound
	}

	oldDagIns, ok := oldDagInsInterface.(*entity.DagInstance)
	if !ok {
		return fmt.Errorf("stored value is not a DagInstance")
	}

	// Use reflection to patch fields
	dagInsValue := reflect.ValueOf(dagIns).Elem()
	oldDagInsValue := reflect.ValueOf(oldDagIns).Elem()

	for _, field := range mustsPatchFields {
		oldField := oldDagInsValue.FieldByName(field)
		newField := dagInsValue.FieldByName(field)

		if oldField.IsValid() && newField.IsValid() {
			oldField.Set(newField)
		}
	}

	// Save the updated DagInstance back to the cache
	m.dagIns.Set(dagIns.ID, oldDagIns, cache.NoExpiration)
	return nil
}

func (m *MemCache) UpdateDagIns(dagIns *entity.DagInstance) error {
	return m.updateItem(dagIns.ID, dagIns, m.dagIns)
}

func (m *MemCache) BatchUpdateDagIns(dagIns []*entity.DagInstance) error {
	for _, di := range dagIns {
		err := m.updateItem(di.ID, di, m.dagIns)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MemCache) GetDagInstance(dagInsId string) (*entity.DagInstance, error) {
	// Similar to GetDag but for DagInstance
	if dagIns, found := m.dagIns.Get(dagInsId); found {
		return dagIns.(*entity.DagInstance), nil
	}
	return nil, data.ErrDataNotFound
}

func (m *MemCache) ListDagInstance(input *mod.ListDagInstanceInput) ([]*entity.DagInstance, error) {
	// This is a naive implementation of ListDagInstance.
	// It iterates over all DagInstances and checks whether each one matches the input criteria.
	// For a large number of DagInstances, this could be slow.
	var dagInsList []*entity.DagInstance
	for _, item := range m.dagIns.Items() {
		dagIns, ok := item.Object.(*entity.DagInstance)
		if !ok {
			continue
		}
		if input.DagID != "" && dagIns.DagID != input.DagID {
			continue
		}
		// other checks for UpdatedEnd, Status, HasCmd, Limit, Offset
		dagInsList = append(dagInsList, dagIns)
	}
	return dagInsList, nil
}

func (m *MemCache) BatchCreatTaskIns(taskIns []*entity.TaskInstance) error {
	for _, ti := range taskIns {
		err := m.createItem(ti.TaskID, ti, m.taskIns)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MemCache) PatchTaskIns(taskIns *entity.TaskInstance) error {
	// Get the existing TaskInstance
	oldTaskInsInterface, found := m.taskIns.Get(taskIns.TaskID)
	if !found {
		return data.ErrDataNotFound
	}

	oldTaskIns, ok := oldTaskInsInterface.(*entity.TaskInstance)
	if !ok {
		return fmt.Errorf("stored value is not a TaskInstance")
	}

	// Use reflection to patch fields
	taskInsValue := reflect.ValueOf(taskIns).Elem()
	oldTaskInsValue := reflect.ValueOf(oldTaskIns).Elem()

	typeOfTaskIns := taskInsValue.Type()
	for i := 0; i < taskInsValue.NumField(); i++ {
		field := typeOfTaskIns.Field(i).Name
		oldField := oldTaskInsValue.FieldByName(field)
		newField := taskInsValue.FieldByName(field)

		if oldField.IsValid() && newField.IsValid() {
			oldField.Set(newField)
		}
	}

	// Save the updated TaskInstance back to the cache
	m.taskIns.Set(taskIns.TaskID, oldTaskIns, cache.NoExpiration)
	return nil
}

func (m *MemCache) UpdateTaskIns(taskIns *entity.TaskInstance) error {
	return m.updateItem(taskIns.TaskID, taskIns, m.taskIns)
}

func (m *MemCache) BatchUpdateTaskIns(taskIns []*entity.TaskInstance) error {
	for _, ti := range taskIns {
		err := m.updateItem(ti.TaskID, ti, m.taskIns)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MemCache) GetTaskIns(taskIns string) (*entity.TaskInstance, error) {
	if taskIns, found := m.taskIns.Get(taskIns); found {
		return taskIns.(*entity.TaskInstance), nil
	}
	return nil, data.ErrDataNotFound
}

func (m *MemCache) ListTaskInstance(input *mod.ListTaskInstanceInput) ([]*entity.TaskInstance, error) {
	var taskInsList []*entity.TaskInstance
	for _, item := range m.taskIns.Items() {
		taskIns, ok := item.Object.(*entity.TaskInstance)
		if !ok {
			continue
		}
		if input.DagInsID != "" && taskIns.DagInsID != input.DagInsID {
			continue
		}
		if len(input.Status) > 0 {
			found := false
			for _, status := range input.Status {
				if taskIns.Status == status {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		// other checks for IDs, Expired, SelectField
		taskInsList = append(taskInsList, taskIns)
	}
	return taskInsList, nil
}

func (m *MemCache) Marshal(obj interface{}) ([]byte, error) {
	// 结构体序列化为[]byte
	return json.Marshal(obj)
}

func (m *MemCache) Unmarshal(bytes []byte, ptr interface{}) error {
	// []byte反序列化为结构体
	return json.Unmarshal(bytes, ptr)
}
