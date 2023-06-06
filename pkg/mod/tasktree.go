package mod

import (
	"errors"
	"fmt"

	"github.com/weeyp/fastflow/pkg/entity"
)

const (
	virtualTaskRootID = "_virtual_root"
)

// TaskInfoGetter get task info
type TaskInfoGetter interface {
	GetDepend() []string
	GetID() string
	GetGraphID() string
	GetStatus() entity.TaskInstanceStatus
}

// MapTaskInsToGetter map task instance to getter
func MapTaskInsToGetter(taskIns []*entity.TaskInstance) (ret []TaskInfoGetter) {
	for i := range taskIns {
		ret = append(ret, taskIns[i])
	}
	return
}

// MapTasksToGetter map tasks to getter
func MapTasksToGetter(taskIns []entity.Task) (ret []TaskInfoGetter) {
	for i := range taskIns {
		ret = append(ret, &taskIns[i])
	}
	return
}

// MustBuildRootNode must build root node
func MustBuildRootNode(tasks []TaskInfoGetter) *TaskNode {
	root, err := BuildRootNode(tasks)
	if err != nil {
		panic(fmt.Errorf("build tasks failed: %s", err))
	}
	return root
}

// BuildRootNode build root node
func BuildRootNode(tasks []TaskInfoGetter) (*TaskNode, error) {
	root := &TaskNode{
		TaskInsID: virtualTaskRootID,
		Status:    entity.TaskInstanceStatusSuccess,
	}
	m, err := buildGraphNodeMap(tasks)
	if err != nil {
		return nil, err
	}

	for i := range tasks {
		if len(tasks[i].GetDepend()) == 0 {
			n := m[tasks[i].GetGraphID()]
			n.AppendParent(root)
			root.children = append(root.children, n)
		}

		if len(tasks[i].GetDepend()) > 0 {
			for _, dependId := range tasks[i].GetDepend() {
				parent, ok := m[dependId]
				if !ok {
					return nil, fmt.Errorf("does not find task[%s] depend: %s", tasks[i].GetGraphID(), dependId)
				}
				parent.AppendChild(m[tasks[i].GetGraphID()])
				m[tasks[i].GetGraphID()].AppendParent(parent)
			}
		}
	}

	if len(root.children) == 0 {
		return nil, errors.New("here is no start nodes")
	}

	if cycleStart := root.HasCycle(); cycleStart != nil {
		return nil, fmt.Errorf("dag has cycle at: %s", cycleStart.TaskInsID)
	}

	return root, nil
}

func buildGraphNodeMap(tasks []TaskInfoGetter) (map[string]*TaskNode, error) {
	m := map[string]*TaskNode{}
	for i := range tasks {
		if _, ok := m[tasks[i].GetGraphID()]; ok {
			return nil, fmt.Errorf("task id is repeat, id: %s", tasks[i].GetGraphID())
		}
		m[tasks[i].GetGraphID()] = NewTaskNodeFromGetter(tasks[i])
	}
	return m, nil
}

// TaskTree task tree
type TaskTree struct {
	DagIns *entity.DagInstance
	Root   *TaskNode
}

// NewTaskNodeFromGetter new task node from getter
func NewTaskNodeFromGetter(instance TaskInfoGetter) *TaskNode {
	return &TaskNode{
		TaskInsID: instance.GetID(),
		Status:    instance.GetStatus(),
	}
}

// TaskNode task node
type TaskNode struct {
	TaskInsID string
	Status    entity.TaskInstanceStatus

	children []*TaskNode
	parents  []*TaskNode
}

type TreeStatus string

const (
	TreeStatusRunning TreeStatus = "running"
	TreeStatusSuccess TreeStatus = "success"
	TreeStatusFailed  TreeStatus = "failed"
	TreeStatusBlocked TreeStatus = "blocked"
)

// HasCycle check cycle
func (t *TaskNode) HasCycle() (cycleStart *TaskNode) {
	visited, incomplete := map[string]struct{}{}, map[string]*TaskNode{}
	waitQueue := []*TaskNode{t}
	bfsCheckCycle(waitQueue, visited, incomplete)
	if len(incomplete) > 0 {
		for k := range incomplete {
			return incomplete[k]
		}
	}
	return
}

func bfsCheckCycle(waitQueue []*TaskNode, visited map[string]struct{}, incomplete map[string]*TaskNode) {
	queueLen := len(waitQueue)
	if queueLen == 0 {
		return
	}

	isParentCompleted := func(node *TaskNode) bool {
		for _, p := range node.parents {
			if _, ok := visited[p.TaskInsID]; !ok {
				return false
			}
		}
		return true
	}

	for i := 0; i < queueLen; i++ {
		cur := waitQueue[i]
		if !isParentCompleted(cur) {
			incomplete[cur.TaskInsID] = cur
			continue
		}
		visited[cur.TaskInsID] = struct{}{}
		delete(incomplete, cur.TaskInsID)
		for _, c := range cur.children {
			waitQueue = append(waitQueue, c)
		}
	}
	waitQueue = waitQueue[queueLen:]
	bfsCheckCycle(waitQueue, visited, incomplete)
	return
}

// ComputeStatus compute status
func (t *TaskNode) ComputeStatus() (status TreeStatus, srcTaskInsId string) {
	walkNode(t, func(node *TaskNode) bool {
		switch node.Status {
		case entity.TaskInstanceStatusFailed, entity.TaskInstanceStatusCanceled:
			status = TreeStatusFailed
			srcTaskInsId = node.TaskInsID
			return true
		case entity.TaskInstanceStatusBlocked:
			status = TreeStatusBlocked
			srcTaskInsId = node.TaskInsID
			return true
		case entity.TaskInstanceStatusSuccess, entity.TaskInstanceStatusSkipped:
			return true
		default:
			status = TreeStatusRunning
			srcTaskInsId = node.TaskInsID
			return false
		}
	}, false)
	if srcTaskInsId != "" {
		return
	}
	return TreeStatusSuccess, ""
}

func walkNode(root *TaskNode, walkFunc func(node *TaskNode) bool, walkChildrenIgnoreStatus bool) {
	dfsWalk(root, walkFunc, walkChildrenIgnoreStatus)
}

func dfsWalk(
	root *TaskNode,
	walkFunc func(node *TaskNode) bool,
	walkChildrenIgnoreStatus bool) bool {

	if root.TaskInsID != virtualTaskRootID {
		if !walkFunc(root) {
			return false
		}
	}

	// we cannot execute children, but should execute brother nodes
	if !walkChildrenIgnoreStatus && !root.CanExecuteChild() {
		return true
	}
	for _, c := range root.children {
		// if children's parent is not just root, we must check it
		if len(c.parents) > 1 && !c.CanBeExecuted() {
			continue
		}

		if !dfsWalk(c, walkFunc, walkChildrenIgnoreStatus) {
			return false
		}
	}
	return true
}

// AppendChild append child
func (t *TaskNode) AppendChild(task *TaskNode) {
	t.children = append(t.children, task)
}

// AppendParent append parent
func (t *TaskNode) AppendParent(task *TaskNode) {
	t.parents = append(t.parents, task)
}

// CanExecuteChild check whether task could execute child
func (t *TaskNode) CanExecuteChild() bool {
	return t.Status == entity.TaskInstanceStatusSuccess || t.Status == entity.TaskInstanceStatusSkipped
}

// CanBeExecuted check whether task could be executed
func (t *TaskNode) CanBeExecuted() bool {
	if len(t.parents) == 0 {
		return true
	}

	for _, p := range t.parents {
		if !p.CanExecuteChild() {
			return false
		}
	}
	return true
}

// GetExecutableTaskIds is unique task id map
func (t *TaskNode) GetExecutableTaskIds() (executables []string) {
	walkNode(t, func(node *TaskNode) bool {
		if node.Executable() {
			executables = append(executables, node.TaskInsID)
		}
		return true
	}, false)
	return
}

// GetNextTaskIds get next task ids
func (t *TaskNode) GetNextTaskIds(completedOrRetryTask *entity.TaskInstance) (executable []string, find bool) {
	walkNode(t, func(node *TaskNode) bool {
		if completedOrRetryTask.ID == node.TaskInsID {
			find = true
			node.Status = completedOrRetryTask.Status

			if node.Status == entity.TaskInstanceStatusInit {
				executable = append(executable, node.TaskInsID)
				return false
			}

			if !node.CanExecuteChild() {
				return false
			}
			for i := range node.children {
				if node.children[i].Executable() {
					executable = append(executable, node.children[i].TaskInsID)
				}
			}
			return false
		}
		return true
	}, false)

	return
}

// Executable check whether task could be executed
func (t *TaskNode) Executable() bool {
	if t.Status == entity.TaskInstanceStatusInit ||
		t.Status == entity.TaskInstanceStatusRetrying ||
		t.Status == entity.TaskInstanceStatusEnding {
		if len(t.parents) == 0 {
			return true
		}

		for i := range t.parents {
			if !t.parents[i].CanExecuteChild() {
				return false
			}
		}
		return true
	}
	return false
}
