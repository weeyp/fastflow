package main

import (
	"errors"
	"fmt"
	"github.com/weeyp/fastflow/store/cache"
	"log"
	"time"

	"github.com/weeyp/fastflow"
	"github.com/weeyp/fastflow/pkg/entity"
	"github.com/weeyp/fastflow/pkg/entity/run"
	"github.com/weeyp/fastflow/pkg/mod"
	"github.com/weeyp/fastflow/pkg/utils/data"
)

type PrintAction struct {
}

// Name define the unique action identity, it will be used by Task
func (a *PrintAction) Name() string {
	return "PrintAction"
}
func (a *PrintAction) Run(ctx run.ExecuteContext, params interface{}) error {
	fmt.Println("action start: ", time.Now())
	return nil
}

func main() {
	// Register action
	fastflow.RegisterAction([]run.Action{
		&PrintAction{},
	})

	// init store
	st := cache.NewMemCache()

	go createDagAndInstance()

	// start fastflow
	if err := fastflow.Start(&fastflow.InitialOption{
		Store: st,
	}); err != nil {
		panic(fmt.Sprintf("init fastflow failed: %s", err))
	}
}

func createDagAndInstance() {
	// wait fast start completed
	time.Sleep(time.Second)

	// create a dag as template
	dag := &entity.Dag{
		ID:   "test-dag",
		Name: "test",
		Tasks: []entity.Task{
			{ID: "task1", ActionName: "PrintAction"},
			{ID: "task2", ActionName: "PrintAction", DependOn: []string{"task1"}},
			{ID: "task3", ActionName: "PrintAction", DependOn: []string{"task2"}},
		},
		Status: entity.DagStatusNormal,
	}
	if err := ensureDagCreated(dag); err != nil {
		log.Fatalf(err.Error())
	}

	// run some dag instance
	for i := 0; i < 10; i++ {
		dagInstance, err := dag.Run(entity.TriggerManually, nil)
		if err != nil {
			log.Fatal(err)
		}
		if err := mod.GetStore().CreateDagIns(dagInstance); err != nil {
			log.Fatal(err)
		}
		time.Sleep(time.Second * 1)
	}
}

func ensureDagCreated(dag *entity.Dag) error {
	oldDag, err := mod.GetStore().GetDag(dag.ID)
	if errors.Is(err, data.ErrDataNotFound) {
		if err := mod.GetStore().CreateDag(dag); err != nil {
			return err
		}
	}
	if oldDag != nil {
		if err := mod.GetStore().UpdateDag(dag); err != nil {
			return err
		}
	}
	return nil
}
