package main

import (
	"fmt"
	"github.com/weeyp/fastflow/store/cache"
	"log"
	"time"

	"github.com/weeyp/fastflow"
	"github.com/weeyp/fastflow/pkg/entity/run"
	"github.com/weeyp/fastflow/pkg/mod"
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
		// use yaml to define dag
		ReadDagFromDir: "./",
	}); err != nil {
		panic(fmt.Sprintf("init fastflow failed: %s", err))
	}
}

func createDagAndInstance() {
	// wait fast start completed
	time.Sleep(time.Second)

	// run some dag instance
	for i := 0; i < 10; i++ {
		_, err := mod.GetCommander().RunDag("test-dag", nil)
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(time.Second * 10)
	}
}
