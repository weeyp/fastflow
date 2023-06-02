package fastflow

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/shiningrush/goevent"
	"github.com/weeyp/fastflow/pkg/actions"
	"github.com/weeyp/fastflow/pkg/entity"
	"github.com/weeyp/fastflow/pkg/entity/run"
	"github.com/weeyp/fastflow/pkg/mod"
	"github.com/weeyp/fastflow/pkg/utils"
	"github.com/weeyp/fastflow/pkg/utils/data"
	"gopkg.in/yaml.v3"
)

var closers []mod.Closer

// RegisterAction you need register all used action to it
func RegisterAction(acts []run.Action) {
	for i := range acts {
		mod.ActionMap[acts[i].Name()] = acts[i]
	}
}

// GetAction get action by name
func GetAction(name string) (run.Action, bool) {
	act, ok := mod.ActionMap[name]
	return act, ok
}

// InitialOption used to initial fastflow
type InitialOption struct {
	Store mod.Store

	// ParserWorkersCnt default 100
	ParserWorkersCnt int
	// ExecutorWorkerCnt default 1000
	ExecutorWorkerCnt int
	// ExecutorTimeout default 30s
	ExecutorTimeout time.Duration
	// ExecutorTimeout default 15s
	DagScheduleTimeout time.Duration

	// Read dag define from directory
	// each file will be pared to a dag, so you CAN'T define all dag in one file
	ReadDagFromDir string
}

// Start will block until accept system signal, if you don't want block, plz check "Init"
func Start(opt *InitialOption, afterInit ...func() error) error {
	if err := Init(opt); err != nil {
		return err
	}
	for i := range afterInit {
		if err := afterInit[i](); err != nil {
			return err
		}
	}

	log.Println("fastflow start success")
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	sig := <-c
	log.Println(fmt.Sprintf("get sig: %s, ready to close component", sig))
	Close()
	log.Println("close completed")
	return nil
}

// Init will not block, but you need to close fastflow after application closing
func Init(opt *InitialOption) error {
	if err := checkOption(opt); err != nil {
		return err
	}

	initCommonComponent(opt)

	RegisterAction([]run.Action{
		&actions.Waiting{},
	})

	if opt.ReadDagFromDir != "" {
		return readDagFromDir(opt.ReadDagFromDir)
	}
	return nil
}

// SetDagInstanceLifecycleHook set hook handler for fastflow
// IMPORTANT: you MUST set hook before you call Init or Start to avoid lost changes.
// (because component will work immediately after you call Init or Start)
func SetDagInstanceLifecycleHook(hook entity.DagInstanceLifecycleHook) {
	entity.HookDagInstance = hook
}

// LeaderChangedHandler used to handle leader chaged event
type LeaderChangedHandler struct {
	opt *InitialOption

	leaderCloser []mod.Closer
	mutex        sync.Mutex
}

// Close all closer
func Close() {
	for i := range closers {
		closers[i].Close()
	}
	goevent.Close()
}

func checkOption(opt *InitialOption) error {
	if opt.Store == nil {
		return fmt.Errorf("store cannot be nil")
	}

	if opt.ExecutorTimeout == 0 {
		opt.ExecutorTimeout = 30 * time.Second
	}
	if opt.DagScheduleTimeout == 0 {
		opt.DagScheduleTimeout = 15 * time.Second
	}
	if opt.ExecutorWorkerCnt == 0 {
		opt.ExecutorWorkerCnt = 1000
	}
	if opt.ParserWorkersCnt == 0 {
		opt.ParserWorkersCnt = 100
	}
	return nil
}

func initCommonComponent(opt *InitialOption) {
	mod.SetStore(opt.Store)

	// Executor must init before parse otherwise will cause a error
	exe := mod.NewDefExecutor(opt.ExecutorTimeout, opt.ExecutorWorkerCnt)
	mod.SetExecutor(exe)
	p := mod.NewDefParser(opt.ParserWorkersCnt, opt.ExecutorTimeout)
	mod.SetParser(p)

	exe.Init()
	closers = append(closers, exe)
	p.Init()
	closers = append(closers, p)

	comm := &mod.DefCommander{}
	mod.SetCommander(comm)

	// keeper and store must close latest
	closers = append(closers, opt.Store)
}

func readDagFromDir(dir string) error {
	paths, err := utils.DefaultReader.ReadPathsFromDir(dir)
	if err != nil {
		return err
	}

	for _, path := range paths {
		bs, err := utils.DefaultReader.ReadDag(path)
		if err != nil {
			return fmt.Errorf("read %s failed: %w", path, err)
		}

		dag := entity.Dag{
			Status: entity.DagStatusNormal,
		}
		err = yaml.Unmarshal(bs, &dag)
		if err != nil {
			return fmt.Errorf("unmarshal %s failed: %w", path, err)
		}

		if dag.ID == "" {
			dag.ID = strings.TrimSuffix(strings.TrimSuffix(filepath.Base(path), ".yaml"), ".yml")
		}

		if err := ensureDagLatest(&dag); err != nil {
			return err
		}
	}
	return nil
}

func ensureDagLatest(dag *entity.Dag) error {
	oDag, err := mod.GetStore().GetDag(dag.ID)
	if err != nil && !errors.Is(err, data.ErrDataNotFound) {
		return err
	}
	if oDag != nil {
		return mod.GetStore().UpdateDag(dag)
	}

	return mod.GetStore().CreateDag(dag)
}
