package entity

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/weeyp/fastflow/pkg/entity/run"
)

func TestTaskInstance_SetStatus(t *testing.T) {
	tests := []struct {
		giveTaskIns  *TaskInstance
		giveStatus   TaskInstanceStatus
		givePatchErr error
		wantPatch    *TaskInstance
		wantErr      error
	}{
		{
			giveTaskIns: &TaskInstance{
				ID:     "test-id",
				Name:   "test",
				Reason: "reason",
				Traces: []TraceInfo{{Message: "traces"}},
			},
			giveStatus: TaskInstanceStatusFailed,
			wantPatch: &TaskInstance{
				ID:     "test-id",
				Reason: "reason",
				Status: TaskInstanceStatusFailed,
			},
		},
		{
			giveTaskIns: &TaskInstance{
				ID:        "test-id",
				Name:      "test",
				Reason:    "reason",
				Traces:    []TraceInfo{{Message: "traces"}},
				bufTraces: []TraceInfo{{Message: "buf-traces"}},
			},
			wantPatch: &TaskInstance{
				ID:     "test-id",
				Reason: "reason",
				Traces: []TraceInfo{
					{Message: "traces"},
					{Message: "buf-traces"},
				},
			},
		},
		{
			giveTaskIns: &TaskInstance{
				ID:     "test-id",
				Name:   "test",
				Reason: "reason",
				Traces: []TraceInfo{{Message: "traces"}},
			},
			givePatchErr: fmt.Errorf("patch failed"),
			wantPatch: &TaskInstance{
				ID:     "test-id",
				Reason: "reason",
			},
			wantErr: fmt.Errorf("patch failed"),
		},
	}

	for _, tc := range tests {
		patchCalled := false
		tc.giveTaskIns.Patch = func(instance *TaskInstance) error {
			patchCalled = true
			assert.Equal(t, tc.wantPatch, instance)
			return tc.givePatchErr
		}
		err := tc.giveTaskIns.SetStatus(tc.giveStatus)
		assert.Equal(t, tc.wantErr, err)
		assert.True(t, patchCalled)
	}
}

func TestTaskInstance_Trace(t *testing.T) {
	tests := []struct {
		giveTaskIns     *TaskInstance
		giveMsg         string
		giveOpt         run.TraceOp
		givePatchErr    error
		wantPatch       *TaskInstance
		wantPatchCalled bool
	}{
		{
			giveOpt: func(opt *run.TraceOption) {},
			giveTaskIns: &TaskInstance{
				ID:     "test-id",
				Name:   "test",
				Reason: "reason",
				Traces: []TraceInfo{{Message: "traces"}},
			},
			giveMsg: "msg",
			wantPatch: &TaskInstance{
				ID: "test-id",
				Traces: []TraceInfo{
					{Message: "traces"},
					{Time: time.Now().Unix(), Message: "msg"},
				},
			},
			wantPatchCalled: true,
		},
		{
			giveOpt: run.TraceOpPersistAfterAction,
			giveTaskIns: &TaskInstance{
				ID:     "test-id",
				Name:   "test",
				Reason: "reason",
				Traces: []TraceInfo{{Message: "traces"}},
			},
			giveMsg: "msg",
			wantPatch: &TaskInstance{
				ID: "test-id",
				Traces: []TraceInfo{
					{Message: "traces"},
				},
				bufTraces: []TraceInfo{
					{Time: time.Now().Unix(), Message: "msg"},
				},
			},
		},
		{
			giveOpt: func(opt *run.TraceOption) {},
			giveTaskIns: &TaskInstance{
				ID:     "test-id",
				Name:   "test",
				Reason: "reason",
				Traces: []TraceInfo{{Message: "traces"}},
			},
			giveMsg: "msg",
			wantPatch: &TaskInstance{
				ID: "test-id",
				Traces: []TraceInfo{
					{Message: "traces"},
					{Time: time.Now().Unix(), Message: "msg"},
				},
			},
			wantPatchCalled: true,
		},
	}

	for _, tc := range tests {
		patchCalled := false
		tc.giveTaskIns.Patch = func(instance *TaskInstance) error {
			patchCalled = true
			assert.Equal(t, tc.wantPatch, instance)
			assert.Equal(t, tc.wantPatch.bufTraces, instance.bufTraces)
			return tc.givePatchErr
		}
		tc.giveTaskIns.Trace(tc.giveMsg, tc.giveOpt)
		assert.Equal(t, tc.wantPatchCalled, patchCalled)
	}
}

func TestTaskConditionSource_BuildKvGetter(t *testing.T) {
	tests := []struct {
		caseDesc   string
		giveSource TaskConditionSource
		giveDagIns *DagInstance
		giveKey    string
		wantFind   bool
		wantVal    string
		wantPanic  string
	}{
		{
			caseDesc:   "vars",
			giveSource: TaskConditionSourceVars,
			giveDagIns: &DagInstance{
				Vars: DagInstanceVars{
					"key1": DagInstanceVar{Value: "value1"},
				},
			},
			giveKey:  "key1",
			wantFind: true,
			wantVal:  "value1",
		},
		{
			caseDesc:   "share data",
			giveSource: TaskConditionSourceShareData,
			giveDagIns: &DagInstance{
				Vars: DagInstanceVars{
					"key1": DagInstanceVar{Value: "value1"},
				},
				ShareData: &ShareData{
					Dict: map[string]string{
						"key2": "value2",
					},
				},
			},
			giveKey:  "key2",
			wantFind: true,
			wantVal:  "value2",
		},
		{
			caseDesc:   "panic",
			giveSource: "test",
			giveDagIns: &DagInstance{
				Vars: DagInstanceVars{
					"key1": DagInstanceVar{Value: "value1"},
				},
			},
			giveKey:   "key1",
			wantPanic: "task condition source test is not valid",
		},
	}

	for _, tc := range tests {
		doTest := func() {
			if tc.wantPanic != "" {
				defer func() {
					err := recover()
					assert.Equal(t, tc.wantPanic, err, tc.caseDesc)
				}()
			}

			g := tc.giveSource.BuildKvGetter(tc.giveDagIns)
			val, ok := g(tc.giveKey)
			assert.Equal(t, tc.wantVal, val, tc.caseDesc)
			assert.Equal(t, tc.wantFind, ok, tc.caseDesc)
		}
		doTest()
	}
}

func TestTaskInstance_DoPreCheck(t *testing.T) {
	tests := []struct {
		caseDesc    string
		giveTaskIns *TaskInstance
		giveDagIns  *DagInstance
		wantRet     bool
		wantErr     error
		wantTaskIns *TaskInstance
	}{
		{
			caseDesc: "share-data",
			giveTaskIns: &TaskInstance{
				PreChecks: PreChecks{
					"first": {
						Conditions: []TaskCondition{
							{
								Source: TaskConditionSourceShareData,
								Key:    "key1",
								Values: []string{"value3"},
								Op:     OperatorIn,
							},
						},
						Act: ActiveActionSkip,
					},
				},
			},
			giveDagIns: &DagInstance{
				ShareData: &ShareData{
					Dict: map[string]string{
						"key1": "value3",
					},
				},
			},
			wantRet: true,
			wantTaskIns: &TaskInstance{
				PreChecks: PreChecks{
					"first": {
						Conditions: []TaskCondition{
							{
								Source: TaskConditionSourceShareData,
								Key:    "key1",
								Values: []string{"value3"},
								Op:     OperatorIn,
							},
						},
						Act: ActiveActionSkip,
					},
				},
				Status: TaskInstanceStatusSkipped,
			},
		},
		{
			caseDesc: "multiple pre-check first meet",
			giveTaskIns: &TaskInstance{
				PreChecks: PreChecks{
					"first": {
						Conditions: []TaskCondition{
							{
								Source: TaskConditionSourceShareData,
								Key:    "key1",
								Values: []string{"value2"},
								Op:     OperatorIn,
							},
						},
						Act: ActiveActionBlock,
					},
					"second": {
						Conditions: []TaskCondition{
							{
								Source: TaskConditionSourceShareData,
								Key:    "key1",
								Values: []string{"value3"},
								Op:     OperatorIn,
							},
						},
						Act: ActiveActionSkip,
					},
				},
			},
			giveDagIns: &DagInstance{
				ShareData: &ShareData{
					Dict: map[string]string{
						"key1": "value2",
					},
				},
			},
			wantRet: true,
			wantTaskIns: &TaskInstance{
				PreChecks: PreChecks{
					"first": {
						Conditions: []TaskCondition{
							{
								Source: TaskConditionSourceShareData,
								Key:    "key1",
								Values: []string{"value2"},
								Op:     OperatorIn,
							},
						},
						Act: ActiveActionBlock,
					},
					"second": {
						Conditions: []TaskCondition{
							{
								Source: TaskConditionSourceShareData,
								Key:    "key1",
								Values: []string{"value3"},
								Op:     OperatorIn,
							},
						},
						Act: ActiveActionSkip,
					},
				},
				Status: TaskInstanceStatusBlocked,
			},
		},
		{
			caseDesc: "multiple pre-check second meet",
			giveTaskIns: &TaskInstance{
				PreChecks: PreChecks{
					"first": {
						Conditions: []TaskCondition{
							{
								Source: TaskConditionSourceShareData,
								Key:    "key1",
								Values: []string{"value3"},
								Op:     OperatorIn,
							},
						},
						Act: ActiveActionBlock,
					},
					"second": {
						Conditions: []TaskCondition{
							{
								Source: TaskConditionSourceShareData,
								Key:    "key1",
								Values: []string{"value4"},
								Op:     OperatorIn,
							},
						},
						Act: ActiveActionSkip,
					},
				},
			},
			giveDagIns: &DagInstance{
				ShareData: &ShareData{
					Dict: map[string]string{
						"key1": "value4",
					},
				},
			},
			wantRet: true,
			wantTaskIns: &TaskInstance{
				PreChecks: PreChecks{
					"first": {
						Conditions: []TaskCondition{
							{
								Source: TaskConditionSourceShareData,
								Key:    "key1",
								Values: []string{"value3"},
								Op:     OperatorIn,
							},
						},
						Act: ActiveActionBlock,
					},
					"second": {
						Conditions: []TaskCondition{
							{
								Source: TaskConditionSourceShareData,
								Key:    "key1",
								Values: []string{"value4"},
								Op:     OperatorIn,
							},
						},
						Act: ActiveActionSkip,
					},
				},
				Status: TaskInstanceStatusSkipped,
			},
		},
		{
			caseDesc: "vars",
			giveTaskIns: &TaskInstance{
				PreChecks: PreChecks{
					"first": {
						Conditions: []TaskCondition{
							{
								Source: TaskConditionSourceVars,
								Key:    "key1",
								Values: []string{"value3"},
								Op:     OperatorIn,
							},
						},
						Act: ActiveActionBlock,
					},
				},
			},
			giveDagIns: &DagInstance{
				Vars: DagInstanceVars{
					"key1": {Value: "value3"},
				},
			},
			wantRet: true,
			wantTaskIns: &TaskInstance{
				PreChecks: PreChecks{
					"first": {
						Conditions: []TaskCondition{
							{
								Source: TaskConditionSourceVars,
								Key:    "key1",
								Values: []string{"value3"},
								Op:     OperatorIn,
							},
						},
						Act: ActiveActionBlock,
					},
				},
				Status: TaskInstanceStatusBlocked,
			},
		},
		{
			caseDesc: "vars not meet",
			giveTaskIns: &TaskInstance{
				PreChecks: PreChecks{
					"first": {
						Conditions: []TaskCondition{
							{
								Source: TaskConditionSourceVars,
								Key:    "key1",
								Values: []string{"value1", "value2"},
								Op:     OperatorIn,
							},
						},
						Act: ActiveActionBlock,
					},
				},
			},
			giveDagIns: &DagInstance{
				Vars: DagInstanceVars{
					"key1": {Value: "value3"},
				},
			},
			wantRet: false,
			wantTaskIns: &TaskInstance{
				PreChecks: PreChecks{
					"first": {
						Conditions: []TaskCondition{
							{
								Source: TaskConditionSourceVars,
								Key:    "key1",
								Values: []string{"value1", "value2"},
								Op:     OperatorIn,
							},
						},
						Act: ActiveActionBlock,
					},
				},
			},
		},
		{
			caseDesc: "invalid-act",
			giveTaskIns: &TaskInstance{
				PreChecks: PreChecks{
					"first": {
						Conditions: []TaskCondition{
							{
								Source: TaskConditionSourceVars,
								Key:    "key1",
								Values: []string{"value3"},
								Op:     OperatorIn,
							},
						},
						Act: "invalid-act",
					},
				},
			},
			giveDagIns: &DagInstance{
				Vars: DagInstanceVars{
					"key1": {Value: "value3"},
				},
			},
			wantRet: false,
			wantErr: fmt.Errorf("pre-check[first] act is invalid: invalid-act"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.caseDesc, func(t *testing.T) {
			ret, err := tc.giveTaskIns.DoPreCheck(tc.giveDagIns)
			assert.Equal(t, tc.wantRet, ret)
			assert.Equal(t, tc.wantErr, err)
			if err == nil {
				assert.Equal(t, tc.wantTaskIns, tc.giveTaskIns)
			}
		})
	}
}
