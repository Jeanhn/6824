package coordinate

import (
	"errors"
	"fmt"

	"6.5840/mr/data"
	"6.5840/mr/util"
)

const (
	MAP_TASK_TYPE    = 1
	REDUCE_TASK_TYPE = 2

	MapResultFormat = "mr-map-result-task%v-id%v-shard%v"

	ReduceResultFormat = "mr-reduce-result-task%v-shard%v"
)

type Task struct {
	Id          int64
	ProjectId   string
	InputFiles  []string
	Type        int
	TargetFiles []string
}

func getMappedFileName(projectId string, id int64, nReduce int) []string {
	ret := make([]string, 0, nReduce)
	for i := 1; i <= nReduce; i++ {
		name := fmt.Sprintf(MapResultFormat, projectId, id, i)
		ret = append(ret, name)
	}
	return ret
}

func newMapTask(taskId string, inputFiles []string, nReduce int) (Task, error) {
	id, err := data.Default().IdGenerate()
	if err != nil {
		return Task{}, err
	}
	return Task{
		Id:          id,
		ProjectId:   taskId,
		InputFiles:  inputFiles,
		Type:        MAP_TASK_TYPE,
		TargetFiles: getMappedFileName(taskId, id, nReduce),
	}, nil
}

type TaskManager struct {
	taskId    string
	initQueue *util.Queue
	waitMap   map[int64]Task
	doneQueue []Task
}

func NewTaskManager(splitFiles []string, taskId string, nWorker, nReduce int) (*TaskManager, error) {
	q := util.NewQueue()

	limit := len(splitFiles) / nWorker
	taskFiles := make([]string, 0, limit)
	for _, file := range splitFiles {
		taskFiles = append(taskFiles, file)
		if len(taskFiles) == limit {
			t, err := newMapTask(taskId, taskFiles, nReduce)
			if err != nil {
				return nil, err
			}
			q.Push(t)
			taskFiles = make([]string, 0, limit)
		}
	}

	if len(taskFiles) != 0 {
		t, err := newMapTask(taskId, taskFiles, nReduce)
		if err != nil {
			return nil, err
		}
		q.Push(t)
	}

	return &TaskManager{
		taskId:    taskId,
		initQueue: &q,
		waitMap:   make(map[int64]Task),
		doneQueue: make([]Task, 0),
	}, nil
}

func getNReduceFileName(projectId string, nReduce int) []string {
	ret := make([]string, 0, nReduce)
	for i := 1; i <= nReduce; i++ {
		name := fmt.Sprintf(ReduceResultFormat, projectId, i)
		ret = append(ret, name)
	}
	return ret
}

func newReduceTask(task Task) ([]Task, error) {
	if task.Type != MAP_TASK_TYPE {
		panic("wrong task type")
	}

	ret := make([]Task, 0)
	for _, shard := range task.TargetFiles {
		id, err := data.Default().IdGenerate()
		if err != nil {
			return nil, err
		}
		t := Task{
			Id:          id,
			ProjectId:   task.ProjectId,
			Type:        REDUCE_TASK_TYPE,
			InputFiles:  []string{shard},
			TargetFiles: getNReduceFileName(task.ProjectId, len(task.TargetFiles)),
		}
		ret = append(ret, t)
	}
	return ret, nil
}

func (tm *TaskManager) Done(id int64) error {
	t, ok := tm.waitMap[id]
	if !ok {
		return errors.New("task is not waitting")
	}
	delete(tm.waitMap, id)
	tm.doneQueue = append(tm.doneQueue, t)

	if t.Type == MAP_TASK_TYPE {
		reduceTasks, err := newReduceTask(t)
		if err != nil {
			return err
		}
		for _, rt := range reduceTasks {
			tm.initQueue.Push(rt)
		}
	}
	return nil
}

func (tm *TaskManager) Acquire() (*Task, error) {
	if tm.initQueue.Empty() {
		return nil, nil
	}
	e := tm.initQueue.Pop()
	t, ok := e.(Task)
	if !ok {
		return nil, errors.New("wrong type in init q")
	}

	tm.waitMap[t.Id] = t
	return &t, nil
}

func (tm *TaskManager) Timeout(id int64) error {
	t, ok := tm.waitMap[id]
	if !ok {
		return errors.New("task is not waitting")
	}
	delete(tm.waitMap, id)
	tm.initQueue.Push(t)
	return nil
}

func (tm *TaskManager) Finish() bool {
	return tm.initQueue.Empty() && len(tm.waitMap) == 0
}
