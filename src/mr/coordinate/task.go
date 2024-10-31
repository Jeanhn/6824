package coordinate

import (
	"errors"

	"6.5840/mr/data"
	"6.5840/mr/util"
)

const (
	MAP_TASK_TYPE    = 1
	REDUCE_TASK_TYPE = 2
)

type Task struct {
	id        int64
	ProjectId string
	File      string
	Type      int
}

func newTask(taskId, file string, taskType int) (Task, error) {
	id, err := data.Default().IdGenerate()
	if err != nil {
		return Task{}, err
	}
	return Task{
		id:        id,
		ProjectId: taskId,
		File:      file,
		Type:      taskType,
	}, nil
}

type TaskManager struct {
	taskId    string
	initQueue *util.Queue
	waitMap   map[int64]Task
	doneQueue []Task
}

func NewTaskManager(splitFiles []string, taskId string) (*TaskManager, error) {
	q := util.NewQueue()
	for _, file := range splitFiles {
		t, err := newTask(taskId, file, MAP_TASK_TYPE)
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

func (tm *TaskManager) Done(id int64) error {
	t, ok := tm.waitMap[id]
	if !ok {
		return errors.New("task is not waitting")
	}
	delete(tm.waitMap, id)
	tm.doneQueue = append(tm.doneQueue, t)
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

	tm.waitMap[t.id] = t
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
