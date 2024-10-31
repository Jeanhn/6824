package coordinate

import "testing"

func TestTaskManager(t *testing.T) {
	tm, err := NewTaskManager([]string{"1", "2", "3"}, randomTaskId(), 3, 2)
	if err != nil {
		t.Fatal(err)
	}

	temp := make([]*Task, 0)
	for {
		task, err := tm.Acquire()
		if err != nil {
			t.Fatal(err)
		}
		if task == nil {
			break
		}
		temp = append(temp, task)
	}

	task := temp[2]
	temp = temp[0:2]
	tm.Timeout(task.Id)

	task, err = tm.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	if task == nil {
		t.Fatal("should have a init which is timout before")
	}

	temp = append(temp, task)

	for i := range temp {
		err := tm.Done(temp[i].Id)
		if err != nil {
			t.Fatal(err)
		}
	}

	if !tm.Finish() {
		t.Fatal("should finish")
	}
}
