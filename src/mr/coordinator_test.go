package mr

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestMakeAndServe(t *testing.T) {
	inputFiles := []string{"/home/jean/6.5840/src/mr/coordinate/demo2.txt", "/home/jean/6.5840/src/mr/coordinate/demo.txt"}
	c := MakeCoordinator(inputFiles, 2)

	wg := sync.WaitGroup{}

	wg2 := sync.WaitGroup{}

	wg.Add(1)
	wg2.Add(16)

	var m, r int32
	m = 0
	r = 0
	for i := 0; i < 16; i++ {
		go func() {
			wg.Wait()
			for {
				acquire := AcquireArgs{
					workerId: "yeah",
				}
				acquireReply := AcquireReply{}

				err := c.Acquire(&acquire, &acquireReply)
				if err != nil {
					t.Fatal(err)
				}

				if acquireReply.task == nil {
					break
				}

				finish := FinishArgs{
					workerId: "yeah",
					task:     *acquireReply.task,
				}
				finishReply := FinishReply{}

				err = c.Finish(&finish, &finishReply)
				if err != nil {
					t.Fatal(err)
				}

				if acquireReply.task.Type == 1 {
					atomic.AddInt32(&m, 1)
				} else {
					atomic.AddInt32(&r, 1)
				}
			}
			wg2.Done()
		}()
	}

	wg.Done()
	wg2.Wait()

	if r != 2 {
		t.Error(r)
		t.Fail()
	}
}
