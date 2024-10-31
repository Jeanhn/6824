package mr

import (
	"sync"
	"testing"
)

func TestMakeAndServe(t *testing.T) {
	inputFiles := []string{"/home/jean/6.5840/src/mr/coordinate/demo2.txt", "/home/jean/6.5840/src/mr/coordinate/demo.txt"}
	c := MakeCoordinator(inputFiles, 2)

	wg := sync.WaitGroup{}

	wg2 := sync.WaitGroup{}

	wg.Add(1)
	wg2.Add(16)
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
			}
			wg2.Done()
		}()
	}

	wg.Done()
	wg2.Wait()
}
