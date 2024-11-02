package work

import (
	"container/heap"
	"os"
	"strconv"
	"strings"
	"testing"
	"unicode"

	"6.5840/mr/coordinate"
)

func mapf(filename string, contents string) []KeyValue {
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []KeyValue{}
	for _, w := range words {
		kv := KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}

func TestMap(t *testing.T) {
	task := coordinate.Task{
		InputFiles:  []string{"/home/jean/6.5840/src/mr/coordinate/demo.txt", "/home/jean/6.5840/src/mr/coordinate/demo2.txt"},
		TargetFiles: []string{"target1", "target2"},
	}

	MapHandler(task, mapf)
}

func TestHeap(t *testing.T) {
	kvs := make([]KeyValue, 0)
	kvh := KeyValueHeap(kvs)

	for i := 0; i < 10; i++ {
		kv := KeyValue{
			Key:   strconv.Itoa(i),
			Value: strconv.Itoa(i),
		}
		heap.Push(&kvh, kv)
	}

	arr := make([]KeyValue, 0)
	for kvh.Len() != 0 {
		v := heap.Pop(&kvh).(KeyValue)
		arr = append(arr, v)
	}
	for i := 0; i < len(arr)-1; i++ {
		if arr[i].Key >= arr[i+1].Key {
			t.Fatal(arr)
		}
	}
}

func TestWrite(t *testing.T) {
	f1, _ := os.OpenFile("yeah", os.O_CREATE|os.O_RDWR, 0666)
	f2, _ := os.OpenFile("yeah", os.O_CREATE|os.O_RDWR, 0666)
	f1.WriteString("yeah")
	f2.WriteString("u")
	f2.Close()
	f1.Close()
}

func TestMerge(t *testing.T) {
	err := mergeSortedFiles([]string{"sorted1", "sorted2", "sorted3"}, "yeah")
	if err != nil {
		t.Fatal(err)
	}
}
