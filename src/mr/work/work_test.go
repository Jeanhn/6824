package work

import (
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
