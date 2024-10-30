package coordinate

import (
	"os"
	"testing"
)

func TestMerge(t *testing.T) {
	_, err := merge([]string{"demo.txt", "mr-merge-temp-1730274037"})
	if err != nil {
		t.Error(err)
	}
}

func TestMergeAndSplit(t *testing.T) {
	se, err := NewSplitExecutor([]string{"demo.txt", "demo2.txt"}, 50)
	if err != nil {
		t.Error(err)
	}

	f, err := os.OpenFile("test-merge.txt", os.O_CREATE|os.O_RDWR, 0666)
	defer f.Close()
	if err != nil {
		t.Error(err)
	}

	contents, err := se.iterate()
	if err != nil {
		t.Error(err)
	}
	for len(contents) != 0 {
		for _, line := range contents {
			_, err = f.WriteString(line)
			if err != nil {
				t.Error(err)
			}
		}
		f.WriteString("\r\n")
		contents, err = se.iterate()
		if err != nil {
			t.Error(err)
		}
	}
}
