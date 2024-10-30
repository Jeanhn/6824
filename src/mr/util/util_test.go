package util

import (
	"testing"
)

func TestItoa(t *testing.T) {
	i := int64(100)
	s := I64ToString(i)
	if s != "100" {
		t.Error()
	}
}
