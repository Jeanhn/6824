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

func TestB2S(t *testing.T) {
	b := []byte("123")
	s := BytesToString(b)
	b[0] = '9'
	if s != "923" {
		t.Fail()
	}
}
