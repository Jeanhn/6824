package data

import "sync/atomic"

type Counter interface {
	Add() (int, error)
	Get() (int, error)
}

type defaultCounter struct {
	i int64
}

func (dc *defaultCounter) Add() (int, error) {
	return int(atomic.AddInt64(&dc.i, 1)), nil
}

func (dc *defaultCounter) Get() (int, error) {
	return int(atomic.LoadInt64(&dc.i)), nil
}

func NewDefaultCounter() Counter {
	return &defaultCounter{
		i: 0,
	}
}
