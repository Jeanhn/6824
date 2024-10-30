package data

import (
	"sync"
	"time"
)

type Data interface {
	IdGenerate() (int64, error)
	Put(string, interface{}) error
	Get(string) (interface{}, error)
}

func Default() Data {
	return &defaultData{
		database: make(map[string]interface{}),
		lock:     sync.RWMutex{},
	}
}

type defaultData struct {
	database map[string]interface{}
	lock     sync.RWMutex
}

func (dd *defaultData) IdGenerate() (int64, error) {
	return time.Now().Unix(), nil
}

func (dd *defaultData) Put(key string, value interface{}) error {
	dd.lock.Lock()
	defer dd.lock.Unlock()
	dd.database[key] = value
	return nil
}

func (dd *defaultData) Get(key string) (interface{}, error) {
	dd.lock.RLock()
	defer dd.lock.RUnlock()
	v, ok := dd.database[key]
	if !ok {
		return nil, nil
	}
	return v, nil
}
