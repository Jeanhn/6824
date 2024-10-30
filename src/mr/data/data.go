package data

import (
	"errors"
	"reflect"
	"sync"
	"time"
)

type Data interface {
	IdGenerate() (int64, error)
	Put(string, interface{}) error
	Get(string, interface{}) (error, bool)
}

var defaultGlobalData Data = &defaultData{
	database: make(map[string]interface{}),
	lock:     sync.RWMutex{},
}

func Default() Data {
	return defaultGlobalData
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

func (dd *defaultData) Get(key string, dest interface{}) (error, bool) {
	dd.lock.RLock()
	defer dd.lock.RUnlock()
	v, ok := dd.database[key]
	if !ok {
		return nil, false
	}

	d := reflect.ValueOf(dest)
	if d.Kind() != reflect.Ptr {
		return errors.New("dest should be pointer"), false
	}

	rv := reflect.ValueOf(v)
	if !rv.Type().AssignableTo(d.Elem().Type()) {
		return errors.New("not assignable"), false
	}
	d.Elem().Set(rv)

	return nil, true
}

func Lock(name string) error {
	return nil
}

func Unlock(name string) error {
	return nil
}
