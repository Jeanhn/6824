package util

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

var logFile *bufio.Writer
var logMutex sync.Mutex = sync.Mutex{}

const LOG_SIZE int = 128 * 1024

func init() {
	f, err := os.OpenFile("logfile.txt", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	logFile = bufio.NewWriter(f)
}

func WriteTo(src interface{}, file io.Writer) error {
	enc := json.NewEncoder(file)
	err := enc.Encode(src)
	if err != nil {
		return err
	}
	return nil
}

func ReadFrom(file io.Reader, desc interface{}) error {
	dec := json.NewDecoder(file)

	err := dec.Decode(desc)
	if err != nil {
		return err
	}
	return nil
}

func I64ToString(i int64) string {
	f := false
	if i < 0 {
		f = true
		i = -i
	}

	buf := make([]byte, 0, 64)
	for i != 0 {
		k := i % 10
		buf = append(buf, byte(k+'0'))
		i /= 10
	}
	if f {
		buf = append(buf, '-')
	}

	for i, j := 0, len(buf)-1; i < j; {
		buf[i], buf[j] = buf[j], buf[i]
		i++
		j--
	}

	return *(*string)(unsafe.Pointer(&buf))
}

var tempFiles = make([]string, 0)
var tempFileLock sync.Mutex = sync.Mutex{}

func CollectTempFile(name string) {
	tempFileLock.Lock()
	defer tempFileLock.Unlock()
	tempFiles = append(tempFiles, name)
}

func RemoveTempFiles() error {
	tempFileLock.Lock()
	defer tempFileLock.Unlock()
	for _, file := range tempFiles {
		err := os.Remove(file)
		if err != nil {
			return err
		}
	}
	return nil
}

func BytesToString(byts []byte) string {
	bh := *(*reflect.SliceHeader)(unsafe.Pointer(&byts))
	sh := reflect.StringHeader{
		Data: bh.Data,
		Len:  bh.Len,
	}
	return *(*string)(unsafe.Pointer(&sh))
}

func StringToBytes(s string) []byte {
	sh := *(*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{
		Data: sh.Data,
		Len:  sh.Len,
	}
	return *(*[]byte)(unsafe.Pointer(&bh))
}

func RandomTaskId() string {
	return "mr-task-" + I64ToString(time.Now().Unix())
}

func Ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func UnmarshalKeyAndValue(byts []byte) ([]string, error) {
	ans := bytes.Split(byts, []byte{' '})
	if len(ans) != 2 {
		return nil, errors.New("UnmarshalKeyAndValue:wrong src input")
	}
	return []string{BytesToString(ans[0]), BytesToString(ans[1])}, nil
}

var localId int64 = 0

func LocalIncreaseId() int64 {
	n := atomic.AddInt64(&localId, 1)
	return n - 1
}

func Log(format string, args ...interface{}) error {
	logf := fmt.Sprintf(format, args...)
	logMutex.Lock()
	defer logMutex.Unlock()
	_, err := logFile.WriteString(logf)
	if err != nil {
		return err
	}
	if logFile.Size() > LOG_SIZE {
		err = logFile.Flush()
		if err != nil {
			return err
		}
	}
	return nil
}

func FlushLogs() error {
	logMutex.Lock()
	defer logMutex.Unlock()
	return logFile.Flush()
}
