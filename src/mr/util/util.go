package util

import (
	"encoding/json"
	"io"
	"os"
	"reflect"
	"unsafe"
)

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

func CollectTempFile(name string) {
	tempFiles = append(tempFiles, name)
}

func RemoveTempFiles() error {
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
