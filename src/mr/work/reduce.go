package work

import (
	"bufio"
	"container/heap"
	"os"

	"6.5840/mr/coordinate"
	"6.5840/mr/util"
)

const (
	MergingFileFormat = "mr-merging-file-id%v"
)

func mergeSortedFiles(filenames []string, dest string) error {
	fileReaders := make([]*bufio.Scanner, 0, len(filenames))
	for _, filename := range filenames {
		f, err := os.Open(filename)
		if err != nil {
			return err
		}
		sc := bufio.NewScanner(f)
		sc.Split(bufio.ScanLines)
		fileReaders = append(fileReaders, sc)
	}

	destFile, err := os.OpenFile(dest, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer destFile.Close()
	wr := bufio.NewWriter(destFile)

	kvh := KeyValueHeap(make([]KeyValue, 0))
	heap.Init(&kvh)
	keyValueIndex := make(map[KeyValue][]int, 0)

	for index, reader := range fileReaders {
		if !reader.Scan() {
			continue
		}
		err := reader.Err()
		if err != nil {
			return err
		}

		ukv, err := util.UnmarshalKeyAndValue(reader.Bytes())
		if err != nil {
			return err
		}
		kv := KeyValue{}
		kv.from(ukv)
		heap.Push(&kvh, kv)
		_, ok := keyValueIndex[kv]
		if !ok {
			keyValueIndex[kv] = make([]int, 0)
		}
		keyValueIndex[kv] = append(keyValueIndex[kv], index)
	}

	for kvh.Len() != 0 {
		kv := heap.Pop(&kvh).(KeyValue)
		wr.WriteString(kv.fmt())
		if wr.Size() > BLOCK_SIZE_LIMIT {
			err = wr.Flush()
			if err != nil {
				return err
			}
		}

		index := keyValueIndex[kv][0]
		keyValueIndex[kv] = keyValueIndex[kv][1:]
		if len(keyValueIndex[kv]) == 0 {
			delete(keyValueIndex, kv)
		}
		var nextLine []byte
		if fileReaders[index].Scan() {
			err = fileReaders[index].Err()
			if err != nil {
				return err
			}
			nextLine = fileReaders[index].Bytes()
			ukv, err := util.UnmarshalKeyAndValue(nextLine)
			if err != nil {
				return err
			}
			nextKeyValue := KeyValue{}
			nextKeyValue.from(ukv)

			heap.Push(&kvh, nextKeyValue)
			_, ok := keyValueIndex[nextKeyValue]
			if !ok {
				keyValueIndex[nextKeyValue] = make([]int, 0)
			}
			keyValueIndex[nextKeyValue] = append(keyValueIndex[nextKeyValue], index)
		}
	}
	err = wr.Flush()
	if err != nil {
		return err
	}
	return nil
}

func ReduceHandler(task coordinate.Task, reducef func(string, []string) string) error {
	targetFiles := make([]*bufio.Writer, 0)
	for _, filename := range task.TargetFiles {
		f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return err
		}
		wr := bufio.NewWriter(f)
		targetFiles = append(targetFiles, wr)
	}

	for _, filename := range task.InputFiles {
		f, err := os.Open(filename)
		if err != nil {
			return err
		}
		sc := bufio.NewScanner(f)
		sc.Split(bufio.ScanLines)

		err = sc.Err()
		if err != nil {
			return err
		}
		for sc.Scan() {
			err = sc.Err()
			if err != nil {
				return err
			}

			byts := sc.Bytes()
			ukv, err := util.UnmarshalKeyAndValue(byts)
			if err != nil {
				return err
			}
			kv := KeyValue{}
			kv.from(ukv)
		}
	}
	return nil
}
