package work

import (
	"bufio"
	"fmt"
	"os"
	"sort"

	"6.5840/mr/coordinate"
	"6.5840/mr/util"
)

const (
	SortFileFormat = "mr-sort-file-prefix-id%v"
)

func SortKeyValueFile(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Split(bufio.ScanLines)

	kvs := make([]KeyValue, 0)
	kvSize := 0
	tempSortedFiles := make([]string, 0)
	tempIndex := 0

	var allocateAndSort func() error = func() error {
		tempName := fmt.Sprintf(SortFileFormat, tempIndex)
		tempIndex++
		f, err := os.OpenFile(tempName, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return err
		}
		defer f.Close()
		wr := bufio.NewWriter(f)
		util.CollectTempFile(tempName)
		tempSortedFiles = append(tempSortedFiles, tempName)
		sort.Sort(KeyValueArray(kvs))
		for _, kv := range kvs {
			_, err = wr.WriteString(kv.fmt())
			if err != nil {
				return err
			}
		}
		err = wr.Flush()
		if err != nil {
			return err
		}

		kvs = make([]KeyValue, 0)
		kvSize = 0
		return nil
	}

	for sc.Scan() {
		err = sc.Err()
		if err != nil {
			return err
		}

		byts := sc.Bytes()
		kvSize += len(byts)
		ukv, err := util.UnmarshalKeyAndValue(byts)
		if err != nil {
			return err
		}
		kv := KeyValue{}
		kv.from(ukv)
		kvs = append(kvs, kv)

		if kvSize > BLOCK_SIZE_LIMIT {
			err = allocateAndSort()
			if err != nil {
				return err
			}
		}

	}
	defer util.RemoveTempFiles()

	err = allocateAndSort()
	if err != nil {
		return err
	}

	mergeSortedFiles(tempSortedFiles, filename)
	return nil
}

func MapHandler(task coordinate.Task, mapf func(string, string) []KeyValue) error {
	targetFiles := make([]*bufio.Writer, 0)
	for _, targetFile := range task.TargetFiles {
		f, err := os.OpenFile(targetFile, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return err
		}
		defer f.Close()
		wr := bufio.NewWriter(f)
		targetFiles = append(targetFiles, wr)
	}

	mapResult := make([]KeyValue, 0)
	resultSize := 0
	for _, filename := range task.InputFiles {
		f, err := os.Open(filename)
		if err != nil {
			return err
		}

		sc := bufio.NewScanner(f)
		sc.Split(bufio.ScanLines)

		for sc.Scan() {
			err = sc.Err()
			if err != nil {
				return err
			}
			byts := sc.Bytes()
			line := util.BytesToString(byts)
			kvs := mapf(filename, line)
			for _, kv := range kvs {
				resultSize += kv.size()
			}
			mapResult = append(mapResult, kvs...)

			if resultSize > BLOCK_SIZE_LIMIT {
				for _, kv := range mapResult {
					kvString := kv.fmt()
					n := ihash(kv.Key) % len(task.TargetFiles)
					wr := targetFiles[n]
					_, err := wr.WriteString(kvString)
					if err != nil {
						return err
					}
					if wr.Size() > BLOCK_SIZE_LIMIT {
						err := wr.Flush()
						if err != nil {
							return err
						}
					}
				}

				mapResult = make([]KeyValue, 0)
				resultSize = 0
			}
		}
	}

	for _, fileWriter := range targetFiles {
		err := fileWriter.Flush()
		if err != nil {
			return err
		}
	}
	return nil
}
