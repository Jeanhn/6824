package work

import (
	"bufio"
	"os"

	"6.5840/mr/coordinate"
	"6.5840/mr/util"
)

func SortKeyValueFile(filename string) error {
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
					wr.WriteString(kvString)
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
