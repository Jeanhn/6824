package work

import (
	"bufio"
	"encoding/json"
	"os"

	"6.5840/mr/coordinate"
)

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
			kv := KeyValue{}
			err := json.Unmarshal(byts, &kv)
			if err != nil {
				return err
			}

		}
	}
	return nil
}
