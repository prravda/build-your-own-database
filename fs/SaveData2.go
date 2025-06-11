package fs

import (
	"fmt"
	"math/rand/v2"

	"os"
)

func SaveData2(path string, data []byte) error {
	// create temp file with path and rand number
	tmp := fmt.Sprintf("%s.tmp.%d", path, rand.Uint64())

	// read file with OpenFile
	fp, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0664)

	if err != nil {
		return err
	}

	// defer function running before SaveDate2 execution is ended
	defer func() {
		fp.Close()
		if err != nil {
			os.Remove(tmp)
		}
	}()
	
	_, err = fp.Write(data)

	if err != nil {
		return err
	}

	err = fp.Sync()

	if err != nil {
		return err
	}

	return os.Rename(tmp, path)
}