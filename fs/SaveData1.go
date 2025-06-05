package fs

import "os"

// Save data with file and data
func SaveData1(path string, data []byte) error {
	// first, open up file with path with write only, create, and truncate.
	// and owner can read/write(6), group can read(4), and others can read(4) too.
	// OpenFile return file pointer and error as a result
	fp, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)

	// error check
	if err != nil {
		return err
	}

	// using defer: if this function 'SaveData1' is about to finish it's execution,
	// regardless of successfully executed or throwing error
	// just execute this fp.Close() function
	defer fp.Close()

	// write data to file
	_, err = fp.Write(data)
	
	// error check
	if err != nil {
		return err
	}
	
	// sync the data with file
	return fp.Sync()
}
