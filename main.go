package main

import (
	"fmt"

	"github.com/pravda/build-your-own-database/fs"
)

func main() {
	// Example usage of your fs package
	data := []byte("Hello, Database!")
	err := fs.SaveData1("test.db", data)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Println("Data successfully saved")
}
