package iowrapper

import (
	"io/ioutil"
	"log"
	"os"
)

func PathExist(_path string) bool {
	_, err := os.Stat(_path)
	if err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}

func CreateSparseFile(pathFile string, fileSize int64) error {
	if PathExist(pathFile) {
		return nil
	}
	f, err := os.Create(pathFile)
	if err != nil {
		log.Fatal(err)
	}

	if err := f.Truncate(fileSize); err != nil {
		log.Fatal(err)
	}
	return nil
}

func TruncateIndexFile(pathFile string, extendSize int64) error {
	fileHandle, err := os.OpenFile(pathFile, os.O_RDWR, 0666)
	defer fileHandle.Close()
	fi, err := fileHandle.Stat()
	fileHandle.Truncate(extendSize + fi.Size())
	fileHandle.Sync()
	return err
}

func MmapIndexFile() {

}

func WriteFile(fileName string, b []byte) {
	err := ioutil.WriteFile("output.txt", b, 0644)
	if err != nil {
		panic(err)
	}
}
