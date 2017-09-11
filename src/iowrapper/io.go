package iowrapper

import (
	"log"
	"os"
)

func CreateSparseFile(pathFile string, fileSize int64) error {
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
