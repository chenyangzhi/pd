package iowrapper

import (
	"fmt"
	"github.com/edsrzf/mmap-go"
	"os"
	"testing"
)

func TestMmap(t *testing.T) {
	configFile, err := os.OpenFile("/tmp/test/logtext", os.O_RDWR, 0666)
	defer configFile.Close()
	if err != nil {
		fmt.Println(err)
	}
	mp, err := mmap.MapRegion(configFile, 1024, mmap.RDWR, 0, 0)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(mp)
	copy(mp, []byte("=================fsdfsdfsdfsdfasdfsd"))
	fmt.Println(mp)
	//some actions happen here
	mp.Flush()
	//configFile.Write([]byte("fadfasfsdf"))
	//configFile.Sync()
	//configFile.Truncate(1024)
}
