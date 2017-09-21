package main

import (
	"time"
	logger "until/xlog4go"
)

func main() {
	if err := logger.SetupLogWithConf("./log.json"); err != nil {
		panic(err)
	}
	defer logger.Close()

	var name = "shengkehua"
	for {
		logger.Trace("log4go by %s", name)
		logger.Debug("log4go by %s", name)
		logger.Info("log4go by %s", name)
		logger.Warn("log4go by %s", name)
		logger.Error("log4go by %s", name)
		logger.Fatal("log4go by %s", name)

		time.Sleep(time.Second * 1)
	}
}
