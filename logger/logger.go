package logger

import (
	"encoding/json"
	"fmt"
	"time"
)

const (
	OFF   int32 = 1
	FATAL int32 = 2
	ERROR int32 = 3
	WARN  int32 = 4
	INFO  int32 = 5
	DEBUG int32 = 6
	TRACE int32 = 7
	ALL   int32 = 8
)

var globalLevel = []string{"", "OFF", "FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE", "ALL"}
var CurLogLevel int32 = 8

func Print(level int32, module string, args ...interface{}) {

	if level <= CurLogLevel {
		logMap := make(map[string]interface{})
		logMap["level"] = globalLevel[level]
		logMap["module"] = module
		if len(args) == 1 {
			logMap["log"] = args[0]
		} else {
			logMap["log"] = fmt.Sprintf(args[0].(string), args[1])
		}
		logMap["timeStamp"] = time.Now().UnixMicro()

		jbyte, _ := json.Marshal(logMap)
		fmt.Println(string(jbyte) + "\n")

	}
}
