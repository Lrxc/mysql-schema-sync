package log

import (
	"fmt"
	logm "github.com/hidu/mysql-schema-sync/internal/logm"
	"log"
)

//conf := 0   // 配置、终端默认设置
//bg   := 0   // 背景色、终端默认设置
//text := 31  // 前景色、红色
//fmt.Printf("\n %c[%d;%d;%dm%s%c[0m\n\n", 0x1B, conf, bg, text, "testPrintColor", 0x1B)

//颜色打印
func Println(v ...interface{}) {
	str := fmt.Sprint(v...)
	//添加颜色
	log.Printf("\033[0;0;%dm%s\033[0m", logm.TextRed, str)
}
