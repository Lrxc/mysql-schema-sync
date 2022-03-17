package logm

import (
	"bufio"
	"os"
)

//日志打印的颜色
const (
	TextBlack = iota + 30
	TextRed
	TextGreen
	TextYellow
	TextBlue
	TextMagenta
	TextCyan
	TextWhite
)

//保存的sql路径
var outpath string

func Init(path string) {
	outpath = path
}

//保存到文件
func SaveFile(text string) {
	file, err := os.OpenFile(outpath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		panic("文件打开失败")
	}
	//及时关闭file句柄
	defer file.Close()

	//写入文件时，使用带缓存的 *Writer
	write := bufio.NewWriter(file)
	write.WriteString(text)
	//Flush将缓存的文件真正写入到文件中
	write.Flush()
}
