package common

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func GetTodayName(dir string) string {
	t := time.Now()
	year, month, day := t.Date()
	res := fmt.Sprintf("%04d%02d%02d.log", year, month, day)
	return filepath.Join(dir, res)
}

func FileExist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}
