package Utils

import (
	"fmt"
	"io/ioutil"
	"regexp"
)

func Find(pattern string) []string {
	files, _ := ioutil.ReadDir("Data")
	fmt.Println(files)
	neededFiles := []string{}
	for _, f := range files {
		str := f.Name()
		reg, _ := regexp.MatchString(pattern, str)
		if reg {
			neededFiles = append(neededFiles, str )}
	}
	return neededFiles
}