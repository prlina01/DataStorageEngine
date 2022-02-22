package Utils

import (
	"io/ioutil"
	"regexp"
)

func Find(pattern string) []string {
	files, _ := ioutil.ReadDir("Data")
	var neededFiles []string
	for _, f := range files {
		str := f.Name()
		reg, _ := regexp.MatchString(pattern, str)
		if reg {
			neededFiles = append(neededFiles, str )}
	}
	return neededFiles
}