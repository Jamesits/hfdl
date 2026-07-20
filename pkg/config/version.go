package config

import (
	"fmt"
	"runtime"
)

var (
	MajorVersion = 0
	MinorVersion = 0
	Revision     = 0
	ProjectURL   = "https://github.com/jamesits/hfdl"
)

func VersionString() string {
	return fmt.Sprintf("%d.%d.%d", MajorVersion, MinorVersion, Revision)
}

func UserAgent() string {
	return fmt.Sprintf("hfdl/%s (+%s ;%s; %s)", VersionString(), ProjectURL, runtime.GOOS, runtime.GOARCH)
}
