package config

import (
	"fmt"
	"runtime"
)

// Version is the build version stamp; goreleaser overrides it via
// -X github.com/jamesits/hfdl/pkg/config.Version=...
var Version = "0.0.0-dev"

var ProjectURL = "https://github.com/jamesits/hfdl"

func VersionString() string {
	return Version
}

func UserAgent() string {
	return fmt.Sprintf("hfdl/%s (+%s ;%s; %s)", VersionString(), ProjectURL, runtime.GOOS, runtime.GOARCH)
}
