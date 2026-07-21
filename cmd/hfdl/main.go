package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/jamesits/hfdl/pkg/config"
)

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "hfdl",
		Short:         "High-speed Hugging Face downloader",
		Long:          "hfdl is a high-speed, drop-in compatible downloader for the Hugging Face Hub.",
		Version:       config.VersionString(),
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(newDownloadCmd(), newLogsCmd())
	return root
}

func main() {
	if err := newRootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}
