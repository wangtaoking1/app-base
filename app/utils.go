// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package app

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/wangtaoking1/app-base/flag"
	"github.com/wangtaoking1/app-base/utils/term"
)

var progressMessage = color.GreenString("==>")

// FormatExecName is formatted as an executable file name under different
// operating systems according to the given name.
func FormatExecName(name string) string {
	// Make case-insensitive and strip executable suffix if present
	if runtime.GOOS == "windows" {
		name = strings.ToLower(name)
		name = strings.TrimSuffix(name, ".exe")
	}

	return name
}

// addHelpFlag adds help flag to the specified FlagSet object.
func addHelpFlag(name string, fs *pflag.FlagSet) {
	fs.BoolP("help", "h", false, fmt.Sprintf("Help for %s.", name))
}

func addCmdTemplate(cmd *cobra.Command, namedFlagSets flag.NamedFlagSets) {
	usageFmt := "Usage:\n  %s\n"
	cols, _, _ := term.TerminalSize(cmd.OutOrStdout())
	cmd.SetUsageFunc(func(cmd *cobra.Command) error {
		writeString(cmd.OutOrStdout(), fmt.Sprintf(usageFmt, cmd.UseLine()))
		flag.PrintSections(cmd.OutOrStderr(), namedFlagSets, cols)

		return nil
	})
	cmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		writeString(cmd.OutOrStdout(), fmt.Sprintf("%s\n\n"+usageFmt, cmd.Long, cmd.UseLine()))
		flag.PrintSections(cmd.OutOrStdout(), namedFlagSets, cols)
	})
}

func writeString(w io.Writer, s string) {
	_, _ = io.WriteString(w, s)
}

func printWorkingDir() {
	wd, _ := os.Getwd()
	fmt.Printf("%v WorkingDir: %s\n", progressMessage, wd)
}
