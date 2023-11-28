// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package app

import (
	"fmt"
	"os"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

// Command is the Interface of command.
type Command interface {
	// AddCommands add children commands to the Command.
	AddCommands(cmds ...Command)
	// Command returns the cobra command instance of the Command.
	Command() *cobra.Command
}

// command is a sub command structure of an application.
// It is recommended that a command be created with the app.NewCommand()
// function.
type command struct {
	name        string
	short       string
	description string
	options     CmdOptions
	commands    []Command
	runFunc     RunFunc
}

// CommandOption defines optional parameters for initializing the command
// structure.
type CommandOption func(*command)

// WithCmdOptions to open the application's function to read from the command line.
func WithCmdOptions(opt CmdOptions) CommandOption {
	return func(c *command) {
		c.options = opt
	}
}

// WithCmdDescription is used to set the description of the command.
func WithCmdDescription(desc string) CommandOption {
	return func(c *command) {
		c.description = desc
	}
}

// WithCmdRunFunc is used to set the application's command startup callback
// function option.
func WithCmdRunFunc(run RunFunc) CommandOption {
	return func(c *command) {
		c.runFunc = run
	}
}

// NewCommand creates a new sub command instance based on the given command name
// and other options.
func NewCommand(name string, short string, opts ...CommandOption) Command {
	c := &command{
		name:  name,
		short: short,
	}

	for _, o := range opts {
		o(c)
	}

	return c
}

func (c *command) AddCommands(cmds ...Command) {
	c.commands = append(c.commands, cmds...)
}

func (c *command) Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   c.name,
		Short: c.short,
		Long:  c.description,
	}
	cmd.SetOut(os.Stdout)
	cmd.SetErr(os.Stderr)
	cmd.Flags().SortFlags = false

	if len(c.commands) > 0 {
		for _, c := range c.commands {
			cmd.AddCommand(c.Command())
		}
	}
	if c.runFunc != nil {
		cmd.Run = c.runCommand
	}
	if c.options != nil {
		fs := cmd.Flags()
		for _, f := range c.options.Flags().FlagSets {
			fs.AddFlagSet(f)
		}
	}
	addHelpFlag(c.name, cmd.Flags())

	return cmd
}

func (c *command) runCommand(cmd *cobra.Command, args []string) {
	if c.runFunc != nil {
		if err := c.runFunc(c.name); err != nil {
			fmt.Printf("%v %v\n", color.RedString("Error:"), err)
			os.Exit(1)
		}
	}
}
