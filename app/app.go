// Copyright 2023 Tao Wang <wangtaoking1@qq.com>. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package app

import (
	"fmt"
	"os"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/wangtaoking1/app-base/errors"
	"github.com/wangtaoking1/app-base/flag"
	"github.com/wangtaoking1/app-base/log"
	"github.com/wangtaoking1/app-base/version"
	"github.com/wangtaoking1/app-base/version/verflag"
)

// App is the Interface of application.
type App interface {
	// Run launch the application.
	Run()

	// Command returns cobra command instance inside the application.
	Command() *cobra.Command
}

// app is the main structure of a cli application.
// It is recommended that an app be created with the app.NewApp() function.
type app struct {
	name        string
	short       string
	description string
	options     CmdOptions
	runFunc     RunFunc
	silence     bool
	noVersion   bool
	noConfig    bool
	commands    []Command
	args        cobra.PositionalArgs
	cmd         *cobra.Command
}

var _ App = (*app)(nil)

// Option defines optional parameters for initializing the application structure.
type Option func(*app)

// WithOptions to open the application's function to read from the command line
// or read parameters from the configuration file.
func WithOptions(opt CmdOptions) Option {
	return func(a *app) {
		a.options = opt
	}
}

// RunFunc defines the application's startup callback function.
type RunFunc func(name string) error

// WithRunFunc is used to set the application startup callback function option.
func WithRunFunc(run RunFunc) Option {
	return func(a *app) {
		a.runFunc = run
	}
}

// WithDescription is used to set the description of the application.
func WithDescription(desc string) Option {
	return func(a *app) {
		a.description = desc
	}
}

// WithSilence sets the application to silent mode, in which the program startup
// information, configuration information, and version information are not
// printed in the console.
func WithSilence() Option {
	return func(a *app) {
		a.silence = true
	}
}

// WithNoVersion set the application does not provide version flag.
func WithNoVersion() Option {
	return func(a *app) {
		a.noVersion = true
	}
}

// WithNoConfig set the application does not provide config flag.
func WithNoConfig() Option {
	return func(a *app) {
		a.noConfig = true
	}
}

// WithValidArgs set the validation function to valid non-flag arguments.
func WithValidArgs(args cobra.PositionalArgs) Option {
	return func(a *app) {
		a.args = args
	}
}

// WithDefaultValidArgs set default validation function to valid non-flag arguments.
func WithDefaultValidArgs() Option {
	return func(a *app) {
		a.args = func(cmd *cobra.Command, args []string) error {
			for _, arg := range args {
				if len(arg) > 0 {
					return fmt.Errorf("%q does not take any arguments, got %q", cmd.CommandPath(), args)
				}
			}

			return nil
		}
	}
}

// WithCommands set children commands for thie application.
func WithCommands(cmds ...Command) Option {
	return func(a *app) {
		a.commands = append(a.commands, cmds...)
	}
}

// NewApp creates a new application instance based on the given application name,
// binary name, and other options.
func NewApp(name string, short string, opts ...Option) App {
	a := &app{
		name:  name,
		short: short,
	}

	for _, o := range opts {
		o(a)
	}

	a.buildCommand()

	return a
}

func (a *app) buildCommand() {
	cmd := &cobra.Command{
		Use:   FormatExecName(a.name),
		Short: a.short,
		Long:  a.description,
		// stop printing usage when the command errors
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          a.args,
	}
	cmd.SetOut(os.Stdout)
	cmd.SetErr(os.Stderr)
	cmd.Flags().SortFlags = true
	flag.InitFlags(cmd.Flags())

	// add children commands
	if len(a.commands) > 0 {
		for _, c := range a.commands {
			cmd.AddCommand(c.Command())
		}
	}
	if a.runFunc != nil {
		cmd.RunE = a.runCommand
	}

	var namedFlagSets flag.NamedFlagSets
	if a.options != nil {
		namedFlagSets = a.options.Flags()
		fs := cmd.Flags()
		for _, f := range namedFlagSets.FlagSets {
			fs.AddFlagSet(f)
		}
	}

	// init global flagsets
	globalFlags := namedFlagSets.FlagSet("global")
	a.initGlobalFlags(globalFlags)
	cmd.Flags().AddFlagSet(globalFlags)

	addCmdTemplate(cmd, namedFlagSets)
	a.cmd = cmd
}

func (a *app) initGlobalFlags(globalFlags *pflag.FlagSet) {
	if !a.noVersion {
		verflag.AddFlags(globalFlags)
	}
	if !a.noConfig {
		addConfigFlag(a.name, globalFlags)
	}
	addHelpFlag(a.name, globalFlags)
}

func (a *app) Run() {
	if err := a.cmd.Execute(); err != nil {
		fmt.Printf("%v %v\n", color.RedString("Error:"), err)
		os.Exit(1)
	}
}

func (a *app) Command() *cobra.Command {
	return a.cmd
}

func (a *app) runCommand(cmd *cobra.Command, args []string) error {
	printWorkingDir()
	flag.PrintFlags(cmd.Flags())
	if !a.noVersion {
		// display application version information
		verflag.PrintAndExitIfRequested()
	}
	if !a.noConfig {
		if err := viper.BindPFlags(cmd.Flags()); err != nil {
			return err
		}
		if err := viper.Unmarshal(a.options); err != nil {
			return err
		}
	}
	if !a.silence {
		log.Infof("%v Starting %s ...", progressMessage, a.short)
		if !a.noVersion {
			log.Infof("%v Version: `%s`", progressMessage, version.Get().ToJSON())
		}
		if !a.noConfig {
			log.Infof("%v Config file used: `%s`", progressMessage, viper.ConfigFileUsed())
		}
	}
	if a.options != nil {
		if err := a.applyOptionRules(); err != nil {
			return err
		}
	}

	// run application
	if a.runFunc != nil {
		return a.runFunc(a.name)
	}

	return nil
}

func (a *app) applyOptionRules() error {
	if completeableOptions, ok := a.options.(CompleteableOptions); ok {
		if err := completeableOptions.Complete(); err != nil {
			return err
		}
	}

	if errs := a.options.Validate(); len(errs) != 0 {
		return errors.NewAggregate(errs)
	}

	if printableOptions, ok := a.options.(PrintableOptions); ok && !a.silence {
		log.Infof("%v Config: `%s`", progressMessage, printableOptions.String())
	}

	return nil
}
