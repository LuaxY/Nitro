package executor

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

type Executor struct {
	logger io.Writer
}

func NewExecutor(logger io.Writer) *Executor {
	e := &Executor{logger: logger}
	return e
}

func (e *Executor) Run(ctx context.Context, command *Cmd) error {
	_, _ = io.WriteString(e.logger, "> "+command.Binary+" "+strings.Join(command.args, " ")+"\n")
	fmt.Println("> " + command.Binary + " " + strings.Join(command.args, " ")) // TEMP

	start := time.Now()

	_, _ = io.WriteString(e.logger, start.String()+"\n")

	cmd := exec.CommandContext(ctx, command.Binary, command.args...)
	cmd.Stdout = io.MultiWriter(e.logger, os.Stdout)
	cmd.Stderr = io.MultiWriter(e.logger, os.Stderr)
	cmd.Env = append(os.Environ(), command.envs...)
	err := cmd.Run()

	_, _ = io.WriteString(e.logger, time.Since(start).String()+"\n")

	if err != nil {
		_, _ = io.WriteString(e.logger, err.Error()+"\n")
		return err
	}

	_, _ = io.WriteString(e.logger, "==============================\n")

	return nil
}

type Cmd struct {
	Binary string
	args   []string
	envs   []string
}

func (c *Cmd) Add(args ...string) {
	c.args = append(c.args, args...)
}

func (c *Cmd) Env(env string) {
	c.envs = append(c.envs, env)
}

func (c *Cmd) Command() []string {
	return c.args
}
