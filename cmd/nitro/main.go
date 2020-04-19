package main

import (
	"math/rand"
	"time"

	_ "nitro/internal/command/autoscaler"
	_ "nitro/internal/command/encoder"
	_ "nitro/internal/command/merger"
	_ "nitro/internal/command/packager"
	"nitro/internal/command/root"
	_ "nitro/internal/command/splitter"
	_ "nitro/internal/command/watcher"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	root.Execute()
}
