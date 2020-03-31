package main

import (
	_ "nitro/internal/command/encoder"
	_ "nitro/internal/command/merger"
	_ "nitro/internal/command/packager"
	"nitro/internal/command/root"
	_ "nitro/internal/command/splitter"
	_ "nitro/internal/command/watcher"
)

func main() {
	root.Execute()
}
