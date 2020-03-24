package main

import (
	_ "trancode/internal/command/encoder"
	_ "trancode/internal/command/merger"
	_ "trancode/internal/command/packager"
	"trancode/internal/command/root"
	_ "trancode/internal/command/splitter"
	_ "trancode/internal/command/watcher"
)

func main() {
	root.Execute()
}
