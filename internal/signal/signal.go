package signal

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
)

func WatchInterrupt(ctx context.Context, forceShutdownDelay time.Duration) context.Context {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		<-sigs // wait for signals
		log.Warnf("interrupt signal received, try shutdown gracefully or kill app in %s...", forceShutdownDelay)
		cancel() // send done signal to context
		timer := time.NewTimer(forceShutdownDelay)
		<-timer.C // wait some time to app shutdown gracefully
		log.Warnf("app still not shutdown after %s, exit immediately", forceShutdownDelay)
		os.Exit(0) // force to kill app
	}()

	return ctx
}
