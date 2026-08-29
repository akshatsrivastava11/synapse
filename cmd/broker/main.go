package main

import (
	"akshat/synapse/internal/config"
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}
	log.Printf("streamdb broker starting: id=%s data_dir=%s max_segment_bytes=%d",
		cfg.BrokerID, cfg.DataDir, cfg.MaxSegmentBytes)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("streamdb broker ready (phase 0: no subsystems wired yet)")

	<-ctx.Done()
	log.Printf("shutdown signal received, exiting cleanly")
}
