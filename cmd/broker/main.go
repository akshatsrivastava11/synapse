package main

import (
	"akshat/synapse/internal/broker"
	"akshat/synapse/internal/config"
	"akshat/synapse/internal/objectstore"
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}
	log.Printf("streamdb broker starting: id=%s data_dir=%s max_segment_bytes=%d",
		cfg.BrokerID, cfg.DataDir, cfg.MaxSegmentBytes)
	store := objectstore.NewS3Client(cfg.S3Endpoint, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Bucket, cfg.S3UseSSL)
	log.Printf("object store configured: endpoint=%s bucket=%s ssl=%v", cfg.S3Endpoint, cfg.S3Bucket, cfg.S3UseSSL)

	srv := broker.NewServer(cfg.DataDir, cfg.MaxSegmentBytes, store, cfg.FlushInterval)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go srv.StartFlushLoop(ctx)
	log.Printf("background flush loop started: interval=%s", cfg.FlushInterval)

	httpServer := &http.Server{
		Addr:    cfg.HTTPListenAddr,
		Handler: srv.Routes(),
	}
	go func() {
		log.Printf("HTTP API listening on %s (POST /produce, GET /fetch, GET /healthz)", cfg.HTTPListenAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("shutdown signal received, draining...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("http server shutdown error: %v", err)
	}
	if err := srv.Close(); err != nil {
		log.Printf("error closing partitions: %v", err)
	}
	log.Printf("exited cleanly")
}
