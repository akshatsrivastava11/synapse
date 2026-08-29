package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	// Identity
	BrokerID string //Unique ID

	// Storage
	DataDir         string
	MaxSegmentBytes int64

	// Object Storage
	S3Endpoint  string
	S3Bucket    string
	S3AccessKey string
	S3SecretKey string
	S3UseSSL    bool

	KafkaListenerAddr string
}

func Load() (Config, error) {
	cfg := Config{
		BrokerID:          getEnv("BROKER_ID", "broker-1"),
		DataDir:           getEnv("DATA_DIR", "./data"),
		MaxSegmentBytes:   getEnvInt64("MAX_SEGMENT_BYTES", 64*1024*1024),
		S3Endpoint:        getEnv("S3_ENDPOINT", "localhost:9000"),
		S3Bucket:          getEnv("S3_BUCKET", "streamdb"),
		S3AccessKey:       getEnv("S3_ACCESS_KEY", "minioadmin"),
		S3SecretKey:       getEnv("S3_SECRET_KEY", "minioadmin"),
		S3UseSSL:          getEnvBool("S3_USE_SSL", false),
		KafkaListenerAddr: getEnv("KAFKA_LISTEN_ADDR", ":9092"),
	}
	if cfg.MaxSegmentBytes <= 0 {
		return Config{}, fmt.Errorf("MAX_SEGMENT_BYTES must be positive, got %d", cfg.MaxSegmentBytes)
	}
	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func getEnvInt64(key string, fallback int64) int64 {
	if v, ok := os.LookupEnv(key); ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return n
		}
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	if v, ok := os.LookupEnv(key); ok {
		b, err := strconv.ParseBool(v)
		if err == nil {
			return b
		}
	}
	return fallback
}
