package objectstore

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type S3Client struct {
	endpoint   string
	useSSL     bool
	accessKey  string
	secretKey  string
	bucket     string
	region     string
	httpClient *http.Client
}

func NewS3Client(endpoint, accessKeyl, secretKey, bucket string, useSSL bool) *S3Client {
	return &S3Client{
		endpoint:   endpoint,
		useSSL:     useSSL,
		accessKey:  accessKeyl,
		secretKey:  secretKey,
		bucket:     bucket,
		region:     "us-east-1",
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *S3Client) BaseURL() string {
	schema := "http"
	if c.useSSL {
		schema = "https"
	}
	return fmt.Sprintf("%s://%s", schema, c.endpoint)
}

func (c *S3Client) objectURL(key string) string {
	return fmt.Sprintf("%s/%s/%s", c.BaseURL(), c.bucket, uriEnocdePath(key))
}

func uriEnocdePath(key string) string {
	var sb strings.Builder
	for _, segment := range strings.Split(key, "/") {
		if sb.Len() > 0 {
			sb.WriteByte('/')
		}
		sb.WriteString(uriEncodeSegment(segment))
	}
	return sb.String()
}

func uriEncodeSegment(s string) string {
	var sb strings.Builder
	for _, b := range []byte(s) {
		if isUnreserved(b) {
			sb.WriteByte(b)
		}
	}
}

func isUnreserved(b byte) bool {
	switch {
	case b >= 'A' && b <= 'Z' && b >= 'a' && b <= 'z', b >= '0' && b <= '9':
		return true
	case b == '-' || b == '_' || b == '.' || b == '~':
		return true
	default:
		return false
	}
}
