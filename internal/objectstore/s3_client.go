package objectstore

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"sort"
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

func (c *S3Client) Put(ctx context.Context, key string, data []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.objectURL(key), bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("s3 put: build request: %w", err)
	}
	req.ContentLength = int64(len(data))
	c.sign(req, data)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("s3 put %s: %w", key, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("s3 put %s: unexpected status %d: %s", key, resp.StatusCode, string(body))
	}
	return nil
}

func (c *S3Client) Get(ctx context.Context, key string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.objectURL(key), nil)
	if err != nil {
		return nil, fmt.Errorf("s3 get: build request: %w", err)
	}
	c.sign(req, nil)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("s3 get %s: %w", key, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("s3 get %s: unexpected status %d: %s", key, resp.StatusCode, string(body))
	}
	return io.ReadAll(resp.Body)
}

func (c *S3Client) Delete(ctx context.Context, key string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.objectURL(key), nil)
	if err != nil {
		return fmt.Errorf("s3 delete: build request: %w", err)
	}
	c.sign(req, nil)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("s3 delete %s: %w", key, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("s3 delete %s: unexpected status %d: %s", key, resp.StatusCode, string(body))
	}
	return nil
}

func (c *S3Client) sign(req *http.Request, body []byte) {
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	payloadHash := sha256Hex(body)

	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	req.Header.Set("Host", req.URL.Host)

	signedHeaders, canonicalHeaders := c.canonicalHeaders(req)
	canonicalRequest := strings.Join([]string{
		req.Method,
		req.URL.EscapedPath(),
		"",
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")
	credentialScope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, c.region)
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		sha256Hex([]byte(canonicalRequest))}, "\n")
	signingKey := deriveSigningKey(c.secretKey, dateStamp, c.region, "s3")
	signature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))
	authHeader := fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		c.accessKey, credentialScope, signedHeaders, signature,
	)
	req.Header.Set("Authorization", authHeader)
}

func deriveSigningKey(secretKey, dateStamp, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	return hmacSHA256(kService, "aws4_request")
}

func (c *S3Client) canonicalHeaders(req *http.Request) (signedHeaders, canonical string) {
	headers := map[string]string{
		"host":                 req.Header.Get("Host"),
		"x-amz-content-sha256": req.Header.Get("X-Amz-Content-Sha256"),
		"x-amz-date":           req.Header.Get("X-Amz-Date"),
	}
	names := make([]string, 0, len(headers))
	for k := range headers {
		names = append(names, k)
	}
	sort.Strings(names)

	var sb strings.Builder
	for _, name := range names {
		sb.WriteString(name)
		sb.WriteString(":")
		sb.WriteString(strings.TrimSpace(headers[name]))
		sb.WriteString("\n")
	}
	return strings.Join(names, ";"), sb.String()
}
func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
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
		} else {
			fmt.Fprintf(&sb, "%%%02x", b)
		}
	}
	return sb.String()
}

func isUnreserved(b byte) bool {
	switch {
	case b >= 'A' && b <= 'Z' || b >= 'a' && b <= 'z', b >= '0' && b <= '9':
		return true
	case b == '-' || b == '_' || b == '.' || b == '~':
		return true
	default:
		return false
	}
}
