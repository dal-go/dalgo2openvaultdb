package dalgo2openvaultdb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/dal-go/dalgo/dal"
	"io"
	"net/http"
	"strings"
	"time"

	dalrecord "github.com/dal-go/record"
)

// httpClient is the internal HTTP helper for talking to OpenVaultDB.
type httpClient struct {
	baseURL     string
	databaseID  string
	client      *http.Client
	bearerToken string
}

// do sends the request, attaching the bearer token when configured
// (servers running `ovdb serve --auth`).
func (c *httpClient) do(req *http.Request) (*http.Response, error) {
	if c.bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	}
	return c.client.Do(req)
}

func (c *httpClient) recordURL(keyPath string) string {
	return fmt.Sprintf("%s/v1/databases/%s/records/%s", c.baseURL, c.databaseID, keyPath)
}

func (c *httpClient) batchURL() string {
	return fmt.Sprintf("%s/v1/databases/%s/batch", c.baseURL, c.databaseID)
}

func (c *httpClient) queryURL() string {
	return fmt.Sprintf("%s/v1/databases/%s/query", c.baseURL, c.databaseID)
}

// getRecord fetches a single record. Returns (body, nil) on 200, (nil, nil) on
// 404 (caller should call record.SetError with ErrNotFoundByKey), or (nil, err)
// on other failures.
func (c *httpClient) getRecord(ctx context.Context, key *dalrecord.Key) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.recordURL(key.String()), nil)
	if err != nil {
		return nil, fmt.Errorf("build GET request: %w", err)
	}

	resp, err := c.do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", key, err)
	}
	if resp.StatusCode == http.StatusOK {
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("read GET response: %w", err)
		}
		return body, nil
	}
	return nil, mapHTTPError(resp, key)
}

// headRecord checks existence via HEAD. Returns (true, nil), (false, nil) on
// 200/404, or (false, err) on other failures.
func (c *httpClient) headRecord(ctx context.Context, key *dalrecord.Key) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.recordURL(key.String()), nil)
	if err != nil {
		return false, fmt.Errorf("build HEAD request: %w", err)
	}

	resp, err := c.do(req)
	if err != nil {
		return false, fmt.Errorf("HEAD %s: %w", key, err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, mapHTTPError(resp, key)
	}
}

// postBatch sends a batch of operations to /batch.
func (c *httpClient) postBatch(ctx context.Context, payload []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.batchURL(), bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("build batch request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.do(req)
	if err != nil {
		return fmt.Errorf("POST batch: %w", err)
	}
	if resp.StatusCode == http.StatusOK {
		_ = resp.Body.Close()
		return nil
	}
	return mapHTTPError(resp, nil)
}

// postQuery executes a query and returns raw JSON response bytes.
func (c *httpClient) postQuery(ctx context.Context, payload []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.queryURL(), bytes.NewBuffer(payload))
	if err != nil {
		return nil, fmt.Errorf("build query request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	required, _ := ctx.Value(providerReadContextKey{}).(*requiredProviderReads)
	client := c.client
	if required != nil {
		req.Header.Set("OVDB-Execution-ID", required.plan.Execution.ID)
		req.Header.Set("Cache-Control", "no-store")
		copyClient := *c.client
		copyClient.CheckRedirect = func(*http.Request, []*http.Request) error {
			return fmt.Errorf("provider query redirects are forbidden")
		}
		if copyClient.Timeout == 0 || copyClient.Timeout > 10*time.Second {
			copyClient.Timeout = 10 * time.Second
		}
		client = &copyClient
	}
	if c.bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("POST query: %w", err)
	}
	if resp.StatusCode == http.StatusOK {
		defer func() { _ = resp.Body.Close() }()
		reader := io.Reader(resp.Body)
		if required != nil {
			noStore := false
			for _, value := range resp.Header.Values("Cache-Control") {
				for _, directive := range strings.Split(value, ",") {
					noStore = noStore || strings.EqualFold(strings.TrimSpace(directive), "no-store")
				}
			}
			if !noStore {
				return nil, fmt.Errorf("provider query response requires no-store")
			}
			reader = io.LimitReader(resp.Body, maxProviderQueryBytes+1)
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("read query response: %w", err)
		}
		if required != nil && len(body) > maxProviderQueryBytes {
			return nil, fmt.Errorf("provider query response exceeds byte budget")
		}
		return body, nil
	}
	if required != nil {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("provider query HTTP status %d", resp.StatusCode)
	}
	return nil, mapHTTPError(resp, nil)
}

// unmarshalRecord parses a GET response body and populates the record.
// The response has the shape {"key":"...","data":{...}}.
// SetError(nil) must already have been called on the record before Data() is called.
func unmarshalRecord(body []byte, record dalrecord.Record) error {
	var wrapper struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &wrapper); err != nil {
		return fmt.Errorf("parse record response: %w", err)
	}
	if err := json.Unmarshal(wrapper.Data, record.Data()); err != nil {
		return fmt.Errorf("unmarshal record data: %w", err)
	}
	return nil
}

const maxProviderQueryBytes = 4 * 1024 * 1024

func (c *httpClient) executeQuery(ctx context.Context, query dal.Query) (dal.RecordsReader, error) {
	q, ok := query.(dal.StructuredQuery)
	if !ok {
		return nil, fmt.Errorf("%w: non-structured query", dal.ErrNotSupported)
	}
	wq, err := buildWireQuery(q)
	if err != nil {
		return nil, err
	}
	required, _ := ctx.Value(providerReadContextKey{}).(*requiredProviderReads)
	if required != nil {
		if err := required.validateTarget(c.databaseID, wq.Collection, wq.Parent); err != nil {
			return nil, err
		}
	}
	payload, err := marshalWireQuery(wq)
	if err != nil {
		return nil, err
	}
	body, err := c.postQuery(ctx, payload)
	if err != nil {
		return nil, err
	}
	if required == nil {
		return newQueryRecordsReader(body, q)
	}
	fields, err := decodeQueryFields(body)
	if err != nil {
		return nil, fmt.Errorf("parse query response: %w", err)
	}
	metadata, evidence, err := queryResponseMetadata(fields, required)
	if err != nil {
		return nil, err
	}
	// buildWireQuery already validated this projection before HTTP.
	projection, _ := queryProjection(q)
	return recordsReaderFromFields(fields, q, projection, metadata, evidence)
}
