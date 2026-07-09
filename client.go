package dalgo2openvaultdb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/dal-go/dalgo/dal"
)

// httpClient is the internal HTTP helper for talking to OpenVaultDB.
type httpClient struct {
	baseURL    string
	databaseID string
	client     *http.Client
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
func (c *httpClient) getRecord(ctx context.Context, key *dal.Key) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.recordURL(key.String()), nil)
	if err != nil {
		return nil, fmt.Errorf("build GET request: %w", err)
	}

	resp, err := c.client.Do(req)
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
func (c *httpClient) headRecord(ctx context.Context, key *dal.Key) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.recordURL(key.String()), nil)
	if err != nil {
		return false, fmt.Errorf("build HEAD request: %w", err)
	}

	resp, err := c.client.Do(req)
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

// putRecord performs a SET (upsert) via PUT.
func (c *httpClient) putRecord(ctx context.Context, key *dal.Key, data []byte) error {
	body := fmt.Sprintf(`{"data":%s}`, string(data))
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.recordURL(key.String()), bytes.NewBufferString(body))
	if err != nil {
		return fmt.Errorf("build PUT request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("PUT %s: %w", key, err)
	}
	if resp.StatusCode == http.StatusNoContent {
		_ = resp.Body.Close()
		return nil
	}
	return mapHTTPError(resp, key)
}

// postRecord performs an INSERT via POST. Returns conflict error on 409.
func (c *httpClient) postRecord(ctx context.Context, key *dal.Key, data []byte) error {
	body := fmt.Sprintf(`{"data":%s}`, string(data))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.recordURL(key.String()), bytes.NewBufferString(body))
	if err != nil {
		return fmt.Errorf("build POST request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", key, err)
	}
	if resp.StatusCode == http.StatusCreated {
		_ = resp.Body.Close()
		return nil
	}
	return mapHTTPError(resp, key)
}

// patchRecord performs an UPDATE via PATCH.
func (c *httpClient) patchRecord(ctx context.Context, key *dal.Key, updates []byte) error {
	body := fmt.Sprintf(`{"updates":%s}`, string(updates))
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, c.recordURL(key.String()), bytes.NewBufferString(body))
	if err != nil {
		return fmt.Errorf("build PATCH request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("PATCH %s: %w", key, err)
	}
	if resp.StatusCode == http.StatusNoContent {
		_ = resp.Body.Close()
		return nil
	}
	return mapHTTPError(resp, key)
}

// deleteRecord removes a record via DELETE (idempotent: 204 even if absent).
func (c *httpClient) deleteRecord(ctx context.Context, key *dal.Key) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.recordURL(key.String()), nil)
	if err != nil {
		return fmt.Errorf("build DELETE request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("DELETE %s: %w", key, err)
	}
	if resp.StatusCode == http.StatusNoContent {
		_ = resp.Body.Close()
		return nil
	}
	return mapHTTPError(resp, key)
}

// postBatch sends a batch of operations to /batch.
func (c *httpClient) postBatch(ctx context.Context, payload []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.batchURL(), bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("build batch request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
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

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("POST query: %w", err)
	}
	if resp.StatusCode == http.StatusOK {
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("read query response: %w", err)
		}
		return body, nil
	}
	return nil, mapHTTPError(resp, nil)
}

// unmarshalRecord parses a GET response body and populates the record.
// The response has the shape {"key":"...","data":{...}}.
// SetError(nil) must already have been called on the record before Data() is called.
func unmarshalRecord(body []byte, record dal.Record) error {
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
