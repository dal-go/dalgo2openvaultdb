package dalgo2openvaultdb

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/dal-go/dalgo/dal"
)

// apiError is the shape returned by OpenVaultDB on non-2xx responses.
type apiError struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// mapHTTPError converts a non-2xx HTTP response to a Go error.
// For 404 responses the key is used to produce an ErrNotFoundByKey.
// For 409 responses an "already exists" error is returned.
// The response body is always consumed and closed.
func mapHTTPError(resp *http.Response, key *dal.Key) error {
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)

	var ae apiError
	if err := json.Unmarshal(body, &ae); err != nil || ae.Error.Code == "" {
		ae.Error.Code = "unknown"
		ae.Error.Message = string(body)
	}

	switch resp.StatusCode {
	case http.StatusNotFound:
		if key != nil {
			return dal.NewErrNotFoundByKey(key, nil)
		}
		return fmt.Errorf("not found: %s", ae.Error.Message)
	case http.StatusConflict:
		if key != nil {
			return fmt.Errorf("record already exists: key=%v: %s", key, ae.Error.Message)
		}
		return fmt.Errorf("record already exists: %s", ae.Error.Message)
	default:
		return fmt.Errorf("openvaultdb error %d %s: %s", resp.StatusCode, ae.Error.Code, ae.Error.Message)
	}
}
