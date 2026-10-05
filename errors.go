package dalgo2openvaultdb

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
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
// For 409 responses an "already exists" error is returned, wrapping
// dalrecord.ErrRecordExists so callers can match it with errors.Is or
// dalrecord.IsAlreadyExists.
// The response body is always consumed and closed.
func mapHTTPError(resp *http.Response, key *dalrecord.Key) error {
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)

	var ae apiError
	if err := json.Unmarshal(body, &ae); err != nil || ae.Error.Code == "" {
		ae.Error.Code = "unknown"
		ae.Error.Message = string(body)
	}

	switch resp.StatusCode {
	case http.StatusUnprocessableEntity, http.StatusNotImplemented:
		if ae.Error.Code == "authorization_unsupported" || ae.Error.Code == "not_supported" || ae.Error.Code == "query_unsupported" {
			return fmt.Errorf("%w: %s", dal.ErrNotSupported, ae.Error.Code)
		}
		return fmt.Errorf("openvaultdb error %d %s: %s", resp.StatusCode, ae.Error.Code, ae.Error.Message)
	case http.StatusNotFound:
		if key != nil {
			return dal.NewErrNotFoundByKey(key, nil)
		}
		return fmt.Errorf("not found: %s", ae.Error.Message)
	case http.StatusConflict:
		// Wrap dalrecord.ErrRecordExists rather than returning a bare
		// fmt.Errorf: the shared dalgo conformance suite (dalgo v0.66.1)
		// unconditionally asserts a duplicate Insert satisfies
		// dalrecord.IsAlreadyExists. dalrecord.ErrRecordExists.Error() is
		// exactly "record already exists", so %w renders identically to the
		// literal this branch returned before — any caller matching on that
		// text (or constructing its own equivalent error) keeps working.
		if key != nil {
			return fmt.Errorf("%w: key=%v: %s", dalrecord.ErrRecordExists, key, ae.Error.Message)
		}
		return fmt.Errorf("%w: %s", dalrecord.ErrRecordExists, ae.Error.Message)
	default:
		return fmt.Errorf("openvaultdb error %d %s: %s", resp.StatusCode, ae.Error.Code, ae.Error.Message)
	}
}
