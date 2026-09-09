package dalgo2openvaultdb

import (
	"errors"
	"github.com/dal-go/dalgo/dal"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestUnsupportedHTTPProfilePreservesDALgoClassification(t *testing.T) {
	for _, tc := range []struct {
		status      int
		code        string
		unsupported bool
	}{
		{422, "authorization_unsupported", true}, {501, "not_supported", true},
		{422, "schema_validation", false}, {403, "authorization_unsupported", false}, {403, "ACCESS_DENIED", false},
	} {
		response := &http.Response{StatusCode: tc.status, Body: io.NopCloser(strings.NewReader(`{"error":{"code":"` + tc.code + `","message":"private internal cause"}}`))}
		err := mapHTTPError(response, nil)
		if errors.Is(err, dal.ErrNotSupported) != tc.unsupported {
			t.Fatalf("status=%d code=%s: %v", tc.status, tc.code, err)
		}
		if tc.unsupported && strings.Contains(err.Error(), "private internal") {
			t.Fatal("reflected unsupported evaluator details")
		}
	}
}
