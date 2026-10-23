package dalgo2openvaultdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	"github.com/dal-go/dalgo/tugql"
	az "github.com/dal-go/dalgo/tugql/authorization"
	dalrecord "github.com/dal-go/record"
)

// Bounds of the guarded read profile (see WithGuardedReads).
const (
	GuardedMaxRequestBytes  = 1 << 20
	GuardedMaxResponseBytes = 8 << 20
	GuardedQueryTimeout     = 10 * time.Second
)

// WithGuardedReads turns the DB into a read-only, query-only source for a
// credential bound to one principal — the profile a host uses when an
// operator, not the caller, configures where a bearer token may be sent.
//
// Every query must run in a context whose access.Principal has no Subject and
// the legacy ID principalID; any other caller is refused with
// access.ErrAccessDenied before a request is made. Queries are serialized as
// TugQL and sent to the ACL-evaluated /tugql route. A 401/403 decision the
// server explains is returned as *AuthorizationError. Requests are capped at
// GuardedMaxRequestBytes, responses at GuardedMaxResponseBytes, each query at
// GuardedQueryTimeout; redirects are refused, the response must be a JSON
// object without duplicate keys, and no error carries response text, the URL
// or the token. Get, Exists, GetMulti, transactions and recordset readers
// return dal.ErrNotSupported.
//
// NewDB validates the target when this option is set: an http(s) base URL
// without user info, query or fragment, a database ID without path
// characters or padding, and a non-empty bearer token.
func WithGuardedReads(principalID string) Option {
	return func(db *database) {
		db.guardedPrincipal = &principalID
	}
}

func newGuardedDB(db *database) (dal.DB, error) {
	u, err := url.Parse(db.c.baseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("invalid OpenVaultDB base URL")
	}
	if strings.ContainsAny(db.id, "/\\?#") || strings.TrimSpace(db.id) != db.id {
		return nil, errors.New("invalid OpenVaultDB database ID")
	}
	if strings.TrimSpace(db.c.bearerToken) == "" {
		return nil, errors.New("missing OpenVaultDB bearer token")
	}
	if strings.TrimSpace(*db.guardedPrincipal) == "" {
		return nil, errors.New("missing OpenVaultDB principal ID")
	}
	base := db.c.client
	if base == nil {
		base = http.DefaultClient
	}
	client := *base
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return dal.NewDB(&guardedDB{
		id:          db.id,
		endpoint:    strings.TrimRight(db.c.baseURL, "/") + "/v1/databases/" + url.PathEscape(db.id) + "/tugql",
		token:       db.c.bearerToken,
		client:      &client,
		principalID: *db.guardedPrincipal,
	}), nil
}

type guardedDB struct {
	dal.ConcurrencyAvailable
	id          string
	endpoint    string
	token       string
	client      *http.Client // never follows redirects
	principalID string
}

func (db *guardedDB) ID() string                                      { return db.id }
func (*guardedDB) Adapter() dal.Adapter                               { return dal.NewAdapter("openvaultdb", driverVersion) }
func (*guardedDB) Schema() dal.Schema                                 { return nil }
func (*guardedDB) Get(context.Context, dalrecord.Record) error        { return dal.ErrNotSupported }
func (*guardedDB) GetMulti(context.Context, []dalrecord.Record) error { return dal.ErrNotSupported }
func (*guardedDB) Exists(context.Context, *dalrecord.Key) (bool, error) {
	return false, dal.ErrNotSupported
}
func (*guardedDB) RunReadonlyTransaction(context.Context, dal.ROTxWorker, ...dal.TransactionOption) error {
	return dal.ErrNotSupported
}
func (*guardedDB) RunReadwriteTransaction(context.Context, dal.RWTxWorker, ...dal.TransactionOption) error {
	return dal.ErrNotSupported
}
func (*guardedDB) ExecuteQueryToRecordsetReader(context.Context, dal.Query, ...recordset.Option) (dal.RecordsetReader, error) {
	return nil, dal.ErrNotSupported
}

// AuthorizationError carries the server's validated structured denial. Its
// Error string is deliberately generic so it is safe to show to callers.
type AuthorizationError struct{ Decision az.Result }

func (*AuthorizationError) Error() string { return "OpenVaultDB access denied" }
func (*AuthorizationError) Unwrap() error { return access.ErrAccessDenied }

func (db *guardedDB) ExecuteQueryToRecordsReader(ctx context.Context, q dal.Query) (dal.RecordsReader, error) {
	principal, ok := access.PrincipalFrom(ctx)
	if !ok || principal.Subject != nil || principal.ID != db.principalID {
		return nil, fmt.Errorf("%w: OpenVaultDB connection principal mismatch", access.ErrAccessDenied)
	}
	query, ok := q.(dal.StructuredQuery)
	if !ok {
		return nil, dal.ErrNotSupported
	}
	doc, err := tugql.Serialize(query)
	if err != nil {
		return nil, err
	}
	status, body, err := db.post(ctx, doc)
	if err != nil {
		return nil, err
	}
	if status == http.StatusForbidden || status == http.StatusUnauthorized {
		decision, present, decodeErr := decodeDenial(body)
		switch {
		case decodeErr == nil:
			return nil, &AuthorizationError{Decision: decision}
		case present:
			// A decision that is malformed, ambiguous or allowing is a broken
			// upstream, not an ordinary denial.
			return nil, errors.New("OpenVaultDB returned an invalid authorization error")
		default:
			return nil, access.ErrAccessDenied
		}
	}
	if status != http.StatusOK {
		return nil, errors.New("OpenVaultDB query unavailable")
	}
	return guardedRecords(body, query.From().Base().Name())
}

// post sends one TugQL document and returns the status and a body that is a
// bounded JSON object without duplicate keys.
func (db *guardedDB) post(ctx context.Context, doc []byte) (int, []byte, error) {
	if len(doc) > GuardedMaxRequestBytes {
		return 0, nil, errors.New("invalid OpenVaultDB request size")
	}
	if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > GuardedQueryTimeout {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, GuardedQueryTimeout)
		defer cancel()
	}
	// The endpoint was validated by NewDB, so building the request cannot fail.
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, db.endpoint, bytes.NewReader(doc))
	req.Header.Set("Authorization", "Bearer "+db.token)
	req.Header.Set("Content-Type", "application/yaml")
	req.Header.Set("Accept", "application/json")
	res, err := db.client.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, nil, fmt.Errorf("OpenVaultDB request failed: %w", ctxErr)
		}
		return 0, nil, errors.New("OpenVaultDB request failed")
	}
	defer func() { _ = res.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(res.Body, GuardedMaxResponseBytes+1))
	if err != nil {
		return 0, nil, errors.New("failed to read OpenVaultDB response")
	}
	if len(body) > GuardedMaxResponseBytes {
		return 0, nil, errors.New("OpenVaultDB response exceeds size limit")
	}
	mediaType, _, mediaErr := mime.ParseMediaType(res.Header.Get("Content-Type"))
	if mediaErr != nil || mediaType != "application/json" || validateStrictObject(body) != nil {
		return 0, nil, errors.New("OpenVaultDB returned an invalid JSON response")
	}
	return res.StatusCode, body, nil
}

// decodeDenial accepts a denial as {"authorization":…} or
// {"error":{"authorization":…}}, never both, and never an allowed decision.
// present reports whether the body carried a decision at all; a non-object
// "error" carries none.
func decodeDenial(body []byte) (result az.Result, present bool, err error) {
	var envelope map[string]json.RawMessage
	_ = json.Unmarshal(body, &envelope) // post already proved body is an object
	authData := envelope["authorization"]
	if detail := envelope["error"]; len(detail) != 0 {
		if len(authData) != 0 {
			return az.Result{}, true, errors.New("ambiguous authorization response")
		}
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(detail, &fields)
		authData = fields["authorization"]
	}
	if len(authData) == 0 {
		return az.Result{}, false, errors.New("response omitted authorization result")
	}
	result, err = az.DecodeResultCompatible(authData)
	if err != nil || result.Allowed {
		return az.Result{}, true, errors.New("invalid authorization result")
	}
	return result, true, nil
}

// guardedRecords requires every key to be <collection>/<escaped valid ID>.
func guardedRecords(body []byte, collection string) (dal.RecordsReader, error) {
	var payload struct {
		Records json.RawMessage `json:"records"`
	}
	_ = json.Unmarshal(body, &payload) // post already proved body is an object
	var rows []struct {
		Key  string         `json:"key"`
		Data map[string]any `json:"data"`
	}
	if len(payload.Records) == 0 || string(payload.Records) == "null" || json.Unmarshal(payload.Records, &rows) != nil {
		return nil, errors.New("invalid OpenVaultDB query result")
	}
	prefix := collection + "/"
	reader := &guardedReader{records: make([]dalrecord.Record, 0, len(rows))}
	for _, row := range rows {
		escaped, ok := strings.CutPrefix(row.Key, prefix)
		id, decodeErr := url.PathUnescape(escaped)
		if !ok || escaped == "" || decodeErr != nil || dalrecord.ValidateStringID(id) != nil {
			return nil, errors.New("invalid OpenVaultDB record identity")
		}
		reader.records = append(reader.records, dalrecord.NewRecordWithData(dalrecord.NewKeyWithID(collection, id), row.Data))
	}
	return reader, nil
}

type guardedReader struct {
	records  []dalrecord.Record
	position int
}

func (r *guardedReader) Next() (dalrecord.Record, error) {
	if r.position == len(r.records) {
		return nil, dal.ErrNoMoreRecords
	}
	rec := r.records[r.position]
	r.position++
	return rec, nil
}
func (r *guardedReader) Close() error          { r.position = len(r.records); return nil }
func (*guardedReader) Cursor() (string, error) { return "", dal.ErrNotSupported }

// validateStrictObject rejects duplicate keys at every depth and anything but
// one JSON object.
func validateStrictObject(data []byte) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if err := scanStrictJSON(d); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("expected one JSON value")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil || object == nil {
		return errors.New("expected JSON object")
	}
	return nil
}

func scanStrictJSON(d *json.Decoder) error {
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	if delim == '{' {
		keys := map[string]bool{}
		for d.More() {
			// Inside an object the decoder only yields string keys.
			keyToken, err := d.Token()
			if err != nil {
				return err
			}
			key := keyToken.(string)
			if keys[key] {
				return errors.New("duplicate JSON object key")
			}
			keys[key] = true
			if err := scanStrictJSON(d); err != nil {
				return err
			}
		}
	} else {
		// d.Token never yields a closing delimiter here, so this is '['.
		for d.More() {
			if err := scanStrictJSON(d); err != nil {
				return err
			}
		}
	}
	_, err = d.Token()
	return err
}
