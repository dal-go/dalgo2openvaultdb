package dalgo2openvaultdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
	"github.com/openvaultdb/openvaultdb-go/pkg/auth"
	"github.com/openvaultdb/openvaultdb-go/pkg/core"
	"github.com/openvaultdb/openvaultdb-go/pkg/mount"
	ovserver "github.com/openvaultdb/openvaultdb-go/pkg/server"
)

const guardedDenial = `{"apiVersion":"tugql.org/authorization/v1","requestId":"remote-1","mode":"execution","scope":"request","result":"deny","allowed":false,"hypothetical":false,"operations":[{"id":"q1","requestOperationId":"q1","action":"query","resource":{"databaseId":"crm","path":"/customers"},"result":"deny","restrictionIds":[],"allOf":[],"executionClass":"tugql"}],"layers":[{"layerId":"ovdb","source":{"ownerId":"ovdb","provider":"ovdb","databaseId":"crm","kind":"openvaultdb"},"aclState":"enabled","result":"deny","decisions":[{"operationId":"q1","result":"deny","scope":"operation","restrictionIds":[]}]}],"blockers":[{"operationId":"q1","code":"ACCESS_DENIED","scope":"operation","layerId":"ovdb"}],"coverage":{"evaluation":"complete","disclosure":"full","truncated":false,"unevaluated":[]},"restrictions":[]}`

func aliceCtx() context.Context {
	return access.WithPrincipal(context.Background(), access.Principal{ID: "alice"})
}

func customersQuery() dal.StructuredQuery {
	return guardedFrom("customers").OrderBy(dal.AscendingField("name")).Limit(10).
		SelectColumns(dal.Column{Expression: dal.Field("name")})
}

func guardedServer(t *testing.T, status int, contentType, body string) dal.DB {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/databases/crm/tugql" || r.Header.Get("Authorization") != "Bearer secret" || r.Header.Get("Content-Type") != "application/yaml" {
			t.Errorf("unexpected request %s %v", r.URL.Path, r.Header)
		}
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(server.Close)
	db, err := NewDB(server.URL+"/", "crm", WithBearerToken("secret"), WithGuardedReads("alice"))
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestGuardedNewDBValidatesTarget(t *testing.T) {
	for name, tc := range map[string]struct {
		baseURL, databaseID, token, principal string
	}{
		"unparsable URL": {"http://[::1", "crm", "t", "p"},
		"scheme":         {"ftp://host", "crm", "t", "p"},
		"user info":      {"https://u:p@host", "crm", "t", "p"},
		"query":          {"https://host?x=1", "crm", "t", "p"},
		"database path":  {"https://host", "crm/x", "t", "p"},
		"database pad":   {"https://host", " crm", "t", "p"},
		"token":          {"https://host", "crm", " ", "p"},
		"principal":      {"https://host", "crm", "t", " "},
	} {
		if _, err := NewDB(tc.baseURL, tc.databaseID, WithBearerToken(tc.token), WithGuardedReads(tc.principal)); err == nil {
			t.Errorf("%s: NewDB accepted an invalid target", name)
		}
	}
	db, err := NewDB("https://host", "crm", WithBearerToken("t"), WithGuardedReads("p"))
	if err != nil || db.ID() != "crm" || db.Adapter().Name() != "openvaultdb" || db.Schema() != nil {
		t.Fatalf("db=%v err=%v", db, err)
	}
}

func TestGuardedDBIsQueryOnly(t *testing.T) {
	db := &guardedDB{}
	ctx := context.Background()
	key := dalrecord.NewKeyWithID("customers", "1")
	_, existsErr := db.Exists(ctx, key)
	_, recordsetErr := db.ExecuteQueryToRecordsetReader(ctx, customersQuery())
	for _, err := range []error{
		db.Get(ctx, dalrecord.NewRecord(key)), db.GetMulti(ctx, nil), existsErr, recordsetErr,
		db.RunReadonlyTransaction(ctx, nil), db.RunReadwriteTransaction(ctx, nil),
	} {
		if !errors.Is(err, dal.ErrNotSupported) {
			t.Fatalf("err=%v", err)
		}
	}
	if _, err := db.ExecuteQueryToRecordsReader(aliceCtx(), customersQuery()); !errors.Is(err, access.ErrAccessDenied) {
		t.Fatalf("zero-value DB must not run queries: %v", err)
	}
}

func TestGuardedQueryReturnsRecords(t *testing.T) {
	db := guardedServer(t, 200, "application/json; charset=utf-8", `{"records":[{"key":"customers/a%2Fb","data":{"name":"A"}},{"key":"customers/c","data":{"name":"C"}}],"usedSourceIds":[]}`)
	reader, err := db.ExecuteQueryToRecordsReader(aliceCtx(), customersQuery())
	if err != nil {
		t.Fatal(err)
	}
	first, err := reader.Next()
	if err != nil || first.Key().ID != "a/b" || first.Key().Collection() != "customers" || first.Data().(map[string]any)["name"] != "A" {
		t.Fatalf("first=%v err=%v", first, err)
	}
	if _, err = reader.Cursor(); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("cursor err=%v", err)
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = reader.Next(); !errors.Is(err, dal.ErrNoMoreRecords) {
		t.Fatalf("after close err=%v", err)
	}
}

func TestGuardedQueryRefusesBeforeRequest(t *testing.T) {
	db := guardedServer(t, 500, "application/json", `{}`)
	if _, err := db.ExecuteQueryToRecordsReader(context.Background(), customersQuery()); !errors.Is(err, access.ErrAccessDenied) {
		t.Fatalf("no principal: %v", err)
	}
	bob := access.WithPrincipal(context.Background(), access.Principal{ID: "bob"})
	if _, err := db.ExecuteQueryToRecordsReader(bob, customersQuery()); !errors.Is(err, access.ErrAccessDenied) {
		t.Fatalf("other principal: %v", err)
	}
	subject, err := access.NewPrincipal(access.PrincipalRef{Kind: access.PrincipalKindUser, Realm: "r", ID: "alice"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecuteQueryToRecordsReader(access.WithPrincipal(context.Background(), subject), customersQuery()); !errors.Is(err, access.ErrAccessDenied) {
		t.Fatalf("subject principal: %v", err)
	}
	if _, err = db.ExecuteQueryToRecordsReader(aliceCtx(), dal.NewTextQuery("select 1", nil)); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("text query: %v", err)
	}
	if _, err = db.ExecuteQueryToRecordsReader(aliceCtx(), guardedFrom("customers").StartFrom(dal.Cursor("c")).SelectKeysOnly(0)); err == nil || !strings.Contains(err.Error(), "TugQL") {
		t.Fatal("unrepresentable query accepted")
	}
	huge := guardedFrom("customers").WhereField("name", dal.Equal, strings.Repeat("x", GuardedMaxRequestBytes)).SelectKeysOnly(0)
	if _, err = db.ExecuteQueryToRecordsReader(aliceCtx(), huge); err == nil || !strings.Contains(err.Error(), "request size") {
		t.Fatalf("oversized request: %v", err)
	}
}

func TestGuardedQueryResponses(t *testing.T) {
	for name, tc := range map[string]struct {
		status      int
		contentType string
		body        string
		want        string
	}{
		"server error":         {500, "application/json", `{"error":{"code":"internal","message":"secret detail"}}`, "OpenVaultDB query unavailable"},
		"text body":            {200, "text/plain", `{}`, "invalid JSON response"},
		"bad media type":       {200, ";", `{}`, "invalid JSON response"},
		"duplicate keys":       {200, "application/json", `{"records":[],"x":{"a":1,"a":2}}`, "invalid JSON response"},
		"missing records":      {200, "application/json", `{}`, "invalid OpenVaultDB query result"},
		"null records":         {200, "application/json", `{"records":null}`, "invalid OpenVaultDB query result"},
		"records not array":    {200, "application/json", `{"records":{}}`, "invalid OpenVaultDB query result"},
		"foreign collection":   {200, "application/json", `{"records":[{"key":"orders/1"}]}`, "record identity"},
		"empty id":             {200, "application/json", `{"records":[{"key":"customers/"}]}`, "record identity"},
		"bad escape":           {200, "application/json", `{"records":[{"key":"customers/%zz"}]}`, "record identity"},
		"invalid id":           {200, "application/json", `{"records":[{"key":"customers/a%2525"}]}`, "record identity"},
		"oversized":            {200, "application/json", `{"records":[],"pad":"` + strings.Repeat("x", GuardedMaxResponseBytes) + `"}`, "exceeds size limit"},
		"denial without body":  {403, "application/json", `{"error":{"code":"forbidden"}}`, "dalgo access denied"},
		"unauthorized":         {401, "application/json", `{"error":{"code":"unauthorized"}}`, "dalgo access denied"},
		"ambiguous denial":     {403, "application/json", `{"authorization":` + guardedDenial + `,"error":{"authorization":` + guardedDenial + `}}`, "dalgo access denied"},
		"error not object":     {403, "application/json", `{"error":"no"}`, "dalgo access denied"},
		"invalid decision":     {403, "application/json", `{"authorization":{"allowed":false}}`, "dalgo access denied"},
		"top-level decision":   {403, "application/json", `{"authorization":` + guardedDenial + `}`, "OpenVaultDB access denied"},
		"error-level decision": {403, "application/json", `{"error":{"code":"access_denied","authorization":` + guardedDenial + `}}`, "OpenVaultDB access denied"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := guardedServer(t, tc.status, tc.contentType, tc.body).ExecuteQueryToRecordsReader(aliceCtx(), customersQuery())
			if err == nil || !strings.Contains(err.Error(), tc.want) || strings.Contains(err.Error(), "secret") {
				t.Fatalf("err=%v, want %q", err, tc.want)
			}
			if tc.status == 403 || tc.status == 401 {
				var denial *AuthorizationError
				if !errors.Is(err, access.ErrAccessDenied) || errors.As(err, &denial) != strings.Contains(tc.want, "OpenVaultDB") {
					t.Fatalf("denial classification err=%v", err)
				}
			}
		})
	}
	if _, err := decodeDenial([]byte(`{"authorization":` + strings.Replace(guardedDenial, `"allowed":false`, `"allowed":true`, 1) + `}`)); err == nil {
		t.Fatal("allowed decision accepted as a denial")
	}
}

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type failingBody struct{}

func (failingBody) Read([]byte) (int, error) { return 0, errors.New("broken secret") }
func (failingBody) Close() error             { return nil }

func TestGuardedTransport(t *testing.T) {
	redirect := httptest.NewServer(http.RedirectHandler("https://elsewhere.invalid/", http.StatusFound))
	defer redirect.Close()
	db, _ := NewDB(redirect.URL, "crm", WithBearerToken("secret"), WithGuardedReads("alice"))
	if _, err := db.ExecuteQueryToRecordsReader(aliceCtx(), customersQuery()); err == nil || !strings.Contains(err.Error(), "invalid JSON response") {
		t.Fatalf("redirect was followed or misreported: %v", err)
	}

	for name, tc := range map[string]struct {
		transport roundTrip
		want      string
	}{
		"transport": {func(*http.Request) (*http.Response, error) { return nil, errors.New("dial secret") }, "OpenVaultDB request failed"},
		"body": {func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: http.Header{}, Body: failingBody{}}, nil
		}, "failed to read OpenVaultDB response"},
	} {
		db, _ := NewDB("https://ovdb.invalid", "crm", WithBearerToken("secret"), WithGuardedReads("alice"), WithHTTPClient(&http.Client{Transport: tc.transport}))
		_, err := db.ExecuteQueryToRecordsReader(aliceCtx(), customersQuery())
		if err == nil || err.Error() != tc.want {
			t.Fatalf("%s: err=%v", name, err)
		}
	}

	// A caller deadline shorter than GuardedQueryTimeout is kept; a longer one
	// is capped. Both surface the context error, which carries no secret.
	var deadline time.Time
	slow := roundTrip(func(r *http.Request) (*http.Response, error) {
		deadline, _ = r.Context().Deadline()
		<-r.Context().Done()
		return nil, r.Context().Err()
	})
	db, _ = NewDB("https://ovdb.invalid", "crm", WithBearerToken("secret"), WithGuardedReads("alice"), WithHTTPClient(&http.Client{Transport: slow}))
	ctx, cancel := context.WithTimeout(aliceCtx(), 10*time.Millisecond)
	defer cancel()
	if _, err := db.ExecuteQueryToRecordsReader(ctx, customersQuery()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("short deadline err=%v", err)
	}
	long, cancelLong := context.WithTimeout(aliceCtx(), time.Hour)
	cancelLong()
	if _, err := db.ExecuteQueryToRecordsReader(long, customersQuery()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled err=%v", err)
	}
	if time.Until(deadline) > GuardedQueryTimeout {
		t.Fatalf("deadline %v was not capped", deadline)
	}
}

func TestValidateStrictObject(t *testing.T) {
	for _, bad := range []string{``, `[]`, `null`, `{} {}`, `{1:2}`, `{"a":[1,}`, `{"a":{"b":1,"b":2}}`, `{"a":1`} {
		if validateStrictObject([]byte(bad)) == nil {
			t.Errorf("accepted %q", bad)
		}
	}
	if err := validateStrictObject([]byte(`{"a":[{"b":1},2],"c":{"b":1}}`)); err != nil {
		t.Fatal(err)
	}
}

// TestGuardedReadsAgainstOpenVaultDB runs the guarded profile against the
// real OVDB server with ACL policies on both storage engines: rows and fields
// are filtered by the server, a field denial keeps its structured decision,
// and a revoked token takes effect on the next query.
func TestGuardedReadsAgainstOpenVaultDB(t *testing.T) {
	for _, engine := range []string{"sqlite", "ingitdb"} {
		t.Run(engine, func(t *testing.T) {
			dir := t.TempDir()
			write := func(path, text string) {
				t.Helper()
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(text), 0600); err != nil {
					t.Fatal(err)
				}
			}
			storage := "data"
			if engine == "sqlite" {
				storage = "data.sqlite"
			}
			manifest := fmt.Sprintf("database: {id: crm, schema_mode: strict}\nstorage: {engine: %s, path: %s}\nschemas:\n  collections:\n    customers:\n      fields:\n        name: {type: string}\n        tenant: {type: string}\n        country: {type: string}\n", engine, storage)
			path := filepath.Join(dir, "db.yaml")
			write(path, manifest)
			db, err := mount.File(path)
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range []struct{ id, tenant, country string }{{"1", "B", "IE"}, {"2", "A", "US"}, {"3", "A", "IE"}} {
				if _, err = db.Apply(context.Background(), []core.Op{{Op: "insert", Key: dalrecord.NewKeyWithID("customers", row.id), Data: map[string]any{"name": "Customer " + row.id, "tenant": row.tenant, "country": row.country}}}, "seed"); err != nil {
					t.Fatal(err)
				}
			}
			policy := func(name, field, value string) string {
				return fmt.Sprintf(`apiVersion: tugql.org/access/v1
kind: AccessPolicy
metadata: {name: %s}
target: {database: crm}
composition: dalgo-hierarchical-v1
default: deny
ruleSets:
  read:
    - path: /customers
      rules:
        - id: visible
          effect: allow
          operations: [query, get]
          where: {op: '==', left: {field: %s}, right: {value: %s}}
          fields: [id, name]
bindings: {roles: {reader: [read]}}
`, name, field, value)
			}
			write(filepath.Join(dir, "upper.yaml"), policy("ovdb", "country", "IE"))
			write(path, manifest+"acl: {enabled: true, policies: [upper.yaml]}\n")
			if engine == "ingitdb" {
				root := filepath.Join(dir, "data", ".ingitdb", "access")
				write(filepath.Join(root, "lower.yaml"), policy("ingitdb", "tenant", "A"))
				write(filepath.Join(root, "manifest.yaml"), "enabled: true\ndatabase: crm\npolicies: [lower.yaml]\n")
			}
			if db, err = mount.File(path); err != nil {
				t.Fatal(err)
			}
			store, err := auth.OpenStore(filepath.Join(dir, "auth.json"))
			if err != nil {
				t.Fatal(err)
			}
			token := "fixture-scoped-token"
			if err = store.CreateGrant(&auth.Grant{PrincipalID: "alice", DatabaseID: "crm", Capabilities: []auth.Capability{{Action: auth.CapRecordsRead, Collection: "customers"}}}, token); err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(ovserver.New("test", map[string]*core.Database{"crm": db}, ovserver.WithAuth(&auth.Config{OwnerToken: "fixture-owner", Store: store}), ovserver.WithPrincipalResolver(func(context.Context, *auth.Principal) (access.Principal, error) {
				return access.Principal{ID: "alice", Roles: []string{"reader"}}, nil
			})).Handler())
			defer server.Close()

			source, err := NewDB(server.URL, "crm", WithBearerToken(token), WithGuardedReads("alice"))
			if err != nil {
				t.Fatal(err)
			}
			reader, err := source.ExecuteQueryToRecordsReader(aliceCtx(), customersQuery())
			if err != nil {
				t.Fatal(err)
			}
			want := map[string]bool{"1": true, "3": true}
			if engine == "ingitdb" {
				want = map[string]bool{"3": true}
			}
			for {
				rec, nextErr := reader.Next()
				if errors.Is(nextErr, dal.ErrNoMoreRecords) {
					break
				}
				id := fmt.Sprint(rec.Key().ID)
				if !want[id] {
					t.Fatalf("row %s bypassed a restriction", id)
				}
				if _, leaked := rec.Data().(map[string]any)["tenant"]; leaked {
					t.Fatal("restricted field leaked")
				}
				delete(want, id)
			}
			if len(want) != 0 {
				t.Fatalf("missing rows %v", want)
			}

			tenants := guardedFrom("customers").SelectColumns(dal.Column{Expression: dal.Field("tenant")})
			_, err = source.ExecuteQueryToRecordsReader(aliceCtx(), tenants)
			var denial *AuthorizationError
			if !errors.Is(err, access.ErrAccessDenied) || !errors.As(err, &denial) || len(denial.Decision.Blockers) == 0 || len(denial.Decision.Layers) == 0 {
				t.Fatalf("field denial lost its structured decision: %v", err)
			}

			revoked, _ := NewDB(server.URL, "crm", WithBearerToken("revoked"), WithGuardedReads("alice"))
			if _, err = revoked.ExecuteQueryToRecordsReader(aliceCtx(), customersQuery()); !errors.Is(err, access.ErrAccessDenied) {
				t.Fatalf("revoked token: %v", err)
			}
		})
	}
}

func guardedFrom(name string) *dal.QueryBuilder {
	return dal.NewQueryBuilder(dal.From(dal.NewRootCollectionRef(name, "")))
}
