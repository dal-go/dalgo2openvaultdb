package dalgo2openvaultdb_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
	"github.com/dal-go/record/update"

	dalgo2openvaultdb "github.com/dal-go/dalgo2openvaultdb"
)

// ---- helpers ----------------------------------------------------------------

type contactData struct {
	Name   string `json:"name"`
	Status string `json:"status,omitempty"`
}

func mustNewDB(t *testing.T, server *httptest.Server) dal.DB {
	t.Helper()
	db, err := dalgo2openvaultdb.NewDB(server.URL, "testdb",
		dalgo2openvaultdb.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}
	return db
}

// collectionFrom returns a QueryBuilder starting from a root collection.
func collectionFrom(name string) *dal.QueryBuilder {
	return dal.NewQueryBuilder(dal.From(dal.NewRootCollectionRef(name, "")))
}

// fakeStore holds records keyed by keyPath.
type fakeStore struct {
	mu      sync.Mutex
	records map[string]json.RawMessage
}

func newFakeStore() *fakeStore {
	return &fakeStore{records: make(map[string]json.RawMessage)}
}

func (s *fakeStore) set(key string, data json.RawMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[key] = data
}

func (s *fakeStore) get(key string) (json.RawMessage, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.records[key]
	return v, ok
}

func (s *fakeStore) delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, key)
}

// newRecordServer builds an httptest.Server implementing the minimal records API.
func newRecordServer(t *testing.T, store *fakeStore) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("/v1/databases/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		const batchPath = "/v1/databases/testdb/batch"
		const queryPath = "/v1/databases/testdb/query"
		const recordsPrefix = "/v1/databases/testdb/records/"

		switch {
		case path == batchPath && r.Method == http.MethodPost:
			body, _ := io.ReadAll(r.Body)
			var batch struct {
				Message string `json:"message"`
				Ops     []struct {
					Op      string          `json:"op"`
					Key     string          `json:"key"`
					Data    json.RawMessage `json:"data"`
					Updates json.RawMessage `json:"updates"`
				} `json:"ops"`
			}
			if err := json.Unmarshal(body, &batch); err != nil {
				apiErr(w, http.StatusBadRequest, "bad_request", "bad json")
				return
			}
			for _, op := range batch.Ops {
				switch op.Op {
				case "set":
					store.set(op.Key, op.Data)
				case "insert":
					if _, exists := store.get(op.Key); exists {
						apiErr(w, http.StatusConflict, "already_exists", "already exists")
						return
					}
					store.set(op.Key, op.Data)
				case "delete":
					store.delete(op.Key)
				case "update":
					// Store the updates for inspection.
					store.set(op.Key+"__updates", op.Updates)
				}
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(w, `{"applied":%d}`, len(batch.Ops))

		case path == queryPath && r.Method == http.MethodPost:
			body, _ := io.ReadAll(r.Body)
			var q struct {
				Collection string `json:"collection"`
			}
			_ = json.Unmarshal(body, &q)
			store.mu.Lock()
			type wireRec struct {
				Key  string `json:"key"`
				Data any    `json:"data,omitempty"`
			}
			var recs []wireRec
			for k, v := range store.records {
				coll := q.Collection + "/"
				if strings.HasPrefix(k, coll) {
					rest := strings.TrimPrefix(k, coll)
					if !strings.Contains(rest, "/") {
						var data any
						_ = json.Unmarshal(v, &data)
						recs = append(recs, wireRec{Key: k, Data: data})
					}
				}
			}
			store.mu.Unlock()
			out, _ := json.Marshal(map[string]any{"records": recs})
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(out)

		case strings.HasPrefix(path, recordsPrefix):
			keyPath := strings.TrimPrefix(path, recordsPrefix)
			switch r.Method {
			case http.MethodGet:
				data, ok := store.get(keyPath)
				if !ok {
					apiErr(w, http.StatusNotFound, "not_found", "not found")
					return
				}
				out, _ := json.Marshal(map[string]any{"key": keyPath, "data": json.RawMessage(data)})
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(out)

			case http.MethodHead:
				if _, ok := store.get(keyPath); ok {
					w.WriteHeader(http.StatusOK)
				} else {
					w.WriteHeader(http.StatusNotFound)
				}

			case http.MethodPut:
				body, _ := io.ReadAll(r.Body)
				var payload struct {
					Data json.RawMessage `json:"data"`
				}
				_ = json.Unmarshal(body, &payload)
				store.set(keyPath, payload.Data)
				w.WriteHeader(http.StatusNoContent)

			case http.MethodPost:
				if _, exists := store.get(keyPath); exists {
					apiErr(w, http.StatusConflict, "already_exists", "already exists")
					return
				}
				body, _ := io.ReadAll(r.Body)
				var payload struct {
					Data json.RawMessage `json:"data"`
				}
				_ = json.Unmarshal(body, &payload)
				store.set(keyPath, payload.Data)
				w.WriteHeader(http.StatusCreated)

			case http.MethodDelete:
				store.delete(keyPath)
				w.WriteHeader(http.StatusNoContent)

			default:
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			}

		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	})

	return httptest.NewServer(mux)
}

func apiErr(w http.ResponseWriter, code int, errCode, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = fmt.Fprintf(w, `{"error":{"code":%q,"message":%q}}`, errCode, msg)
}

// ---- tests ------------------------------------------------------------------

func TestNewDB_MissingArgs(t *testing.T) {
	t.Parallel()
	if _, err := dalgo2openvaultdb.NewDB("", "db"); err == nil {
		t.Error("expected error for empty baseURL")
	}
	if _, err := dalgo2openvaultdb.NewDB("http://localhost", ""); err == nil {
		t.Error("expected error for empty databaseID")
	}
}

func TestDB_IDAndAdapter(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	srv := newRecordServer(t, store)
	defer srv.Close()
	db := mustNewDB(t, srv)

	if db.ID() != "testdb" {
		t.Errorf("ID() = %q, want %q", db.ID(), "testdb")
	}
	if db.Adapter().Name() != "openvaultdb" {
		t.Errorf("Adapter().Name() = %q, want %q", db.Adapter().Name(), "openvaultdb")
	}
	if db.Schema() != nil {
		t.Error("Schema() should return nil")
	}
	if !db.SupportsConcurrentConnections() {
		t.Error("SupportsConcurrentConnections() should be true")
	}
}

func TestGet_Found(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	srv := newRecordServer(t, store)
	defer srv.Close()

	store.set("contacts/c1", json.RawMessage(`{"name":"Alice","status":"active"}`))
	db := mustNewDB(t, srv)

	key := dalrecord.NewKeyWithID("contacts", "c1")
	data := &contactData{}
	rec := dalrecord.NewRecordWithData(key, data)

	if err := db.Get(context.Background(), rec); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !rec.Exists() {
		t.Error("record.Exists() should be true")
	}
	if data.Name != "Alice" {
		t.Errorf("Name = %q, want Alice", data.Name)
	}
}

func TestGet_NotFound(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	srv := newRecordServer(t, store)
	defer srv.Close()
	db := mustNewDB(t, srv)

	key := dalrecord.NewKeyWithID("contacts", "missing")
	data := &contactData{}
	rec := dalrecord.NewRecordWithData(key, data)

	err := db.Get(context.Background(), rec)
	if !dalrecord.IsNotFound(err) {
		t.Fatalf("Get should return a not-found error (as dalgo2memory does), got: %v", err)
	}
	if rec.Exists() {
		t.Error("record.Exists() should be false for a missing record")
	}
	// record.Error() returns nil for not-found (per dalgo design).
	// Existence is checked via record.Exists().
}

func TestExists(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	srv := newRecordServer(t, store)
	defer srv.Close()

	store.set("contacts/c1", json.RawMessage(`{"name":"Bob"}`))
	db := mustNewDB(t, srv)
	ctx := context.Background()

	exists, err := db.Exists(ctx, dalrecord.NewKeyWithID("contacts", "c1"))
	if err != nil || !exists {
		t.Errorf("Exists('contacts/c1'): (%v, %v), want (true, nil)", exists, err)
	}

	exists, err = db.Exists(ctx, dalrecord.NewKeyWithID("contacts", "ghost"))
	if err != nil || exists {
		t.Errorf("Exists('contacts/ghost'): (%v, %v), want (false, nil)", exists, err)
	}
}

// TestInsert_Conflict proves a duplicate-key Insert surfaces as an HTTP 409
// from the batch commit and is classified as dalrecord.IsAlreadyExists, while
// still rendering the pre-existing "already exists" text so any caller
// matching on that literal string keeps working unchanged (errors.go wraps
// dalrecord.ErrRecordExists with %w rather than replacing the message).
func TestInsert_Conflict(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	srv := newRecordServer(t, store)
	defer srv.Close()

	store.set("contacts/c1", json.RawMessage(`{"name":"Existing"}`))
	db := mustNewDB(t, srv)

	err := db.RunReadwriteTransaction(context.Background(), func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		key := dalrecord.NewKeyWithID("contacts", "c1")
		data := &contactData{Name: "Duplicate"}
		rec := dalrecord.NewRecordWithData(key, data)
		return tx.Insert(ctx, rec)
	})
	if err == nil {
		t.Fatal("expected conflict error, got nil")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("error should mention 'already exists', got: %v", err)
	}
	if !dalrecord.IsAlreadyExists(err) {
		t.Errorf("a duplicate Insert should satisfy dalrecord.IsAlreadyExists, got: %v", err)
	}
}

// TestInsert_OtherFailureNotClassifiedAsExists proves mapHTTPError's 409
// branch is the only path that satisfies dalrecord.IsAlreadyExists — a
// different failure during the same batch commit (a 500 from the server,
// unrelated to a duplicate key) must NOT be misclassified as "already
// exists".
func TestInsert_OtherFailureNotClassifiedAsExists(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "batch") {
			apiErr(w, http.StatusInternalServerError, "internal", "boom")
			return
		}
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer srv.Close()

	db, err := dalgo2openvaultdb.NewDB(srv.URL, "testdb", dalgo2openvaultdb.WithHTTPClient(srv.Client()))
	if err != nil {
		t.Fatalf("NewDB: %v", err)
	}

	err = db.RunReadwriteTransaction(context.Background(), func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		key := dalrecord.NewKeyWithID("contacts", "c1")
		data := &contactData{Name: "Alice"}
		rec := dalrecord.NewRecordWithData(key, data)
		return tx.Insert(ctx, rec)
	})
	if err == nil {
		t.Fatal("expected an error from the 500 batch response, got nil")
	}
	if dalrecord.IsAlreadyExists(err) {
		t.Errorf("a non-409 failure must not satisfy dalrecord.IsAlreadyExists, got: %v", err)
	}
}

// TestReadwriteTx_Buffering verifies read-your-writes and that exactly one batch
// is sent with ops in order.
func TestReadwriteTx_Buffering(t *testing.T) {
	t.Parallel()
	var (
		batchMu    sync.Mutex
		batchCalls int
		lastBatch  struct {
			Message string `json:"message"`
			Ops     []struct {
				Op  string `json:"op"`
				Key string `json:"key"`
			} `json:"ops"`
		}
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/databases/testdb/batch" && r.Method == http.MethodPost {
			batchMu.Lock()
			batchCalls++
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &lastBatch)
			batchMu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"applied":2}`))
			return
		}
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer srv.Close()

	db, _ := dalgo2openvaultdb.NewDB(srv.URL, "testdb",
		dalgo2openvaultdb.WithHTTPClient(srv.Client()))

	ctx := context.Background()
	var readYourWriteName string

	err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		key1 := dalrecord.NewKeyWithID("contacts", "c1")
		data1 := &contactData{Name: "Alice"}
		rec1 := dalrecord.NewRecordWithData(key1, data1)
		if err := tx.Set(ctx, rec1); err != nil {
			return err
		}

		// Read-your-writes: Get should return the buffered record.
		readBack := &contactData{}
		recRead := dalrecord.NewRecordWithData(key1, readBack)
		if err := tx.Get(ctx, recRead); err != nil {
			return fmt.Errorf("get after set: %w", err)
		}
		if !recRead.Exists() {
			return fmt.Errorf("read-your-writes: record should exist after Set")
		}
		readYourWriteName = readBack.Name

		key2 := dalrecord.NewKeyWithID("contacts", "c2")
		data2 := &contactData{Name: "Bob"}
		rec2 := dalrecord.NewRecordWithData(key2, data2)
		return tx.Insert(ctx, rec2)
	}, dal.TxWithMessage("test commit"))

	if err != nil {
		t.Fatalf("RunReadwriteTransaction: %v", err)
	}
	if readYourWriteName != "Alice" {
		t.Errorf("read-your-writes Name = %q, want Alice", readYourWriteName)
	}
	if batchCalls != 1 {
		t.Errorf("expected 1 batch call, got %d", batchCalls)
	}
	if lastBatch.Message != "test commit" {
		t.Errorf("batch message = %q, want 'test commit'", lastBatch.Message)
	}
	if len(lastBatch.Ops) != 2 {
		t.Fatalf("expected 2 ops, got %d: %+v", len(lastBatch.Ops), lastBatch.Ops)
	}
	if lastBatch.Ops[0].Op != "set" || lastBatch.Ops[0].Key != "contacts/c1" {
		t.Errorf("op[0] = %+v, want {set contacts/c1}", lastBatch.Ops[0])
	}
	if lastBatch.Ops[1].Op != "insert" || lastBatch.Ops[1].Key != "contacts/c2" {
		t.Errorf("op[1] = %+v, want {insert contacts/c2}", lastBatch.Ops[1])
	}
}

func TestReadwriteTx_FailedWorkerSendsNothing(t *testing.T) {
	t.Parallel()
	var batchCalls int

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "batch") {
			batchCalls++
		}
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer srv.Close()

	db, _ := dalgo2openvaultdb.NewDB(srv.URL, "testdb",
		dalgo2openvaultdb.WithHTTPClient(srv.Client()))

	workerErr := fmt.Errorf("worker failed")
	err := db.RunReadwriteTransaction(context.Background(), func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		key := dalrecord.NewKeyWithID("contacts", "c1")
		data := &contactData{Name: "Alice"}
		rec := dalrecord.NewRecordWithData(key, data)
		_ = tx.Set(ctx, rec)
		return workerErr
	})
	if err != workerErr {
		t.Errorf("expected worker error, got: %v", err)
	}
	if batchCalls != 0 {
		t.Errorf("expected 0 batch calls on worker failure, got %d", batchCalls)
	}
}

func TestReadwriteTx_DeleteInBuffer(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	srv := newRecordServer(t, store)
	defer srv.Close()

	store.set("contacts/c1", json.RawMessage(`{"name":"ToDelete"}`))
	db := mustNewDB(t, srv)

	err := db.RunReadwriteTransaction(context.Background(), func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		key := dalrecord.NewKeyWithID("contacts", "c1")
		_ = tx.Delete(ctx, key)

		// Get should see not-found from buffer, not from HTTP.
		data := &contactData{}
		rec := dalrecord.NewRecordWithData(key, data)
		if err := tx.Get(ctx, rec); !dalrecord.IsNotFound(err) {
			return fmt.Errorf("expected not-found error after buffered delete, got: %w", err)
		}
		if rec.Exists() {
			return fmt.Errorf("expected not-found after buffered delete, but Exists() = true")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RunReadwriteTransaction: %v", err)
	}
}

// TestUpdateWireEncoding verifies the JSON update op encoding for
// plain value, delete sentinel, increment transform, and serverTimestamp.
func TestUpdateWireEncoding(t *testing.T) {
	t.Parallel()
	var capturedOps json.RawMessage

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/databases/testdb/batch" {
			body, _ := io.ReadAll(r.Body)
			var batch struct {
				Ops []struct {
					Op      string          `json:"op"`
					Updates json.RawMessage `json:"updates"`
				} `json:"ops"`
			}
			_ = json.Unmarshal(body, &batch)
			if len(batch.Ops) > 0 {
				capturedOps = batch.Ops[0].Updates
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"applied":1}`))
			return
		}
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer srv.Close()

	db, _ := dalgo2openvaultdb.NewDB(srv.URL, "testdb",
		dalgo2openvaultdb.WithHTTPClient(srv.Client()))

	err := db.RunReadwriteTransaction(context.Background(), func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		key := dalrecord.NewKeyWithID("spaces", "s1")
		return tx.Update(ctx, key, []update.Update{
			update.ByFieldName("title", "New Title"),
			update.DeleteByFieldName("obsolete"),
			update.ByFieldPath(update.FieldPath{"counters", "n"}, dal.Increment(5)),
			update.ByFieldName("updatedAt", update.ServerTimestamp),
		})
	})
	if err != nil {
		t.Fatalf("RunReadwriteTransaction: %v", err)
	}

	var ops []map[string]json.RawMessage
	if err := json.Unmarshal(capturedOps, &ops); err != nil {
		t.Fatalf("unmarshal ops: %v (raw=%s)", err, capturedOps)
	}
	if len(ops) != 4 {
		t.Fatalf("expected 4 update ops, got %d", len(ops))
	}

	// op[0]: plain value
	if name := rawStr(ops[0]["fieldName"]); name != "title" {
		t.Errorf("op[0].fieldName = %q, want title", name)
	}
	if val := rawStr(ops[0]["value"]); val != "New Title" {
		t.Errorf("op[0].value = %q, want 'New Title'", val)
	}

	// op[1]: delete
	if name := rawStr(ops[1]["fieldName"]); name != "obsolete" {
		t.Errorf("op[1].fieldName = %q, want obsolete", name)
	}
	if string(ops[1]["delete"]) != "true" {
		t.Errorf("op[1].delete = %s, want true", ops[1]["delete"])
	}

	// op[2]: increment via fieldPath
	if string(ops[2]["fieldPath"]) != `["counters","n"]` {
		t.Errorf("op[2].fieldPath = %s, want [counters,n]", ops[2]["fieldPath"])
	}
	if string(ops[2]["transform"]) != `"increment"` {
		t.Errorf("op[2].transform = %s, want \"increment\"", ops[2]["transform"])
	}
	if string(ops[2]["value"]) != "5" {
		t.Errorf("op[2].value = %s, want 5", ops[2]["value"])
	}

	// op[3]: server timestamp
	if string(ops[3]["serverTimestamp"]) != "true" {
		t.Errorf("op[3].serverTimestamp = %s, want true", ops[3]["serverTimestamp"])
	}
}

// TestQuery_WireAndReader verifies that a structured query is correctly
// serialized and that the records reader iterates results.
func TestQuery_WireAndReader(t *testing.T) {
	t.Parallel()
	var capturedBody []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/databases/testdb/query" {
			capturedBody, _ = io.ReadAll(r.Body)
			resp := `{"records":[{"key":"contacts/c1","data":{"name":"Alice","status":"active"}},{"key":"contacts/c2","data":{"name":"Bob","status":"active"}}]}`
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(resp))
			return
		}
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer srv.Close()

	db, _ := dalgo2openvaultdb.NewDB(srv.URL, "testdb",
		dalgo2openvaultdb.WithHTTPClient(srv.Client()))

	q := collectionFrom("contacts").
		WhereField("status", dal.Equal, "active").
		OrderBy(dal.AscendingField("name")).
		Limit(10).
		SelectIntoRecord(func() dalrecord.Record {
			return dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("contacts", ""), &contactData{})
		})

	reader, err := db.ExecuteQueryToRecordsReader(context.Background(), q)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsReader: %v", err)
	}
	defer func() { _ = reader.Close() }()

	// Verify wire encoding.
	var wq map[string]any
	if err := json.Unmarshal(capturedBody, &wq); err != nil {
		t.Fatalf("unmarshal captured query: %v", err)
	}
	if wq["collection"] != "contacts" {
		t.Errorf("collection = %v, want contacts", wq["collection"])
	}
	if wq["limit"] != float64(10) {
		t.Errorf("limit = %v, want 10", wq["limit"])
	}

	// Drain reader.
	var names []string
	for {
		rec, err := reader.Next()
		if err == dal.ErrNoMoreRecords {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		names = append(names, rec.Data().(*contactData).Name)
	}
	if !reflect.DeepEqual(names, []string{"Alice", "Bob"}) {
		t.Errorf("names = %v, want [Alice Bob]", names)
	}
}

// TestQuery_KeysOnly verifies that a keys-only query sends keysOnly=true.
func TestQuery_KeysOnly(t *testing.T) {
	t.Parallel()
	var capturedBody []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/databases/testdb/query" {
			capturedBody, _ = io.ReadAll(r.Body)
			resp := `{"records":[{"key":"contacts/c1"},{"key":"contacts/c2"}]}`
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(resp))
			return
		}
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer srv.Close()

	db, _ := dalgo2openvaultdb.NewDB(srv.URL, "testdb",
		dalgo2openvaultdb.WithHTTPClient(srv.Client()))

	q := collectionFrom("contacts").SelectKeysOnly(reflect.String)
	reader, err := db.ExecuteQueryToRecordsReader(context.Background(), q)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsReader: %v", err)
	}
	defer func() { _ = reader.Close() }()

	var wq map[string]any
	_ = json.Unmarshal(capturedBody, &wq)
	if wq["keysOnly"] != true {
		t.Errorf("keysOnly = %v, want true", wq["keysOnly"])
	}

	rec, err := reader.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if rec.Key().Collection() != "contacts" {
		t.Errorf("Key.Collection = %q, want contacts", rec.Key().Collection())
	}
}

// TestQuery_ArrayContains verifies that WhereInArrayField/WhereArrayContains
// produces an "array-contains" wire op.
func TestQuery_ArrayContains(t *testing.T) {
	t.Parallel()
	var capturedBody []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/databases/testdb/query" {
			capturedBody, _ = io.ReadAll(r.Body)
			_, _ = w.Write([]byte(`{"records":[]}`))
			return
		}
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer srv.Close()

	db, _ := dalgo2openvaultdb.NewDB(srv.URL, "testdb",
		dalgo2openvaultdb.WithHTTPClient(srv.Client()))

	// WhereInArrayField: value In fieldName → array-contains.
	q := collectionFrom("contacts").
		WhereInArrayField("accounts", "acc1").
		SelectIntoRecord(func() dalrecord.Record {
			return dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("contacts", ""), &contactData{})
		})

	reader, err := db.ExecuteQueryToRecordsReader(context.Background(), q)
	if err != nil {
		t.Fatalf("ExecuteQueryToRecordsReader: %v", err)
	}
	_ = reader.Close()

	var wq struct {
		Where []struct {
			Field string `json:"field"`
			Op    string `json:"op"`
			Value any    `json:"value"`
		} `json:"where"`
	}
	if err := json.Unmarshal(capturedBody, &wq); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(wq.Where) != 1 {
		t.Fatalf("expected 1 where clause, got %d", len(wq.Where))
	}
	if wq.Where[0].Op != "array-contains" {
		t.Errorf("op = %q, want array-contains", wq.Where[0].Op)
	}
	if wq.Where[0].Field != "accounts" {
		t.Errorf("field = %q, want accounts", wq.Where[0].Field)
	}
}

// TestIncompleteKey_Insert verifies that Insert with an incomplete key
// generates a random string ID.
func TestIncompleteKey_Insert(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	srv := newRecordServer(t, store)
	defer srv.Close()

	db := mustNewDB(t, srv)

	key := dalrecord.NewIncompleteKey("contacts", reflect.String, nil)
	data := &contactData{Name: "New"}
	rec := dalrecord.NewRecordWithData(key, data)

	err := db.RunReadwriteTransaction(context.Background(), func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Insert(ctx, rec, dal.WithRandomStringKey(16, 5))
	})
	if err != nil {
		t.Fatalf("Insert with incomplete key: %v", err)
	}
	if rec.Key().ID == nil {
		t.Error("expected key.ID to be set after Insert")
	}
	id, ok := rec.Key().ID.(string)
	if !ok || len(id) != 16 {
		t.Errorf("ID = %v (%T), want a 16-char string", rec.Key().ID, rec.Key().ID)
	}
}

// TestPreconditionsNotSupported verifies that passing preconditions to Update
// returns an ErrNotSupported-wrapped error.
func TestPreconditionsNotSupported(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	srv := newRecordServer(t, store)
	defer srv.Close()

	db := mustNewDB(t, srv)

	err := db.RunReadwriteTransaction(context.Background(), func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		key := dalrecord.NewKeyWithID("contacts", "c1")
		return tx.Update(ctx, key, []update.Update{
			update.ByFieldName("name", "x"),
		}, dal.WithExistsPrecondition())
	})
	if err == nil {
		t.Fatal("expected ErrNotSupported for preconditions")
	}
	if !strings.Contains(err.Error(), "not supported") {
		t.Errorf("error should mention 'not supported', got: %v", err)
	}
}

// TestRecordsetReaderUnsupported verifies ExecuteQueryToRecordsetReader → ErrNotSupported.
func TestRecordsetReaderUnsupported(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	srv := newRecordServer(t, store)
	defer srv.Close()

	db := mustNewDB(t, srv)
	q := collectionFrom("contacts").SelectIntoRecord(func() dalrecord.Record {
		return dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("contacts", ""), &contactData{})
	})
	_, err := db.ExecuteQueryToRecordsetReader(context.Background(), q)
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Errorf("expected ErrNotSupported, got: %v", err)
	}
}

// TestGetMulti verifies per-record not-found doesn't abort.
func TestGetMulti(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	srv := newRecordServer(t, store)
	defer srv.Close()

	store.set("contacts/c1", json.RawMessage(`{"name":"Alice"}`))

	db := mustNewDB(t, srv)
	k1 := dalrecord.NewKeyWithID("contacts", "c1")
	k2 := dalrecord.NewKeyWithID("contacts", "c2")
	d1, d2 := &contactData{}, &contactData{}
	r1 := dalrecord.NewRecordWithData(k1, d1)
	r2 := dalrecord.NewRecordWithData(k2, d2)

	if err := db.GetMulti(context.Background(), []dalrecord.Record{r1, r2}); err != nil {
		t.Fatalf("GetMulti: %v", err)
	}
	if !r1.Exists() {
		t.Error("r1 should exist")
	}
	if r2.Exists() {
		t.Error("r2 should not exist")
	}
	if d1.Name != "Alice" {
		t.Errorf("d1.Name = %q, want Alice", d1.Name)
	}
}

// TestEmptyTx_NoBatch verifies that an empty transaction sends no batch request.
func TestEmptyTx_NoBatch(t *testing.T) {
	t.Parallel()
	var batchCalls int

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "batch") {
			batchCalls++
		}
		// Don't need to respond; batchCalls check is all we care about.
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer srv.Close()

	db, _ := dalgo2openvaultdb.NewDB(srv.URL, "testdb",
		dalgo2openvaultdb.WithHTTPClient(srv.Client()))

	err := db.RunReadwriteTransaction(context.Background(), func(_ context.Context, _ dal.ReadwriteTransaction) error {
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if batchCalls != 0 {
		t.Errorf("expected 0 batch calls for empty tx, got %d", batchCalls)
	}
}

// TestReadonlyTx verifies that reads within a readonly tx pass through to HTTP.
func TestReadonlyTx(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	srv := newRecordServer(t, store)
	defer srv.Close()

	store.set("contacts/c1", json.RawMessage(`{"name":"ReadOnly"}`))
	db := mustNewDB(t, srv)

	var gotName string
	err := db.RunReadonlyTransaction(context.Background(), func(ctx context.Context, tx dal.ReadTransaction) error {
		key := dalrecord.NewKeyWithID("contacts", "c1")
		data := &contactData{}
		rec := dalrecord.NewRecordWithData(key, data)
		if err := tx.Get(ctx, rec); err != nil {
			return err
		}
		if !rec.Exists() {
			return fmt.Errorf("record not found")
		}
		gotName = data.Name
		return nil
	})
	if err != nil {
		t.Fatalf("RunReadonlyTransaction: %v", err)
	}
	if gotName != "ReadOnly" {
		t.Errorf("Name = %q, want ReadOnly", gotName)
	}
}

// ---- utility ----------------------------------------------------------------

// rawStr JSON-unmarshals a string from a raw JSON value.
func rawStr(raw json.RawMessage) string {
	if raw == nil {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return string(raw)
	}
	return s
}
