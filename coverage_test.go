package dalgo2openvaultdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
	"github.com/dal-go/record/update"
)

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type errReader struct{}

func (errReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("read error")
}

func (errReader) Close() error {
	return nil
}

type testDoc struct {
	Name string `json:"name"`
}

type dummyQuery struct{}

func (dummyQuery) String() string { return "dummy" }
func (dummyQuery) Offset() int    { return 0 }
func (dummyQuery) Limit() int     { return 0 }
func (dummyQuery) GetRecordsReader(_ context.Context, _ dal.QueryExecutor) (dal.RecordsReader, error) {
	return nil, nil
}
func (dummyQuery) GetRecordsetReader(_ context.Context, _ dal.QueryExecutor) (dal.RecordsetReader, error) {
	return nil, nil
}

type dummyCondition struct{}

func (dummyCondition) String() string { return "dummy" }

type dummyExpression struct{}

func (dummyExpression) String() string { return "dummy" }



func colFrom(name string) *dal.QueryBuilder {
	return dal.NewQueryBuilder(dal.From(dal.NewRootCollectionRef(name, "")))
}

func TestClient_PostQuery_Errors(t *testing.T) {
	ctx := context.Background()

	// 1. HTTP Do error
	cDoErr := &httpClient{
		baseURL:    "http://example.com",
		databaseID: "testdb",
		client: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return nil, errors.New("network failure")
			}),
		},
	}
	if _, err := cDoErr.postQuery(ctx, []byte("{}")); err == nil {
		t.Fatal("expected error on do failure")
	}

	// 2. Read response body error on 200 OK
	cReadErr := &httpClient{
		baseURL:    "http://example.com",
		databaseID: "testdb",
		client: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       errReader{},
					Header:     make(http.Header),
				}, nil
			}),
		},
	}
	if _, err := cReadErr.postQuery(ctx, []byte("{}")); err == nil {
		t.Fatal("expected error on read body failure")
	}

	// 3. Status not OK (e.g. 500)
	cStatusErr := &httpClient{
		baseURL:    "http://example.com",
		databaseID: "testdb",
		client: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body:       io.NopCloser(strings.NewReader(`{"error":{"code":"internal","message":"failed"}}`)),
					Header:     make(http.Header),
				}, nil
			}),
		},
	}
	if _, err := cStatusErr.postQuery(ctx, []byte("{}")); err == nil {
		t.Fatal("expected error on non-200 status")
	}
}

func TestClient_UnmarshalRecord_Errors(t *testing.T) {
	key := dalrecord.NewKeyWithID("docs", "d1")
	var doc testDoc
	rec := dalrecord.NewRecordWithData(key, &doc)
	rec.SetError(nil)

	// Invalid outer JSON
	if err := unmarshalRecord([]byte(`{invalid`), rec); err == nil {
		t.Fatal("expected unmarshal error on invalid json")
	}

	// Wrapper.Data cannot unmarshal into record data
	if err := unmarshalRecord([]byte(`{"data":"not-an-object"}`), rec); err == nil {
		t.Fatal("expected unmarshal error when data type mismatches struct")
	}
}

func TestDB_OptionsAndMethods(t *testing.T) {
	client := &http.Client{}
	db := &database{
		id: "testdb",
		c: httpClient{
			baseURL:    "http://example.com",
			databaseID: "testdb",
			client:     client,
		},
	}
	optClient := WithHTTPClient(client)
	optClient(db)

	optBearer := WithBearerToken("secret-token")
	optBearer(db)
	if db.c.bearerToken != "secret-token" {
		t.Fatalf("expected bearerToken 'secret-token', got %q", db.c.bearerToken)
	}

	ctx := context.Background()

	// Get with unmarshalRecord error
	db.c.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{invalid json`)),
				Header:     make(http.Header),
			}, nil
		}),
	}
	rec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("docs", "1"), &testDoc{})
	if err := db.Get(ctx, rec); err == nil {
		t.Fatal("expected error from db.Get on unmarshal error")
	}

	// GetMulti with non-not-found error
	db.c.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return nil, errors.New("connection failed")
		}),
	}
	if err := db.GetMulti(ctx, []dalrecord.Record{rec}); err == nil {
		t.Fatal("expected error from GetMulti on non-not-found error")
	}

	// ExecuteQueryToRecordsReader: non-structured query
	if _, err := db.ExecuteQueryToRecordsReader(ctx, dummyQuery{}); err == nil {
		t.Fatal("expected error for non-structured query")
	}

	// ExecuteQueryToRecordsReader: buildWireQuery error (offset > 0)
	qOffset := colFrom("docs").Offset(10).SelectKeysOnly(reflect.String)
	if _, err := db.ExecuteQueryToRecordsReader(ctx, qOffset); err == nil {
		t.Fatal("expected error for query with offset")
	}

	// ExecuteQueryToRecordsReader: marshalWireQuery error (unmarshallable Value in comparison)
	cmpBadVal := dal.Comparison{
		Left:     dal.Field("name"),
		Operator: dal.Equal,
		Right:    dal.Constant{Value: make(chan int)},
	}
	qBadVal := colFrom("docs").Where(cmpBadVal).SelectKeysOnly(reflect.String)
	if _, err := db.ExecuteQueryToRecordsReader(ctx, qBadVal); err == nil {
		t.Fatal("expected error when marshalWireQuery fails")
	}

	// ExecuteQueryToRecordsReader: postQuery error
	qValid := colFrom("docs").SelectKeysOnly(reflect.String)
	if _, err := db.ExecuteQueryToRecordsReader(ctx, qValid); err == nil {
		t.Fatal("expected error when postQuery fails")
	}

	// ExecuteQueryToRecordsetReader
	if _, err := db.ExecuteQueryToRecordsetReader(ctx, qValid); err == nil {
		t.Fatal("expected error from ExecuteQueryToRecordsetReader")
	}
}

func TestErrors_EdgeCases(t *testing.T) {
	// 1. Non-JSON body or code empty -> unknown
	resp := &http.Response{
		StatusCode: http.StatusBadRequest,
		Body:       io.NopCloser(strings.NewReader("plain text error")),
		Header:     make(http.Header),
	}
	err := mapHTTPError(resp, nil)
	if !strings.Contains(err.Error(), "unknown") || !strings.Contains(err.Error(), "plain text error") {
		t.Fatalf("unexpected error format: %v", err)
	}

	// 2. 404 with nil key
	resp404 := &http.Response{
		StatusCode: http.StatusNotFound,
		Body:       io.NopCloser(strings.NewReader(`{"error":{"code":"not_found","message":"missing resource"}}`)),
		Header:     make(http.Header),
	}
	err404 := mapHTTPError(resp404, nil)
	if !strings.Contains(err404.Error(), "not found: missing resource") {
		t.Fatalf("unexpected 404 error: %v", err404)
	}

	// 3. 409 with non-nil key
	key := dalrecord.NewKeyWithID("col", "k1")
	resp409 := &http.Response{
		StatusCode: http.StatusConflict,
		Body:       io.NopCloser(strings.NewReader(`{"error":{"code":"already_exists","message":"collision"}}`)),
		Header:     make(http.Header),
	}
	err409 := mapHTTPError(resp409, key)
	if !errors.Is(err409, dalrecord.ErrRecordExists) || !strings.Contains(err409.Error(), "col/k1") {
		t.Fatalf("unexpected 409 error: %v", err409)
	}
}

func TestQuery_BuildAndWire(t *testing.T) {
	// StartFrom
	qStart := colFrom("docs").StartFrom("cursor123").SelectKeysOnly(reflect.String)
	if _, err := buildWireQuery(qStart); err == nil {
		t.Fatal("expected StartFrom error")
	}

	// GroupBy
	qGroup := colFrom("docs").GroupBy(dal.Field("name")).SelectKeysOnly(reflect.String)
	if _, err := buildWireQuery(qGroup); err == nil {
		t.Fatal("expected GroupBy error")
	}

	// Having
	qHaving := colFrom("docs").Having(dal.WhereField("name", dal.Equal, "test")).SelectKeysOnly(reflect.String)
	if _, err := buildWireQuery(qHaving); err == nil {
		t.Fatal("expected Having error")
	}

	// Columns
	qCols := colFrom("docs").SelectColumns(dal.Column{Expression: dal.Field("name")})
	if _, err := buildWireQuery(qCols); err == nil {
		t.Fatal("expected Columns error")
	}

	// Parent subcollection query
	parentKey := dalrecord.NewKeyWithID("spaces", "s1")
	colRef := dal.NewCollectionRef("happenings", "", parentKey)
	qParent := dal.NewQueryBuilder(dal.From(colRef)).SelectKeysOnly(reflect.String)
	wq, err := buildWireQuery(qParent)
	if err != nil {
		t.Fatalf("unexpected parent query error: %v", err)
	}
	if wq.Parent != "spaces/s1" {
		t.Fatalf("expected Parent 'spaces/s1', got %q", wq.Parent)
	}

	// ConditionToWire: OR group condition
	orCond := dal.NewGroupCondition(
		dal.Or,
		dal.WhereField("a", dal.Equal, 1),
		dal.WhereField("b", dal.Equal, 2),
	)
	qOr := colFrom("docs").Where(orCond).SelectKeysOnly(reflect.String)
	if _, err := buildWireQuery(qOr); err == nil {
		t.Fatal("expected OR condition error")
	}

	// ConditionToWire: nested group error
	nestedGroupErr := dal.NewGroupCondition(
		dal.And,
		orCond,
	)
	qNestedErr := colFrom("docs").Where(nestedGroupErr).SelectKeysOnly(reflect.String)
	if _, err := buildWireQuery(qNestedErr); err == nil {
		t.Fatal("expected nested group error")
	}

	// ConditionToWire: unsupported condition type
	qCustomCond := colFrom("docs").Where(dummyCondition{}).SelectKeysOnly(reflect.String)
	if _, err := buildWireQuery(qCustomCond); err == nil {
		t.Fatal("expected unsupported condition error")
	}

	// OrderBy: non-FieldRef expression
	qBadOrder := colFrom("docs").OrderBy(dal.Ascending(dummyExpression{})).SelectKeysOnly(reflect.String)
	if _, err := buildWireQuery(qBadOrder); err == nil {
		t.Fatal("expected order-by expression error")
	}

	// Comparison operators: <, <=, >, >=, In
	ops := []struct {
		op   dal.Operator
		want string
	}{
		{dal.LessThen, "<"},
		{dal.LessOrEqual, "<="},
		{dal.GreaterThen, ">"},
		{dal.GreaterOrEqual, ">="},
		{dal.In, "in"},
	}
	for _, tc := range ops {
		qOp := colFrom("docs").Where(dal.WhereField("n", tc.op, 10)).SelectKeysOnly(reflect.String)
		wq, err := buildWireQuery(qOp)
		if err != nil {
			t.Fatalf("op %v error: %v", tc.op, err)
		}
		if len(wq.Where) != 1 || wq.Where[0].Op != tc.want {
			t.Fatalf("op %v: expected %q, got %+v", tc.op, tc.want, wq.Where)
		}
	}

	// Unsupported operator
	cmpBadOp := dal.Comparison{
		Left:     dal.Field("x"),
		Operator: dal.Operator("UNKNOWN"),
		Right:    dal.Constant{Value: 1},
	}
	if _, err := comparisonToWire(cmpBadOp); err == nil {
		t.Fatal("expected unsupported operator error")
	}

	// FieldRef vs Array with non-In operator
	cmpArrayNotOp := dal.Comparison{
		Left:     dal.Field("x"),
		Operator: dal.Equal,
		Right:    dal.Array{Value: []int{1, 2}},
	}
	if _, err := comparisonToWire(cmpArrayNotOp); err == nil {
		t.Fatal("expected FieldRef vs Array non-In error")
	}

	// FieldRef vs unsupported Right
	cmpRightUnsupported := dal.Comparison{
		Left:     dal.Field("x"),
		Operator: dal.Equal,
		Right:    dummyExpression{},
	}
	if _, err := comparisonToWire(cmpRightUnsupported); err == nil {
		t.Fatal("expected unsupported Right operand error")
	}

	// Constant comparison with right != FieldRef or op != In
	cmpConstNotField := dal.Comparison{
		Left:     dal.Constant{Value: "v"},
		Operator: dal.In,
		Right:    dal.Constant{Value: "other"},
	}
	if _, err := comparisonToWire(cmpConstNotField); err == nil {
		t.Fatal("expected Constant vs non-FieldRef error")
	}
	cmpConstNotOp := dal.Comparison{
		Left:     dal.Constant{Value: "v"},
		Operator: dal.Equal,
		Right:    dal.Field("x"),
	}
	if _, err := comparisonToWire(cmpConstNotOp); err == nil {
		t.Fatal("expected Constant vs FieldRef non-In error")
	}

	// Unsupported Left operand
	cmpLeftUnsupported := dal.Comparison{
		Left:     dummyExpression{},
		Operator: dal.Equal,
		Right:    dal.Constant{Value: "v"},
	}
	if _, err := comparisonToWire(cmpLeftUnsupported); err == nil {
		t.Fatal("expected unsupported Left operand error")
	}
}

func TestQuery_ReaderAndPaths(t *testing.T) {
	// splitKeyPath errors
	if _, _, err := splitKeyPath("single"); err == nil {
		t.Fatal("expected error for single segment")
	}
	if _, _, err := splitKeyPath("a/b/c"); err == nil {
		t.Fatal("expected error for odd segment count")
	}

	// unescapeSegment hex sequences
	escaped := "%2E_%24_%23_%5B_%5D_%2F"
	unescaped := unescapeSegment(escaped)
	if unescaped != "._$_#_[_]_/" {
		t.Fatalf("unexpected unescaped result: %q", unescaped)
	}

	// newQueryRecordsReader invalid JSON
	q := colFrom("docs").SelectKeysOnly(reflect.String)
	if _, err := newQueryRecordsReader([]byte(`{invalid`), q); err == nil {
		t.Fatal("expected error on invalid reader json")
	}

	// queryRecordsReader.Next key parsing error
	reader := &queryRecordsReader{
		records: []wireQueryRecord{
			{Key: "invalid_key", Data: json.RawMessage(`{"name":"val"}`)},
		},
	}
	if _, err := reader.Next(); err == nil {
		t.Fatal("expected error on invalid key path in Next()")
	}

	// queryRecordsReader.Next unmarshal error with intoRec
	readerDataErr := &queryRecordsReader{
		records: []wireQueryRecord{
			{Key: "docs/1", Data: json.RawMessage(`"not-an-object"`)},
		},
		intoRec: func() dalrecord.Record {
			return dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("docs", "tmpl"), &testDoc{})
		},
	}
	if _, err := readerDataErr.Next(); err == nil {
		t.Fatal("expected error on data unmarshal in Next()")
	}

	// Cursor and Close
	if c, err := reader.Cursor(); c != "" || err != nil {
		t.Fatalf("unexpected cursor: %v, %v", c, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("unexpected close: %v", err)
	}

	// recordsetReaderUnsupported
	var rsu recordsetReaderUnsupported
	if rsu.Recordset() != nil {
		t.Fatal("expected nil recordset")
	}
	if _, _, err := rsu.Next(); err == nil {
		t.Fatal("expected ErrNotSupported from Next()")
	}
	if c, err := rsu.Cursor(); c != "" || err != nil {
		t.Fatalf("unexpected cursor: %v, %v", c, err)
	}
	if err := rsu.Close(); err != nil {
		t.Fatalf("unexpected close: %v", err)
	}
}

func TestTX_OptionsAndCoverage(t *testing.T) {
	opts := &txOptions{}
	opts.SetMessage("msg1")
	if opts.Message() != "msg1" {
		t.Fatalf("expected message 'msg1', got %q", opts.Message())
	}
	if opts.IsolationLevel() != dal.TxUnspecified {
		t.Fatal("expected TxUnspecified")
	}
	if opts.IsReadonly() {
		t.Fatal("expected IsReadonly false")
	}
	if opts.IsCrossGroup() {
		t.Fatal("expected IsCrossGroup false")
	}
	if opts.Attempts() != 0 {
		t.Fatal("expected Attempts 0")
	}

	ctx := context.Background()

	// readwriteTx methods
	rwTx := &readwriteTx{
		c: &httpClient{
			baseURL:    "http://example.com",
			databaseID: "testdb",
		},
		opts:         opts,
		bufferedData: make(map[string]bufferedRecord),
	}
	if rwTx.ID() != "" {
		t.Fatal("expected empty ID")
	}
	if rwTx.Options() != opts {
		t.Fatal("expected matching Options")
	}

	// Get with buffered record failing unmarshal
	rwTx.bufferedData["docs/bad"] = bufferedRecord{
		opType: "set",
		data:   json.RawMessage(`"not-an-object"`),
	}
	recBad := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("docs", "bad"), &testDoc{})
	if err := rwTx.Get(ctx, recBad); err == nil {
		t.Fatal("expected unmarshal error on buffered record")
	}

	// Get fall-through to HTTP with error
	rwTx.c.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return nil, errors.New("network fail")
		}),
	}
	recHttp := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("docs", "http1"), &testDoc{})
	if err := rwTx.Get(ctx, recHttp); err == nil {
		t.Fatal("expected http error on rwTx.Get")
	}

	// Get fall-through to HTTP with unmarshalRecord error
	rwTx.c.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{bad json`)),
				Header:     make(http.Header),
			}, nil
		}),
	}
	if err := rwTx.Get(ctx, recHttp); err == nil {
		t.Fatal("expected unmarshal error on rwTx.Get")
	}

	// Exists with buffered data
	rwTx.bufferedData["docs/exists"] = bufferedRecord{opType: "set"}
	if exists, err := rwTx.Exists(ctx, dalrecord.NewKeyWithID("docs", "exists")); !exists || err != nil {
		t.Fatalf("expected exists true, got %v, %v", exists, err)
	}

	// GetMulti with non-not-found error
	rwTx.c.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return nil, errors.New("network fail")
		}),
	}
	if err := rwTx.GetMulti(ctx, []dalrecord.Record{recHttp}); err == nil {
		t.Fatal("expected GetMulti error")
	}

	// ExecuteQueryToRecordsReader non-structured query
	if _, err := rwTx.ExecuteQueryToRecordsReader(ctx, dummyQuery{}); err == nil {
		t.Fatal("expected non-structured query error")
	}
	// buildWireQuery error
	qOffset := colFrom("docs").Offset(5).SelectKeysOnly(reflect.String)
	if _, err := rwTx.ExecuteQueryToRecordsReader(ctx, qOffset); err == nil {
		t.Fatal("expected buildWireQuery error")
	}
	// marshalWireQuery error
	cmpBadVal := dal.Comparison{
		Left:     dal.Field("n"),
		Operator: dal.Equal,
		Right:    dal.Constant{Value: make(chan int)},
	}
	qBadVal := colFrom("docs").Where(cmpBadVal).SelectKeysOnly(reflect.String)
	if _, err := rwTx.ExecuteQueryToRecordsReader(ctx, qBadVal); err == nil {
		t.Fatal("expected marshalWireQuery error")
	}
	// postQuery error
	qValid := colFrom("docs").SelectKeysOnly(reflect.String)
	if _, err := rwTx.ExecuteQueryToRecordsReader(ctx, qValid); err == nil {
		t.Fatal("expected postQuery error")
	}
	// ExecuteQueryToRecordsetReader
	if _, err := rwTx.ExecuteQueryToRecordsetReader(ctx, qValid); err == nil {
		t.Fatal("expected ExecuteQueryToRecordsetReader error")
	}

	// Set marshal error
	recChan := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("docs", "chan"), make(chan int))
	if err := rwTx.Set(ctx, recChan); err == nil {
		t.Fatal("expected Set marshal error")
	}

	// SetMulti error
	if err := rwTx.SetMulti(ctx, []dalrecord.Record{recChan}); err == nil {
		t.Fatal("expected SetMulti error")
	}

	// Insert with AdapterGeneratedID and random string generator
	rwTx.c.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodHead {
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(bytes.NewReader(nil)),
					Header:     make(http.Header),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader(nil)),
				Header:     make(http.Header),
			}, nil
		}),
	}
	recInsert := dalrecord.NewRecordWithData(dalrecord.NewKeyWithFields("docs"), &testDoc{Name: "gen"})
	if err := rwTx.Insert(ctx, recInsert, dal.WithAdapterGeneratedID()); err != nil {
		t.Fatalf("expected successful insert with generated ID: %v", err)
	}

	// Insert with key already existing in buffer then checking generator
	rwTx.bufferedData["docs/existKey"] = bufferedRecord{opType: "insert"}
	// Check exists error in ID generator
	rwTx.c.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return nil, errors.New("head failed")
		}),
	}
	recInsertFail := dalrecord.NewRecordWithData(dalrecord.NewKeyWithFields("docs"), &testDoc{Name: "gen"})
	if err := rwTx.Insert(ctx, recInsertFail, dal.WithAdapterGeneratedID()); err == nil {
		t.Fatal("expected error from check exists for insert")
	}

	// bufferInsert marshal error
	if err := rwTx.bufferInsert(recChan); err == nil {
		t.Fatal("expected error on bufferInsert marshal")
	}

	// InsertMulti error
	if err := rwTx.InsertMulti(ctx, []dalrecord.Record{recChan}); err == nil {
		t.Fatal("expected InsertMulti error")
	}

	// DeleteMulti and UpdateMulti
	k1 := dalrecord.NewKeyWithID("docs", "k1")
	k2 := dalrecord.NewKeyWithID("docs", "k2")
	if err := rwTx.DeleteMulti(ctx, []*dalrecord.Key{k1, k2}); err != nil {
		t.Fatalf("unexpected DeleteMulti error: %v", err)
	}
	updates := []update.Update{update.ByFieldName("name", "new")}
	if err := rwTx.UpdateMulti(ctx, []*dalrecord.Key{k1, k2}, updates); err != nil {
		t.Fatalf("unexpected UpdateMulti error: %v", err)
	}
	// UpdateMulti error (precondition error)
	if err := rwTx.UpdateMulti(ctx, []*dalrecord.Key{k1}, updates, dal.WithExistsPrecondition()); err == nil {
		t.Fatal("expected UpdateMulti error on precondition")
	}

	// Commit with marshal batch ops error
	// To cause json.Marshal(tx.ops) to error, put invalid json.RawMessage in txOp.Data
	rwTx.ops = append(rwTx.ops, txOp{
		Op:   "set",
		Key:  "docs/bad_raw",
		Data: json.RawMessage(`{"invalid": `),
	})
	if err := rwTx.commit(ctx); err == nil {
		t.Fatal("expected commit error on invalid batch ops JSON")
	}
}

func TestTX_ReadonlyCoverage(t *testing.T) {
	ctx := context.Background()
	opts := &txOptions{}
	roTx := &readonlyTx{
		c: &httpClient{
			baseURL:    "http://example.com",
			databaseID: "testdb",
		},
		opts: opts,
	}

	if roTx.Options() != opts {
		t.Fatal("expected matching Options")
	}

	// Get error paths
	roTx.c.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return nil, errors.New("network fail")
		}),
	}
	rec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("docs", "1"), &testDoc{})
	if err := roTx.Get(ctx, rec); err == nil {
		t.Fatal("expected Get network error")
	}

	roTx.c.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{bad json`)),
				Header:     make(http.Header),
			}, nil
		}),
	}
	if err := roTx.Get(ctx, rec); err == nil {
		t.Fatal("expected Get unmarshal error")
	}

	// Exists
	roTx.c.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader(nil)),
				Header:     make(http.Header),
			}, nil
		}),
	}
	if exists, err := roTx.Exists(ctx, rec.Key()); !exists || err != nil {
		t.Fatalf("expected Exists true: %v, %v", exists, err)
	}

	// GetMulti error
	roTx.c.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return nil, errors.New("network fail")
		}),
	}
	if err := roTx.GetMulti(ctx, []dalrecord.Record{rec}); err == nil {
		t.Fatal("expected GetMulti error")
	}

	// ExecuteQueryToRecordsReader non-structured query
	if _, err := roTx.ExecuteQueryToRecordsReader(ctx, dummyQuery{}); err == nil {
		t.Fatal("expected non-structured query error")
	}
	// buildWireQuery error
	qOffset := colFrom("docs").Offset(5).SelectKeysOnly(reflect.String)
	if _, err := roTx.ExecuteQueryToRecordsReader(ctx, qOffset); err == nil {
		t.Fatal("expected buildWireQuery error")
	}
	// marshalWireQuery error
	cmpBadVal := dal.Comparison{
		Left:     dal.Field("n"),
		Operator: dal.Equal,
		Right:    dal.Constant{Value: make(chan int)},
	}
	qBadVal := colFrom("docs").Where(cmpBadVal).SelectKeysOnly(reflect.String)
	if _, err := roTx.ExecuteQueryToRecordsReader(ctx, qBadVal); err == nil {
		t.Fatal("expected marshalWireQuery error")
	}
	// postQuery error
	qValid := colFrom("docs").SelectKeysOnly(reflect.String)
	if _, err := roTx.ExecuteQueryToRecordsReader(ctx, qValid); err == nil {
		t.Fatal("expected postQuery error")
	}
	// ExecuteQueryToRecordsetReader
	if _, err := roTx.ExecuteQueryToRecordsetReader(ctx, qValid); err == nil {
		t.Fatal("expected ExecuteQueryToRecordsetReader error")
	}
}

func TestUpdates_Coverage(t *testing.T) {
	// marshalUpdates with convertUpdate error
	badUpdate := update.ByFieldName("chan", make(chan int))
	if _, err := marshalUpdates([]update.Update{badUpdate}, nil); err == nil {
		t.Fatal("expected marshalUpdates error for unmarshallable value")
	}

	// ServerTimestamp
	wu, err := convertUpdate(update.ByFieldName("ts", update.ServerTimestamp))
	if err != nil || !wu.ServerTimestamp {
		t.Fatalf("expected ServerTimestamp: %v, %+v", err, wu)
	}

	// Transform increment marshal error
	type iface struct {
		tab  unsafe.Pointer
		data unsafe.Pointer
	}
	type rawTransform struct {
		name  string
		value any
	}
	tInc := dal.Increment(1)
	rawT := (*rawTransform)((*iface)(unsafe.Pointer(&tInc)).data)
	rawT.value = make(chan int)
	if _, err := convertUpdate(update.ByFieldName("inc", tInc)); err == nil {
		t.Fatal("expected error for unmarshallable increment value")
	}

	// Unsupported transform
	tOther := dal.Increment(1)
	rawTOther := (*rawTransform)((*iface)(unsafe.Pointer(&tOther)).data)
	rawTOther.name = "decrement"
	if _, err := convertUpdate(update.ByFieldName("dec", tOther)); err == nil {
		t.Fatal("expected error for unsupported transform")
	}
}

func TestClient_URLBuildingAndEdgeErrors(t *testing.T) {
	ctx := context.Background()
	badClient := &httpClient{
		baseURL:    ":/invalid-url\x7f",
		databaseID: "testdb",
	}
	key := dalrecord.NewKeyWithID("docs", "k1")

	if _, err := badClient.getRecord(ctx, key); err == nil {
		t.Fatal("expected error from getRecord on invalid URL")
	}
	if _, err := badClient.headRecord(ctx, key); err == nil {
		t.Fatal("expected error from headRecord on invalid URL")
	}
	if err := badClient.postBatch(ctx, []byte("{}")); err == nil {
		t.Fatal("expected error from postBatch on invalid URL")
	}
	if _, err := badClient.postQuery(ctx, []byte("{}")); err == nil {
		t.Fatal("expected error from postQuery on invalid URL")
	}

	// getRecord: read body error on 200 OK
	cReadErr := &httpClient{
		baseURL:    "http://example.com",
		databaseID: "testdb",
		client: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       errReader{},
					Header:     make(http.Header),
				}, nil
			}),
		},
	}
	if _, err := cReadErr.getRecord(ctx, key); err == nil {
		t.Fatal("expected error reading getRecord body")
	}

	// headRecord: non-200 and non-404 status
	cHead500 := &httpClient{
		baseURL:    "http://example.com",
		databaseID: "testdb",
		client: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body:       io.NopCloser(strings.NewReader(`{"error":{"code":"err","message":"fail"}}`)),
					Header:     make(http.Header),
				}, nil
			}),
		},
	}
	if _, err := cHead500.headRecord(ctx, key); err == nil {
		t.Fatal("expected error from headRecord on 500")
	}

	// postBatch: network error
	cBatchNetErr := &httpClient{
		baseURL:    "http://example.com",
		databaseID: "testdb",
		client: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return nil, errors.New("network failure")
			}),
		},
	}
	if err := cBatchNetErr.postBatch(ctx, []byte("{}")); err == nil {
		t.Fatal("expected error on postBatch network failure")
	}
}

func TestQuery_AdditionalCoverage(t *testing.T) {
	// GroupCondition AND with multiple conditions
	andCond := dal.NewGroupCondition(
		dal.And,
		dal.WhereField("a", dal.Equal, 1),
		dal.WhereField("b", dal.Equal, 2),
	)
	clauses, err := conditionToWire(andCond)
	if err != nil || len(clauses) != 2 {
		t.Fatalf("expected 2 clauses from AND condition, got %v, err=%v", clauses, err)
	}

	// Comparison inside conditionToWire returning error
	badCmp := dal.Comparison{
		Left:     dal.Field("x"),
		Operator: dal.Operator("INVALID"),
		Right:    dal.Constant{Value: 1},
	}
	if _, err := conditionToWire(badCmp); err == nil {
		t.Fatal("expected error from conditionToWire with invalid comparison")
	}

	// FieldRef In Array
	inArrayCmp := dal.Comparison{
		Left:     dal.Field("x"),
		Operator: dal.In,
		Right:    dal.Array{Value: []int{1, 2}},
	}
	clause, err := comparisonToWire(inArrayCmp)
	if err != nil || clause.Op != "in" {
		t.Fatalf("expected clause op 'in', got %+v, err=%v", clause, err)
	}
}

func withCustomGenerator(gen func(ctx context.Context, record dalrecord.Record) error) dal.InsertOption {
	type fakeInsertOptions struct {
		idGenerator              func(ctx context.Context, record dalrecord.Record) error
		preferAdapterGeneratedID bool
	}
	f := func(opts *fakeInsertOptions) {
		_ = opts.preferAdapterGeneratedID
		opts.idGenerator = gen
	}
	return *(*dal.InsertOption)(unsafe.Pointer(&f))
}

func TestTX_Insert_GeneratorBufferAndServerExists(t *testing.T) {
	ctx := context.Background()
	rwTx := &readwriteTx{
		c: &httpClient{
			baseURL:    "http://example.com",
			databaseID: "testdb",
		},
		bufferedData: make(map[string]bufferedRecord),
	}

	// Buffer has docs/hit1
	rwTx.bufferedData["docs/hit1"] = bufferedRecord{opType: "insert"}

	// Attempt counter for custom generator
	attempt := 0
	gen := func(ctx context.Context, r dalrecord.Record) error {
		attempt++
		switch attempt {
		case 1:
			r.Key().ID = "hit1" // in buffer!
		case 2:
			r.Key().ID = "hitServer" // on server!
		default:
			r.Key().ID = "hitSuccess" // free to use!
		}
		return nil
	}

	rwTx.c.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if strings.HasSuffix(req.URL.Path, "/hitServer") {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader(nil)),
					Header:     make(http.Header),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(bytes.NewReader(nil)),
				Header:     make(http.Header),
			}, nil
		}),
	}

	rec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithFields("docs"), &testDoc{Name: "Bob"})
	if err := rwTx.Insert(ctx, rec, withCustomGenerator(gen)); err != nil {
		t.Fatalf("expected successful insert after retries: %v", err)
	}
	if rec.Key().ID != "hitSuccess" {
		t.Fatalf("expected ID 'hitSuccess', got %v", rec.Key().ID)
	}
}

func TestTX_SuccessfulPaths(t *testing.T) {
	ctx := context.Background()

	// rwTx.Get successful HTTP fall-through
	rwTx := &readwriteTx{
		c: &httpClient{
			baseURL:    "http://example.com",
			databaseID: "testdb",
			client: &http.Client{
				Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       io.NopCloser(strings.NewReader(`{"key":"docs/1","data":{"name":"Alice"}}`)),
						Header:     make(http.Header),
					}, nil
				}),
			},
		},
		bufferedData: make(map[string]bufferedRecord),
	}
	var doc testDoc
	rec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("docs", "1"), &doc)
	if err := rwTx.Get(ctx, rec); err != nil {
		t.Fatalf("expected successful rwTx.Get: %v", err)
	}
	if doc.Name != "Alice" {
		t.Fatalf("expected name 'Alice', got %q", doc.Name)
	}

	// rwTx.Exists successful HTTP fall-through
	rwTx.c.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader(nil)),
				Header:     make(http.Header),
			}, nil
		}),
	}
	if exists, err := rwTx.Exists(ctx, dalrecord.NewKeyWithID("docs", "serverKey")); !exists || err != nil {
		t.Fatalf("expected exists true: %v, %v", exists, err)
	}

	// rwTx.GetMulti success
	rwTx.c.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"key":"docs/1","data":{"name":"Alice"}}`)),
				Header:     make(http.Header),
			}, nil
		}),
	}
	if err := rwTx.GetMulti(ctx, []dalrecord.Record{rec}); err != nil {
		t.Fatalf("expected successful rwTx.GetMulti: %v", err)
	}

	// rwTx.ExecuteQueryToRecordsReader success
	rwTx.c.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"records":[]}`)),
				Header:     make(http.Header),
			}, nil
		}),
	}
	qValid := colFrom("docs").SelectKeysOnly(reflect.String)
	if reader, err := rwTx.ExecuteQueryToRecordsReader(ctx, qValid); err != nil || reader == nil {
		t.Fatalf("expected successful rwTx.ExecuteQueryToRecordsReader: %v, %v", err, reader)
	}

	// roTx.GetMulti success
	roTx := &readonlyTx{
		c: &httpClient{
			baseURL:    "http://example.com",
			databaseID: "testdb",
			client: &http.Client{
				Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       io.NopCloser(strings.NewReader(`{"key":"docs/1","data":{"name":"Alice"}}`)),
						Header:     make(http.Header),
					}, nil
				}),
			},
		},
	}
	if err := roTx.GetMulti(ctx, []dalrecord.Record{rec}); err != nil {
		t.Fatalf("expected successful roTx.GetMulti: %v", err)
	}

	// roTx.ExecuteQueryToRecordsReader success
	roTx.c.client = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"records":[]}`)),
				Header:     make(http.Header),
			}, nil
		}),
	}
	if reader, err := roTx.ExecuteQueryToRecordsReader(ctx, qValid); err != nil || reader == nil {
		t.Fatalf("expected successful roTx.ExecuteQueryToRecordsReader: %v, %v", err, reader)
	}
}

