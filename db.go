// Package dalgo2openvaultdb provides a DALgo driver for OpenVaultDB.
//
// Use NewDB to create a dal.DB that talks to an OpenVaultDB server over HTTP.
// Reads (Get, Exists, GetMulti, queries) are available directly on the DB.
// Writes must happen inside a RunReadwriteTransaction callback.
package dalgo2openvaultdb

import (
	"context"
	"fmt"
	"net/http"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
)

// Option configures a DB created by NewDB.
type Option func(*database)

// WithHTTPClient sets a custom *http.Client. If not provided, http.DefaultClient
// is used.
func WithHTTPClient(client *http.Client) Option {
	return func(db *database) {
		db.c.client = client
	}
}

// database is the dal.DB implementation for OpenVaultDB.
type database struct {
	dal.ConcurrencyAvailable // SupportsConcurrentConnections → true

	id string
	c  httpClient
}

var _ dal.DB = (*database)(nil)

// NewDB creates a new DALgo DB backed by OpenVaultDB.
//
//	db, err := dalgo2openvaultdb.NewDB("http://127.0.0.1:6832", "sneat-dev")
func NewDB(baseURL, databaseID string, opts ...Option) (dal.DB, error) {
	if baseURL == "" {
		return nil, fmt.Errorf("baseURL must not be empty")
	}
	if databaseID == "" {
		return nil, fmt.Errorf("databaseID must not be empty")
	}
	db := &database{
		id: databaseID,
		c: httpClient{
			baseURL:    baseURL,
			databaseID: databaseID,
			client:     http.DefaultClient,
		},
	}
	for _, o := range opts {
		o(db)
	}
	return db, nil
}

// ID returns the database identifier supplied to NewDB.
func (db *database) ID() string { return db.id }

// Adapter returns the adapter descriptor for this driver.
func (db *database) Adapter() dal.Adapter {
	return dal.NewAdapter("openvaultdb", driverVersion)
}

// Schema returns nil; OpenVaultDB is a schemaless document store.
func (db *database) Schema() dal.Schema { return nil }

// -- ReadSession on DB (direct, non-transactional) --

// Get fetches a single record. On 404, the record's error is set to an
// ErrNotFoundByKey and Get returns that error (matching dalgo2memory:
// callers use dal.IsNotFound(err) and/or record.Exists()).
func (db *database) Get(ctx context.Context, record dal.Record) error {
	body, err := db.c.getRecord(ctx, record.Key())
	if err != nil {
		record.SetError(err)
		return err
	}
	record.SetError(nil)
	if err := unmarshalRecord(body, record); err != nil {
		record.SetError(err)
		return err
	}
	return nil
}

// Exists reports whether a record with the given key exists.
func (db *database) Exists(ctx context.Context, key *dal.Key) (bool, error) {
	return db.c.headRecord(ctx, key)
}

// GetMulti fetches multiple records. Per-record not-found does not abort; the
// error is stored on each individual record.
func (db *database) GetMulti(ctx context.Context, records []dal.Record) error {
	for _, r := range records {
		if err := db.Get(ctx, r); err != nil && !dal.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// ExecuteQueryToRecordsReader executes a structured query and returns a reader.
func (db *database) ExecuteQueryToRecordsReader(ctx context.Context, query dal.Query) (dal.RecordsReader, error) {
	q, ok := query.(dal.StructuredQuery)
	if !ok {
		return nil, fmt.Errorf("%w: non-structured query", dal.ErrNotSupported)
	}
	wq, err := buildWireQuery(q)
	if err != nil {
		return nil, err
	}
	payload, err := marshalWireQuery(wq)
	if err != nil {
		return nil, err
	}
	body, err := db.c.postQuery(ctx, payload)
	if err != nil {
		return nil, err
	}
	return newQueryRecordsReader(body, q)
}

// ExecuteQueryToRecordsetReader always returns ErrNotSupported.
func (db *database) ExecuteQueryToRecordsetReader(_ context.Context, _ dal.Query, _ ...recordset.Option) (dal.RecordsetReader, error) {
	return nil, fmt.Errorf("%w: recordset reader", dal.ErrNotSupported)
}

// -- TransactionCoordinator --

// RunReadonlyTransaction runs f inside a logical readonly transaction.
// Reads pass through directly to the HTTP API.
func (db *database) RunReadonlyTransaction(ctx context.Context, f dal.ROTxWorker, opts ...dal.TransactionOption) error {
	txOpts := parseTransactionOptions(opts)
	tx := &readonlyTx{c: &db.c, opts: txOpts}
	txCtx := dal.NewContextWithTransaction(ctx, tx)
	return f(txCtx, tx)
}

// RunReadwriteTransaction runs f inside a buffered readwrite transaction.
// All write ops are buffered; on worker success they are committed atomically
// via a single POST /batch. If the worker returns an error, the buffer is
// discarded and nothing is sent to the server.
func (db *database) RunReadwriteTransaction(ctx context.Context, f dal.RWTxWorker, opts ...dal.TransactionOption) error {
	txOpts := parseTransactionOptions(opts)
	tx := &readwriteTx{
		c:            &db.c,
		opts:         txOpts,
		bufferedData: make(map[string]bufferedRecord),
	}
	txCtx := dal.NewContextWithTransaction(ctx, tx)
	if err := f(txCtx, tx); err != nil {
		// Worker failed — discard buffer, send nothing.
		return err
	}
	return tx.commit(ctx)
}

// Compile-time interface checks.
var (
	_ dal.DB          = (*database)(nil)
	_ dal.ReadSession = (*database)(nil)
)
