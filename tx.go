package dalgo2openvaultdb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	"github.com/dal-go/dalgo/update"
)

// txOp is an operation buffered in a readwrite transaction.
type txOp struct {
	Op      string          `json:"op"`
	Key     string          `json:"key"`
	Data    json.RawMessage `json:"data,omitempty"`
	Updates json.RawMessage `json:"updates,omitempty"`
}

// txOptions is a mutable options bag for a transaction.
type txOptions struct {
	message string
}

func (o *txOptions) Message() string                      { return o.message }
func (o *txOptions) SetMessage(m string)                  { o.message = m }
func (o *txOptions) IsolationLevel() dal.TxIsolationLevel { return dal.TxUnspecified }
func (o *txOptions) IsReadonly() bool                     { return false }
func (o *txOptions) IsCrossGroup() bool                   { return false }
func (o *txOptions) Attempts() int                        { return 0 }

var _ dal.TransactionOptions = (*txOptions)(nil)

// readwriteTx is the readwrite transaction object passed to the worker.
type readwriteTx struct {
	c    *httpClient
	opts *txOptions
	ops  []txOp
	// bufferedData maps key string → (opType, raw data) for read-your-writes.
	// opType is "set", "insert", or "delete".
	bufferedData map[string]bufferedRecord
}

type bufferedRecord struct {
	opType string // "set", "insert", "delete"
	data   json.RawMessage
}

var _ dal.ReadwriteTransaction = (*readwriteTx)(nil)

func (tx *readwriteTx) ID() string                      { return "" }
func (tx *readwriteTx) Options() dal.TransactionOptions { return tx.opts }

// -- ReadSession methods on readwriteTx --

func (tx *readwriteTx) Get(ctx context.Context, record dal.Record) error {
	keyStr := record.Key().String()
	if buf, ok := tx.bufferedData[keyStr]; ok {
		switch buf.opType {
		case "delete":
			err := dal.NewErrNotFoundByKey(record.Key(), nil)
			record.SetError(err)
			return err
		case "set", "insert":
			record.SetError(nil)
			if err := json.Unmarshal(buf.data, record.Data()); err != nil {
				return fmt.Errorf("unmarshal buffered record: %w", err)
			}
			return nil
		}
	}
	// Fall through to HTTP.
	body, err := tx.c.getRecord(ctx, record.Key())
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

func (tx *readwriteTx) Exists(ctx context.Context, key *dal.Key) (bool, error) {
	keyStr := key.String()
	if buf, ok := tx.bufferedData[keyStr]; ok {
		return buf.opType != "delete", nil
	}
	return tx.c.headRecord(ctx, key)
}

func (tx *readwriteTx) GetMulti(ctx context.Context, records []dal.Record) error {
	for _, r := range records {
		if err := tx.Get(ctx, r); err != nil && !dal.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (tx *readwriteTx) ExecuteQueryToRecordsReader(ctx context.Context, query dal.Query) (dal.RecordsReader, error) {
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
	body, err := tx.c.postQuery(ctx, payload)
	if err != nil {
		return nil, err
	}
	return newQueryRecordsReader(body, q)
}

func (tx *readwriteTx) ExecuteQueryToRecordsetReader(_ context.Context, _ dal.Query, _ ...recordset.Option) (dal.RecordsetReader, error) {
	return nil, fmt.Errorf("%w: recordset reader", dal.ErrNotSupported)
}

// -- WriteSession methods on readwriteTx --

func (tx *readwriteTx) Set(ctx context.Context, record dal.Record) error {
	record.SetError(nil)
	data, err := json.Marshal(record.Data())
	if err != nil {
		record.SetError(err)
		return fmt.Errorf("marshal record data: %w", err)
	}
	keyStr := record.Key().String()
	tx.bufferedData[keyStr] = bufferedRecord{opType: "set", data: data}
	tx.ops = append(tx.ops, txOp{Op: "set", Key: keyStr, Data: data})
	record.SetError(nil)
	return nil
}

func (tx *readwriteTx) SetMulti(ctx context.Context, records []dal.Record) error {
	for _, r := range records {
		if err := tx.Set(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

func (tx *readwriteTx) Insert(ctx context.Context, record dal.Record, opts ...dal.InsertOption) error {
	options := dal.NewInsertOptions(opts...)
	gen := options.IDGenerator()
	if gen == nil && options.PreferAdapterGeneratedID() {
		gen = dal.NewInsertOptions(dal.WithRandomStringKey(dal.DefaultRandomStringIDLength, 5)).IDGenerator()
	}
	if gen != nil {
		return dal.InsertWithIdGenerator(ctx, record, gen, 5,
			func(key *dal.Key) error {
				// Check buffer first, then HTTP.
				if buf, ok := tx.bufferedData[key.String()]; ok && buf.opType != "delete" {
					return nil // exists in buffer
				}
				exists, err := tx.c.headRecord(ctx, key)
				if err != nil {
					return fmt.Errorf("check exists for insert: %w", err)
				}
				if exists {
					return nil // exists on server
				}
				return dal.ErrRecordNotFound // free to use
			},
			func(r dal.Record) error {
				return tx.bufferInsert(r)
			},
		)
	}
	return tx.bufferInsert(record)
}

func (tx *readwriteTx) bufferInsert(record dal.Record) error {
	record.SetError(nil)
	data, err := json.Marshal(record.Data())
	if err != nil {
		record.SetError(err)
		return fmt.Errorf("marshal record data: %w", err)
	}
	keyStr := record.Key().String()
	tx.bufferedData[keyStr] = bufferedRecord{opType: "insert", data: data}
	tx.ops = append(tx.ops, txOp{Op: "insert", Key: keyStr, Data: data})
	record.SetError(nil)
	return nil
}

func (tx *readwriteTx) InsertMulti(ctx context.Context, records []dal.Record, opts ...dal.InsertOption) error {
	for _, r := range records {
		if err := tx.Insert(ctx, r, opts...); err != nil {
			return err
		}
	}
	return nil
}

func (tx *readwriteTx) Delete(_ context.Context, key *dal.Key) error {
	keyStr := key.String()
	tx.bufferedData[keyStr] = bufferedRecord{opType: "delete"}
	tx.ops = append(tx.ops, txOp{Op: "delete", Key: keyStr})
	return nil
}

func (tx *readwriteTx) DeleteMulti(ctx context.Context, keys []*dal.Key) error {
	for _, k := range keys {
		if err := tx.Delete(ctx, k); err != nil {
			return err
		}
	}
	return nil
}

func (tx *readwriteTx) Update(_ context.Context, key *dal.Key, updates []update.Update, preconditions ...dal.Precondition) error {
	wire, err := marshalUpdates(updates, preconditions)
	if err != nil {
		return err
	}
	keyStr := key.String()
	tx.ops = append(tx.ops, txOp{Op: "update", Key: keyStr, Updates: wire})
	return nil
}

func (tx *readwriteTx) UpdateRecord(ctx context.Context, record dal.Record, updates []update.Update, preconditions ...dal.Precondition) error {
	return tx.Update(ctx, record.Key(), updates, preconditions...)
}

func (tx *readwriteTx) UpdateMulti(ctx context.Context, keys []*dal.Key, updates []update.Update, preconditions ...dal.Precondition) error {
	for _, k := range keys {
		if err := tx.Update(ctx, k, updates, preconditions...); err != nil {
			return err
		}
	}
	return nil
}

// commit sends the buffered ops to /batch. If ops is empty, nothing is sent.
func (tx *readwriteTx) commit(ctx context.Context) error {
	if len(tx.ops) == 0 {
		return nil
	}

	opsJSON, err := json.Marshal(tx.ops)
	if err != nil {
		return fmt.Errorf("marshal batch ops: %w", err)
	}

	var buf bytes.Buffer
	buf.WriteString(`{"message":`)
	msgJSON, _ := json.Marshal(tx.opts.message)
	buf.Write(msgJSON)
	buf.WriteString(`,"ops":`)
	buf.Write(opsJSON)
	buf.WriteString(`}`)

	return tx.c.postBatch(ctx, buf.Bytes())
}

// readonlyTx is the readonly transaction object passed to the ROTxWorker.
type readonlyTx struct {
	c    *httpClient
	opts *txOptions
}

var _ dal.ReadTransaction = (*readonlyTx)(nil)

func (tx *readonlyTx) Options() dal.TransactionOptions { return tx.opts }

func (tx *readonlyTx) Get(ctx context.Context, record dal.Record) error {
	body, err := tx.c.getRecord(ctx, record.Key())
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

func (tx *readonlyTx) Exists(ctx context.Context, key *dal.Key) (bool, error) {
	return tx.c.headRecord(ctx, key)
}

func (tx *readonlyTx) GetMulti(ctx context.Context, records []dal.Record) error {
	for _, r := range records {
		if err := tx.Get(ctx, r); err != nil && !dal.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (tx *readonlyTx) ExecuteQueryToRecordsReader(ctx context.Context, query dal.Query) (dal.RecordsReader, error) {
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
	body, err := tx.c.postQuery(ctx, payload)
	if err != nil {
		return nil, err
	}
	return newQueryRecordsReader(body, q)
}

func (tx *readonlyTx) ExecuteQueryToRecordsetReader(_ context.Context, _ dal.Query, _ ...recordset.Option) (dal.RecordsetReader, error) {
	return nil, fmt.Errorf("%w: recordset reader", dal.ErrNotSupported)
}

// Compile-time check that readonlyTx satisfies dal.ReadTransaction.
// readonlyTx embeds no WriteSession methods by design.
var _ dal.ReadTransaction = (*readonlyTx)(nil)

// parseTransactionOptions extracts the message from dal.TransactionOption slice
// using dalgo's own option parser.
func parseTransactionOptions(opts []dal.TransactionOption) *txOptions {
	dalOpts := dal.NewTransactionOptions(opts...)
	return &txOptions{message: dalOpts.Message()}
}

// Prevent use of reflect package for IDKind comparisons from being flagged.
var _ reflect.Kind = reflect.Invalid
