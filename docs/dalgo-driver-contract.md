# DALgo driver contract notes (dalgo v0.62.10)

Distilled reference for implementing this driver. Sources: dal-go/dalgo@v0.62.10,
dalgo2firestore, dalgo/adapters/dalgo2memory.

## Interfaces to implement

```go
// dal.DB (dal/db_database.go)
type DB interface {
    ID() string
    Adapter() Adapter          // dal.NewAdapter("openvaultdb", version)
    Schema() Schema            // may return nil for a document store
    TransactionCoordinator     // RunReadonlyTransaction + RunReadwriteTransaction
    ReadSession                // Getter + MultiGetter + QueryExecutor
    ConcurrencyAware           // embed dal.ConcurrencyAvailable (HTTP pooling)
}

// Sessions (dal/session.go)
ReadSession:      Get, Exists, GetMulti, ExecuteQueryToRecordsReader, ExecuteQueryToRecordsetReader
ReadwriteSession: ReadSession + Set, SetMulti, Delete, DeleteMulti, Update, UpdateRecord,
                  UpdateMulti, Insert, InsertMulti

// Transactions (dal/transaction.go)
RunReadonlyTransaction(ctx, func(ctx, tx dal.ReadTransaction) error, ...dal.TransactionOption) error
RunReadwriteTransaction(ctx, func(ctx, tx dal.ReadwriteTransaction) error, ...dal.TransactionOption) error
// ReadTransaction  = Transaction{Options() TransactionOptions} + ReadSession
// ReadwriteTransaction = ID() string (may be "") + Transaction + ReadwriteSession
// Options() must return a SHARED MUTABLE pointer (SetMessage is called mid-tx;
// dalgo2ingitdb uses Options().Message() as git commit message — we forward it to /batch).
// Derive worker ctx with dal.NewContextWithTransaction(ctx, tx).
```

Exact op signatures:

```go
Get(ctx, record dal.Record) error
Exists(ctx, key *dal.Key) (bool, error)
GetMulti(ctx, records []dal.Record) error
Set(ctx, record dal.Record) error
SetMulti(ctx, records []dal.Record) error
Delete(ctx, key *dal.Key) error
DeleteMulti(ctx, keys []*dal.Key) error
Insert(ctx, record dal.Record, opts ...dal.InsertOption) error
InsertMulti(ctx, records []dal.Record, opts ...dal.InsertOption) error
Update(ctx, key *dal.Key, updates []update.Update, preconditions ...dal.Precondition) error
UpdateRecord(ctx, record dal.Record, updates []update.Update, preconditions ...dal.Precondition) error
UpdateMulti(ctx, keys []*dal.Key, updates []update.Update, preconditions ...dal.Precondition) error
ExecuteQueryToRecordsReader(ctx, query dal.Query) (dal.RecordsReader, error)
ExecuteQueryToRecordsetReader(ctx, query dal.Query, options ...recordset.Option) (dal.RecordsetReader, error)
```

## Record lifecycle (critical)

- On Get success: `record.SetError(nil)` FIRST, then `json.Unmarshal(body, record.Data())`.
  Calling `Data()` before `SetError(nil)` panics.
- On Get 404: `record.SetError(dal.NewErrNotFoundByKey(record.Key(), nil))` and RETURN that
  error from Get (verified against dalgo2memory serialized.go:65 and the end2end suite, which
  checks `dal.IsNotFound(err)` on `db.Get`; an earlier revision of this doc wrongly said
  return nil).
- On other errors: `record.SetError(err)` and return err.
- GetMulti: per-record not-found must NOT abort — set each record's error, return nil overall.
- On write paths (Set/Insert): call `record.SetError(nil)` before `record.Data()`
  (fresh records panic otherwise), then `json.Marshal(record.Data())`.

## Keys

- Wire path = `key.String()` → `collection/id[/sub/id...]`, IDs percent-encode `. $ # [ ] /`
  via dal.EscapeID. Use key.String() directly in URLs (each segment already escaped).
- Incomplete keys (ID == nil): generate via
  `dal.WithRandomStringKey(dal.DefaultRandomStringIDLength /*16*/, 5)` semantics — use
  `dal.InsertWithIdGenerator(ctx, record, gen, attempts, exists, insert)` helper if convenient.

## Updates (update package)

`update.Update`: `FieldName() string` XOR `FieldPath() update.FieldPath` ([]string), `Value() any`.
Map to wire ops:
- plain value → `{"fieldName"|"fieldPath", "value": v}`
- `update.DeleteField` sentinel value → `{"...", "delete": true}`
- `update.ServerTimestamp` sentinel → `{"...", "serverTimestamp": true}`
- `dal.IsTransform(u.Value())` → transform; `transform.Name()=="increment"` →
  `{"...", "transform": "increment", "value": transform.Value()}`
Preconditions: not supported in MVP → if any precondition given, return
`fmt.Errorf("%w: update preconditions", dal.ErrNotSupported)`.

## Queries

Support `dal.StructuredQuery` (type-assert from dal.Query):
- From().Name() → collection; Where() → dal.Comparison / dal.GroupCondition (AND only);
  operators ==, <, <=, >, >=, In; expressions dal.FieldRef / dal.Constant.
- OrderBy() → []OrderExpression (Expression + Descending).
- Limit(), StartFrom() cursor NOT supported (return ErrNotSupported if set).
- IntoRecord() nil + IDKind() set → keys-only query.
- Map "array-contains" style: dalgo2memory supports arrays via In/array conditions; for MVP map
  dal.Comparison with Operator dal.In where left is field/right is array, and Sneat's
  WhereInArrayField produces an array-contains comparison — inspect dal.Query builder output
  and map accordingly (see dalgo2memory/database.go query handling for reference).
- `ExecuteQueryToRecordsetReader` → `fmt.Errorf("%w: recordset reader", dal.ErrNotSupported)`.
- RecordsReader: implement dal.RecordsReader returning records built via query.IntoRecord()
  factory; set key from response, SetError(nil), unmarshal data. Return io.EOF (or dal's
  ErrNoMoreRecords — check dal package) at end; follow dalgo2memory's reader as reference.

## Errors

- Sentinels: dal.ErrRecordNotFound (wrap via dal.NewErrNotFoundByKey), dal.ErrNotSupported,
  dal.ErrNotImplementedYet.
- HTTP mapping: 404 → not-found wrapping; 409 → already-exists (check dal for
  ErrRecordAlreadyExists or similar; else fmt.Errorf with key context); 422/400 → plain errors.

## Transactions over HTTP (this driver's model)

- Readonly tx: pass-through reads.
- Readwrite tx: buffer ordered write ops client-side; Get/Exists within tx first consult the
  op buffer (a set/insert of that key serves the read — best-effort read-your-writes), else
  pass through to HTTP. On worker success, POST the buffered ops as one /batch request with
  message = tx.Options().Message(). On worker error, discard buffer (nothing was sent).
- Update inside tx: buffer the update op (server resolves it read-modify-write in batch order).
- SupportsConcurrentConnections() → true.
