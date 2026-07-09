# dalgo2openvaultdb

A [DALgo](https://github.com/dal-go/dalgo) driver for [OpenVaultDB](https://github.com/openvaultdb-org/openvaultdb).

It talks to an `ovdb serve` HTTP server over its REST API (`/v1/...`). The driver
uses only the Go standard library — no third-party HTTP client.

## Usage

```go
import (
    "context"
    "github.com/dal-go/dalgo/dal"
    dalgo2openvaultdb "github.com/dal-go/dalgo2openvaultdb"
)

db, err := dalgo2openvaultdb.NewDB("http://127.0.0.1:6832", "sneat-dev")
if err != nil {
    log.Fatal(err)
}

// Direct read (no transaction required)
key := dal.NewKeyWithID("contacts", "c1")
data := &ContactData{}
rec := dal.NewRecordWithData(key, data)
if err := db.Get(ctx, rec); err != nil {
    log.Fatal(err)
}
if rec.Exists() {
    fmt.Println(data.Name)
}

// Write — must be inside a transaction
err = db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
    rec := dal.NewRecordWithData(dal.NewKeyWithID("contacts", "c2"), &ContactData{Name: "Bob"})
    return tx.Set(ctx, rec)
}, dal.TxWithMessage("create Bob"))
```

### Custom HTTP client

```go
db, err := dalgo2openvaultdb.NewDB("http://127.0.0.1:6832", "sneat-dev",
    dalgo2openvaultdb.WithHTTPClient(&http.Client{Timeout: 5 * time.Second}))
```

## Supported DALgo capabilities

| Capability | Status |
|---|---|
| `Get` / `Exists` / `GetMulti` | supported |
| `RunReadonlyTransaction` | supported (pass-through reads) |
| `RunReadwriteTransaction` | supported (client-side buffer → POST /batch) |
| `Set` / `Insert` / `Delete` (in tx) | supported |
| `Update` (in tx, field-level) | supported |
| `SetMulti` / `InsertMulti` / `DeleteMulti` | supported |
| `UpdateRecord` / `UpdateMulti` | supported |
| Incomplete-key `Insert` (auto-ID) | supported (random 16-char string, 5 retries) |
| `ExecuteQueryToRecordsReader` | supported |
| Structured query: `From`, `Where`, `OrderBy`, `Limit` | supported |
| Query `Where` operators: `==`, `<`, `<=`, `>`, `>=`, `In` | supported |
| Query `WhereInArrayField` / `WhereArrayContains` | supported (`array-contains`) |
| Query `WhereArrayContainsAny` | supported (`array-contains-any` via `FieldRef In Array`) |
| Read-your-writes in same transaction (Set/Insert) | supported (buffer) |
| `SupportsConcurrentConnections()` | `true` (HTTP pooling) |
| `Schema()` | returns `nil` (schemaless) |
| **Unsupported** | |
| Update preconditions | `ErrNotSupported` |
| `ExecuteQueryToRecordsetReader` | `ErrNotSupported` |
| Query cursors / `StartFrom` | `ErrNotSupported` |
| Query `Offset` | `ErrNotSupported` |
| Query column projections (`Columns`) | `ErrNotSupported` |
| Query `GroupBy` / `Having` | `ErrNotSupported` |
| Collection-group queries | not supported by OpenVaultDB MVP |
| Cross-transaction isolation / optimistic concurrency | not in OpenVaultDB MVP |
| Read-your-writes for `Update` ops inside same tx | server resolves in batch order |

## Local development

1. Start the server:
   ```
   ovdb serve --db-path ./local-data sneat-dev
   ```
2. Point the driver at it:
   ```go
   db, _ := dalgo2openvaultdb.NewDB("http://127.0.0.1:6832", "sneat-dev")
   ```
3. Run tests:
   ```
   go test ./...
   ```
