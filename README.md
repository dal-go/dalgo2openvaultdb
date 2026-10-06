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
| Query direct field projections (`FieldRef`) | supported (trimmed client-side) |
| Read-your-writes in same transaction (Set/Insert) | supported (buffer) |
| `SupportsConcurrentConnections()` | `true` (HTTP pooling) |
| `Schema()` | returns `nil` (schemaless) |
| **Unsupported** | |
| Update preconditions | `ErrNotSupported` |
| `ExecuteQueryToRecordsetReader` | `ErrNotSupported` |
| Query cursors / `StartFrom` | `ErrNotSupported` |
| Query `Offset` | `ErrNotSupported` |
| Query expression, aggregate, wildcard, and nested field projections | `ErrNotSupported` |
| Query `GroupBy` / `Having` | `ErrNotSupported` |
| Collection-group queries | not supported by OpenVaultDB MVP |
| Cross-transaction isolation / optimistic concurrency | not in OpenVaultDB MVP |
| Read-your-writes for `Update` ops inside same tx | server resolves in batch order |

Direct field projections are applied by the client adapter after the regular
OpenVaultDB query response arrives. The HTTP query API has no projection
parameter, so this does not reduce the fields transferred over HTTP. The
server remains authoritative for authorization, and the adapter removes
unselected fields before returning records to DALgo callers. Aggregate
queries may use these projections as source reads and calculate results in
DALgo; aggregates themselves are not sent to OpenVaultDB.

## Source rights and live-read evidence

Query readers preserve optional `sourceRights` and `usedSourceIds` inventories.
Use `dal.ReadQueryMetadata(reader)` before `Next` to read a detached snapshot,
including attribution, original free-source links and transformations. Omitted
metadata remains unknown; supplied empty arrays remain known-empty. Source usage
comes from execution metadata, including empty results and projected-away fields.
These inventories describe source terms, not a licence assigned to query output.

For an independently admitted proxy query, call
`RequireProviderReads(ctx, plan, admittedUsedSourceIDs)` before executing it.
The `providerreads.Plan` must come from trusted admission metadata, separately
from the response, and bind the expected executor, database, collection, original
resource, definition, decoder and complete source rights. Supply independently
admitted actual-use expectations and a fresh 32-character lowercase hex execution
ID for each query. The returned context is for that single query execution.

The adapter freezes the plan and usage expectations, sends `OVDB-Execution-ID`
and request `Cache-Control: no-store`, refuses redirects, caps the response at
4 MiB and caps the HTTP client timeout at ten seconds. It requires response
`no-store` and validates full rights/usage equality plus the closed
`ovdb-provider-read/1` envelope before parsing records or returning a reader.
Missing or malformed required evidence fails closed, including on empty results.
Required-mode response, metadata and row-decoding failures use fixed errors
without response content or an underlying decoder cause. The trusted
`dal.ErrNoMoreRecords` sentinel is preserved. Optional legacy diagnostics retain
their existing behavior.
`ReadProviderReads(reader)` returns a detached envelope with reference date,
fetch time, consumed-body digest and byte count. It carries executor observations;
it does not certify immutable live input or grant source rights.

Without the required context, supplied envelopes are decoded and preserved as
provider claims. They are not checked against independent admission. The strict
path supports direct structured adapter queries, including transaction query
methods. DALgo's generic joins, aggregation and federation are outside this
admitted route; generic wrappers can omit adapter-local capabilities. Consumers
must require `ReadProviderReads` on the returned reader before accessing data.
No source activation, retained-copy fallback or data persistence is provided by
this capability. Callers remain responsible for trusted endpoint selection and
their own storage, cache, logging and result-lifetime controls.

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
