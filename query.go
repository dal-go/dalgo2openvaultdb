package dalgo2openvaultdb

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	dalrecord "github.com/dal-go/record"
)

// wireWhereClause is a single WHERE condition sent to OpenVaultDB.
type wireWhereClause struct {
	Field string `json:"field"`
	Op    string `json:"op"`
	Value any    `json:"value"`
}

// wireOrderBy is a single ORDER BY clause sent to OpenVaultDB.
type wireOrderBy struct {
	Field string `json:"field"`
	Desc  bool   `json:"desc"`
}

// wireQuery is the JSON body sent to POST /query.
type wireQuery struct {
	Collection string            `json:"collection"`
	Parent     string            `json:"parent,omitempty"` // dal-escaped parent key path for scoped subcollection queries
	Where      []wireWhereClause `json:"where,omitempty"`
	OrderBy    []wireOrderBy     `json:"orderBy,omitempty"`
	Limit      int               `json:"limit,omitempty"`
	KeysOnly   bool              `json:"keysOnly,omitempty"`
}

// buildWireQuery converts a dal.StructuredQuery to wireQuery.
// Returns ErrNotSupported for unsupported features (offset, cursor, group-by,
// having, columns, OR conditions).
func buildWireQuery(q dal.StructuredQuery) (wireQuery, error) {
	if q.Offset() > 0 {
		return wireQuery{}, fmt.Errorf("%w: query offset", dal.ErrNotSupported)
	}
	if q.StartFrom() != "" {
		return wireQuery{}, fmt.Errorf("%w: query cursor/StartFrom", dal.ErrNotSupported)
	}
	if len(q.GroupBy()) > 0 {
		return wireQuery{}, fmt.Errorf("%w: query group-by", dal.ErrNotSupported)
	}
	if q.Having() != nil {
		return wireQuery{}, fmt.Errorf("%w: query having", dal.ErrNotSupported)
	}
	if len(q.Columns()) > 0 {
		return wireQuery{}, fmt.Errorf("%w: query column projection", dal.ErrNotSupported)
	}

	wq := wireQuery{
		Collection: q.From().Base().Name(),
		Limit:      q.Limit(),
	}
	// Parent-scoped subcollection query (e.g. happenings under a space
	// module): the parent record key travels as a dal-escaped key path.
	if colRef, ok := q.From().Base().(dal.CollectionRef); ok {
		if parent := colRef.Parent(); parent != nil {
			wq.Parent = parent.String()
		}
	}

	// Keys-only: IntoRecord nil and IDKind is set.
	if q.IntoRecord() == nil && q.IDKind() != reflect.Invalid {
		wq.KeysOnly = true
	}

	// WHERE
	if cond := q.Where(); cond != nil {
		clauses, err := conditionToWire(cond)
		if err != nil {
			return wireQuery{}, err
		}
		wq.Where = clauses
	}

	// ORDER BY
	for _, ob := range q.OrderBy() {
		field, err := expressionToFieldName(ob.Expression())
		if err != nil {
			return wireQuery{}, fmt.Errorf("order-by expression: %w", err)
		}
		wq.OrderBy = append(wq.OrderBy, wireOrderBy{Field: field, Desc: ob.Descending()})
	}

	return wq, nil
}

// conditionToWire recursively converts a dal.Condition to a flat list of AND-ed
// wire WHERE clauses. Returns an error for OR groups.
func conditionToWire(cond dal.Condition) ([]wireWhereClause, error) {
	switch c := cond.(type) {
	case dal.GroupCondition:
		if c.Operator() != dal.And {
			return nil, fmt.Errorf("%w: query OR group conditions", dal.ErrNotSupported)
		}
		var clauses []wireWhereClause
		for _, sub := range c.Conditions() {
			sub, err := conditionToWire(sub)
			if err != nil {
				return nil, err
			}
			clauses = append(clauses, sub...)
		}
		return clauses, nil

	case dal.Comparison:
		clause, err := comparisonToWire(c)
		if err != nil {
			return nil, err
		}
		return []wireWhereClause{clause}, nil

	default:
		return nil, fmt.Errorf("%w: unsupported condition type %T", dal.ErrNotSupported, cond)
	}
}

// comparisonToWire converts a dal.Comparison to a wireWhereClause.
// Handles:
//   - FieldRef op Constant  → field op value  (==, <, <=, >, >=, In)
//   - FieldRef op Array     → field "in" [values]  (in operator)
//   - Constant In FieldRef  → field "array-contains" value  (WhereInArrayField pattern)
func comparisonToWire(cmp dal.Comparison) (wireWhereClause, error) {
	switch left := cmp.Left.(type) {
	case dal.FieldRef:
		fieldName := left.Name()
		switch right := cmp.Right.(type) {
		case dal.Constant:
			op, err := dalOperatorToWire(cmp.Operator)
			if err != nil {
				return wireWhereClause{}, err
			}
			return wireWhereClause{Field: fieldName, Op: op, Value: right.Value}, nil
		case dal.Array:
			// FieldRef In Array → "in" (array-contains-any is not directly reachable from WhereField)
			// dal.WhereField with an Array uses "In" operator.
			// Map to wire "in" op.
			if cmp.Operator != dal.In {
				return wireWhereClause{}, fmt.Errorf("%w: FieldRef vs Array with operator %q", dal.ErrNotSupported, cmp.Operator)
			}
			return wireWhereClause{Field: fieldName, Op: "in", Value: right.Value}, nil
		default:
			return wireWhereClause{}, fmt.Errorf("%w: unsupported right operand %T", dal.ErrNotSupported, cmp.Right)
		}

	case dal.Constant:
		// Constant In FieldRef → "array-contains" (WhereInArrayField pattern)
		right, ok := cmp.Right.(dal.FieldRef)
		if !ok || cmp.Operator != dal.In {
			return wireWhereClause{}, fmt.Errorf("%w: unsupported Constant comparison %T op=%q right=%T",
				dal.ErrNotSupported, cmp.Left, cmp.Operator, cmp.Right)
		}
		return wireWhereClause{Field: right.Name(), Op: "array-contains", Value: left.Value}, nil

	default:
		return wireWhereClause{}, fmt.Errorf("%w: unsupported left operand %T", dal.ErrNotSupported, cmp.Left)
	}
}

// dalOperatorToWire maps a DALgo Operator to the wire string.
func dalOperatorToWire(op dal.Operator) (string, error) {
	switch op {
	case dal.Equal:
		return "==", nil
	case dal.LessThen:
		return "<", nil
	case dal.LessOrEqual:
		return "<=", nil
	case dal.GreaterThen:
		return ">", nil
	case dal.GreaterOrEqual:
		return ">=", nil
	case dal.In:
		return "in", nil
	default:
		return "", fmt.Errorf("%w: unsupported operator %q", dal.ErrNotSupported, op)
	}
}

// expressionToFieldName extracts a field name from an Expression.
func expressionToFieldName(expr dal.Expression) (string, error) {
	if f, ok := expr.(dal.FieldRef); ok {
		return f.Name(), nil
	}
	return "", fmt.Errorf("%w: order-by expression must be a FieldRef, got %T", dal.ErrNotSupported, expr)
}

// marshalWireQuery serialises wireQuery to JSON.
func marshalWireQuery(wq wireQuery) ([]byte, error) {
	b, err := json.Marshal(wq)
	if err != nil {
		return nil, fmt.Errorf("marshal query: %w", err)
	}
	return b, nil
}

// wireQueryResponse is the shape of the POST /query response.
type wireQueryResponse struct {
	Records []wireQueryRecord `json:"records"`
}

// wireQueryRecord is a single record in the query response.
type wireQueryRecord struct {
	Key  string          `json:"key"`
	Data json.RawMessage `json:"data"`
}

// queryRecordsReader implements dal.RecordsReader over a slice of wireQueryRecord.
type queryRecordsReader struct {
	records  []wireQueryRecord
	pos      int
	intoRec  func() dalrecord.Record
	idKind   reflect.Kind
	keysOnly bool
}

func (r *queryRecordsReader) Next() (dalrecord.Record, error) {
	if r.pos >= len(r.records) {
		return nil, dal.ErrNoMoreRecords
	}
	wr := r.records[r.pos]
	r.pos++

	// Parse the key path: last segment is ID, second-to-last is collection.
	collection, id, err := splitKeyPath(wr.Key)
	if err != nil {
		return nil, fmt.Errorf("parse query record key %q: %w", wr.Key, err)
	}

	key := dalrecord.NewKeyWithID(collection, id)

	var rec dalrecord.Record
	if r.keysOnly || r.intoRec == nil {
		rec = dalrecord.NewRecord(key)
		rec.SetError(nil)
	} else {
		tmpl := r.intoRec()
		// SetError(nil) is required before Data() can be called.
		tmpl.SetError(nil)
		data := tmpl.Data()
		// Build a new record with the correct key from the wire response.
		rec = dalrecord.NewRecordWithData(key, data)
		rec.SetError(nil)
		if len(wr.Data) > 0 && string(wr.Data) != "null" {
			if err := json.Unmarshal(wr.Data, rec.Data()); err != nil {
				return nil, fmt.Errorf("unmarshal query record data: %w", err)
			}
		}
	}
	return rec, nil
}

func (r *queryRecordsReader) Cursor() (string, error) { return "", nil }
func (r *queryRecordsReader) Close() error            { return nil }

// splitKeyPath splits a wire key path like "contacts/c1" or
// "spaces/s1/members/m1" into the innermost (collection, id) pair.
// Only the last two segments are needed for dalrecord.NewKeyWithID.
func splitKeyPath(keyPath string) (collection, id string, err error) {
	// Walk segments.
	segs := splitPath(keyPath)
	if len(segs) < 2 || len(segs)%2 != 0 {
		return "", "", fmt.Errorf("invalid key path %q", keyPath)
	}
	// segs is [collection, id, subcollection, subid, ...]
	// The last pair is the innermost.
	n := len(segs)
	return segs[n-2], segs[n-1], nil
}

// splitPath splits a '/' separated path and unescapes each segment.
func splitPath(path string) []string {
	// Manual split to avoid importing strings and to unescape.
	var segs []string
	start := 0
	for i := 0; i <= len(path); i++ {
		if i == len(path) || path[i] == '/' {
			seg := path[start:i]
			segs = append(segs, unescapeSegment(seg))
			start = i + 1
		}
	}
	return segs
}

// unescapeSegment reverses dal.EscapeID percent-encoding.
func unescapeSegment(s string) string {
	// Only unescape the specific sequences that EscapeID produces.
	result := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s) {
			hex := s[i+1 : i+3]
			switch hex {
			case "2E":
				result = append(result, '.')
				i += 2
				continue
			case "24":
				result = append(result, '$')
				i += 2
				continue
			case "23":
				result = append(result, '#')
				i += 2
				continue
			case "5B":
				result = append(result, '[')
				i += 2
				continue
			case "5D":
				result = append(result, ']')
				i += 2
				continue
			case "2F":
				result = append(result, '/')
				i += 2
				continue
			}
		}
		result = append(result, s[i])
	}
	return string(result)
}

// newQueryRecordsReader parses the raw query response and returns a reader.
func newQueryRecordsReader(body []byte, q dal.StructuredQuery) (dal.RecordsReader, error) {
	var resp wireQueryResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parse query response: %w", err)
	}

	intoRec := func() dalrecord.Record { return q.IntoRecord() }
	if q.IntoRecord() == nil {
		intoRec = nil
	}

	keysOnly := q.IntoRecord() == nil && q.IDKind() != reflect.Invalid

	return &queryRecordsReader{
		records:  resp.Records,
		intoRec:  intoRec,
		idKind:   q.IDKind(),
		keysOnly: keysOnly,
	}, nil
}

// Ensure recordset reader is not supported.
var _ dal.RecordsetReader = (*recordsetReaderUnsupported)(nil)

type recordsetReaderUnsupported struct{}

func (recordsetReaderUnsupported) Recordset() recordset.Recordset { return nil }
func (recordsetReaderUnsupported) Next() (recordset.Row, recordset.Recordset, error) {
	return nil, nil, fmt.Errorf("%w: recordset reader", dal.ErrNotSupported)
}
func (recordsetReaderUnsupported) Cursor() (string, error) { return "", nil }
func (recordsetReaderUnsupported) Close() error            { return nil }
