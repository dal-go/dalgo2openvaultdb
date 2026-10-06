package dalgo2openvaultdb

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/openvaultdb/openvaultdb-go/pkg/license"
	"github.com/openvaultdb/openvaultdb-go/pkg/providerreads"
)

func syntheticProviderResponse(t *testing.T) (providerreads.Plan, []string, map[string]any) {
	t.Helper()
	identity := license.Identity{ServerID: "synthetic-proxy", DatabaseID: "synthetic-db", Recordset: "synthetic-rates"}
	right := license.SourceRight{SourceID: identity.SourceID(), Source: identity, Declaration: license.Declaration{Name: "Invented test terms", Text: "Fabricated source for tests only"}, DeclarationScope: license.RecordsetScope, DeclaredAt: identity, EvidenceOrigin: "test-declared", Pins: []license.Pin{}, Transformations: []string{"Synthetic XML restructuring"}, Attribution: &license.Notice{Text: "Synthetic provider", URL: "https://synthetic.example/"}, FreeSource: &license.Notice{Text: "Synthetic free original", URL: "https://synthetic.example/rates.xml"}}
	rightsDigest, err := providerreads.RightsDigest(right)
	if err != nil {
		t.Fatal(err)
	}
	binding := providerreads.Binding{ProviderSourceID: "synthetic-original", RightsSourceID: right.SourceID, ResourceID: "synthetic-daily", DefinitionDigest: strings.Repeat("a", 64), DecoderDigest: strings.Repeat("b", 64), RightsDigest: rightsDigest}
	request := providerreads.Request{ResourceID: binding.ResourceID, Method: "GET", UpstreamURL: right.FreeSource.URL, Params: map[string]any{}}
	plan := providerreads.Plan{Execution: providerreads.Execution{ID: strings.Repeat("c", 32), Mode: "proxy", ExecutorID: identity.ServerID}, Bindings: []providerreads.Binding{binding}, Requests: []providerreads.Request{request}, SourceRights: []license.SourceRight{right}, MaxReads: new(1), MaxMetadataBytes: providerreads.MaxMetadataBytes}
	requestDigest, err := providerreads.RequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	observation := providerreads.Observation{ResourceID: binding.ResourceID, RequestDigest: requestDigest, FetchedAt: "2099-04-03T12:00:00Z", UpstreamURL: request.UpstreamURL, Status: 200, ContentType: "application/xml", SHA256: strings.Repeat("d", 64), Bytes: 123, ReferenceDate: "2099-04-02", Attestation: "proxy-executor-observed"}
	observation.ObservationID, err = providerreads.ObservationDigest(plan.Execution, binding, observation)
	if err != nil {
		t.Fatal(err)
	}
	envelope := providerreads.Envelope{Format: providerreads.Format, Execution: plan.Execution, Bindings: plan.Bindings, Reads: []providerreads.Observation{observation}, Usage: []providerreads.Usage{{ProviderSourceID: binding.ProviderSourceID, RightsSourceID: right.SourceID, ObservationIDs: []string{observation.ObservationID}}}}
	used := []string{right.SourceID}
	if err := providerreads.Validate(envelope, plan, used); err != nil {
		t.Fatal(err)
	}
	return plan, used, map[string]any{"records": []any{map[string]any{"key": "synthetic-rates/FAB", "data": map[string]any{"rate": "9.8700"}}}, "sourceRights": plan.SourceRights, "usedSourceIds": used, "providerReads": envelope}
}

func encodeSynthetic(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func syntheticClient(raw []byte, headers http.Header) *http.Client {
	return &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: headers, Body: io.NopCloser(strings.NewReader(string(raw)))}, nil
	})}
}

func TestProviderMetadataSurvivesEmptyProjectionAndTransactions(t *testing.T) {
	for _, empty := range []bool{false, true} {
		for _, path := range []string{"direct", "readonly", "readwrite", "optional"} {
			t.Run(path+"/empty="+strings.TrimSpace(strings.ReplaceAll(string(encodeSynthetic(t, empty)), "\"", "")), func(t *testing.T) {
				plan, used, response := syntheticProviderResponse(t)
				if empty {
					response["records"] = []any{}
				}
				var sentID string
				client := syntheticClient(encodeSynthetic(t, response), http.Header{"Cache-Control": {"private, NO-STORE"}})
				original := client.Transport
				client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
					sentID = req.Header.Get("OVDB-Execution-ID")
					if path != "optional" && req.Header.Get("Cache-Control") != "no-store" {
						t.Fatal("missing request no-store")
					}
					return original.RoundTrip(req)
				})
				db, err := NewDB("https://synthetic.example", "synthetic-db", WithHTTPClient(client))
				if err != nil {
					t.Fatal(err)
				}
				ctx := context.Background()
				if path != "optional" {
					ctx, err = RequireProviderReads(ctx, plan, used)
					if err != nil {
						t.Fatal(err)
					}
					// Admission is detached before request; caller mutation is harmless.
					plan.SourceRights[0].FreeSource.Text = "mutated"
					plan.Bindings[0].DefinitionDigest = "mutated"
					plan.Requests[0].Params["mutated"] = true
					used[0] = "mutated"
				}
				query := colFrom("synthetic-rates").SelectColumns(dal.Column{Expression: dal.NewFieldRef("", "missing")})
				var reader dal.RecordsReader
				switch path {
				case "readonly":
					err = db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
						reader, err = tx.ExecuteQueryToRecordsReader(ctx, query)
						return err
					})
				case "readwrite":
					err = db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
						reader, err = tx.ExecuteQueryToRecordsReader(ctx, query)
						return err
					})
				default:
					reader, err = db.ExecuteQueryToRecordsReader(ctx, query)
				}
				if err != nil {
					t.Fatal(err)
				}
				if path != "optional" && sentID != strings.Repeat("c", 32) {
					t.Fatal("lost execution correlation")
				}
				metadata, ok := dal.ReadQueryMetadata(reader)
				if !ok || len(metadata.SourceRights) != 1 || len(metadata.UsedSourceIDs) != 1 || metadata.SourceRights[0].FreeSource.Text != "Synthetic free original" {
					t.Fatal("notices or usage missing before Next")
				}
				metadata.SourceRights[0].FreeSource.Text = "changed"
				metadata.SourceRights[0].Transformations[0] = "changed"
				again, _ := dal.ReadQueryMetadata(reader)
				if again.SourceRights[0].FreeSource.Text != "Synthetic free original" || again.SourceRights[0].Transformations[0] == "changed" {
					t.Fatal("mutable metadata")
				}
				evidence, ok := ReadProviderReads(reader)
				if !ok || evidence.Reads[0].ReferenceDate != "2099-04-02" {
					t.Fatal("live evidence missing")
				}
				evidence.Usage[0].ObservationIDs[0] = "changed"
				evidence.Bindings[0].ResourceID = "changed"
				fresh, _ := ReadProviderReads(reader)
				if fresh.Usage[0].ObservationIDs[0] == "changed" || fresh.Bindings[0].ResourceID == "changed" {
					t.Fatal("mutable evidence")
				}
				record, err := reader.Next()
				if empty {
					if !errors.Is(err, dal.ErrNoMoreRecords) {
						t.Fatal(err)
					}
				} else {
					if err != nil || len(record.Data().(map[string]any)) != 0 {
						t.Fatalf("projected result: %v %v", record, err)
					}
				}
				_ = reader.Close()
			})
		}
	}
}

func TestRequiredEvidenceRefusesBeforeReader(t *testing.T) {
	mutations := map[string]func(map[string]any){
		"missing": func(m map[string]any) { delete(m, "providerReads") },
		"null":    func(m map[string]any) { m["providerReads"] = nil },
		"changed execution": func(m map[string]any) {
			e := m["providerReads"].(providerreads.Envelope)
			e.Execution.ID = strings.Repeat("e", 32)
			m["providerReads"] = e
		},
		"changed executor": func(m map[string]any) {
			e := m["providerReads"].(providerreads.Envelope)
			e.Execution.ExecutorID = "other"
			m["providerReads"] = e
		},
		"changed definition": func(m map[string]any) {
			e := m["providerReads"].(providerreads.Envelope)
			e.Bindings[0].DefinitionDigest = strings.Repeat("e", 64)
			m["providerReads"] = e
		},
		"changed request": func(m map[string]any) {
			e := m["providerReads"].(providerreads.Envelope)
			e.Reads[0].UpstreamURL = "https://synthetic.example/other"
			m["providerReads"] = e
		},
		"missing usage": func(m map[string]any) {
			e := m["providerReads"].(providerreads.Envelope)
			e.Usage = []providerreads.Usage{}
			m["providerReads"] = e
		},
		"changed rights": func(m map[string]any) {
			r := m["sourceRights"].([]license.SourceRight)
			r[0].FreeSource.Text = "wrong"
			m["sourceRights"] = r
		},
		"missing rights": func(m map[string]any) { delete(m, "sourceRights") },
		"null rights":    func(m map[string]any) { m["sourceRights"] = nil },
		"missing used":   func(m map[string]any) { delete(m, "usedSourceIds") },
		"null used":      func(m map[string]any) { m["usedSourceIds"] = nil },
		"changed used":   func(m map[string]any) { m["usedSourceIds"] = []string{} },
		"bad records":    func(m map[string]any) { m["records"] = true },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			plan, used, response := syntheticProviderResponse(t)
			ctx, err := RequireProviderReads(context.Background(), plan, used)
			if err != nil {
				t.Fatal(err)
			}
			mutate(response)
			c := &httpClient{baseURL: "https://synthetic.example", databaseID: "synthetic-db", client: syntheticClient(encodeSynthetic(t, response), http.Header{"Cache-Control": {"no-store"}})}
			reader, err := c.executeQuery(ctx, colFrom("synthetic-rates").SelectIntoRecord(nil))
			if err == nil || reader != nil {
				t.Fatalf("invalid evidence exposed reader: %v %v", reader, err)
			}
		})
	}
}

func TestRequiredPlanAndTargetValidation(t *testing.T) {
	for _, name := range []string{"mode", "id", "empty rights", "rights digest", "nil used", "unknown used", "duplicate used", "invalid params", "invalid declaration"} {
		t.Run(name, func(t *testing.T) {
			plan, used, _ := syntheticProviderResponse(t)
			switch name {
			case "mode":
				plan.Execution.Mode = "direct"
			case "id":
				plan.Execution.ID = "bad"
			case "empty rights":
				plan.SourceRights = nil
			case "rights digest":
				plan.Bindings[0].RightsDigest = strings.Repeat("e", 64)
			case "nil used":
				used = nil
			case "unknown used":
				used = []string{"unknown"}
			case "duplicate used":
				used = append(used, used[0])
			case "invalid params":
				plan.Requests[0].Params["bad"] = func() {}
			case "invalid declaration":
				plan.SourceRights[0].Declaration.URL = "http://insecure.example/"
			}
			if _, err := RequireProviderReads(context.Background(), plan, used); err == nil {
				t.Fatal("invalid plan accepted")
			}
		})
	}
	plan, used, _ := syntheticProviderResponse(t)
	ctx, err := RequireProviderReads(context.Background(), plan, used)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	c := &httpClient{baseURL: "https://synthetic.example", databaseID: "wrong-db", client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { calls++; return nil, errors.New("must not send") })}}
	if reader, err := c.executeQuery(ctx, colFrom("synthetic-rates").SelectIntoRecord(nil)); err == nil || reader != nil || calls != 0 {
		t.Fatal("target mismatch performed HTTP")
	}
	if err := ctx.Value(providerReadContextKey{}).(*requiredProviderReads).validateTarget("synthetic-db", "synthetic-rates", "parent/p1"); err == nil {
		t.Fatal("parent query accepted")
	}
}

func TestRequiredTransportBounds(t *testing.T) {
	plan, used, response := syntheticProviderResponse(t)
	ctx, err := RequireProviderReads(context.Background(), plan, used)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"no-store missing", "no-store prefix", "oversized", "body error", "redirect", "error status", "malformed", "opening only", "wrong closing", "duplicate root", "extra value", "truncated field", "truncated object"} {
		t.Run(name, func(t *testing.T) {
			body := io.NopCloser(strings.NewReader(string(encodeSynthetic(t, response))))
			header := http.Header{"Cache-Control": {"no-store"}}
			status := 200
			switch name {
			case "no-store missing":
				header.Del("Cache-Control")
			case "no-store prefix":
				header.Set("Cache-Control", "no-store-pretend")
			case "oversized":
				body = io.NopCloser(strings.NewReader(strings.Repeat(" ", maxProviderQueryBytes+1)))
			case "body error":
				body = errReader{}
			case "redirect":
				status = 302
				header.Set("Location", "https://other.synthetic.example/")
			case "error status":
				status = 500
			case "wrong closing":
				body = io.NopCloser(strings.NewReader(`{"records":[]]`))
			case "opening only":
				body = io.NopCloser(strings.NewReader(`{`))
			case "malformed":
				body = io.NopCloser(strings.NewReader("[]"))
			case "duplicate root":
				body = io.NopCloser(strings.NewReader(`{"records":[],"records":[]}`))
			case "truncated field":
				body = io.NopCloser(strings.NewReader(`{"records":`))
			case "truncated object":
				body = io.NopCloser(strings.NewReader(`{"records":[]`))
			case "extra value":
				body = io.NopCloser(strings.NewReader(`{} {}`))
			}
			client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: status, Header: header, Body: body}, nil
			})}
			c := &httpClient{baseURL: "https://synthetic.example", databaseID: "synthetic-db", client: client}
			if reader, err := c.executeQuery(ctx, colFrom("synthetic-rates").SelectIntoRecord(nil)); err == nil || reader != nil {
				t.Fatalf("unsafe transport exposed reader: %v %v", reader, err)
			}
			if client.CheckRedirect != nil || client.Timeout != 0 {
				t.Fatal("mutated caller HTTP client")
			}
		})
	}
}

func TestMetadataOptionalAbsenceAndKnownEmpty(t *testing.T) {
	q := colFrom("synthetic-rates").SelectIntoRecord(nil)
	for _, raw := range []string{`{"records":[]}`, `{"records":[],"sourceRights":[],"usedSourceIds":[]}`} {
		reader, err := newQueryRecordsReader([]byte(raw), q)
		if err != nil {
			t.Fatal(err)
		}
		metadata, ok := dal.ReadQueryMetadata(reader)
		if ok != strings.Contains(raw, "sourceRights") {
			t.Fatal("omitted/known-empty distinction lost")
		}
		if ok && (!reflect.DeepEqual(metadata.UsedSourceIDs, []string{}) || len(metadata.SourceRights) != 0) {
			t.Fatal("known-empty lost")
		}
		if _, ok := ReadProviderReads(reader); ok {
			t.Fatal("invented provider evidence")
		}
	}
}

func TestMetadataWireDuplicatesAndMalformedRows(t *testing.T) {
	q := colFrom("synthetic-rates").SelectIntoRecord(nil)
	for _, raw := range []string{
		`{"records":true}`,
		`{"records":[],"sourceRights":[{"sourceId":"a","sourceId":"b"}]}`,
		`{"records":[],"sourceRights":[{"source":{"serverId":"a","serverId":"b"}}]}`,
		`{"records":[],"sourceRights":[{"source":{"serverId":"a"},"pins":[],"transformations":[]}]}`,
	} {
		reader, err := newQueryRecordsReader([]byte(raw), q)
		if strings.Contains(raw, `"serverId":"a"},"pins"`) {
			if err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err == nil || reader != nil {
			t.Fatalf("invalid wire returned reader: %v %v", reader, err)
		}
	}
}

func TestMetadataUnicodeEscapes(t *testing.T) {
	for _, raw := range []string{`"\ud800"`, `"\udc00"`, `"\ud800x"`, `"\ud800\u0041"`} {
		if validMetadataEscapes([]byte(raw)) {
			t.Fatalf("invalid surrogate accepted: %s", raw)
		}
	}
	for _, raw := range []string{`"plain"`, `"\ud83d\ude00"`, `"\\ud800"`, `"\u0041"`} {
		if !validMetadataEscapes([]byte(raw)) {
			t.Fatalf("valid Unicode rejected: %s", raw)
		}
	}
	reader, err := newQueryRecordsReader([]byte(`{"records":[],"usedSourceIds":["\ud800"]}`), colFrom("synthetic-rates").SelectIntoRecord(nil))
	if err == nil || reader != nil {
		t.Fatal("invalid wire metadata exposed reader")
	}
}
