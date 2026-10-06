package dalgo2openvaultdb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"unicode/utf8"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/datarights"
	"github.com/openvaultdb/openvaultdb-go/pkg/providerreads"
)

type providerReadContextKey struct{}
type requiredProviderReads struct {
	plan providerreads.Plan
	used []string
}

// RequireProviderReads freezes independently admitted evidence for one query
// execution. Never construct plan or usedSourceIDs from the query response.
// The returned context requires proxy evidence and no-store before any reader
// is returned. Give each execution a fresh 32-character lowercase hex ID.
// This capability does not grant source rights or activate a live source.
func RequireProviderReads(ctx context.Context, plan providerreads.Plan, usedSourceIDs []string) (context.Context, error) {
	raw, err := providerreads.Canonical(plan)
	if err != nil {
		return nil, fmt.Errorf("freeze provider plan: %w", err)
	}
	var frozen providerreads.Plan
	if err := json.Unmarshal(raw, &frozen); err != nil {
		return nil, fmt.Errorf("freeze provider plan: %w", err)
	}
	if frozen.Execution.Mode != "proxy" || !regexp.MustCompile(`^[a-f0-9]{32}$`).MatchString(frozen.Execution.ID) || len(frozen.SourceRights) == 0 {
		return nil, fmt.Errorf("provider reads require a proxy source plan and 32 lowercase hex execution ID")
	}
	// An empty synthetic observation inventory validates the admitted plan alone;
	// actual usage is validated separately against the response before rows.
	empty := providerreads.Envelope{Format: providerreads.Format, Execution: frozen.Execution, Bindings: frozen.Bindings, Reads: []providerreads.Observation{}, Usage: []providerreads.Usage{}}
	if err := providerreads.Validate(empty, frozen, []string{}); err != nil {
		return nil, fmt.Errorf("invalid provider plan: %w", err)
	}
	if usedSourceIDs == nil {
		return nil, fmt.Errorf("admitted used source IDs are required")
	}
	seen := make(map[string]bool)
	for _, id := range usedSourceIDs {
		known := false
		for _, right := range frozen.SourceRights {
			known = known || right.SourceID == id
		}
		if !known || seen[id] {
			return nil, fmt.Errorf("invalid admitted used source IDs")
		}
		seen[id] = true
	}
	used := append([]string{}, usedSourceIDs...)
	return context.WithValue(ctx, providerReadContextKey{}, &requiredProviderReads{plan: frozen, used: used}), nil
}

func (r *requiredProviderReads) validateTarget(databaseID, collection, parent string) error {
	if parent != "" {
		return fmt.Errorf("provider plan cannot authorize a parent-scoped query")
	}
	for _, right := range r.plan.SourceRights {
		if right.Source.ServerID != r.plan.Execution.ExecutorID || right.Source.DatabaseID != databaseID || right.Source.Recordset != collection {
			return fmt.Errorf("provider plan does not match query executor/database/collection")
		}
	}
	return nil
}

// ReadProviderReads returns detached executor-observed evidence when supplied.
// Without RequireProviderReads, this is a decoded provider claim, not evidence
// checked against independent admission. DALgo's generic metadata carries rights;
// this adapter-local capability carries the additional live-read envelope.
func ReadProviderReads(reader dal.Reader) (providerreads.Envelope, bool) {
	if r, ok := reader.(interface {
		ProviderReads() (providerreads.Envelope, bool)
	}); ok {
		return r.ProviderReads()
	}
	return providerreads.Envelope{}, false
}

type queryMetadataReader struct {
	dal.RecordsReader
	metadata datarights.QueryMetadata
	evidence json.RawMessage
}

func (r *queryMetadataReader) QueryMetadata() datarights.QueryMetadata { return r.metadata.Clone() }
func (r *queryMetadataReader) ProviderReads() (providerreads.Envelope, bool) {
	if len(r.evidence) == 0 {
		return providerreads.Envelope{}, false
	}
	e, _ := providerreads.Decode(r.evidence) // Decoded successfully before reader construction.
	return e, true
}

// decodeQueryFields does not parse records until the metadata gate has passed.
func decodeQueryFields(body []byte) (map[string]json.RawMessage, error) {
	d := json.NewDecoder(bytes.NewReader(body))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return nil, fmt.Errorf("query response must be an object")
	}
	fields := make(map[string]json.RawMessage)
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return nil, err
		}
		name := key.(string)
		if _, exists := fields[name]; exists {
			return nil, fmt.Errorf("duplicate query response field")
		}
		var raw json.RawMessage
		if err := d.Decode(&raw); err != nil {
			return nil, err
		}
		fields[name] = raw
	}
	if _, err := d.Token(); err != nil {
		return nil, err
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, fmt.Errorf("extra query response value")
	}
	return fields, nil
}

func queryResponseMetadata(fields map[string]json.RawMessage, required *requiredProviderReads) (datarights.QueryMetadata, json.RawMessage, error) {
	// Keep the existing omitted/known-empty contract and validate null arrays.
	// Omitted fields must remain omitted, rather than being encoded as null.
	metadataFields := make(map[string]json.RawMessage)
	for _, name := range []string{"sourceRights", "usedSourceIds"} {
		if value, ok := fields[name]; ok {
			if !utf8.Valid(value) || !validMetadataEscapes(value) || hasDuplicateMetadataKeys(value) {
				return datarights.QueryMetadata{}, nil, fmt.Errorf("invalid or duplicate metadata field")
			}
			metadataFields[name] = value
		}
	}
	raw, _ := json.Marshal(metadataFields)
	var metadata datarights.QueryMetadata
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return metadata, nil, fmt.Errorf("query metadata: %w", err)
	}
	evidence := fields["providerReads"]
	var envelope *providerreads.Envelope
	if len(evidence) != 0 {
		e, err := providerreads.Decode(evidence)
		if err != nil {
			return metadata, nil, fmt.Errorf("provider reads: %w", err)
		}
		envelope = &e
	}
	if required != nil {
		// Compare the full wire rights, including unknown fields, to admission;
		// decoding to the narrower DALgo carrier alone could discard information.
		var responseRights any
		if err := json.Unmarshal(fields["sourceRights"], &responseRights); err != nil {
			return metadata, nil, fmt.Errorf("required source rights: %w", err)
		}
		// The decoder has produced finite JSON values with valid UTF-8 strings.
		actual, _ := providerreads.Canonical(responseRights)
		expected, _ := providerreads.Canonical(required.plan.SourceRights)
		if !bytes.Equal(actual, expected) {
			return metadata, nil, fmt.Errorf("response rights preflight mismatch")
		}
		m := providerreads.Metadata{SourceRights: required.plan.SourceRights, UsedSourceIDs: metadata.UsedSourceIDs, ProviderReads: envelope}
		if err := providerreads.ValidateMetadata(m, required.plan, required.used); err != nil {
			return metadata, nil, fmt.Errorf("provider reads: %w", err)
		}
	}
	return metadata, evidence, nil
}

// RawMessages are already syntactically valid. Refuse lone escaped surrogates
// which encoding/json would otherwise replace with a different notice string.
func validMetadataEscapes(raw []byte) bool {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		i++
		if raw[i] != 'u' {
			continue
		}
		n, _ := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		i += 4
		if n >= 0xdc00 && n <= 0xdfff {
			return false
		}
		if n >= 0xd800 && n <= 0xdbff {
			if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
				return false
			}
			low, _ := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
			if low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 6
		}
	}
	return true
}

// Values are valid JSON RawMessages. Track object keys without decoding rows
// or interpreting terms; duplicate metadata must never be normalized away.
func hasDuplicateMetadataKeys(raw []byte) bool {
	d := json.NewDecoder(bytes.NewReader(raw))
	var visit func() bool
	visit = func() bool {
		token, _ := d.Token()
		switch token {
		case json.Delim('{'):
			seen := map[string]bool{}
			for d.More() {
				key, _ := d.Token()
				name := key.(string)
				if seen[name] {
					return true
				}
				seen[name] = true
				if visit() {
					return true
				}
			}
			_, _ = d.Token()
		case json.Delim('['):
			for d.More() {
				if visit() {
					return true
				}
			}
			_, _ = d.Token()
		}
		return false
	}
	return visit()
}
