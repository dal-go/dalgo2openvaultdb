package dalgo2openvaultdb

import (
	"encoding/json"
	"fmt"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/update"
)

// wireUpdate is the JSON representation of a single field update sent to
// OpenVaultDB. Only one of FieldName/FieldPath is set; exactly one of
// Value/Delete/ServerTimestamp/Transform is set.
type wireUpdate struct {
	FieldName       string          `json:"fieldName,omitempty"`
	FieldPath       []string        `json:"fieldPath,omitempty"`
	Value           json.RawMessage `json:"value,omitempty"`
	Delete          bool            `json:"delete,omitempty"`
	ServerTimestamp bool            `json:"serverTimestamp,omitempty"`
	Transform       string          `json:"transform,omitempty"`
}

// marshalUpdates converts []update.Update to a JSON byte slice suitable for
// the "updates" field in a PATCH or batch update op.
// Returns an ErrNotSupported-wrapped error if any precondition is given.
func marshalUpdates(updates []update.Update, preconditions []dal.Precondition) ([]byte, error) {
	if len(preconditions) > 0 {
		return nil, fmt.Errorf("%w: update preconditions", dal.ErrNotSupported)
	}

	wire := make([]wireUpdate, 0, len(updates))
	for _, u := range updates {
		wu, err := convertUpdate(u)
		if err != nil {
			return nil, err
		}
		wire = append(wire, wu)
	}
	return json.Marshal(wire)
}

func convertUpdate(u update.Update) (wireUpdate, error) {
	wu := wireUpdate{}

	// Populate field name or path.
	if fp := u.FieldPath(); len(fp) > 0 {
		wu.FieldPath = []string(fp)
	} else {
		wu.FieldName = u.FieldName()
	}

	v := u.Value()

	// Check for sentinel values first.
	switch v {
	case update.DeleteField:
		wu.Delete = true
		return wu, nil
	case update.ServerTimestamp:
		wu.ServerTimestamp = true
		return wu, nil
	}

	// Check for a Transform.
	if t, ok := dal.IsTransform(v); ok {
		if t.Name() == "increment" {
			wu.Transform = "increment"
			raw, err := json.Marshal(t.Value())
			if err != nil {
				return wireUpdate{}, fmt.Errorf("marshal increment value: %w", err)
			}
			wu.Value = raw
			return wu, nil
		}
		return wireUpdate{}, fmt.Errorf("unsupported transform: %s", t.Name())
	}

	// Plain value.
	raw, err := json.Marshal(v)
	if err != nil {
		return wireUpdate{}, fmt.Errorf("marshal update value for field %q: %w", wu.FieldName, err)
	}
	wu.Value = raw
	return wu, nil
}
