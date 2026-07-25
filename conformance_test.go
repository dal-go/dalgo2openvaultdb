package dalgo2openvaultdb_test

import (
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dalgotest"
)

// TestConformance runs the shared dalgotest suite against an in-process fake
// OpenVaultDB HTTP server — the same httptest harness (newFakeStore,
// newRecordServer, mustNewDB) every other test in this file uses — so it
// needs no live OpenVaultDB server and no env-gate.
//
// dalgo2openvaultdb validates nothing itself today. Every check here passes
// because dal.NewDB's write pipeline runs BeforeSave validation and hooks
// before this adapter's code is ever entered — see db.go's NewDB.
func TestConformance(t *testing.T) {
	dalgotest.RunConformance(t, func(t *testing.T) (dal.DB, func()) {
		store := newFakeStore()
		srv := newRecordServer(t, store)
		db := mustNewDB(t, srv)
		return db, srv.Close
	})
}
