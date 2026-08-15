// Package ids derives every event identity in the system from the state that produced
// it, so that an event re-derived after a crash carries the identity it had before.
//
// This is the difference between the fill-recovery path being idempotent and being
// money-creating. If a matching shard died after generating a fill but before publishing
// it, the shard rebuilds its book from the log and generates that fill again. A random
// UUID would sail past the transactions.event_id unique constraint and apply the fill a
// second time. A UUIDv5 over (shard, symbol, book_seq, side) collides with itself by
// construction, and the constraint does its job.
package ids

import (
	"fmt"

	"github.com/google/uuid"
)

var (
	nsFill    = uuid.MustParse("6f1f0a1e-9c4b-5c7a-9f1d-2b7a3c4d5e60")
	nsEvent   = uuid.MustParse("6f1f0a1e-9c4b-5c7a-9f1d-2b7a3c4d5e61")
	nsLedger  = uuid.MustParse("6f1f0a1e-9c4b-5c7a-9f1d-2b7a3c4d5e62")
	nsRelease = uuid.MustParse("6f1f0a1e-9c4b-5c7a-9f1d-2b7a3c4d5e63")
)

// FillID names a fill. Both half-fills of one execution share it, which is what lets the
// reconciler pair them across partitions.
//
// The venue is part of the identity, not decoration. Once a symbol trades on more than one
// venue, each venue's book keeps its own book_seq, so (shard, symbol, seq) stops being
// unique — two venues would mint the same identity for two different executions, and the
// ledger's dedup would silently discard one of them as a duplicate. Adding the venue is
// what keeps a derived identity derivable.
func FillID(shardID int, venue, symbol string, bookSeq uint64) uuid.UUID {
	return uuid.NewSHA1(nsFill, []byte(fmt.Sprintf("%d|%s|%s|%d", shardID, venue, symbol, bookSeq)))
}

// FillEventID names one half of a fill. Distinct per side so the two halves are separate
// idempotency keys applied in separate partitions.
func FillEventID(shardID int, venue, symbol string, bookSeq uint64, side string) uuid.UUID {
	return uuid.NewSHA1(nsEvent, []byte(fmt.Sprintf("%d|%s|%s|%d|%s", shardID, venue, symbol, bookSeq, side)))
}

// LedgerEventID names a transaction whose identity comes from the account's own clock:
// deposits and seeding, where there is no book_seq to derive from.
func LedgerEventID(accountID uuid.UUID, accountSeq int64) uuid.UUID {
	return uuid.NewSHA1(nsLedger, []byte(fmt.Sprintf("%s|%d", accountID, accountSeq)))
}

// ReleaseEventID names the reservation release that closes out an order.
func ReleaseEventID(orderID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(nsRelease, []byte(orderID.String()))
}
