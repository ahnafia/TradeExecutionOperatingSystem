// Package events defines the wire contract between the trading core and the matching
// engine.
//
// # On the encoding
//
// ARCHITECTURE.md §15 specifies protobuf, with the schemas doubling as Kafka envelopes.
// That is still the intent. This uses versioned JSON instead, for one reason: protoc is
// not available in this environment, and hand-maintaining generated-looking code is worse
// than an honest substitution. The properties the design actually depends on are preserved
// and are the ones enforced here:
//
//   - Every envelope carries SchemaVersion, so a consumer can refuse what it cannot read
//     rather than misread it.
//   - Unknown fields are ignored and missing fields take zero values, which is proto3's
//     evolution model — a field added in month three reads as a default when replaying
//     month-one events.
//   - Field names are part of the contract and are never reused for a different meaning.
//
// The migration is mechanical: these structs map one-to-one onto messages. What would
// change is the codec, not the shape.
//
// # On ordering
//
// The partition key of each topic is a design decision, not a detail. Inbound topics are
// keyed by SYMBOL, so one book's input is totally ordered and price-time priority is
// well defined. The outbound topic is keyed by ACCOUNT, so one account's effects arrive
// in the order the book decided them. Nothing is ordered across keys, and nothing needs
// to be.
package events

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// SchemaVersion is the current envelope version. Bump it only for a change a reader of
// the previous version could not safely interpret.
const SchemaVersion = 1

// Envelope wraps every record on every topic.
type Envelope struct {
	SchemaVersion int             `json:"schema_version"`
	Type          string          `json:"type"`
	Payload       json.RawMessage `json:"payload"`
}

// Message types.
const (
	TypeOrderAccepted   = "ORDER_ACCEPTED"
	TypeCancelRequested = "CANCEL_REQUESTED"
	TypeFillHalf        = "FILL_HALF"
	TypeDisposition     = "ORDER_DISPOSITION"
	TypeCancelOutcome   = "CANCEL_OUTCOME"
)

// OrderAccepted is an order that survived risk and reservation, on its way to a book.
//
// RefPrice travels with the order rather than being looked up by the matching engine.
// That is what keeps the book free of external dependencies and therefore deterministic
// on replay: the collar it enforces is the collar the reservation was sized against, even
// if the market has moved since, and even if replay happens a week later.
type OrderAccepted struct {
	OrderID    uuid.UUID `json:"order_id"`
	AccountID  uuid.UUID `json:"account_id"`
	Symbol     string    `json:"symbol"`
	Side       string    `json:"side"` // BUY | SELL
	Type       string    `json:"type"` // MARKET | LIMIT
	TIF        string    `json:"tif"`  // IOC | GTC
	Qty        int64     `json:"qty"`
	LimitPrice int64     `json:"limit_price,omitempty"`
	RefPrice   int64     `json:"ref_price,omitempty"`

	// Venue is where the remainder should rest. Empty means the primary venue.
	Venue string `json:"venue,omitempty"`
}

// CancelRequested asks the book to withdraw a resting order.
type CancelRequested struct {
	OrderID   uuid.UUID `json:"order_id"`
	AccountID uuid.UUID `json:"account_id"`
	Symbol    string    `json:"symbol"`
}

// FillHalf is one side of one execution, addressed to one account.
//
// A fill produces exactly two of these, sharing FillID and differing in Side. They are
// separate messages because they are settled by separate transactions in separate
// partitions — the bilateral fill decomposed into two unilateral ones (§3.2).
//
// EventID is derived from (shard, symbol, book_seq, side) and is therefore stable across
// a rebuild. It is the idempotency key the core's unique constraint enforces, which is
// what makes a fill re-derived after a crash a no-op instead of money.
type FillHalf struct {
	EventID   uuid.UUID `json:"event_id"`
	FillID    uuid.UUID `json:"fill_id"`
	ShardID   int       `json:"shard_id"`
	Symbol    string    `json:"symbol"`
	Venue     string    `json:"venue"`
	BookSeq   uint64    `json:"book_seq"`
	Side      string    `json:"side"` // TAKER | MAKER
	AccountID uuid.UUID `json:"account_id"`
	OrderID   uuid.UUID `json:"order_id"`
	Price     int64     `json:"price"`
	Qty       int64     `json:"qty"`
	Buying    bool      `json:"buying"` // whether THIS side acquired shares

	TakerOrderID uuid.UUID `json:"taker_order_id"`
	MakerOrderID uuid.UUID `json:"maker_order_id"`
}

// Disposition reports what the book did with an incoming order's unfilled remainder.
//
// It is a separate message from the fills rather than a field on the last one because the
// book may produce no fills at all, and because the core must be able to apply fills
// without knowing whether more are coming.
type Disposition struct {
	OrderID     uuid.UUID `json:"order_id"`
	AccountID   uuid.UUID `json:"account_id"`
	Symbol      string    `json:"symbol"`
	Disposition string    `json:"disposition"` // RESTED | CANCELLED | COMPLETE
	Remaining   int64     `json:"remaining"`
	Reason      string    `json:"reason,omitempty"` // COLLAR_BREACH, BOOK_EXHAUSTED, ...
}

// CancelOutcome is the book's verdict on a cancel request.
//
// A verdict of Cancelled=false is not an error: it means the fill won the race, which is
// an ordinary outcome the client is entitled to be told about precisely.
type CancelOutcome struct {
	OrderID   uuid.UUID `json:"order_id"`
	AccountID uuid.UUID `json:"account_id"`
	Symbol    string    `json:"symbol"`
	Cancelled bool      `json:"cancelled"`
	Remaining int64     `json:"remaining"`
	Reason    string    `json:"reason"`
}

// Encode wraps a message in a versioned envelope.
func Encode(msgType string, payload any) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode %s: %w", msgType, err)
	}
	return json.Marshal(Envelope{SchemaVersion: SchemaVersion, Type: msgType, Payload: raw})
}

// Decode unwraps an envelope, refusing versions this build cannot interpret.
//
// Refusing loudly matters more than it looks: a consumer that silently ignored an
// unreadable record would advance its offset past an order or a fill, and the loss would
// surface later as an invariant violation with no trace of the cause.
func Decode(b []byte) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(b, &env); err != nil {
		return env, fmt.Errorf("decode envelope: %w", err)
	}
	if env.SchemaVersion > SchemaVersion {
		return env, fmt.Errorf("record uses schema version %d, this build understands %d",
			env.SchemaVersion, SchemaVersion)
	}
	return env, nil
}

// Into unmarshals an envelope's payload.
func Into[T any](env Envelope) (T, error) {
	var out T
	if err := json.Unmarshal(env.Payload, &out); err != nil {
		return out, fmt.Errorf("decode %s payload: %w", env.Type, err)
	}
	return out, nil
}
