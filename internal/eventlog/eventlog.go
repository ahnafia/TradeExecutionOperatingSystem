// Package eventlog is the ordered, partitioned, replayable log that sits between the
// trading core and the matching engine.
//
// There are two implementations and they are held to the same semantics on purpose:
//
//	MemLog   — in process, for tests and the single-binary demo.
//	KafkaLog — a real broker, for the deployed system.
//
// The temptation in Phase 3 is to keep an in-process "direct" path that skips the log
// entirely and only use Kafka in production. That would be a second code path through the
// most delicate part of the system — the part where duplicates, crashes, and reordering
// live — and it would be the path the tests exercise. So MemLog is not a bypass: it
// assigns real offsets, preserves per-partition order, and can be told to duplicate
// records. Everything above it consumes a log and cannot tell which one.
//
// What both guarantee, and what the rest of the design leans on:
//
//   - Records within one partition are delivered in the order they were produced.
//   - Nothing is guaranteed ACROSS partitions, and nothing needs it.
//   - Delivery is AT LEAST ONCE. Consumers make the effect exactly once by advancing
//     their offset in the same database transaction as the state change.
package eventlog

import (
	"context"
	"fmt"
)

// Topics used by the system. Partition keys are chosen so that ordering falls where
// correctness needs it: by symbol before the book, by account after it.
const (
	// TopicOrdersInbound carries everything a book consumes — accepted orders AND cancel
	// requests — keyed by symbol.
	//
	// ARCHITECTURE.md §7 lists these as two topics, orders.accepted and
	// orders.cancel_requested. They are merged here because two topics cannot give a book
	// one totally ordered input, and §5.4's determinism claim requires exactly that. With
	// separate topics, the interleaving of an order and a cancel is whatever the two
	// consumers happened to produce; on replay it can differ, the book linearizes them
	// the other way, and a rebuild yields different executions than the ones clients were
	// told about. One topic, one order, one outcome. See §0.7.8.
	TopicOrdersInbound = "orders.inbound"

	// TopicOrdersOutcomes carries everything the book decides back to the core, keyed by
	// account_id so one account's effects arrive in the order the book produced them.
	TopicOrdersOutcomes = "orders.outcomes"
)

// Record is one entry in the log.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64 // assigned by the log on produce; meaningless on input
	Key       string
	Value     []byte
}

// Log is an ordered partitioned record log.
type Log interface {
	// Produce appends records, assigning each a partition (from its key) and an offset.
	Produce(ctx context.Context, recs ...Record) error
	// Reader returns a reader over one partition, positioned at fromOffset.
	Reader(topic string, partition int32, fromOffset int64) (Reader, error)
	// Partitions reports how many partitions a topic has.
	Partitions(topic string) int32
	Close() error
}

// Reader consumes one partition of one topic.
type Reader interface {
	// Fetch returns up to max records from the current position, advancing it. It blocks
	// until at least one record is available, the context is cancelled, or the
	// implementation's poll interval elapses — in which case it returns an empty slice
	// rather than an error, so callers can loop without special-casing idleness.
	Fetch(ctx context.Context, max int) ([]Record, error)
	Close() error
}

// PartitionFor maps a key to a partition.
//
// The function is written out here rather than left to a client library's default because
// two different processes must agree on it: the core decides which partition an order
// goes to, and a matching shard decides which partitions it owns. If those disagree, a
// shard silently consumes orders for books it does not have, and the failure looks like
// missing liquidity rather than a routing bug.
//
// This is Kafka's murmur2 partitioner, reimplemented so that MemLog and a real broker
// place the same key in the same partition.
func PartitionFor(key string, partitions int32) int32 {
	if partitions <= 1 {
		return 0
	}
	h := murmur2([]byte(key)) & 0x7fffffff
	return int32(h % uint32(partitions))
}

// murmur2 is the hash Kafka's default partitioner uses.
func murmur2(data []byte) uint32 {
	const (
		seed = uint32(0x9747b28c)
		m    = uint32(0x5bd1e995)
		r    = 24
	)
	length := len(data)
	h := seed ^ uint32(length)

	for i := 0; i+4 <= length; i += 4 {
		k := uint32(data[i]) | uint32(data[i+1])<<8 | uint32(data[i+2])<<16 | uint32(data[i+3])<<24
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
	}

	switch length & 3 {
	case 3:
		h ^= uint32(data[(length & ^3)+2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[(length & ^3)+1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[length & ^3])
		h *= m
	}

	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return h
}

// ErrUnknownTopic is returned for a topic the log was not configured with.
type ErrUnknownTopic struct{ Topic string }

func (e ErrUnknownTopic) Error() string { return fmt.Sprintf("unknown topic %q", e.Topic) }

// Topics returns the topic set with the partition counts a deployment should create.
//
// Partition counts are the ceiling on worker count and are set once, high, at creation.
// Kafka partitions are cheap; repartitioning a live keyed topic is not, because it moves
// keys to different partitions and breaks the per-key ordering everything downstream
// assumes. inbound partitions bound matching shards; outcome partitions bound core
// partitions.
func Topics(inbound, outcomes int32) map[string]int32 {
	if inbound < 1 {
		inbound = 1
	}
	if outcomes < 1 {
		outcomes = 1
	}
	return map[string]int32{
		TopicOrdersInbound:  inbound,
		TopicOrdersOutcomes: outcomes,
	}
}
