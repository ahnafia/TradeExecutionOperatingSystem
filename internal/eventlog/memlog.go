package eventlog

import (
	"context"
	"sync"
	"time"
)

// MemLog is an in-process log with real offsets and real per-partition ordering.
//
// It is not a stub. Tests that run against it exercise the same consumer code, the same
// offset arithmetic, and the same idempotency paths as the deployed system — which is the
// point, because those are exactly the paths that only misbehave under duplication and
// restart. It can also be told to duplicate records, so "the relay published twice" is a
// scenario tests can produce on demand rather than wait for.
type MemLog struct {
	mu        sync.Mutex
	cond      *sync.Cond
	parts     map[string]int32              // topic → partition count
	records   map[string]map[int32][]Record // topic → partition → records, offset == index
	closed    bool
	duplicate func(Record) bool // chaos hook: return true to append the record twice
}

// NewMemLog creates a log with the given topics and partition counts.
func NewMemLog(topics map[string]int32) *MemLog {
	l := &MemLog{
		parts:   make(map[string]int32, len(topics)),
		records: make(map[string]map[int32][]Record, len(topics)),
	}
	l.cond = sync.NewCond(&l.mu)
	for t, n := range topics {
		if n < 1 {
			n = 1
		}
		l.parts[t] = n
		l.records[t] = make(map[int32][]Record, n)
	}
	return l
}

// DuplicateWhen installs a chaos hook. When it returns true for a record, that record is
// appended twice at consecutive offsets — which is exactly what an at-least-once relay
// does when it crashes between producing and marking the row published.
func (l *MemLog) DuplicateWhen(fn func(Record) bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.duplicate = fn
}

func (l *MemLog) Partitions(topic string) int32 { return l.parts[topic] }

func (l *MemLog) Produce(ctx context.Context, recs ...Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, r := range recs {
		n, ok := l.parts[r.Topic]
		if !ok {
			return ErrUnknownTopic{Topic: r.Topic}
		}
		p := PartitionFor(r.Key, n)
		l.append(r, p)
		if l.duplicate != nil && l.duplicate(r) {
			l.append(r, p)
		}
	}
	l.cond.Broadcast()
	return nil
}

// append writes one record at the next offset for its partition. Caller holds the lock.
func (l *MemLog) append(r Record, p int32) {
	r.Partition = p
	r.Offset = int64(len(l.records[r.Topic][p]))
	l.records[r.Topic][p] = append(l.records[r.Topic][p], r)
}

// Len reports how many records a partition holds, for tests and the drain loop.
func (l *MemLog) Len(topic string, partition int32) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return int64(len(l.records[topic][partition]))
}

func (l *MemLog) Reader(topic string, partition int32, fromOffset int64) (Reader, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.parts[topic]; !ok {
		return nil, ErrUnknownTopic{Topic: topic}
	}
	if fromOffset < 0 {
		fromOffset = 0
	}
	return &memReader{log: l, topic: topic, partition: partition, next: fromOffset}, nil
}

func (l *MemLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	l.cond.Broadcast()
	return nil
}

type memReader struct {
	log       *MemLog
	topic     string
	partition int32
	next      int64
	closed    bool
}

// Fetch returns whatever is available from the current position without blocking
// indefinitely.
//
// It returns an empty slice rather than blocking forever when the partition is caught up.
// A consumer loop that must distinguish "nothing yet" from "something is wrong" needs the
// first case to be ordinary and cheap, and a drain loop needs to be able to tell that the
// log is quiet.
func (r *memReader) Fetch(ctx context.Context, max int) ([]Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	r.log.mu.Lock()
	defer r.log.mu.Unlock()

	part := r.log.records[r.topic][r.partition]
	if int64(len(part)) <= r.next {
		return nil, nil
	}

	end := int64(len(part))
	if max > 0 && end-r.next > int64(max) {
		end = r.next + int64(max)
	}
	out := make([]Record, end-r.next)
	copy(out, part[r.next:end])
	r.next = end
	return out, nil
}

// SeekTo repositions the reader. Used on restart, where the durable offset is the truth
// and whatever the reader believed is not. Named SeekTo rather than Seek to stay clear of
// io.Seeker's signature, which means something different.
func (r *memReader) SeekTo(offset int64) {
	r.log.mu.Lock()
	defer r.log.mu.Unlock()
	r.next = offset
}

func (r *memReader) Close() error { r.closed = true; return nil }

// WaitQuiet blocks until every partition of every topic has stopped growing for the given
// settle period, or the context is cancelled. Test-support: it makes an asynchronous
// pipeline testable without sleeping arbitrarily and hoping.
func (l *MemLog) WaitQuiet(ctx context.Context, settle time.Duration) error {
	last := l.total()
	stable := time.Now()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		time.Sleep(2 * time.Millisecond)
		now := l.total()
		if now != last {
			last, stable = now, time.Now()
			continue
		}
		if time.Since(stable) >= settle {
			return nil
		}
	}
}

func (l *MemLog) total() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	var n int64
	for _, parts := range l.records {
		for _, recs := range parts {
			n += int64(len(recs))
		}
	}
	return n
}
