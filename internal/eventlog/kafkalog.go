package eventlog

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaLog is the deployed transport. Redpanda locally, Kafka anywhere.
//
// Two choices here differ from what a client library would do by default, and both are
// load-bearing:
//
//  1. Partitions are assigned by PartitionFor, not by the producer's partitioner. The
//     matching engine derives which partitions it owns from the same function, so the two
//     cannot drift apart the way they would if one used a library default.
//
//  2. Consumer group offset management is not used at all. Offsets live in Postgres and
//     advance in the same transaction as the state change they authorize. Letting Kafka
//     track them would reintroduce exactly the gap between "processed" and "recorded as
//     processed" that the design exists to close.
type KafkaLog struct {
	client *kgo.Client
	seeds  []string
	parts  map[string]int32

	mu      sync.Mutex
	readers []*kafkaReader
}

// NewKafkaLog connects, and creates any topic that does not exist yet with the requested
// partition count.
//
// Partition counts are set once, high, at creation. Kafka partitions are cheap; changing
// the count of a live keyed topic is not, because it moves keys to different partitions
// and breaks the per-key ordering everything downstream assumes.
func NewKafkaLog(ctx context.Context, seeds []string, topics map[string]int32) (*KafkaLog, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ProducerBatchMaxBytes(16<<20),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("kafka unreachable at %v: %w", seeds, err)
	}

	admin := kadm.NewClient(client)
	for topic, n := range topics {
		resp, err := admin.CreateTopic(ctx, int32(n), -1, nil, topic)
		if err != nil && !isTopicExists(err) {
			client.Close()
			return nil, fmt.Errorf("create topic %s: %w", topic, err)
		}
		if resp.Err != nil && !isTopicExists(resp.Err) {
			client.Close()
			return nil, fmt.Errorf("create topic %s: %w", topic, resp.Err)
		}
	}

	return &KafkaLog{client: client, seeds: seeds, parts: topics}, nil
}

func isTopicExists(err error) bool {
	return err != nil && (kerr.TopicAlreadyExists.Code == errCode(err))
}

func errCode(err error) int16 {
	var kerrErr *kerr.Error
	if ok := asKerr(err, &kerrErr); ok {
		return kerrErr.Code
	}
	return -1
}

func asKerr(err error, target **kerr.Error) bool {
	for err != nil {
		if e, ok := err.(*kerr.Error); ok {
			*target = e
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func (l *KafkaLog) Partitions(topic string) int32 {
	if n, ok := l.parts[topic]; ok {
		return n
	}
	return 1
}

func (l *KafkaLog) Produce(ctx context.Context, recs ...Record) error {
	krecs := make([]*kgo.Record, 0, len(recs))
	for _, r := range recs {
		n, ok := l.parts[r.Topic]
		if !ok {
			return ErrUnknownTopic{Topic: r.Topic}
		}
		krecs = append(krecs, &kgo.Record{
			Topic:     r.Topic,
			Partition: PartitionFor(r.Key, n),
			Key:       []byte(r.Key),
			Value:     r.Value,
		})
	}
	// Synchronous produce: the relay marks a row published only after the broker has it,
	// so a crash here leaves the row unpublished and it is retried. Fire-and-forget would
	// let the relay mark rows published that never landed, turning at-least-once into
	// at-most-once and losing orders outright.
	return l.client.ProduceSync(ctx, krecs...).FirstErr()
}

func (l *KafkaLog) Reader(topic string, partition int32, fromOffset int64) (Reader, error) {
	if _, ok := l.parts[topic]; !ok {
		return nil, ErrUnknownTopic{Topic: topic}
	}
	if fromOffset < 0 {
		fromOffset = 0
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(l.seeds...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {partition: kgo.NewOffset().At(fromOffset)},
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka reader: %w", err)
	}

	r := &kafkaReader{client: client, topic: topic, partition: partition}
	l.mu.Lock()
	l.readers = append(l.readers, r)
	l.mu.Unlock()
	return r, nil
}

func (l *KafkaLog) Close() error {
	l.mu.Lock()
	readers := l.readers
	l.readers = nil
	l.mu.Unlock()
	for _, r := range readers {
		_ = r.Close()
	}
	l.client.Close()
	return nil
}

type kafkaReader struct {
	client    *kgo.Client
	topic     string
	partition int32
}

// pollWindow bounds how long a Fetch waits before reporting an empty batch. Short enough
// that a consumer loop notices shutdown promptly; long enough that an idle consumer is
// not spinning.
const pollWindow = 250 * time.Millisecond

func (r *kafkaReader) Fetch(ctx context.Context, max int) ([]Record, error) {
	ctx, cancel := context.WithTimeout(ctx, pollWindow)
	defer cancel()

	fetches := r.client.PollRecords(ctx, max)
	if err := fetches.Err0(); err != nil && ctx.Err() == nil {
		return nil, fmt.Errorf("fetch %s/%d: %w", r.topic, r.partition, err)
	}

	var out []Record
	fetches.EachRecord(func(kr *kgo.Record) {
		out = append(out, Record{
			Topic:     kr.Topic,
			Partition: kr.Partition,
			Offset:    kr.Offset,
			Key:       string(kr.Key),
			Value:     kr.Value,
		})
	})
	return out, nil
}

func (r *kafkaReader) Close() error {
	r.client.Close()
	return nil
}
