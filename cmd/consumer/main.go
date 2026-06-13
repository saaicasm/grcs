// Command consumer reads events off the Redpanda topic and batch-inserts them
// into ClickHouse (the analytics store).
//
// Two things matter here:
//
//   - Batching. ClickHouse is built for large, infrequent inserts, not one row
//     per request. We accumulate records and flush either when the batch is
//     full or a short timer fires.
//   - Delivery. We use a consumer group and only commit offsets AFTER a batch
//     is successfully written to ClickHouse. That gives at-least-once delivery:
//     a crash mid-batch replays those records rather than dropping them.
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/saaicasm/grcs/internal/event"
)

type config struct {
	redpandaBroker string
	topic          string
	group          string
	clickhouseAddr string
	clickhouseDB   string
	batchSize      int
	flushInterval  time.Duration
}

func loadConfig() config {
	return config{
		redpandaBroker: env("REDPANDA_BROKER", "127.0.0.1:9092"),
		topic:          env("EVENTS_TOPIC", "events"),
		group:          env("CONSUMER_GROUP", "grcs-clickhouse"),
		clickhouseAddr: env("CLICKHOUSE_ADDR", "127.0.0.1:9000"),
		clickhouseDB:   env("CLICKHOUSE_DB", "grcs"),
		batchSize:      envInt("BATCH_SIZE", 1000),
		flushInterval:  time.Duration(envInt("FLUSH_MS", 1000)) * time.Millisecond,
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func main() {
	cfg := loadConfig()

	ch, err := newClickHouse(cfg)
	if err != nil {
		log.Fatalf("connect clickhouse: %v", err)
	}
	defer ch.Close()

	// Manual offset commits: we commit only after a successful ClickHouse write.
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.redpandaBroker),
		kgo.ConsumerGroup(cfg.group),
		kgo.ConsumeTopics(cfg.topic),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(500*time.Millisecond),
	)
	if err != nil {
		log.Fatalf("connect redpanda: %v", err)
	}
	defer client.Close()

	// Cancel the run loop on SIGINT/SIGTERM for a clean shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("consumer started (topic=%s group=%s -> clickhouse %s/%s, batch=%d flush=%s)",
		cfg.topic, cfg.group, cfg.clickhouseAddr, cfg.clickhouseDB, cfg.batchSize, cfg.flushInterval)

	run(ctx, client, ch, cfg)
}

// newClickHouse opens a connection and verifies it with a ping.
func newClickHouse(cfg config) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: strings.Split(cfg.clickhouseAddr, ","),
		Auth: clickhouse.Auth{Database: cfg.clickhouseDB},
	})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		return nil, err
	}
	return conn, nil
}

// run is the poll/batch/flush loop. It flushes when the batch reaches batchSize
// or when flushInterval elapses with records buffered, whichever comes first.
func run(ctx context.Context, client *kgo.Client, ch driver.Conn, cfg config) {
	ticker := time.NewTicker(cfg.flushInterval)
	defer ticker.Stop()

	batch := make([]event.Event, 0, cfg.batchSize)

	// flush writes the buffered events to ClickHouse, then commits offsets so
	// those records are not redelivered. Order matters: insert first, commit
	// second (at-least-once).
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := insert(ctx, ch, cfg.clickhouseDB, batch); err != nil {
			// Keep the batch (and uncommitted offsets) so the next flush retries
			// these same events instead of dropping them.
			log.Printf("clickhouse insert failed (%d events), will retry next flush: %v", len(batch), err)
			return
		}
		if err := client.CommitUncommittedOffsets(ctx); err != nil {
			log.Printf("offset commit failed: %v", err)
		}
		log.Printf("flushed %d events to clickhouse", len(batch))
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			log.Println("shutdown: flushing remaining events")
			// Use a fresh context; ctx is already cancelled.
			flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if len(batch) > 0 {
				if err := insert(flushCtx, ch, cfg.clickhouseDB, batch); err != nil {
					log.Printf("final flush failed: %v", err)
				} else if err := client.CommitUncommittedOffsets(flushCtx); err != nil {
					log.Printf("final commit failed: %v", err)
				}
			}
			cancel()
			return

		case <-ticker.C:
			flush()

		default:
			fetches := client.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				for _, e := range errs {
					if errors.Is(e.Err, context.Canceled) {
						continue
					}
					log.Printf("fetch error (topic=%s partition=%d): %v", e.Topic, e.Partition, e.Err)
				}
			}

			fetches.EachRecord(func(rec *kgo.Record) {
				ev, err := event.Unmarshal(rec.Value)
				if err != nil {
					// Poison record: log and skip rather than stalling the group.
					log.Printf("skipping malformed record at offset %d: %v", rec.Offset, err)
					return
				}
				batch = append(batch, ev)
			})

			if len(batch) >= cfg.batchSize {
				flush()
			}
		}
	}
}

// insert batch-inserts events into ClickHouse using the native batch API.
func insert(ctx context.Context, ch driver.Conn, db string, events []event.Event) error {
	batch, err := ch.PrepareBatch(ctx, "INSERT INTO "+db+".events (id, user_id, type, source, timestamp, payload)")
	if err != nil {
		return err
	}
	for i := range events {
		ev := &events[i]
		payload, err := ev.PayloadJSON()
		if err != nil {
			return err
		}
		if err := batch.Append(ev.ID, ev.UserID, ev.Type, ev.Source, ev.Timestamp, payload); err != nil {
			return err
		}
	}
	return batch.Send()
}
