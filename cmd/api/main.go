// Command api exposes POST /events. For each accepted event it:
//
//  1. validates the request body,
//  2. writes the event to ScyllaDB (the operational store), then
//  3. produces the event to a Redpanda topic (the analytics path).
//
// The Scylla write is synchronous and must succeed before we return 2xx — that
// is the durable operational record. The Redpanda produce is what feeds the
// decoupled analytics pipeline (consumer -> ClickHouse).
package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/saaicasm/grcs/internal/event"
)

// config holds the connection settings, all overridable via environment so the
// same binary works against docker-compose locally or real hosts elsewhere.
type config struct {
	httpAddr      string
	scyllaHosts   []string
	scyllaKeyspace string
	redpandaBroker string
	topic          string
}

func loadConfig() config {
	return config{
		httpAddr:       env("API_ADDR", ":8080"),
		scyllaHosts:    strings.Split(env("SCYLLA_HOSTS", "127.0.0.1:9042"), ","),
		scyllaKeyspace: env("SCYLLA_KEYSPACE", "grcs"),
		redpandaBroker: env("REDPANDA_BROKER", "127.0.0.1:9092"),
		topic:          env("EVENTS_TOPIC", "events"),
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// server bundles the long-lived clients the handler depends on.
type server struct {
	scylla   *gocql.Session
	producer *kgo.Client
	topic    string
}

func main() {
	cfg := loadConfig()

	scylla, err := newScylla(cfg)
	if err != nil {
		log.Fatalf("connect scylla: %v", err)
	}
	defer scylla.Close()

	producer, err := newProducer(cfg)
	if err != nil {
		log.Fatalf("connect redpanda: %v", err)
	}
	defer producer.Close()

	srv := &server{scylla: scylla, producer: producer, topic: cfg.topic}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", srv.handleEvents)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	httpSrv := &http.Server{
		Addr:         cfg.httpAddr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	log.Printf("api listening on %s (scylla=%v topic=%s)", cfg.httpAddr, cfg.scyllaHosts, cfg.topic)
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("http server: %v", err)
	}
}

// newScylla opens a session to the keyspace.
func newScylla(cfg config) (*gocql.Session, error) {
	cluster := gocql.NewCluster(cfg.scyllaHosts...)
	cluster.Keyspace = cfg.scyllaKeyspace
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 5 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	return cluster.CreateSession()
}

// newProducer opens a Redpanda/Kafka producer client.
func newProducer(cfg config) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(cfg.redpandaBroker),
		kgo.DefaultProduceTopic(cfg.topic),
		// Wait for the broker to ack the write so a 2xx means the event is on
		// the log, not just buffered locally.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5*time.Millisecond),
	)
}

const insertCQL = `INSERT INTO events (user_id, timestamp, id, type, source, payload)
                   VALUES (?, ?, ?, ?, ?, ?)`

// handleEvents validates the body, writes to Scylla, then produces to Redpanda.
func (s *server) handleEvents(w http.ResponseWriter, r *http.Request) {
	var ev event.Event
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&ev); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	ev.Normalize()
	if err := ev.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	payload, err := ev.PayloadJSON()
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid payload: "+err.Error())
		return
	}

	ctx := r.Context()

	// 1. Operational write. Must succeed before we acknowledge the event.
	if err := s.scylla.Query(insertCQL,
		ev.UserID, ev.Timestamp, ev.ID, ev.Type, ev.Source, payload,
	).WithContext(ctx).Exec(); err != nil {
		log.Printf("scylla write failed for event %s: %v", ev.ID, err)
		writeError(w, http.StatusServiceUnavailable, "failed to persist event")
		return
	}

	// 2. Analytics write. Produce to Redpanda. We surface a produce failure to
	// the caller here for demo clarity; a production system might instead ack
	// the event (Scylla already has it) and reconcile asynchronously.
	rec, err := ev.Marshal()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to encode event")
		return
	}
	res := s.producer.ProduceSync(ctx, &kgo.Record{
		Key:   []byte(ev.UserID), // key by user so a user's events keep order
		Value: rec,
	})
	if err := res.FirstErr(); err != nil {
		log.Printf("redpanda produce failed for event %s: %v", ev.ID, err)
		writeError(w, http.StatusServiceUnavailable, "failed to publish event")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"id": ev.ID, "status": "accepted"})
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
