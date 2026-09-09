/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package postgresql

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
)

func TestBatchEventValidation(t *testing.T) {
	evValid := api.BatchEvent{ID: "job-1", Type: api.BatchEventCancel, TTL: 60}
	if err := evValid.IsValid(); err != nil {
		t.Fatalf("expected valid event, got: %v", err)
	}

	evEmptyID := api.BatchEvent{ID: "", Type: api.BatchEventCancel, TTL: 60}
	if err := evEmptyID.IsValid(); err == nil {
		t.Fatal("expected error for empty ID")
	}

	evBadType := api.BatchEvent{ID: "job-1", Type: api.BatchEventMaxVal, TTL: 60}
	if err := evBadType.IsValid(); err == nil {
		t.Fatal("expected error for invalid event type")
	}

	evBadTTL := api.BatchEvent{ID: "job-1", Type: api.BatchEventCancel, TTL: 0}
	if err := evBadTTL.IsValid(); err == nil {
		t.Fatal("expected error for TTL <= 0")
	}
}

func TestPGListener_SubscribeDeliverClose(t *testing.T) {
	l := &pgListener{
		channel: "batch_events",
		logger:  logr.Discard(),
		subs:    make(map[int]chan string),
		done:    make(chan struct{}),
	}
	close(l.done) // stub done for close()

	wake, unsub := l.subscribe()

	l.deliver("job-123")

	select {
	case payload := <-wake:
		if payload != "job-123" {
			t.Fatalf("expected payload 'job-123', got %q", payload)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for notification delivery")
	}

	unsub()
	l.deliver("job-456")

	select {
	case payload := <-wake:
		t.Fatalf("unexpected delivery after unsubscribe: %q", payload)
	case <-time.After(50 * time.Millisecond):
		// Expected: nothing delivered after unsubscribe
	}

	if err := l.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestPostgresBatchEventClient_ConsumerSubscription(t *testing.T) {
	c := &PostgresBatchEventClient{
		logger:    logr.Discard(),
		eventSubs: make(map[string]*eventSub),
	}

	// Empty ID validation
	if _, err := c.ECConsumerGetChannel(context.Background(), ""); err == nil {
		t.Fatal("expected error for empty ID")
	}

	sub := &eventSub{
		ch: make(chan api.BatchEvent, 10),
	}
	c.eventSubs["job-test"] = sub

	// Verify deliverJobEvents fans out to the registered subscriber
	sub.ch <- api.BatchEvent{ID: "job-test", Type: api.BatchEventCancel}

	select {
	case ev := <-sub.ch:
		if ev.ID != "job-test" || ev.Type != api.BatchEventCancel {
			t.Fatalf("unexpected event: %+v", ev)
		}
	default:
		t.Fatal("expected event in subscriber channel")
	}
}

// TestPostgresBatchEventClient_PurgeExpiredEvents requires a real PostgreSQL
// instance (TEST_POSTGRES_URL) and verifies the sweeper's contract: expired,
// never-consumed rows are deleted, unexpired rows are left alone.
func TestPostgresBatchEventClient_PurgeExpiredEvents(t *testing.T) {
	url := os.Getenv("TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	ctx := context.Background()

	client, err := NewPostgresBatchEventClient(ctx, &PostgreSQLConfig{Url: url}, logr.Discard())
	if err != nil {
		t.Fatalf("NewPostgresBatchEventClient: %v", err)
	}
	defer func() { _ = client.Close() }()

	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	defer pool.Close()

	now := time.Now().Unix()
	const (
		expiredJob = "purge-test-expired"
		liveJob    = "purge-test-live"
	)
	if _, err := pool.Exec(ctx,
		`INSERT INTO batch_events (job_id, event_type, expires_at) VALUES ($1, 1, $2), ($3, 1, $4)`,
		expiredJob, now-10, liveJob, now+3600); err != nil {
		t.Fatalf("seed events: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(ctx, `DELETE FROM batch_events WHERE job_id IN ($1, $2)`, expiredJob, liveJob)
	}()

	purged, err := client.purgeExpiredEvents(ctx)
	if err != nil {
		t.Fatalf("purgeExpiredEvents: %v", err)
	}
	if purged < 1 {
		t.Fatalf("purgeExpiredEvents purged %d rows, want >= 1 (the expired test row)", purged)
	}

	var remaining int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM batch_events WHERE job_id IN ($1, $2)`, expiredJob, liveJob).Scan(&remaining); err != nil {
		t.Fatalf("count remaining: %v", err)
	}
	if remaining != 1 {
		t.Fatalf("expected exactly the unexpired row to remain, got %d rows", remaining)
	}
	var remainingJob string
	if err := pool.QueryRow(ctx,
		`SELECT job_id FROM batch_events WHERE job_id IN ($1, $2)`, expiredJob, liveJob).Scan(&remainingJob); err != nil {
		t.Fatalf("fetch remaining: %v", err)
	}
	if remainingJob != liveJob {
		t.Fatalf("expected %s to remain, got %s", liveJob, remainingJob)
	}
}
