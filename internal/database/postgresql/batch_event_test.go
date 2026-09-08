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
	"testing"
	"time"

	"github.com/go-logr/logr"

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
