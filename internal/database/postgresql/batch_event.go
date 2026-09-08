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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
)

const (
	eventChanBufSize = 100
	channelEvents    = "batch_events"
)

var errEventChannelFull = errors.New("event channel full")

const ecSendEventSQL = `WITH ins AS (
	INSERT INTO batch_events (job_id, event_type, expires_at)
	VALUES ($1, $2, $3)
	RETURNING job_id
)
SELECT pg_notify('` + channelEvents + `', (SELECT job_id FROM ins))`

const ecDrainEventsSQL = `DELETE FROM batch_events
WHERE id IN (
	SELECT id FROM batch_events
	WHERE job_id = $1 AND expires_at > EXTRACT(EPOCH FROM NOW())::BIGINT
	ORDER BY id
	FOR UPDATE SKIP LOCKED
)
RETURNING event_type`

type eventSub struct {
	ch        chan api.BatchEvent
	closeOnce sync.Once
}

// PostgresBatchEventClient implements api.BatchEventChannelClient using PostgreSQL.
// The batch_events table is the durable source of truth (late-attach safe);
// PostgreSQL LISTEN/NOTIFY provides low-latency notification delivery without polling.
type PostgresBatchEventClient struct {
	pool      *pgxpool.Pool
	listener  *pgListener
	logger    logr.Logger
	closeOnce sync.Once

	eventsMu     sync.Mutex
	eventSubs    map[string]*eventSub
	eventsCancel context.CancelFunc
	eventsDone   chan struct{}
}

var _ api.BatchEventChannelClient = (*PostgresBatchEventClient)(nil)

func NewPostgresBatchEventClient(ctx context.Context, config *PostgreSQLConfig, logger logr.Logger) (*PostgresBatchEventClient, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if config == nil {
		return nil, fmt.Errorf("postgresql config cannot be nil")
	}

	pool, err := newPool(ctx, config)
	if err != nil {
		return nil, err
	}

	if _, err := pool.Exec(ctx, batchSchemaSql); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to apply batch events schema: %w", err)
	}

	c := &PostgresBatchEventClient{
		pool:      pool,
		logger:    logger,
		eventSubs: make(map[string]*eventSub),
	}

	c.listener = newPGListener(pool, channelEvents, logger, c.onReconnect)
	c.startEventDispatcher()

	logger.V(logging.INFO).Info("NewPostgresBatchEventClient: client created successfully")
	return c, nil
}

func (c *PostgresBatchEventClient) Close() error {
	c.closeOnce.Do(func() {
		if c.eventsCancel != nil {
			c.eventsCancel()
			<-c.eventsDone
		}
		if c.listener != nil {
			_ = c.listener.close()
		}
		if c.pool != nil {
			c.pool.Close()
		}
	})
	return nil
}

func (c *PostgresBatchEventClient) ECProducerSendEvents(ctx context.Context, events []api.BatchEvent) (sentIDs []string, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("empty events")
	}
	for i := range events {
		if err = events[i].IsValid(); err != nil {
			return nil, err
		}
	}

	sentIDs = make([]string, 0, len(events))
	for i := range events {
		event := events[i]
		expiresAt := time.Now().Unix() + int64(event.TTL)
		if _, err = c.pool.Exec(ctx, ecSendEventSQL, event.ID, int(event.Type), expiresAt); err != nil {
			return sentIDs, fmt.Errorf("ECProducerSendEvents: %w", err)
		}
		sentIDs = append(sentIDs, event.ID)
	}

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("ECProducerSendEvents: succeeded", "nIDs", len(sentIDs))
	return sentIDs, nil
}

func (c *PostgresBatchEventClient) ECConsumerGetChannel(ctx context.Context, ID string) (*api.BatchEventsChan, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(ID) == 0 {
		return nil, fmt.Errorf("ID is empty")
	}

	sub := &eventSub{
		ch: make(chan api.BatchEvent, eventChanBufSize),
	}

	c.eventsMu.Lock()
	c.eventSubs[ID] = sub
	c.eventsMu.Unlock()

	// Proactively drain existing events (late-attach safe)
	c.deliverJobEvents(ctx, ID)

	closeFn := func() {
		c.eventsMu.Lock()
		if cur, ok := c.eventSubs[ID]; ok && cur == sub {
			delete(c.eventSubs, ID)
		}
		c.eventsMu.Unlock()
		sub.closeOnce.Do(func() { close(sub.ch) })
	}

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("ECConsumerGetChannel: succeeded", "ID", ID)
	return &api.BatchEventsChan{ID: ID, Events: sub.ch, CloseFn: closeFn}, nil
}

func (c *PostgresBatchEventClient) onReconnect() {
	c.eventsMu.Lock()
	defer c.eventsMu.Unlock()
	// Trigger deliverAllJobEvents asynchronously so listen loop is never blocked
	go c.deliverAllJobEvents(context.Background())
}

func (c *PostgresBatchEventClient) startEventDispatcher() {
	dispCtx, cancel := context.WithCancel(context.Background())
	c.eventsCancel = cancel
	c.eventsDone = make(chan struct{})

	wake, unsubscribe := c.listener.subscribe()
	go c.runEventDispatcher(dispCtx, wake, unsubscribe)
}

func (c *PostgresBatchEventClient) runEventDispatcher(ctx context.Context, wake <-chan string, unsubscribe func()) {
	defer close(c.eventsDone)
	defer unsubscribe()
	c.logger.V(logging.INFO).Info("event dispatcher: start")

	for {
		select {
		case <-ctx.Done():
			c.logger.V(logging.INFO).Info("event dispatcher: stop")
			return
		case payload, ok := <-wake:
			if !ok || ctx.Err() != nil {
				return
			}
			if payload == "" {
				c.deliverAllJobEvents(ctx)
			} else {
				c.deliverJobEvents(ctx, payload)
			}
		}
	}
}

func (c *PostgresBatchEventClient) deliverAllJobEvents(ctx context.Context) {
	c.eventsMu.Lock()
	jobIDs := make([]string, 0, len(c.eventSubs))
	for id := range c.eventSubs {
		jobIDs = append(jobIDs, id)
	}
	c.eventsMu.Unlock()

	for _, id := range jobIDs {
		c.deliverJobEvents(ctx, id)
	}
}

func (c *PostgresBatchEventClient) deliverJobEvents(ctx context.Context, jobID string) {
	c.eventsMu.Lock()
	sub, ok := c.eventSubs[jobID]
	c.eventsMu.Unlock()
	if !ok {
		return
	}

	events, err := c.drainJobEvents(ctx, jobID)
	if err != nil {
		c.logger.Error(err, "event dispatcher: drain failed", "ID", jobID)
		return
	}
	if len(events) == 0 {
		return
	}

	c.eventsMu.Lock()
	defer c.eventsMu.Unlock()
	if cur, present := c.eventSubs[jobID]; !present || cur != sub {
		return
	}
	for _, event := range events {
		select {
		case sub.ch <- event:
		default:
			c.logger.Error(errEventChannelFull, "event dispatcher: dropping event", "ID", jobID, "type", event.Type)
		}
	}
}

func (c *PostgresBatchEventClient) drainJobEvents(ctx context.Context, jobID string) ([]api.BatchEvent, error) {
	rows, err := c.pool.Query(ctx, ecDrainEventsSQL, jobID)
	if err != nil {
		return nil, fmt.Errorf("drain events: %w", err)
	}
	defer rows.Close()

	var events []api.BatchEvent
	for rows.Next() {
		var eventType int
		if err := rows.Scan(&eventType); err != nil {
			return nil, fmt.Errorf("drain events scan: %w", err)
		}
		events = append(events, api.BatchEvent{ID: jobID, Type: api.BatchEventType(eventType)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("drain events rows: %w", err)
	}

	return events, nil
}
