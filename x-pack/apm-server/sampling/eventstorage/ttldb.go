// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package eventstorage

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/elastic/apm-data/model/modelpb"
	"github.com/elastic/elastic-agent-libs/paths"
)

type TTLStorage struct {
	mu            sync.RWMutex
	prevPath      string
	prev          *badger.DB
	prevStorage   *Storage
	prevRW        IReadWriter
	activePath    string
	active        *badger.DB
	activeStorage *Storage
	activeRW      IReadWriter
	activeTime    time.Time
}

func NewTTLStorage(ttl time.Duration, codec Codec) (*TTLStorage, error) {
	t := time.Now()
	prevPath := paths.Resolve(paths.Data, fmt.Sprintf("tail_sampling_%d", t.Add(-ttl).Truncate(ttl).Unix()))
	prev, err := OpenBadger(prevPath, -1)
	if err != nil {
		return nil, err
	}
	activePath := paths.Resolve(paths.Data, fmt.Sprintf("tail_sampling_%d", t.Truncate(ttl).Unix()))
	active, err := OpenBadger(activePath, -1)
	if err != nil {
		return nil, err
	}
	s := &TTLStorage{
		prevPath:   prevPath,
		prev:       prev,
		prevRW:     New(prev, codec).NewShardedReadWriter(),
		activePath: activePath,
		active:     active,
		activeRW:   New(active, codec).NewShardedReadWriter(),
		activeTime: t.Truncate(ttl),
	}
	go func() {
		tick := time.NewTicker(ttl)
		defer tick.Stop()
		select {
		case <-tick.C:
			prev := s.prev
			prevRW := s.prevRW
			prevPath := s.prevPath
			s.mu.Lock()
			s.prev = s.active
			s.prevRW = s.activeRW
			s.prevPath = s.activePath

			s.activeTime = s.activeTime.Add(ttl)
			s.activePath = paths.Resolve(paths.Data, fmt.Sprintf("tail_sampling_%d", s.activeTime.Unix()))
			active, err := OpenBadger(s.activePath, -1)
			if err != nil {
				panic(err) // FIXME
			}
			s.active = active
			s.activeRW = New(active, codec).NewShardedReadWriter()
			s.mu.Unlock()
			s.prevRW.Flush()

			prevRW.Close()
			prev.Close()
			os.RemoveAll(prevPath)
		}
	}()
	return s, nil
}

type TTLReadWriter struct {
	s *TTLStorage
}

func (rw *TTLReadWriter) Close() {
	rw.s.mu.RLock()
	defer rw.s.mu.RUnlock()
	rw.s.prevRW.Close()
	rw.s.activeRW.Close()
}

func (rw *TTLReadWriter) Flush() error {
	rw.s.mu.RLock()
	defer rw.s.mu.RUnlock()
	if err := rw.s.prevRW.Flush(); err != nil {
		return err
	}
	return rw.s.activeRW.Flush()
}

func (rw *TTLReadWriter) ReadTraceEvents(traceID string, out *modelpb.Batch) error {
	rw.s.mu.RLock()
	defer rw.s.mu.RUnlock()
	if err := rw.s.prevRW.ReadTraceEvents(traceID, out); err != nil {
		return err
	}
	return rw.s.activeRW.ReadTraceEvents(traceID, out)
}

func (rw *TTLReadWriter) WriteTraceEvent(traceID string, id string, event *modelpb.APMEvent, opts WriterOpts) error {
	rw.s.mu.RLock()
	defer rw.s.mu.RUnlock()
	return rw.s.activeRW.WriteTraceEvent(traceID, id, event, opts)
}

func (rw *TTLReadWriter) WriteTraceSampled(traceID string, sampled bool, opts WriterOpts) error {
	rw.s.mu.RLock()
	defer rw.s.mu.RUnlock()
	return rw.s.activeRW.WriteTraceSampled(traceID, sampled, opts)
}

func (rw *TTLReadWriter) IsTraceSampled(traceID string) (bool, error) {
	rw.s.mu.RLock()
	defer rw.s.mu.RUnlock()
	prevSampled, prevErr := rw.s.prevRW.IsTraceSampled(traceID)
	if prevErr != nil && prevErr != ErrNotFound {
		return false, prevErr
	}
	activeSampled, activeErr := rw.s.activeRW.IsTraceSampled(traceID)
	if activeErr != nil && activeErr != ErrNotFound {
		return false, activeErr
	}
	if prevErr == ErrNotFound && activeErr == ErrNotFound {
		return false, ErrNotFound
	}
	return prevSampled || activeSampled, nil
}

func (rw *TTLReadWriter) DeleteTraceEvent(traceID, id string) error {
	rw.s.mu.RLock()
	defer rw.s.mu.RUnlock()
	if err := rw.s.prevRW.DeleteTraceEvent(traceID, id); err != nil {
		return err
	}
	return rw.s.activeRW.DeleteTraceEvent(traceID, id)
}

func (s *TTLStorage) NewReadWriter() *TTLReadWriter {
	return &TTLReadWriter{s: s}
}
