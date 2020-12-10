package main

import (
	"context"
	"strconv"
	"testing"

	"go.uber.org/atomic"
)

func TestQueue(t *testing.T) {
	q := NewQueue()

	q.Push(&requestPayload{Name: "1"})
	if q.Len() == 0 {
		t.Fatalf("expected Len to be 1 but was %d", q.Len())
	}

	q.Clear()
	if q.Len() != 0 {
		t.Fatalf("expected Len to be 0 but was %d", q.Len())
	}
}

func TestJob(t *testing.T) {
	job := NewJob()
	storage := newInMemoryStorage()
	job.Storage = storage
	numItems := 100000

	for i := 0; i < numItems; i++ {
		job.Push(requestPayload{Name: strconv.Itoa(i)})
	}
	job.flush()

	if storage.count() != numItems {
		t.Fatalf("expected only %d item to be saved, got %d", numItems, storage.count())
	}
}

type inMemoryStorage struct {
	numItems *atomic.Uint64
}

func newInMemoryStorage() *inMemoryStorage {
	return &inMemoryStorage{numItems: atomic.NewUint64(0)}
}

func (s *inMemoryStorage) Save(ctx context.Context, rows []*requestPayload) error {
	s.numItems.Add(uint64(len(rows)))
	return nil
}

func (s *inMemoryStorage) count() int {
	return int(s.numItems.Load())
}
