package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/atomic"

	sctx "github.com/SentimensRG/ctx"
	"github.com/SentimensRG/ctx/sigctx"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

func main() {
	db := mustConnectDB()
	mustSetupDB(db)
	defer db.Close()

	job := NewJob()
	job.Storage = &PostgresStore{DB: db}

	ctx := sctx.AsContext(sigctx.New())
	go job.Run(ctx)

	r := gin.New()
	r.Use(gin.Recovery())
	r.POST("/ping", func(c *gin.Context) {
		var r requestPayload
		if err := c.BindJSON(&r); err != nil {
			c.AbortWithStatus(http.StatusBadRequest)
			return
		}

		if db == nil {
			c.AbortWithError(http.StatusInternalServerError, errors.New("failed to created db"))
			return
		}

		job.Push(r)

		c.JSON(200, gin.H{"message": "pong"})
	})
	go r.Run()
	<-ctx.Done()
	<-job.Done()
}

type requestPayload struct {
	Name string `json:"name" db:"name"`
}

func mustConnectDB() *sqlx.DB {
	db := sqlx.MustOpen("postgres", os.Getenv("DATABASE_URL"))
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(50)
	return db
}

func mustSetupDB(db *sqlx.DB) {
	sqlx.MustExec(db, `
	CREATE TABLE IF NOT EXISTS records (
		id serial primary key,
		name text
	)`)
}

type Job struct {
	context.Context
	finish func()

	enabled *atomic.Bool

	qIndex *atomic.Uint32
	qMask  uint32
	queues []*Queue

	Storage interface {
		Save(ctx context.Context, items []*requestPayload) error
	}
}

func NewJob() *Job {
	numQueues := 16
	job := &Job{
		enabled: atomic.NewBool(true),
		qIndex:  atomic.NewUint32(0),
		qMask:   uint32(numQueues - 1),
		queues:  NewQueues(numQueues),
	}

	job.Context, job.finish = context.WithCancel(context.Background())
	return job
}

var errJobDisabled = errors.New("job disabled")

func (j *Job) Push(r requestPayload) error {
	if j.enabled.Load() {
		idx := j.qIndex.Inc() & j.qMask
		return j.queues[idx].Push(&r)
	}

	return errJobDisabled
}

func (j *Job) Run(ctx context.Context) {
	log.Println("Started background job")

	defer j.finish()

	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ticker.C:
			j.flush()
			continue
		case <-ctx.Done():
			j.enabled.Store(false)
			j.flush()
		}
		break
	}

	log.Println("Exiting run")
}

func (j *Job) flush() {
	var batchInsertSize int
	for k, q := range j.queues {
		rows := q.Clear()
		for len(rows) > 0 {
			batchInsertSize = 1000
			if len(rows) < batchInsertSize {
				batchInsertSize = len(rows)
			}

			log.Println("[flush] queue", k, "remaining", len(rows))
			if err := j.Storage.Save(j.Context, rows[:batchInsertSize]); err != nil {
				log.Println("Error flushing data", err.Error())
			}
			rows = rows[batchInsertSize:]
		}
	}
}

type Queue struct {
	sync.RWMutex
	items []*requestPayload
}

func NewQueue() *Queue {
	return &Queue{items: make([]*requestPayload, 0)}
}

func NewQueues(size int) []*Queue {
	qs := make([]*Queue, size)
	for i := 0; i < size; i++ {
		qs[i] = NewQueue()
	}
	return qs
}

func (q *Queue) Push(r *requestPayload) error {
	q.Lock()
	defer q.Unlock()
	q.items = append(q.items, r)
	return nil
}

func (q *Queue) Clear() []*requestPayload {
	length := q.Len()
	q.Lock()
	defer q.Unlock()
	qs := q.items[:length]
	q.items = q.items[length:]
	return qs
}

func (q *Queue) Len() int {
	q.RLock()
	defer q.RUnlock()
	return len(q.items)
}

type PostgresStore struct {
	DB *sqlx.DB
}

func (s *PostgresStore) Save(ctx context.Context, rows []*requestPayload) error {
	if len(rows) == 0 {
		return nil
	}

	valueStrings := make([]string, 0, len(rows))
	valueArgs := make([]interface{}, 0, len(rows))
	for idx, r := range rows {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d)", idx+1))
		valueArgs = append(valueArgs, r.Name)
	}

	query := fmt.Sprintf("INSERT INTO records (name) VALUES %s", strings.Join(valueStrings, ","))
	_, err := s.DB.ExecContext(ctx, query, valueArgs...)
	return err
}
