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

	ctx := sctx.AsContext(sigctx.New())

	job := NewJob(ctx, db)

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

func bulkInsert(ctx context.Context, db *sqlx.DB, rows []requestPayload) error {
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
	_, err := db.ExecContext(ctx, query, valueArgs...)
	return err
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
	db      *sqlx.DB

	mu   sync.Mutex
	rows []requestPayload
}

func NewJob(ctx context.Context, db *sqlx.DB) *Job {
	job := &Job{
		db:      db,
		enabled: atomic.NewBool(true),
		rows:    []requestPayload{},
	}

	job.Context, job.finish = context.WithCancel(context.Background())
	go job.run(ctx)
	return job
}

func (j *Job) Push(r requestPayload) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.rows = append(j.rows, r)
}

func (j *Job) run(ctx context.Context) {
	log.Println("Started background job")

	defer j.finish()

	ticker := time.NewTicker(time.Second)
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
	log.Println("flushing", len(j.rows))
	err := bulkInsert(j.Context, j.db, j.rows)
	if err != nil {
		log.Printf("failed to bulk insert %v\n", err)
	}
	j.rows = []requestPayload{}
}
