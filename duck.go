package quackfka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/loicalleyne/couac"
	"github.com/panjf2000/ants/v2"
)

type duckConf struct {
	quack         *couac.Quacker
	path          string
	driverPath    string
	destTable     string
	duckConnCount atomic.Int32
}

type duckJob struct {
	ctxCancel context.CancelFunc
	quack     *couac.Quacker
	destTable string
	rChan     chan arrow.Record
	wg        *sync.WaitGroup
}

type (
	DuckOption func(duckConfig)
	duckConfig *duckConf
)

func WithPath(p string) DuckOption {
	return func(cfg duckConfig) {
		cfg.path = p
	}
}

func WithDriverPath(p string) DuckOption {
	return func(cfg duckConfig) {
		cfg.driverPath = p
	}
}

func WithDestinationTable(p string) DuckOption {
	return func(cfg duckConfig) {
		cfg.destTable = p
	}
}

func WithDuckConnections(p int) DuckOption {
	return func(cfg duckConfig) {
		cfg.duckConnCount.Store(int32(p))
	}
}

func (o *Orchestrator[T]) ConfigureDuck(opts ...DuckOption) error {
	var err error
	if o.duckConf != nil && o.duckConf.quack != nil {
		o.duckConf.quack.Close()
	}
	d := new(duckConf)
	d.duckConnCount.Store(1)
	for _, opt := range opts {
		opt(d)
	}
	// fmt.Printf("dopts %+v\n", d)
	if d.duckConnCount.Load() < 1 {
		d.duckConnCount.Store(1)
		return errors.New("quacfka: duckdb connection count must be >= 1")
	}
	var cOpts []couac.Option
	if d.path != "" {
		cOpts = append(cOpts, couac.WithPath(d.path))
	}
	if d.driverPath != "" {
		cOpts = append(cOpts, couac.WithDriverPath(d.driverPath))
	}
	d.quack, err = couac.NewDuck(cOpts...)
	if err != nil {
		return fmt.Errorf("duckdb config error: %w", err)
	}
	o.duckConf = d
	// fmt.Println("dopts ", d)
	return nil
}

func (o *Orchestrator[T]) DuckIngest(ctx context.Context, w *sync.WaitGroup) {
	// fmt.Println("di cn ", o.duckConf, o.duckConf.destTable)
	defer w.Done()
	dpool, _ := ants.NewPoolWithFuncGeneric[*duckJob](o.DuckConnCount(), o.adbcInsert, ants.WithPreAlloc(true))
	defer dpool.Release()
	// ctxD, ctxDFunc := context.WithCancel(ctx)
	var cwg sync.WaitGroup
	d := new(duckJob)
	// d.ctxCancel = ctxDFunc
	d.quack = o.duckConf.quack
	d.destTable = o.duckConf.destTable
	d.rChan = o.rChan
	d.wg = &cwg
	duck, err := d.quack.NewConnection()
	if err != nil {
		if errorLog != nil {
			errorLog("quacfka: new connection: %v", err)
		}
		// c.ctxCancel()
		return
	}
	record, ok := <-o.rChan
	if !ok {
		duck.Close()
		o.rChanClosed = true
		return
	}
	numRows := record.NumRows()
	_, err = duck.IngestCreateAppend(ctx, d.destTable, record)
	if err != nil {
		if errorLog != nil {
			errorLog("quacfka: duck ingestcreateappend %v\n", err)
		}
	} else {
		o.Metrics.recordsInserted.Add(numRows)
	}
	duck.Close()

	for dpool.Running() < o.DuckConnCount() && !o.rChanClosed {
		cwg.Add(1)
		dpool.Invoke(d)
		if debugLog != nil {
			debugLog("quacfka: duck pool size %d\tctx.Err %v\n", dpool.Running(), ctx.Err())
		}
	}
	cwg.Wait()
}

func (o *Orchestrator[T]) adbcInsert(c *duckJob) {
	var tick time.Time
	defer c.wg.Done()
	duck, err := c.quack.NewConnection()
	if err != nil {
		if errorLog != nil {
			errorLog("quacfka: new connection: %v", err)
		}
		// c.ctxCancel()
		return
	}
	defer duck.Close()
	ctx := context.Background()
	var numRows int64
	for record := range c.rChan {
		if debugLog != nil {
			tick = time.Now()
			debugLog("quacfka: duck inserter - pull record - %d\n", len(c.rChan))
		}
		numRows = record.NumRows()
		// fmt.Printf("rows %v\n", numRows)
		_, err := duck.IngestCreateAppend(ctx, c.destTable, record)
		if err != nil {
			if errorLog != nil {
				errorLog("quacfka: duck ingestcreateappend %v\n", err)
			}
		} else {
			o.Metrics.recordsInserted.Add(numRows)
			if debugLog != nil {
				debugLog("quacfka: duckdb - rows ingested: %d -%d ms-  %f rows/sec\n", numRows, time.Since(tick).Milliseconds(), (float64(numRows) / float64(time.Since(tick).Seconds())))
			}
		}
	}
	duck.Close()
}
