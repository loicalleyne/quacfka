package quacfka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/loicalleyne/couac"
	"github.com/panjf2000/ants/v2"
	"github.com/spf13/cast"
)

type duckConf struct {
	quack          *couac.Quacker
	path           string
	pathPrefix     string
	driverPath     string
	destTable      string
	duckConnCount  atomic.Int32
	destTableIndex atomic.Int32
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

func WithPathPrefix(p string) DuckOption {
	return func(cfg duckConfig) {
		cfg.pathPrefix = p
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
	d.destTableIndex.Store(-1)
	for _, opt := range opts {
		opt(d)
	}
	if d.duckConnCount.Load() < 1 {
		d.duckConnCount.Store(1)
		return errors.New("quacfka: duckdb connection count must be >= 1")
	}
	err = o.configureDuck(d)
	if err != nil {
		return fmt.Errorf("quacfka: duckdb open error %w", err)
	}
	o.duckConf = d
	return nil
}

func (o *Orchestrator[T]) configureDuck(d *duckConf) error {
	var err error
	// Configure and create database handle
	var cOpts []couac.Option
	// Using WithPathPrefix() to set a duckdb file path prefix overrides WithPath() path
	if d.pathPrefix != "" {
		if d.destTableIndex.Load() > 9 {
			d.destTableIndex.Store(-1)
		}
		duckPrefixedPath := d.pathPrefix + "_" + cast.ToString(d.destTableIndex.Load()+1) + "_" + time.Now().Format("2006-01-02_15-04-05") + ".db"
		cOpts = append(cOpts, couac.WithPath(duckPrefixedPath))
	} else {
		if d.path != "" {
			cOpts = append(cOpts, couac.WithPath(d.path))
		}
	}
	if d.driverPath != "" {
		cOpts = append(cOpts, couac.WithDriverPath(d.driverPath))
	}
	d.quack, err = couac.NewDuck(cOpts...)
	if err != nil {
		return fmt.Errorf("duckdb config error: %w", err)
	}
	return nil
}

func (o *Orchestrator[T]) DuckIngestWithRotate(ctx context.Context, w *sync.WaitGroup) {
	defer w.Done()
	var rwg sync.WaitGroup
	for !o.rChanClosed {
		rwg.Add(1)
		go o.DuckIngest(context.Background(), &rwg)
		rwg.Wait()
		if debugLog != nil {
			debugLog("db size: %d\n", checkDBSize(o.duckConf.quack.Path()))
		}
		o.Metrics.recordBytes.Store(0)
		o.duckConf.quack.Close()
		o.configureDuck(o.duckConf)
	}
}

func (o *Orchestrator[T]) DuckIngest(ctx context.Context, w *sync.WaitGroup) {
	defer w.Done()
	dpool, _ := ants.NewPoolWithFuncGeneric[*duckJob](o.DuckConnCount(), o.adbcInsert, ants.WithPreAlloc(true))
	defer dpool.Release()
	var cwg sync.WaitGroup
	d := new(duckJob)
	d.quack = o.duckConf.quack
	d.destTable = o.duckConf.destTable
	d.rChan = o.rChan
	d.wg = &cwg
	// Inserting one Arrow record ensures table is created if it does not already exist
	duck, err := d.quack.NewConnection()
	if err != nil {
		if errorLog != nil {
			errorLog("quacfka: new connection: %v", err)
		}
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

	// Start using duck inserter pool
	for dpool.Running() < o.DuckConnCount() && !o.rChanClosed {
		cwg.Add(1)
		dpool.Invoke(d)
		if debugLog != nil {
			debugLog("quacfka: duck pool size %d\tctx.Err %v\n", dpool.Running(), ctx.Err())
		}
		if o.opt.fileRotateThresholdMB > 0 && o.duckConf.quack.Path() != "" && o.Metrics.recordBytes.Load()/1024/1024 >= o.opt.fileRotateThresholdMB {
			break
		}
	}
	cwg.Wait()
	o.duckPaths <- o.duckConf.quack.Path()
}

func (o *Orchestrator[T]) adbcInsert(c *duckJob) {
	var tick time.Time
	path := c.quack.Path()
	defer c.wg.Done()
	duck, err := c.quack.NewConnection()
	if err != nil {
		if errorLog != nil {
			errorLog("quacfka: new connection: %v", err)
		}
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
		// If file rotation is enabled, exit every time threshold is met.
		if o.opt.fileRotateThresholdMB > 0 && path != "" {
			var s uint64
			var offset int64
			for _, c := range record.Columns() {
				s = s + c.Data().SizeInBytes()
			}
			o.Metrics.recordBytes.Add(int64(s))
			if int64(float64(o.DuckConnCount())*3) > o.opt.fileRotateThresholdMB {
				offset = 15
			} else {
				offset = int64(float64(o.DuckConnCount()) * 5)
			}
			if o.Metrics.recordBytes.Load()/1024/1024 >= o.opt.fileRotateThresholdMB-offset {
				if checkDBSize(o.duckConf.quack.Path()) >= o.opt.fileRotateThresholdMB-offset {
					break
				}
			}
		}
	}
}

func checkDBSize(path string) int64 {
	if path == "" {
		return 0
	}
	var dbSize, walSize int64
	fileInfo, err := os.Stat(path)
	if err != nil {
		if errorLog != nil {
			errorLog("quacfka: error checking file size %v\n", err)
		}
		return -1
	} else {
		dbSize = fileInfo.Size() / 1024 / 1024
	}
	walFileInfo, err := os.Stat(path + ".wal")
	if err != nil {
		if errorLog != nil {
			errorLog("quacfka: error checking file size %v\n", err)
		}
		return -1
	} else {
		walSize = walFileInfo.Size() / 1024 / 1024
	}
	return dbSize + walSize
}
