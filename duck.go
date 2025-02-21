package quacfka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
	runner         *DuckRunner
}

type duckJob struct {
	quack     *couac.Quacker
	destTable string
	rChan     chan Record
	wg        *sync.WaitGroup
}

type DuckRunner struct {
	parent         *duckConf
	path           string
	queryFunc      func(*DuckRunner) error
	queries        []string
	exec           bool
	deleteDBOnDone bool
	err            error
}

// AddQueries adds queries to a runner and sets whether the runner should use RunExec instead of RunFunc to run the queries (RunExec should be used when no results from queries are expected).
func (d *DuckRunner) AddQueries(queries []string, exec bool) {
	d.queries = append(d.queries, queries...)
	d.exec = exec
}
func (d *DuckRunner) Err() error                        { return d.err }
func (d *DuckRunner) IsDeleteDBOnDone() bool            { return d.deleteDBOnDone }
func (d *DuckRunner) Path() string                      { return d.path }
func (d *DuckRunner) Queries() []string                 { return d.queries }
func (d *DuckRunner) IsExec() bool                      { return d.exec }
func (d *DuckRunner) SetFunc(f func(*DuckRunner) error) { d.queryFunc = f }
func (d *DuckRunner) SetErr(err error)                  { d.err = errors.Join(d.err, err) }
func (d *DuckRunner) SetPath(p string)                  { d.path = p }
func (d *DuckRunner) SetDeleteOnDone(b bool)            { d.deleteDBOnDone = b }
func (d *DuckRunner) GetDB() *couac.Quacker {
	if d.parent != nil && d.parent.quack != nil {
		return d.parent.quack
	}
	return nil
}

// Run runs the defined queries, if exec is set to true does not expect queries to return any results
// otherwise will use query function to coordinate queries.
// Exec set to true is meant for running queries that aggregate to another table as well
// as EXPORT/COPY TO statements.
func (d *DuckRunner) Run(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if len(d.Queries()) == 0 {
		return fmt.Errorf("no queries specified")
	}
	if d.parent == nil {
		return fmt.Errorf("runner has no parent")
	}
	db := d.GetDB()
	if db == nil {
		return fmt.Errorf("runner parent db not open")
	}
	if !d.exec && d.queryFunc == nil {
		return fmt.Errorf("runner query func not set")
	}
	d.path = db.Path()
	switch d.exec {
	case false:
		err := d.queryFunc(d)
		if err != nil {
			if errorLog != nil {
				errorLog("quacfka: runfunc %s error: %w", d.Path(), err)
			}
			d.SetErr(fmt.Errorf("quacfka: runfunc %s error: %w", d.Path(), err))
			return fmt.Errorf("quacfka: runfunc %s error: %w", d.Path(), err)
		}
	case true:
		conn, err := db.NewConnection()
		if err != nil {
			if errorLog != nil {
				errorLog("quacfka: runexec newconnection %v\n", err)
			}
			d.err = fmt.Errorf("quacfka: runexec newconnection %w", err)
			return fmt.Errorf("quacfka: runexec newconnection %w", err)
		}
		for _, query := range d.queries {
			_, err := conn.Exec(ctx, query)
			if err != nil {
				if errorLog != nil {
					errorLog("quacfka: runexec exec %v\n", err)
				}
				d.err = fmt.Errorf("quacfka: runexec exec %w", err)
				return fmt.Errorf("quacfka: runexec exec %w", err)
			}
		}
	}

	if d.deleteDBOnDone {
		db.Close()
		err := os.Remove(d.path)
		if err != nil {
			if errorLog != nil {
				errorLog("quacfka: runner remove %v - %s\n", err, d.path)
			}
			d.SetErr(fmt.Errorf("quacfka: runner remove %w - %s", err, d.path))
			return fmt.Errorf("quacfka: runner remove %w - %s", err, d.path)
		}
	}
	return nil
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

func WithDuckRunner(p *DuckRunner) DuckOption {
	return func(cfg duckConfig) {
		p.parent = cfg
		cfg.runner = p
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
	o.Metrics.duckMetrics.duckFiles.Add(1)
	return nil
}

func (o *Orchestrator[T]) DuckIngestWithRotate(ctx context.Context, w *sync.WaitGroup) {
	defer w.Done()
	var rwg sync.WaitGroup
	for !(o.rChanClosed && o.rChanRecs.Load() == 0) {
		if o.rChanRecs.Load() > 0 {
			rwg.Add(1)
			o.Metrics.duckMetrics.duckStart()
			go o.DuckIngest(context.Background(), &rwg)
			rwg.Wait()
			if debugLog != nil {
				debugLog("db size: %d\n", o.CurrentDBSize())
			}
			o.Metrics.recordBytes.Store(0)
			// record cumulative size of duckdb files
			duckDBSize := o.CurrentDBSize()
			o.Metrics.duckMetrics.duckFilesSizeMB.Add(duckDBSize)

			// runner to run queries on db before close
			if o.duckConf.runner != nil {
				err := o.duckConf.runner.Run(ctx)
				if err != nil {
					if errorLog != nil {
						errorLog("quacfka: duckdb runner error - %w\n", err)
					}
					o.err = errors.Join(o.err, err)
				}
			}
			if o.duckConf.runner == nil || (o.duckConf.runner != nil && !o.duckConf.runner.IsDeleteDBOnDone()) {
				o.duckConf.quack.Close()
				// send duckPath to channel for external consumption
				if o.opt.withDuckPathsChan {
					o.duckPaths <- o.duckConf.quack.Path()
				}
			}
			o.Metrics.duckMetrics.duckStop(duckDBSize)
			if !(o.rChanClosed && o.rChanRecs.Load() == 0) {
				o.configureDuck(o.duckConf)
			}
		}
	}
	close(o.duckPaths)
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
	defer duck.Close()
	select {
	case record, ok := <-o.rChan:
		if !ok {
			o.rChanClosed = true
		}
		if len(o.opt.customArrow) > 0 {
			if record.Raw != nil {
				for _, a := range o.opt.customArrow {
					record.Raw.Retain()
					modRec := a.CustomFunc(ctx, a.DestinationTable, record.Raw)
					_, err := duck.IngestCreateAppend(ctx, a.DestinationTable, modRec)
					if err != nil {
						if errorLog != nil {
							errorLog("quacfka: duck ingestcreateappend %v\n", err)
						}
						modRec.Release()
						record.Raw.Release()
						continue
					}
					modRec.Release()
					record.Raw.Release()
				}
			}
		}
		if record.Norm != nil {
			numRows := record.Norm.NumRows()
			_, err = duck.IngestCreateAppend(ctx, d.destTable+"_norm", record.Norm)
			if err != nil {
				if errorLog != nil {
					errorLog("quacfka: duck ingestcreateappend %v\n", err)
				}
			} else {
				o.Metrics.normRecordsInserted.Add(numRows)
			}
		}
		if !o.opt.withoutDuckIngestRaw && record.Raw != nil {
			numRows := record.Raw.NumRows()
			_, err = duck.IngestCreateAppend(ctx, d.destTable, record.Raw)
			if err != nil {
				if errorLog != nil {
					errorLog("quacfka: duck ingestcreateappend %v\n", err)
				}
			} else {
				o.Metrics.recordsInserted.Add(numRows)
			}
		}
		o.rChanRecs.Add(-1)
	default:
	}
	if o.shouldRotateFile(ctx, duck) {
		o.duckPaths <- o.duckConf.quack.Path()
		return
	}

	// Start using duck inserter pool
	for dpool.Running() < o.DuckConnCount() && !(o.rChanClosed && o.rChanRecs.Load() == 0) {
		cwg.Add(1)
		dpool.Invoke(d)
		if debugLog != nil {
			debugLog("quacfka: duck pool size %d\n", dpool.Running())
		}
		if o.shouldRotateFile(ctx, duck) {
			break
		}
	}
	cwg.Wait()
	if o.opt.fileRotateThresholdMB == 0 && o.duckConf.quack.Path() != "" {
		o.duckPaths <- o.duckConf.quack.Path()
		close(o.duckPaths)
		o.duckConf.quack.Close()
	}
}

func (o *Orchestrator[T]) shouldRotateFile(ctx context.Context, duck *couac.QuackCon) bool {
	if o.opt.fileRotateThresholdMB > 0 && o.duckConf.quack.Path() != "" && checkDuckDBSizeMB(ctx, duck) >= o.opt.fileRotateThresholdMB {
		return true
	}
	return false
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
		o.rChanRecs.Add(-1)
		dbSizeBeforeInsert := checkDuckDBSizeMB(ctx, duck)
		if debugLog != nil {
			tick = time.Now()
			debugLog("quacfka: duck inserter - pull record - %d\n", o.rChanRecs.Load())
		}
		numRows = record.Raw.NumRows()
		// Custom Arrow data manipulation
		if len(o.opt.customArrow) > 0 {
			if record.Raw != nil {
				var cNumRows int64
				for _, a := range o.opt.customArrow {

					record.Raw.Retain()
					modRec := a.CustomFunc(ctx, a.DestinationTable, record.Raw)
					cNumRows = cNumRows + modRec.NumRows()
					_, err := duck.IngestCreateAppend(ctx, a.DestinationTable, modRec)
					if err != nil {
						if errorLog != nil {
							errorLog("quacfka: duck custom arrow ingestcreateappend %v\n", err)
						}
						modRec.Release()
						record.Raw.Release()
						continue
					}
					modRec.Release()
					record.Raw.Release()
				}
				o.Metrics.customRecordsInserted.Add(cNumRows)
				if debugLog != nil {
					debugLog("quacfka: duckdb - custom arrow rows ingested: %d -%d ms-  %f rows/sec\n", cNumRows, time.Since(tick).Milliseconds(), (float64(numRows) / float64(time.Since(tick).Seconds())))
				}
			}
		}
		// Normalizer data insertion
		if record.Norm != nil {
			tock := time.Now()
			nNumRows := record.Norm.NumRows()
			_, err = duck.IngestCreateAppend(ctx, c.destTable+"_norm", record.Norm)
			if err != nil {
				if errorLog != nil {
					errorLog("quacfka: duck normalizer ingestcreateappend %v\n", err)
				}
			} else {
				o.Metrics.normRecordsInserted.Add(numRows)
				if debugLog != nil {
					debugLog("quacfka: duckdb - normalizer arrow rows ingested: %d -%d ms-  %f rows/sec\n", nNumRows, time.Since(tock).Milliseconds(), (float64(numRows) / float64(time.Since(tick).Seconds())))
				}
			}
		}
		// Insert main record
		if !o.opt.withoutDuckIngestRaw {
			_, err := duck.IngestCreateAppend(ctx, c.destTable, record.Raw)
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

		// If file rotation is enabled, exit every time threshold is met.
		if o.opt.fileRotateThresholdMB > 0 && path != "" {
			dbSizeAfterInsert := checkDuckDBSizeMB(ctx, duck)
			if dbSizeAfterInsert+(dbSizeAfterInsert-dbSizeBeforeInsert)/int64(o.DuckConnCount()-1) >= o.opt.fileRotateThresholdMB {
				break
			}
		}
	}
}

func (o *Orchestrator[T]) CurrentDBSize() int64 {
	if o.duckConf.quack == nil {
		return 0
	}
	duck, err := o.duckConf.quack.NewConnection()
	if err != nil {
		if errorLog != nil {
			errorLog("quacfka: new connection: %v", err)
		}
		return -1
	}
	defer duck.Close()
	size := checkDuckDBSizeMB(context.Background(), duck)
	return size
}

func checkDuckDBSizeMB(ctx context.Context, duck *couac.QuackCon) int64 {
	var sizeBytes int64
	var err error
	recReader, statement, _, err := duck.Query(ctx, "CALL pragma_database_size()")
	if err != nil {
		return -1
	}
	defer statement.Close()
	for recReader.Next() {
		record := recReader.Record()
		for i := 0; i < int(record.NumRows()); i++ {
			block_size := record.Column(2).GetOneForMarshal(i)
			total_blocks := record.Column(3).GetOneForMarshal(i)
			wal := record.Column(6).ValueStr(i)
			walBytes := strings.Split(wal, " ")[0]
			if len(strings.Split(wal, " ")) > 1 {
				switch walUnit := strings.Split(wal, " ")[1]; walUnit {
				case "KiB":
					sizeBytes = sizeBytes + cast.ToInt64(walBytes)*1024
				case "MiB":
					sizeBytes = sizeBytes + cast.ToInt64(walBytes)*1024*1024
				case "GiB":
					sizeBytes = sizeBytes + cast.ToInt64(walBytes)*1024*1024*1024
				}
			}
			sizeBytes = sizeBytes + (block_size.(int64) * total_blocks.(int64))
		}
	}
	return sizeBytes / 1024 / 1024
}
