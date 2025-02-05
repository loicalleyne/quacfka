package quacfka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	bufa "github.com/loicalleyne/bufarrow"
	"google.golang.org/protobuf/proto"
)

var (
	ErrMissingDuckDBConfig = errors.New("missing duckdb configuration")
)

type CustomArrow struct {
	CustomFunc       func(context.Context, string, arrow.Record) arrow.Record
	DestinationTable string
}

type Opt struct {
	withoutKafka          bool
	withoutProc           bool
	withoutDuck           bool
	fileRotateThresholdMB int64
	customArrow           []CustomArrow
}

type (
	Option func(config)
	config *Opt
)

func WithoutKafka() Option {
	return func(cfg config) {
		cfg.withoutKafka = true
	}
}

func WithoutProcessing() Option {
	return func(cfg config) {
		cfg.withoutProc = true
	}
}

func WithoutDuck() Option {
	return func(cfg config) {
		cfg.withoutDuck = true
	}
}

func WithCustomArrows(p []CustomArrow) Option {
	return func(cfg config) {
		for _, c := range p {
			if c.CustomFunc != nil && c.DestinationTable != "" {
				cfg.customArrow = append(cfg.customArrow, c)
			}
		}
	}
}

// WithFileRotateThresholdMB sets the database file rotation size.
// Minimum rotation threshold is 100MB.
func WithFileRotateThresholdMB(p int64) Option {
	return func(cfg config) {
		if p < 100 {
			cfg.fileRotateThresholdMB = 100
			return
		}
		cfg.fileRotateThresholdMB = p
	}
}

type Orchestrator[T proto.Message] struct {
	bufArrowSchema         *bufa.Schema[T]
	kafkaConf              *KafkaClientConf[T]
	processorConf          *processorConf[T]
	duckConf               *duckConf
	duckPaths              chan string
	mChan                  chan []byte
	rChan                  chan arrow.Record
	rChanRecs              atomic.Int32
	mungeFunc              func([]byte, any) error
	err                    error
	Metrics                *Metrics
	rowGroupSizeMultiplier int
	msgProcessorsCount     atomic.Int32
	duckConnCount          atomic.Int32
	mChanClosed            bool
	rChanClosed            bool
	opt                    Opt
}

func NewOrchestrator[T proto.Message]() (*Orchestrator[T], error) {
	var err error
	o := new(Orchestrator[T])
	o.bufArrowSchema, err = bufa.New[T](memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}
	o.NewKafkaConfig()
	o.rowGroupSizeMultiplier = 1
	o.msgProcessorsCount.Store(1)
	o.duckConnCount.Store(1)
	o.duckPaths = make(chan string, 1000)
	o.NewMetrics()
	return o, nil
}
func (o *Orchestrator[T]) Close() {
	if o.duckConf != nil && o.duckConf.quack != nil {
		// Send db file path to channel for further aggregations/querying
		o.duckPaths <- o.duckConf.quack.Path()
		o.duckConf.quack.Close()
	}
}

func (o *Orchestrator[T]) Run(ctx context.Context, wg *sync.WaitGroup, opts ...Option) {
	o.NewMetrics()
	defer wg.Done()
	var runOpts Opt
	var runWG sync.WaitGroup
	for _, f := range opts {
		f(&runOpts)
	}
	o.opt = runOpts
	if debugLog != nil {
		debugLog("w/o Kafka: %v\tw/o Proc: %v\tw/o duckdb: %v\trotation threshold MB:%d\n", o.opt.withoutKafka, o.opt.withoutProc, o.opt.withoutDuck, o.opt.fileRotateThresholdMB)
	}
	o.StartMetrics()
	go o.benchmark(ctx)
	if !runOpts.withoutKafka {
		o.mChan = make(chan []byte, o.kafkaConf.MsgChanCap)
		runWG.Add(1)
		go o.startKafka(ctx, &runWG)
	}
	if !runOpts.withoutProc && o.Error() == nil {
		o.rChan = make(chan arrow.Record, o.processorConf.rChanCap)
		runWG.Add(1)
		go o.ProcessMessages(ctx, &runWG)
	}
	if !runOpts.withoutDuck && o.Error() == nil {
		switch dc := o.duckConf; dc {
		case nil:
			o.err = fmt.Errorf("quacfka: %w", ErrMissingDuckDBConfig)
			if errorLog != nil {
				errorLog("quacfka: %v", ErrMissingDuckDBConfig)
			}
		default:
			runWG.Add(1)
			switch rt := o.opt.fileRotateThresholdMB; rt > 0 {
			case true:
				go o.DuckIngestWithRotate(context.Background(), &runWG)
			default:
				go o.DuckIngest(context.Background(), &runWG)
			}
		}
	}
	runWG.Wait()
	o.UpdateMetrics()
}
func (o *Orchestrator[T]) Error() error                  { return o.err }
func (o *Orchestrator[T]) Schema() *bufa.Schema[T]       { return o.bufArrowSchema }
func (o *Orchestrator[T]) MessageChan() chan []byte      { return o.mChan }
func (o *Orchestrator[T]) RecordChan() chan arrow.Record { return o.rChan }
func (o *Orchestrator[T]) KafkaClientCount() int         { return int(o.kafkaConf.ClientCount.Load()) }
func (o *Orchestrator[T]) MsgProcessorsCount() int       { return int(o.msgProcessorsCount.Load()) }
func (o *Orchestrator[T]) DuckConnCount() int            { return int(o.duckConf.duckConnCount.Load()) }
func (o *Orchestrator[T]) DuckPaths() chan string        { return o.duckPaths }
func newBufarrowSchema[T proto.Message]() (*bufa.Schema[T], error) {
	return bufa.New[T](memory.DefaultAllocator)
}
