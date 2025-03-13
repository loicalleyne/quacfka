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
	withoutKafka           bool
	withoutProc            bool
	withoutDuck            bool
	withoutDuckIngestRaw   bool
	withDuckPathsChan      bool
	duckPathChanCap        int
	fileRotateThresholdMB  int64
	customArrow            []CustomArrow
	normalizerFieldStrings []string
	normalizerAliasStrings []string
	failOnRangeErr         bool
	customFields           []bufa.CustomField
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

func WithDuckPathsChan(s int) Option {
	return func(cfg config) {
		cfg.withDuckPathsChan = true
		cfg.duckPathChanCap = s
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

// WithNormalizer configures the scalars to add to a flat Arrow Record suitable for efficient aggregation.
// Protobuf data with nested messages converted to Arrow records is not only slower to insert into duckdb,
// running aggregation queries on nested data is much slower(by orders of magnitude).
// Fields should be specified by their path (field names separated by a period ie. 'field1.field2.field3').
// The Arrow field types of the selected fields will be used to build the new schema. If coaslescing
// data between multiple fields of the same type, specify only one of the paths.
// List fields should have an index to retrieve specified, otherwise defaults to all elements;
// ranges are not yet implemented.
func WithNormalizer(fields, aliases []string, failOnRangeError bool) Option {
	return func(cfg config) {
		cfg.normalizerFieldStrings = append(cfg.normalizerFieldStrings, fields...)
		cfg.normalizerAliasStrings = append(cfg.normalizerAliasStrings, aliases...)
		cfg.failOnRangeErr = failOnRangeError
	}
}

func WithoutDuckIngestRaw() Option {
	return func(cfg config) {
		cfg.withoutDuckIngestRaw = true
	}
}

type Record struct {
	Raw  arrow.Record
	Norm arrow.Record
}

type Orchestrator[T proto.Message] struct {
	bufArrowSchema         *bufa.Schema[T]
	kafkaConf              *KafkaClientConf[T]
	processorConf          *processorConf[T]
	duckConf               *duckConf
	duckPaths              chan string
	mChan                  chan []byte
	rChan                  chan Record
	rChanRecs              atomic.Int32
	mungeFunc              func([]byte, any) error
	err                    error
	Metrics                *Metrics
	rowGroupSizeMultiplier int
	msgProcessorsCount     atomic.Int32
	duckConnCount          atomic.Int32
	mChanClosed            bool
	rChanClosed            bool
	opt                    *Opt
}

func NewOrchestrator[T proto.Message](opts ...Option) (*Orchestrator[T], error) {
	var err error
	o := new(Orchestrator[T])
	newOpt := new(Opt)
	for _, f := range opts {
		f(newOpt)
	}
	o.opt = newOpt

	if len(o.opt.normalizerFieldStrings) > 0 && len(o.opt.customFields) > 0 {
		o.bufArrowSchema, err = bufa.New[T](memory.DefaultAllocator, bufa.WithNormalizer(o.opt.normalizerFieldStrings, o.opt.normalizerAliasStrings, false), bufa.WithCustomFields(o.opt.customFields))
		if err != nil {
			return nil, err
		}
		goto skipbufa
	}
	if len(o.opt.normalizerFieldStrings) > 0 {
		o.bufArrowSchema, err = bufa.New[T](memory.DefaultAllocator, bufa.WithNormalizer(o.opt.normalizerFieldStrings, o.opt.normalizerAliasStrings, false))
		if err != nil {
			return nil, err
		}
		goto skipbufa
	}
	if len(o.opt.customFields) > 0 {
		o.bufArrowSchema, err = bufa.New[T](memory.DefaultAllocator, bufa.WithCustomFields(o.opt.customFields))
		if err != nil {
			return nil, err
		}
		goto skipbufa
	}
	o.bufArrowSchema, err = bufa.New[T](memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}
skipbufa:
	if o.opt.withDuckPathsChan {
		if o.opt.duckPathChanCap < 1 {
			return nil, fmt.Errorf("invalid duck path channel capacity: %d", o.opt.duckPathChanCap)
		}
		o.duckPaths = make(chan string, o.opt.duckPathChanCap)
	}
	o.NewKafkaConfig()
	o.rowGroupSizeMultiplier = 1
	o.msgProcessorsCount.Store(1)
	o.duckConnCount.Store(1)
	o.NewMetrics()
	return o, nil
}

// Close closes the DuckDB database, if open.
func (o *Orchestrator[T]) Close() {
	if o.duckConf != nil && o.duckConf.quack != nil {
		o.duckConf.quack.Close()
		o.duckConf.quack = nil
	}
}

// IsClosed returns whether DuckDB database is open or not.
func (o *Orchestrator[T]) IsClosed() bool {
	if o.duckConf == nil || o.duckConf.quack == nil {
		return true
	}
	return false
}

func (o *Orchestrator[T]) Run(ctx context.Context, wg *sync.WaitGroup) {
	o.NewMetrics()
	defer wg.Done()
	var runWG sync.WaitGroup

	if debugLog != nil {
		debugLog("w/o Kafka: %v\tw/o Proc: %v\tw/o duckdb: %v\trotation threshold MB:%d\tcustom arrows %d\tnormalizer fields %d\n", o.opt.withoutKafka, o.opt.withoutProc, o.opt.withoutDuck, o.opt.fileRotateThresholdMB, len(o.opt.customArrow), len(o.opt.normalizerFieldStrings))
	}
	o.StartMetrics()
	go o.benchmark(ctx)
	if !o.opt.withoutKafka {
		o.mChan = make(chan []byte, o.kafkaConf.MsgChanCap)
		runWG.Add(1)
		go o.startKafka(ctx, &runWG)
	}
	if !o.opt.withoutProc && o.Error() == nil {
		o.rChan = make(chan Record, o.processorConf.rChanCap)
		runWG.Add(1)
		go o.ProcessMessages(ctx, &runWG)
	}
	if !o.opt.withoutDuck && o.Error() == nil {
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

func (o *Orchestrator[T]) ArrowQueueCapacity() int  { return o.processorConf.rChanCap }
func (o *Orchestrator[T]) Error() error             { return o.err }
func (o *Orchestrator[T]) Schema() *bufa.Schema[T]  { return o.bufArrowSchema }
func (o *Orchestrator[T]) MessageChan() chan []byte { return o.mChan }
func (o *Orchestrator[T]) MessageChanClose() {
	close(o.mChan)
	o.mChanClosed = true
}
func (o *Orchestrator[T]) MessageChanSend(m []byte) {
	if o.kafkaConf.Munger != nil {
		o.mChan <- o.byteCounter(o.kafkaConf.Munger(m))
	} else {
		o.mChan <- o.byteCounter(m)
	}
	o.Metrics.kafkaMessagesConsumed.Add(1)
}
func (o *Orchestrator[T]) RecordChan() chan Record { return o.rChan }
func (o *Orchestrator[T]) RecordChanClose() {
	close(o.rChan)
	o.rChanClosed = true
}
func (o *Orchestrator[T]) RecordChanSend(r Record) {
	o.rChan <- r
	o.rChanRecs.Add(1)
}
func (o *Orchestrator[T]) KafkaClientCount() int   { return int(o.kafkaConf.ClientCount.Load()) }
func (o *Orchestrator[T]) KafkaQueueCapacity() int { return int(o.kafkaConf.MsgChanCap) }
func (o *Orchestrator[T]) MsgProcessorsCount() int { return int(o.msgProcessorsCount.Load()) }
func (o *Orchestrator[T]) DuckConnCount() int      { return int(o.duckConf.duckConnCount.Load()) }
func (o *Orchestrator[T]) DuckPaths() chan string  { return o.duckPaths }
func newBufarrowSchema[T proto.Message]() (*bufa.Schema[T], error) {
	return bufa.New[T](memory.DefaultAllocator)
}
