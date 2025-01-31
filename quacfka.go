package quackfka

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	bufa "github.com/loicalleyne/bufarrow"
	"google.golang.org/protobuf/proto"
)

type Opt struct {
	withoutKafka bool
	withoutProc  bool
	withoutDuck  bool
}

type (
	Option func(Opt)
	config Opt
)

func WithoutKafka(p bool) Option {
	return func(cfg Opt) {
		cfg.withoutKafka = p
	}
}

func WithoutProcessing(p bool) Option {
	return func(cfg Opt) {
		cfg.withoutProc = p
	}
}

func WithoutDuck(p bool) Option {
	return func(cfg Opt) {
		cfg.withoutDuck = p
	}
}

type Orchestrator[T proto.Message] struct {
	bufArrowSchema         *bufa.Schema[T]
	kafkaConf              *KafkaClientConf[T]
	processorConf          *processorConf[T]
	duckConf               *duckConf
	mChan                  chan []byte
	rChan                  chan arrow.Record
	mungeFunc              func([]byte, any) error
	err                    error
	Metrics                *Metrics
	rowGroupSizeMultiplier int
	msgProcessorsCount     atomic.Int32
	duckConnCount          atomic.Int32
	rChanClosed            bool
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
	o.NewMetrics()
	return o, nil
}
func (o *Orchestrator[T]) Close() {
	o.duckConf.quack.Close()
}
func (o *Orchestrator[T]) Run(ctx context.Context, wg *sync.WaitGroup, opts ...Option) {
	o.NewMetrics()
	o.mChan = make(chan []byte, o.kafkaConf.MsgChanCap)
	o.rChan = make(chan arrow.Record, o.processorConf.rChanCap)
	defer wg.Done()
	var runOpts Opt
	var runWG sync.WaitGroup
	for _, opt := range opts {
		opt(runOpts)
	}
	o.StartMetrics()
	go o.benchmark(ctx)
	if !runOpts.withoutKafka {
		runWG.Add(1)
		go o.startKafka(ctx, &runWG)
	}
	if !runOpts.withoutProc && o.Error() == nil {
		runWG.Add(1)
		go o.ProcessMessages(ctx, &runWG)
	}
	if !runOpts.withoutDuck && o.Error() == nil {
		runWG.Add(1)
		go o.DuckIngest(context.Background(), &runWG)
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

func newBufarrowSchema[T proto.Message]() (*bufa.Schema[T], error) {
	return bufa.New[T](memory.DefaultAllocator)
}
