package quacfka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/memory"

	bufa "github.com/loicalleyne/bufarrow"
	"github.com/panjf2000/ants/v2"
	"google.golang.org/protobuf/proto"
)

var (
	ErrWaitGroupIsNil                = errors.New("*sync.waitgroup is nil")
	ErrMessageChanCapacityZero       = errors.New("chan []byte must have capacity > 0")
	ErrRecordChanCapacityZero        = errors.New("chan []arrow.record must have capacity > 0")
	ErrProcessingFuncIsNil           = errors.New("func([]byte, *bufa.Schema[T]) error is nil")
	ErrProcessingRoutineCountInvalid = errors.New("deserializer routine count not > 0")
	ErrRowGroupSizeMultiplier        = errors.New("row group size multiplier not > 0")
)

type processorConf[T proto.Message] struct {
	parent        *Orchestrator[T]
	ctx           context.Context
	wg            *sync.WaitGroup
	s             *bufa.Schema[T]
	unmarshalFunc func([]byte, any) error
	rChanCap      int
}

func (o *Orchestrator[T]) configureProcessor(ctx context.Context, wg *sync.WaitGroup, unmarshalFunc func([]byte, any) error) (*processorConf[T], error) {
	if unmarshalFunc == nil {
		return nil, ErrProcessingFuncIsNil
	}
	d := new(processorConf[T])
	d.ctx = ctx
	d.parent = o
	if wg != nil {
		d.wg = wg
	}
	d.unmarshalFunc = unmarshalFunc
	return d, nil
}

func (o *Orchestrator[T]) ConfigureProcessor(rChanCap, rowGroupSizeMultiplier, routineCount int, unmarshalFunc func([]byte, any) error) error {
	if rChanCap < 1 {
		return ErrRecordChanCapacityZero
	}

	if routineCount < 1 {
		return ErrProcessingRoutineCountInvalid
	}
	o.msgProcessorsCount.Store(int32(routineCount))
	if rowGroupSizeMultiplier < 1 {
		return ErrRowGroupSizeMultiplier
	}
	o.rowGroupSizeMultiplier = rowGroupSizeMultiplier
	if unmarshalFunc == nil {
		return ErrProcessingFuncIsNil
	}
	o.mungeFunc = unmarshalFunc
	d, err := o.configureProcessor(context.Background(), nil, unmarshalFunc)
	if err != nil {
		return err
	}
	d.rChanCap = rChanCap
	o.processorConf = d
	return nil
}

// ProcessMessages creates a pool of deserializer goroutines
func (o *Orchestrator[T]) ProcessMessages(ctx context.Context, wg *sync.WaitGroup) {
	defer o.RecordChanClose()
	defer wg.Done()
	cpool, _ := ants.NewPoolWithFunc(o.MsgProcessorsCount(), convertMessages[T], ants.WithPreAlloc(true))
	defer cpool.Release()

	var cwg sync.WaitGroup

	for cpool.Running() < o.MsgProcessorsCount() && ctx.Err() == nil {
		sc, err := o.bufArrowSchema.Clone(memory.DefaultAllocator)
		if err != nil {
			o.err = fmt.Errorf("quacfka: bufarrow schema clone: %w", err)
			return
		}
		cwg.Add(1)
		c, _ := o.configureProcessor(ctx, &cwg, o.processorConf.unmarshalFunc)
		c.s = sc
		cpool.Invoke(c)
		if debugLog != nil {
			debugLog("quacfka: processing pool size %d\n", cpool.Running())
		}
	}
	cwg.Wait()
	if debugLog != nil {
		debugLog("quacfka: processing closing rChan: %v recs\n", o.Metrics.recordsProcessed.Load())
	}
}

func convertMessages[T proto.Message](c any) {
	defer func() {
		e := recover()
		if e != nil {
			switch x := e.(type) {
			case error:
				err := x
				c.(*processorConf[T]).parent.err = fmt.Errorf("recover convertmessages: %w", err)
				if errorLog != nil {
					errorLog("recover convertmessages: %v\n", err)
				}
			case string:
				err := errors.New(x)
				c.(*processorConf[T]).parent.err = fmt.Errorf("recover convertmessages: %w", err)
				if errorLog != nil {
					errorLog("recover convertmessages: %s\n", err)
				}
			default:
			}
			return
		}
	}()
	defer c.(*processorConf[T]).wg.Done()
	bc := 0

	for m := range c.(*processorConf[T]).parent.mChan {
		err := c.(*processorConf[T]).unmarshalFunc(m, c.(*processorConf[T]).s)
		if err != nil {
			log.Println("unmarshal ", err)
			c.(*processorConf[T]).parent.err = fmt.Errorf("unmarshal error: %w", err)
			if errorLog != nil {
				errorLog("quacfka: processing - unmarshal error: %v\n", err)
			}
			continue
		}
		bc++
		c.(*processorConf[T]).parent.Metrics.recordsProcessed.Add(1)
		if bc == 122880*c.(*processorConf[T]).parent.rowGroupSizeMultiplier {
			c.(*processorConf[T]).parent.rChan <- Record{Raw: c.(*processorConf[T]).s.NewRecord(),
				Norm: c.(*processorConf[T]).s.NewNormalizerRecord()}
			c.(*processorConf[T]).parent.rChanRecs.Add(1)
			if debugLog != nil {
				debugLog("quacfka: new arrow record - %d\n", c.(*processorConf[T]).parent.rChanRecs.Load())
			}
			bc = 0
		}
		select {
		case <-c.(*processorConf[T]).ctx.Done():
			break
		default:
		}
	}
	if bc != 0 {
		c.(*processorConf[T]).parent.rChan <- Record{Raw: c.(*processorConf[T]).s.NewRecord(),
			Norm: c.(*processorConf[T]).s.NewNormalizerRecord()}
		c.(*processorConf[T]).parent.rChanRecs.Add(1)
		if debugLog != nil {
			debugLog("quacfka: new arrow record - %d\n", len(c.(*processorConf[T]).parent.rChan))
		}
	}
	c.(*processorConf[T]).s.Release()
}
