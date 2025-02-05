package quacfka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SAP/go-dblib/namepool"
	_ "github.com/joho/godotenv/autoload"
	"github.com/loicalleyne/protorand"
	"github.com/panjf2000/ants/v2"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"google.golang.org/protobuf/proto"
)

type KafkaClientConf[T proto.Message] struct {
	parent *Orchestrator[T]
	ctx    context.Context
	// franz-go/pkg/kgo.Opt configurations. If any are set, these will override all
	//  of the subsequently listed client settings.
	ClientConf []kgo.Opt
	// Number of Kafka clients to open. Default value is 1. If using more than one
	// use of a consumer group is recommended.
	ClientCount atomic.Int32
	// Consumer group
	ConsumerGroup string
	// Message channel capacity, must be greater than 0. Default capacity is 122880.
	MsgChanCap int
	// Function to munge message bytes prior to deserialization.
	// As an example, Confluent java client adds 6 magic bytes at
	// beginning of message data for use with Schema Registry which must
	// be removed from the message prior to deserialization.
	Munger func([]byte) []byte
	// Kafka TLS dialer
	TlsDialer *tls.Dialer
	// Kafka topic to consume
	Topic string
	// Kafka seed brokers
	Seeds []string
	// SASL Auth User
	User string
	// SASL Auth password
	Password   string
	wg         *sync.WaitGroup
	instanceID string
}

type kafkaJob[T proto.Message] struct {
	parent     *Orchestrator[T]
	ctx        context.Context
	wg         *sync.WaitGroup
	instanceID string
}

func (o *Orchestrator[T]) newKafkaJob() *kafkaJob[T] {
	k := new(kafkaJob[T])
	k.parent = o
	return k
}

func (o *Orchestrator[T]) NewKafkaConfig() *KafkaClientConf[T] {
	o.kafkaConf = new(KafkaClientConf[T])
	o.kafkaConf.parent = o
	o.kafkaConf.ClientCount.Store(1)
	o.kafkaConf.MsgChanCap = 122880
	return o.kafkaConf
}

func consumeKafka[T proto.Message](k any) {
	var (
		cl  *kgo.Client
		err error
	)
	kc := k.(*kafkaJob[T])
	defer kc.wg.Done()
	if len(kc.parent.kafkaConf.ClientConf) > 0 {
		cl, err = kgo.NewClient(kc.parent.kafkaConf.ClientConf...)
		if err != nil {
			kc.parent.err = fmt.Errorf("kafka client error:%w", err)
			if errorLog != nil {
				errorLog("quacfka: new kafka client: %v\n", err)
			}
			return
		}
	} else {
		var opts []kgo.Opt
		if kc.parent.kafkaConf.ConsumerGroup != "" {
			opts = append(opts, kgo.ConsumerGroup(kc.parent.kafkaConf.ConsumerGroup))
		}
		tlsDialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}}
		opts = append(opts, kgo.SeedBrokers(kc.parent.kafkaConf.Seeds...),
			kgo.InstanceID(kc.instanceID),
			kgo.ConsumeTopics(kc.parent.kafkaConf.Topic),
			kgo.Dialer(tlsDialer.DialContext),
			kgo.SASL(plain.Auth{User: kc.parent.kafkaConf.User, Pass: kc.parent.kafkaConf.Password}.AsMechanism()))
		cl, err = kgo.NewClient(opts...)
		if err != nil {
			kc.parent.err = fmt.Errorf("kafka client error:%w", err)
			if errorLog != nil {
				errorLog("quacfka: new kafka client: %v\n", err)
			}
			return
		}
	}
	defer cl.Close()
	ctx := context.Background()
	for {
		select {
		case <-kc.ctx.Done():
			return
		default:
			fetches := cl.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				// All errors are retried internally when fetching, but non-retriable errors are
				// returned from polls so that users can notice and take action.
				log.Printf("fetches: %v\n", errs)
				for _, e := range errs {
					if strings.Contains(e.Err.Error(), "context") {
						if errorLog != nil {
							errorLog("quacfka: kafka client context: %v\n", e)
						}
						return
					}
				}
			}
			iter := fetches.RecordIter()
			for !iter.Done() {
				m := iter.Next()
				if kc.parent.kafkaConf.Munger != nil {
					kc.parent.mChan <- kc.parent.byteCounter(kc.parent.kafkaConf.Munger(m.Value))
				} else {
					kc.parent.mChan <- kc.parent.byteCounter(m.Value)
				}
				kc.parent.Metrics.kafkaMessagesConsumed.Add(1)
				select {
				case <-kc.ctx.Done():
					return
				default:
				}
			}
		}
	}
}

func (o *Orchestrator[T]) startKafka(ctx context.Context, w *sync.WaitGroup) {
	defer func() {
		e := recover()
		if e != nil {
			switch x := e.(type) {
			case error:
				err := x
				o.err = fmt.Errorf("recover startkafka: %w", err)
				if errorLog != nil {
					errorLog("recover startkafka: %v\n", err)
				}
			case string:
				err := errors.New(x)
				o.err = fmt.Errorf("recover startkafka: %w", err)
				if errorLog != nil {
					errorLog("recover startkafka: %s\n", err)
				}
			default:
			}
			if errorLog != nil {
				errorLog("recover startKafka: %v\n", e)
			}
			return
		}
	}()
	defer w.Done()
	defer close(o.mChan)
	instanceIDPool := namepool.Pool("quacfka%d")
	kpool, _ := ants.NewPoolWithFunc(int(o.kafkaConf.ClientCount.Load()), consumeKafka[T], ants.WithPreAlloc(true))
	defer kpool.Release()

	var kwg sync.WaitGroup
	for kpool.Running() < o.KafkaClientCount() && ctx.Err() == nil {
		kwg.Add(1)
		k := o.newKafkaJob()
		k.ctx = ctx
		k.wg = &kwg
		k.instanceID = instanceIDPool.Acquire().Name()
		kpool.Invoke(k)
		if debugLog != nil {
			debugLog("quacfka: kpool size: %d\n", kpool.Running())
		}
		select {
		case <-ctx.Done():
			if debugLog != nil {
				debugLog("quacfka: main kafka context done\n")
			}
			return
		default:
		}
	}
	kwg.Wait()
	if debugLog != nil {
		debugLog("quacfka: closing mChan: %v recs\n", o.Metrics.kafkaMessagesConsumed.Load())
	}
	o.mChanClosed = true
}

func (o *Orchestrator[T]) MockKafka(ctx context.Context, w *sync.WaitGroup, p T) {
	defer w.Done()

	var kg sync.WaitGroup
	o.mChan = make(chan []byte, o.kafkaConf.MsgChanCap)
	defer close(o.mChan)
	for i := 0; i < 10; i++ {
		kg.Add(1)
		go func(ctx context.Context, g *sync.WaitGroup) {
			defer g.Done()
			pr := protorand.New()
			pr.Seed(time.Now().UnixMicro())
			for ctx.Err() == nil {
				m, err := pr.Gen(p)
				if err != nil {
					panic(err)
				}
				j, err := proto.Marshal(m.(T))
				if err != nil {
					panic(err)
				}
				o.mChan <- o.byteCounter(j)
				o.Metrics.kafkaMessagesConsumed.Add(1)
			}
		}(ctx, &kg)
	}
	kg.Wait()
	o.mChanClosed = true
}
