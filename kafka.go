package quacfka

import (
	"context"
	"crypto/tls"
	"encoding/binary"
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
	// Instance prefix
	InstancePrefix string
	// Message channel capacity, must be greater than 0. Default capacity is 122880.
	MsgChanCap int
	// MsgTimeAppend sets whether to append the Kafka message timestamp as an 8 byte uint64
	// at end of message bytes. It is the deserializing's function responsibility
	// to truncate these prior to reading the protobuf message.
	// Use `time.Milli(int64(binary.LittleEndian.Uint64(b)))` to retrieve the timestamp.
	MsgTimeAppend bool
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

// WithMessageCutConfluencePrefix removes 6 bytes that Confluence producer adds for schema registry metadata.
func WithMessageCutConfluencePrefix(m []byte) []byte {
	return m[6:]
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
				if !kc.parent.kafkaConf.MsgTimeAppend {
					kc.parent.MessageChanSend(m.Value)
				} else {
					b := make([]byte, 8)
					binary.LittleEndian.PutUint64(b, uint64(m.Timestamp.UnixMilli()))
					msg := append(m.Value, b...)
					kc.parent.MessageChanSend(msg)
				}

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
	defer o.MessageChanClose()
	var instancePrefix string
	if o.kafkaConf.InstancePrefix == "" {
		instancePrefix = "quacfka"
	} else {
		instancePrefix = o.kafkaConf.InstancePrefix
	}
	instanceIDPool := namepool.Pool(fmt.Sprintf("%s%%d", instancePrefix))
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
}

// MockKafka produces protobuf messages with random data in each field to the message channel.
// An initialized message with at least one field containing data must be passed as an argument
// for generation to work.
// Usage:
// wg.Add(1)
// go o.MockKafka(ctxT, &wg, &gen.RequestEvent{Id: "1233242423243"})
func (o *Orchestrator[T]) MockKafka(ctx context.Context, w *sync.WaitGroup, p T) {
	defer w.Done()

	var kg sync.WaitGroup
	o.mChan = make(chan []byte, o.kafkaConf.MsgChanCap)
	defer o.MessageChanClose()
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
				o.MessageChanSend(j)
			}
		}(ctx, &kg)
	}
	kg.Wait()
}
