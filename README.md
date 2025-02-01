Quacfka 🏹🦆
===================
[![Go Reference](https://pkg.go.dev/badge/github.com/loicalleyne/quacfka.svg)](https://pkg.go.dev/github.com/loicalleyne/quacfka)

Go library to stream Kafka protobuf messages to DuckDB.
Uses generics. Use your protobuf message as a type parameter to autogenerate an Arrow schema, provide a protobuf unmarshaling func, and stream data into DuckDB with a very high throughput.

## Features
### Arrow schema generation from a protobuf message type parameter
- Converts a proto.Message into an Apache Arrow schema
	- Supports nested types 
### Configurable loggers
- Set Debug, Error, and benchmark loggers

## 🚀 Install

Using Quacfka is easy. First, use `go get` to install the latest version
of the library.

```sh
go get -u github.com/loicalleyne/quacfka@latest
```

## 💡 Usage

You can import `quacfka` using:

```go
import "github.com/loicalleyne/quacfka"
```

Create a new Orchestrator, configure the Kafka client, processing and DuckDB, then Run().
Kafka client can be configured with a slice of `franz-go/pkg/kgo.Opt` or SASL user/pass auth.
```go
    o, err := q.NewOrchestrator[*your.CustomProtoMessageType]()
	if err != nil {
		panic(err)
	}
	defer o.Close()
	q.SetDebugLogger(log.Printf)
	q.SetErrorLogger(log.Printf)
	q.SetFatalLogger(log.Fatalf)
	q.SetBenchmarkLogger(log.Printf)
    k := o.NewKafkaConfig()
	k.ClientCount.Store(int32(*kafkaRoutines))
	k.MsgChanCap = 122880 * 5
	k.ConsumerGroup = os.Getenv("CONSUMER_GROUP")
	k.Seeds = append(k.Seeds, os.Getenv("KAFKA_SEED"))
	k.User = os.Getenv("KAFKA_USER")
	k.Password = os.Getenv("KAFKA_PW")
	k.Munger = messageMunger
	k.Topic = "kafka.topic01"
    // Tune record channel capacity, row group size, number of processing routines, set custom unmarshal func
	err = o.ConfigureProcessor(*duckRoutines*3, 1, *routines, customProtoUnmarshal)
	if err != nil {
		log.Println(err)
		panic(err)
	}
	err = o.ConfigureDuck(q.WithPath("duck.db"), q.WithDriverPath("/usr/local/lib/libduckdb.so"), q.WithDestinationTable("mytable"), q.WithDuckConnections(*duckRoutines))
	if err != nil {
		panic(err)
	}
	wg.Add(1)
	// Run with DuckDB file rotation
	go o.Run(ctxT, &wg, q.WithFileRotateThresholdMB(5000))
	// Get chan string of closed, rotated DuckDB files
	duckFiles := o.DuckPaths()
	...
	// Query duckdb files to aggregate, activate alerts, etc...
	...
	wg.Wait()
	// Check for processing errors
	if o.Error() != nil {
		log.Println(err)
	}
	// Print pipeline metrics
	log.Printf("%v\n", o.Report())
...
func customProtoUnmarshal(m []byte, s any) error {
	newMessage := rr.BidRequestEventFromVTPool()
	err := newMessage.UnmarshalVTUnsafe(m)
	if err != nil {
		return err
	}
	// Assert s to `*bufarrow.Schema[*your.CustomProtoMessageType]`
	s.(*bufarrow.Schema[*your.CustomProtoMessageType]).Append(newMessage)
	newMessage.ReturnToVTPool()
	return nil
}

func messageMunger(m []byte) []byte {
	return m[6:]
}
// {
//   "start_time": "2025-01-30T23:59:33Z",
//   "end_time": "2025-01-31T00:04:09Z",
//   "records": "152_838_106.00",
//   "data_transferred": "176.14 GB",
//   "duration": "4m36.229s",
//   "records_per_second": "553_301.35",
//   "transfer_rate": "652.95 MB/second"
// }
```
Generate random data to emulate the Kafka topic
```go
	wg.Add(1)
	// Instantiate a sample proto.Message to provide a description,
	// random data will be generated for all fields.
	go o.MockKafka(ctxT, &wg, &your.CustomProtoMessageType{Id: "1233242423243"})
	wg.Add(1)
	// WithFileRotateThresholdMB specifies a file rotation threshold target in MB (not very accurate yet)
	go o.Run(ctxT, &wg, q.WithoutKafka(), q.WithFileRotateThresholdMB(250))
	wg.Wait()
```

## 💫 Show your support

Give a ⭐️ if this project helped you!
Feedback and PRs welcome.

## Licence

Quacfka is released under the Apache 2.0 license. See [LICENCE.txt](LICENCE.txt)