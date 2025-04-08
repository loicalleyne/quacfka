module github.com/loicalleyne/quacfka

go 1.23.3

retract (
	v0.2.2
	v0.2.1
	v0.2.0
	v0.1.1
	v0.1.0
)

replace github.com/loicalleyne/bufarrow => ../bufarrow

require (
	github.com/SAP/go-dblib v0.0.0-20230911064405-b779cef8f299
	github.com/apache/arrow-go/v18 v18.2.0
	github.com/goccy/go-json v0.10.5
	github.com/joho/godotenv v1.5.1
	github.com/loicalleyne/bufarrow v0.5.1
	github.com/loicalleyne/couac v0.5.3
	github.com/loicalleyne/protorand v0.0.0-20250201052828-ef9589bb5a9a
	github.com/panjf2000/ants/v2 v2.11.2
	github.com/spf13/cast v1.7.1
	github.com/twmb/franz-go v1.18.1
	google.golang.org/protobuf v1.36.6
)

require (
	github.com/andybalholm/brotli v1.1.1 // indirect
	github.com/apache/arrow-adbc/go/adbc v1.5.0 // indirect
	github.com/apache/thrift v0.21.0 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/flatbuffers v25.2.10+incompatible // indirect
	github.com/jhump/protoreflect/v2 v2.0.0-beta.2 // indirect
	github.com/klauspost/asmfmt v1.3.2 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/minio/asm2plan9s v0.0.0-20200509001527-cdd76441f9d8 // indirect
	github.com/minio/c2goasm v0.0.0-20190812172519-36a3d3bbc4f3 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/sryoya/protorand v0.0.0-20250114120907-8c1a8e3138f2 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.9.0 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	go.opentelemetry.io/proto/otlp v1.5.0 // indirect
	golang.org/x/exp v0.0.0-20250207012021-f9890c6ad9f3 // indirect
	golang.org/x/mod v0.23.0 // indirect
	golang.org/x/net v0.35.0 // indirect
	golang.org/x/sync v0.11.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.22.0 // indirect
	golang.org/x/tools v0.30.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250219182151-9fdb1cabc7b2 // indirect
	google.golang.org/grpc v1.71.0 // indirect
)
