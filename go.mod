module github.com/loicalleyne/quacfka

go 1.24.0

toolchain go1.24.1

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
	github.com/apache/arrow-go/v18 v18.5.0
	github.com/goccy/go-json v0.10.5
	github.com/joho/godotenv v1.5.1
	github.com/loicalleyne/bufarrow v0.5.1
	github.com/loicalleyne/couac v0.5.3
	github.com/loicalleyne/protorand v0.0.0-20250201052828-ef9589bb5a9a
	github.com/panjf2000/ants/v2 v2.11.3
	github.com/spf13/cast v1.10.0
	github.com/twmb/franz-go v1.20.4
	google.golang.org/protobuf v1.36.10
)

require (
	github.com/andybalholm/brotli v1.2.0 // indirect
	github.com/apache/arrow-adbc/go/adbc v1.5.0 // indirect
	github.com/apache/thrift v0.22.0 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/flatbuffers v25.9.23+incompatible // indirect
	github.com/jhump/protoreflect/v2 v2.0.0-beta.2 // indirect
	github.com/klauspost/asmfmt v1.3.2 // indirect
	github.com/klauspost/compress v1.18.2 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/minio/asm2plan9s v0.0.0-20200509001527-cdd76441f9d8 // indirect
	github.com/minio/c2goasm v0.0.0-20190812172519-36a3d3bbc4f3 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/sryoya/protorand v0.0.0-20250114120907-8c1a8e3138f2 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.12.0 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	go.opentelemetry.io/proto/otlp v1.5.0 // indirect
	golang.org/x/exp v0.0.0-20250408133849-7e4ce0ab07d0 // indirect
	golang.org/x/mod v0.30.0 // indirect
	golang.org/x/net v0.47.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/telemetry v0.0.0-20251111182119-bc8e575c7b54 // indirect
	golang.org/x/text v0.31.0 // indirect
	golang.org/x/tools v0.39.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251022142026-3a174f9686a8 // indirect
	google.golang.org/grpc v1.77.0 // indirect
)
