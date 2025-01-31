package quacfka

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	json "github.com/goccy/go-json"
)

type Metrics struct {
	kafkaMessagesConsumed atomic.Int64
	recordsProcessed      atomic.Int64
	recordsInserted       atomic.Int64
	totalBytes            atomic.Int64

	maxMChanLen    atomic.Int32
	maxRChanLen    atomic.Int32
	benchStartTime time.Time
	benchEndTime   time.Time

	startTime       time.Time
	endTime         time.Time
	totalDuration   atomic.Int64 // nanoseconds
	kthroughput     atomic.Int64 // records per second * 100 (for two decimal places)
	pthroughput     atomic.Int64 // records per second * 100 (for two decimal places)
	throughput      atomic.Int64 // records per second * 100 (for two decimal places)
	throughputBytes atomic.Int64 // bytes per second
	endTimeUnix     atomic.Int64
}

func (o *Orchestrator[T]) NewMetrics() {
	o.Metrics = new(Metrics)
}

func (o *Orchestrator[T]) StartMetrics() {
	if o.Metrics != nil {
		o.Metrics.startTime = time.Now()
		return
	}
	o.NewMetrics()
	o.StartMetrics()
}

func (o *Orchestrator[T]) benchmark(ctx context.Context) {
	var recordsConsumedSnapshot, recordsProcessedSnapshot, recordsInsertedSnapshot, totalBytesSnapshot int64
	for {
		recordsConsumedSnapshot = o.Metrics.kafkaMessagesConsumed.Load()
		recordsProcessedSnapshot = o.Metrics.recordsProcessed.Load()
		recordsInsertedSnapshot = o.Metrics.recordsInserted.Load()
		totalBytesSnapshot = o.Metrics.totalBytes.Load()
		o.Metrics.benchStartTime = time.Now()
		if o.mChan != nil && o.Metrics.maxMChanLen.Load() < int32(len(o.mChan)) {
			o.Metrics.maxMChanLen.Store(int32(len(o.mChan)))
		}
		if o.rChan != nil && o.Metrics.maxRChanLen.Load() < int32(len(o.rChan)) {
			o.Metrics.maxRChanLen.Store(int32(len(o.rChan)))
		}
		delay := time.NewTimer(10 * time.Second)
		select {
		case <-ctx.Done():
			return
		case <-delay.C:
			o.Metrics.benchEndTime = time.Now()
			recordsConsumed := o.Metrics.kafkaMessagesConsumed.Load() - recordsConsumedSnapshot
			recordsProcessed := o.Metrics.recordsProcessed.Load() - recordsProcessedSnapshot
			recordsInserted := o.Metrics.recordsInserted.Load() - recordsInsertedSnapshot
			totalBytes := o.Metrics.totalBytes.Load() - totalBytesSnapshot
			duration := o.Metrics.benchEndTime.Sub(o.Metrics.benchStartTime)
			if duration > 0 {
				o.Metrics.kthroughput.Store(int64(float64(recordsConsumed) / duration.Seconds() * 100))
				o.Metrics.pthroughput.Store(int64(float64(recordsProcessed) / duration.Seconds() * 100))
				o.Metrics.throughput.Store(int64(float64(recordsInserted) / duration.Seconds() * 100))
				o.Metrics.throughputBytes.Store(int64(float64(totalBytes) / duration.Seconds()))
			}
			if benchmarkLog != nil {
				benchmarkLog("quacfka: benchmark\n%s", o.BenchmarksReport())
			}
			continue
		}
	}
}

func (o *Orchestrator[T]) BenchmarksReport() string {
	report := o.generateBenchmarksReport()
	jsonData, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Sprintf("Error generating report: %v", err)
	}
	return string(jsonData)
}

func (o *Orchestrator[T]) generateBenchmarksReport() MetricsReport {
	recordsProcessed := o.Metrics.recordsProcessed.Load()
	totalBytes := o.Metrics.totalBytes.Load()
	duration := o.Metrics.benchEndTime.Sub(o.Metrics.startTime)
	throughput := float64(o.Metrics.throughput.Load()) / 100
	throughputBytes := o.Metrics.throughputBytes.Load()

	return MetricsReport{
		KafkaClientCount: o.KafkaClientCount(),
		ProcessorCount:   o.MsgProcessorsCount(),
		DuckConnCount:    o.DuckConnCount(),
		StartTime:        o.Metrics.startTime.Format(time.RFC3339),
		EndTime:          o.Metrics.benchEndTime.Format(time.RFC3339),
		Records:          formatLargeNumber(float64(recordsProcessed)),
		DataTransferred:  formatBytes(totalBytes),
		Duration:         formatDuration(duration),
		RecordsPerSec:    formatThroughput(throughput),
		TransferRate:     formatThroughputBytes(float64(throughputBytes)),
	}
}

// UpdateMetrics calculates the total duration, throughput, and throughput in bytes.
func (o *Orchestrator[T]) UpdateMetrics() {
	o.Metrics.endTime = time.Now()
	o.Metrics.endTimeUnix.Store(o.Metrics.endTime.UnixNano())

	recordsConsumed := o.Metrics.kafkaMessagesConsumed.Load()
	recordsProcessed := o.Metrics.recordsProcessed.Load()
	recordsInserted := o.Metrics.recordsInserted.Load()
	totalBytes := o.Metrics.totalBytes.Load()

	duration := o.Metrics.endTime.Sub(o.Metrics.startTime)
	o.Metrics.totalDuration.Store(int64(duration))
	if duration > 0 {
		o.Metrics.kthroughput.Store(int64(float64(recordsConsumed) / duration.Seconds() * 100))
		o.Metrics.pthroughput.Store(int64(float64(recordsProcessed) / duration.Seconds() * 100))
		o.Metrics.throughput.Store(int64(float64(recordsInserted) / duration.Seconds() * 100))
		o.Metrics.throughputBytes.Store(int64(float64(totalBytes) / duration.Seconds()))
	}
}

type MetricsReport struct {
	KafkaClientCount int    `json:"kafka_clients"`
	ProcessorCount   int    `json:"processor_routines"`
	DuckConnCount    int    `json:"duckdb_connections"`
	StartTime        string `json:"start_time"`
	EndTime          string `json:"end_time"`
	Records          string `json:"records"`
	DataTransferred  string `json:"data_transferred"`
	Duration         string `json:"duration"`
	RecordsPerSec    string `json:"records_per_second"`
	TransferRate     string `json:"transfer_rate"`
}

type TypedMetricsReport struct {
	KafkaClientCount int     `json:"kafka_clients"`
	ProcessorCount   int     `json:"processor_routines"`
	DuckConnCount    int     `json:"duckdb_connections"`
	StartTime        string  `json:"start_time"`
	EndTime          string  `json:"end_time"`
	Records          int64   `json:"records"`
	DataTransferred  int64   `json:"bytes_transferred"`
	Duration         int64   `json:"duration_nano"`
	RecordsPerSec    float64 `json:"records_per_second"`
	TransferRate     string  `json:"transfer_rate"`
}

func (o *Orchestrator[T]) generateMetricsReport() MetricsReport {
	recordsProcessed := o.Metrics.recordsProcessed.Load()
	totalBytes := o.Metrics.totalBytes.Load()
	duration := time.Duration(o.Metrics.totalDuration.Load())
	throughput := float64(o.Metrics.throughput.Load()) / 100
	throughputBytes := o.Metrics.throughputBytes.Load()

	return MetricsReport{
		KafkaClientCount: o.KafkaClientCount(),
		ProcessorCount:   o.MsgProcessorsCount(),
		DuckConnCount:    o.DuckConnCount(),
		StartTime:        o.Metrics.startTime.Format(time.RFC3339),
		EndTime:          time.Unix(0, o.Metrics.endTimeUnix.Load()).Format(time.RFC3339),
		Records:          formatLargeNumber(float64(recordsProcessed)),
		DataTransferred:  formatBytes(totalBytes),
		Duration:         formatDuration(duration),
		RecordsPerSec:    formatThroughput(throughput),
		TransferRate:     formatThroughputBytes(float64(throughputBytes)),
	}
}

func (o *Orchestrator[T]) generateUnformatedMetricsReport() TypedMetricsReport {
	recordsProcessed := o.Metrics.recordsProcessed.Load()
	totalBytes := o.Metrics.totalBytes.Load()
	duration := time.Duration(o.Metrics.totalDuration.Load())
	throughput := float64(o.Metrics.throughput.Load()) / 100
	throughputBytes := o.Metrics.throughputBytes.Load()

	return TypedMetricsReport{
		KafkaClientCount: o.KafkaClientCount(),
		ProcessorCount:   o.MsgProcessorsCount(),
		DuckConnCount:    o.DuckConnCount(),
		StartTime:        o.Metrics.startTime.Format(time.RFC3339),
		EndTime:          time.Unix(0, o.Metrics.endTimeUnix.Load()).Format(time.RFC3339),
		Records:          recordsProcessed, // Format records
		DataTransferred:  totalBytes,
		Duration:         duration.Nanoseconds(),
		RecordsPerSec:    throughput,
		TransferRate:     formatThroughputBytes(float64(throughputBytes)),
	}
}

// Report generates a summary of the collected Metrics
func (o *Orchestrator[T]) ReportJSONL() string {
	var b strings.Builder
	report := o.generateUnformatedMetricsReport()
	jsonData, err := json.Marshal(report)
	if err != nil {
		return fmt.Sprintf("Error generating report: %v", err)
	}
	fmt.Fprintf(&b, "%s\n", string(jsonData))
	return b.String()
}

// Report generates a summary of the collected Metrics
func (o *Orchestrator[T]) Report() string {
	report := o.generateMetricsReport()
	jsonData, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Sprintf("Error generating report: %v", err)
	}
	return string(jsonData)
}

func (o *Orchestrator[T]) byteCounter(b []byte) []byte {
	o.Metrics.totalBytes.Add(int64(len(b)))
	return b
}
