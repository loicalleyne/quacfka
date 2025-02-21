package quacfka

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	json "github.com/goccy/go-json"
	"github.com/spf13/cast"
)

type Metrics struct {
	numCPU                atomic.Int32
	runtimeOS             string
	kafkaMessagesConsumed atomic.Int64
	recordsProcessed      atomic.Int64
	recordsInserted       atomic.Int64
	customRecordsInserted atomic.Int64
	normRecordsInserted   atomic.Int64
	totalBytes            atomic.Int64
	recordBytes           atomic.Int64
	duckMetrics           duckMetrics

	maxMChanLen    atomic.Int32
	maxRChanLen    atomic.Int32
	benchStartTime time.Time
	benchEndTime   time.Time

	startTime        time.Time
	endTime          time.Time
	totalDuration    atomic.Int64 // nanoseconds
	kthroughput      atomic.Int64 // records per second * 100 (for two decimal places)
	pthroughput      atomic.Int64 // records per second * 100 (for two decimal places)
	throughput       atomic.Int64 // records per second * 100 (for two decimal places)
	customThroughput atomic.Int64 // records per second * 100 (for two decimal places)
	totalThroughput  atomic.Int64 // records per second * 100 (for two decimal places)
	throughputBytes  atomic.Int64 // bytes per second
	endTimeUnix      atomic.Int64
}

type duckMetrics struct {
	duckFileStart                 time.Time
	duckFileWriteDurationNanosAvg time.Duration
	duckFileDurations             []time.Duration
	duckFiles                     atomic.Int64
	duckFilesSizeMB               atomic.Int64
	duckFilesSizesMB              []int64
}

func (d *duckMetrics) duckStart() { d.duckFileStart = time.Now() }
func (d *duckMetrics) duckStop(dbSizeMB int64) {
	duration := time.Since(d.duckFileStart)
	if len(d.duckFileDurations) >= 100 {
		d.duckFileDurations = append(d.duckFileDurations[24:], duration)
		d.duckFilesSizesMB = append(d.duckFilesSizesMB[24:], dbSizeMB)
	} else {
		d.duckFileDurations = append(d.duckFileDurations, duration)
		d.duckFilesSizesMB = append(d.duckFilesSizesMB, dbSizeMB)
	}

	var sumDurations int64
	for _, d := range d.duckFileDurations {
		sumDurations = sumDurations + d.Nanoseconds()
	}
	d.duckFileWriteDurationNanosAvg = cast.ToDuration(sumDurations / int64(len(d.duckFileDurations)))
}

func (o *Orchestrator[T]) NewMetrics() {
	o.Metrics = new(Metrics)
	o.Metrics.numCPU.Store(int32(runtime.NumCPU()))
	o.Metrics.runtimeOS = runtime.GOOS
	o.Metrics.duckMetrics.duckFiles.Store(1)
}

func (o *Orchestrator[T]) StartMetrics() {
	if o.Metrics != nil {
		o.Metrics.startTime = time.Now()
		return
	}
	o.NewMetrics()
	o.StartMetrics()
}

func (o *Orchestrator[T]) ResetMetrics() {
	o.Metrics.kafkaMessagesConsumed.Store(0)
	o.Metrics.recordsProcessed.Store(0)
	o.Metrics.recordsInserted.Store(0)
	o.Metrics.customRecordsInserted.Store(0)
	o.Metrics.normRecordsInserted.Store(0)
	o.Metrics.totalBytes.Store(0)
	o.Metrics.recordBytes.Store(0)
	o.Metrics.duckMetrics.duckFiles.Store(0)
	o.Metrics.duckMetrics.duckFilesSizeMB.Store(0)
	o.Metrics.startTime = time.Now()
}

func (o *Orchestrator[T]) benchmark(ctx context.Context) {
	var (
		recordsConsumedSnapshot       int64
		recordsProcessedSnapshot      int64
		recordsInsertedSnapshot       int64
		customRecordsInsertedSnapshot int64
		normRecordsInsertedSnapshot   int64
		totalBytesSnapshot            int64
	)
	// Reset metrics every 24 hours
	go func() {
		for {
			delay := time.NewTimer(24 * time.Hour)
			select {
			case <-ctx.Done():
				return
			case <-delay.C:
				o.ResetMetrics()
			}
		}
	}()
	for {
		recordsConsumedSnapshot = o.Metrics.kafkaMessagesConsumed.Load()
		recordsProcessedSnapshot = o.Metrics.recordsProcessed.Load()
		recordsInsertedSnapshot = o.Metrics.recordsInserted.Load()
		customRecordsInsertedSnapshot = o.Metrics.customRecordsInserted.Load()
		normRecordsInsertedSnapshot = o.Metrics.normRecordsInserted.Load()
		totalBytesSnapshot = o.Metrics.totalBytes.Load()
		o.Metrics.benchStartTime = time.Now()
		if o.mChan != nil && o.Metrics.maxMChanLen.Load() < int32(len(o.mChan)) {
			o.Metrics.maxMChanLen.Store(int32(len(o.mChan)))
		}
		if o.rChan != nil && o.Metrics.maxRChanLen.Load() < int32(len(o.rChan)) {
			o.Metrics.maxRChanLen.Store(int32(len(o.rChan)))
		}
		delay := time.NewTimer(30 * time.Second)
		select {
		case <-ctx.Done():
			return
		case <-delay.C:
			o.Metrics.benchEndTime = time.Now()
			recordsConsumed := o.Metrics.kafkaMessagesConsumed.Load() - recordsConsumedSnapshot
			recordsProcessed := o.Metrics.recordsProcessed.Load() - recordsProcessedSnapshot
			recordsInserted := o.Metrics.recordsInserted.Load() - recordsInsertedSnapshot
			customRecordsInserted := o.Metrics.customRecordsInserted.Load() - customRecordsInsertedSnapshot
			normRecordsInserted := o.Metrics.normRecordsInserted.Load() - normRecordsInsertedSnapshot
			totalBytes := o.Metrics.totalBytes.Load() - totalBytesSnapshot
			duration := o.Metrics.benchEndTime.Sub(o.Metrics.benchStartTime)
			if duration > 0 {
				o.Metrics.kthroughput.Store(int64(float64(recordsConsumed) / duration.Seconds() * 100))
				o.Metrics.pthroughput.Store(int64(float64(recordsProcessed) / duration.Seconds() * 100))
				o.Metrics.throughput.Store(int64(float64(recordsInserted) / duration.Seconds() * 100))
				o.Metrics.customThroughput.Store(int64(float64(recordsInserted+customRecordsInserted) / duration.Seconds() * 100))
				o.Metrics.totalThroughput.Store(int64(float64(normRecordsInserted+recordsInserted) / duration.Seconds() * 100))
				o.Metrics.throughputBytes.Store(int64(float64(totalBytes) / duration.Seconds()))
			}
			if benchmarkLog != nil {
				if debugLog != nil {
					printMemUsage()
				}
				benchmarkLog("quacfka: benchmark\n%s", o.BenchmarksReport())
			}
			continue
		}
	}
}

func printMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if debugLog != nil {
		debugLog("\tAlloc = %v MiB", m.Alloc/1024/1024)
		debugLog("\tTotalAlloc = %v MiB", m.TotalAlloc/1024/1024)
		debugLog("\tSys = %v MiB", m.Sys/1024/1024)
		debugLog("\tNumGC = %v\n", m.NumGC)
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
	normRecordsInserted := o.Metrics.normRecordsInserted.Load()
	totalBytes := o.Metrics.totalBytes.Load()
	duration := o.Metrics.benchEndTime.Sub(o.Metrics.startTime)
	throughput := float64(o.Metrics.throughput.Load()) / 100
	totalThroughput := float64(o.Metrics.totalThroughput.Load()) / 100
	throughputBytes := o.Metrics.throughputBytes.Load()
	duckFiles := o.Metrics.duckMetrics.duckFiles.Load()
	duckFilesSize := o.Metrics.duckMetrics.duckFilesSizeMB.Load()
	return MetricsReport{
		NumCPU:             int(o.Metrics.numCPU.Load()),
		RuntimeOS:          o.Metrics.runtimeOS,
		KafkaClientCount:   o.KafkaClientCount(),
		ProcessorCount:     o.MsgProcessorsCount(),
		DuckConnCount:      o.DuckConnCount(),
		StartTime:          o.Metrics.startTime.Format(time.RFC3339),
		EndTime:            o.Metrics.benchEndTime.Format(time.RFC3339),
		Records:            formatLargeNumber(float64(recordsProcessed)),
		NormRecords:        formatLargeNumber(float64(normRecordsInserted)),
		DataTransferred:    formatBytes(totalBytes),
		Duration:           formatDuration(duration),
		RecordsPerSec:      formatThroughput(throughput),
		TotalRecordsPerSec: formatThroughput(totalThroughput),
		TransferRate:       formatThroughputBytes(float64(throughputBytes)),
		OutputFiles:        duckFiles,
		OutputFilesMB:      duckFilesSize,
		AvgDurationPerFile: formatDuration(o.Metrics.duckMetrics.duckFileWriteDurationNanosAvg),
	}
}

// UpdateMetrics calculates the total duration, throughput, and throughput in bytes.
func (o *Orchestrator[T]) UpdateMetrics() {
	o.Metrics.endTime = time.Now()
	o.Metrics.endTimeUnix.Store(o.Metrics.endTime.UnixNano())

	recordsConsumed := o.Metrics.kafkaMessagesConsumed.Load()
	recordsProcessed := o.Metrics.recordsProcessed.Load()
	recordsInserted := o.Metrics.recordsInserted.Load()
	customRecordsInserted := o.Metrics.customRecordsInserted.Load()
	normRecordsInserted := o.Metrics.normRecordsInserted.Load()
	totalBytes := o.Metrics.totalBytes.Load()

	duration := o.Metrics.endTime.Sub(o.Metrics.startTime)
	o.Metrics.totalDuration.Store(int64(duration))

	if duration > 0 {
		o.Metrics.kthroughput.Store(int64(float64(recordsConsumed) / duration.Seconds() * 100))
		o.Metrics.pthroughput.Store(int64(float64(recordsProcessed) / duration.Seconds() * 100))
		o.Metrics.throughput.Store(int64(float64(recordsInserted) / duration.Seconds() * 100))
		o.Metrics.totalThroughput.Store(int64(float64(recordsInserted+normRecordsInserted+customRecordsInserted) / duration.Seconds() * 100))
		o.Metrics.throughputBytes.Store(int64(float64(totalBytes) / duration.Seconds()))
	}
}

type MetricsReport struct {
	NumCPU             int    `json:"num_cpu"`
	RuntimeOS          string `json:"runtime_os"`
	KafkaClientCount   int    `json:"kafka_clients"`
	KafkaQueueCap      int    `json:"kafka_queue_cap"`
	ProcessorCount     int    `json:"processor_routines"`
	ArrowQueueCap      int    `json:"arrow_queue_cap"`
	DuckConnCount      int    `json:"duckdb_connections,omitzero"`
	CustomArrows       *int   `json:"custom_arrows,omitempty"`
	NormalizerFields   *int   `json:"normalizer_fields,omitempty"`
	StartTime          string `json:"start_time"`
	EndTime            string `json:"end_time,omitzero"`
	Records            string `json:"records"`
	NormRecords        string `json:"norm_records"`
	DataTransferred    string `json:"data_transferred"`
	Duration           string `json:"duration"`
	RecordsPerSec      string `json:"records_per_second"`
	TotalRecordsPerSec string `json:"total_rows_per_second"`
	TransferRate       string `json:"transfer_rate"`
	OutputFiles        int64  `json:"duckdb_files"`
	OutputFilesMB      int64  `json:"duckdb_files_MB"`
	AvgDurationPerFile string `json:"file_avg_duration,omitempty"`
}

type TypedMetricsReport struct {
	NumCPU             int     `json:"num_cpu"`
	RuntimeOS          string  `json:"runtime_os"`
	KafkaClientCount   int     `json:"kafka_clients"`
	KafkaQueueCap      int     `json:"kafka_queue_cap"`
	ProcessorCount     int     `json:"processor_routines"`
	ArrowQueueCap      int     `json:"arrow_queue_cap"`
	DuckConnCount      int     `json:"duckdb_connections,omitzero"`
	CustomArrows       *int    `json:"custom_arrows,omitempty"`
	NormalizerFields   *int    `json:"normalizer_fields,omitempty"`
	StartTime          string  `json:"start_time"`
	EndTime            string  `json:"end_time"`
	Records            int64   `json:"records"`
	NormRecords        int64   `json:"norm_records"`
	DataTransferred    int64   `json:"bytes_transferred"`
	Duration           int64   `json:"duration_nano"`
	RecordsPerSec      float64 `json:"records_per_second"`
	TotalRecordsPerSec float64 `json:"total_rows_per_second"`
	TransferRate       string  `json:"transfer_rate"`
	OutputFiles        int64   `json:"duckdb_files"`
	OutputFilesMB      int64   `json:"duckdb_files_MB"`
	AvgDurationPerFile float64 `json:"file_avg_duration,omitzero"`
}

func (o *Orchestrator[T]) generateMetricsReport() MetricsReport {
	var customArrows, normFieldCount *int
	recordsProcessed := o.Metrics.recordsProcessed.Load()
	normRecordsInserted := o.Metrics.normRecordsInserted.Load()
	totalBytes := o.Metrics.totalBytes.Load()
	duration := time.Duration(o.Metrics.totalDuration.Load())
	throughput := float64(o.Metrics.throughput.Load()) / 100
	totalThroughput := float64(o.Metrics.totalThroughput.Load()) / 100
	throughputBytes := o.Metrics.throughputBytes.Load()
	if len(o.opt.normalizerFieldStrings) > 0 {
		normFields := len(o.opt.normalizerFieldStrings)
		normFieldCount = &normFields
	}
	if len(o.opt.customArrow) > 0 {
		customArrowCount := len(o.opt.customArrow)
		customArrows = &customArrowCount
	}
	duckFiles := o.Metrics.duckMetrics.duckFiles.Load()
	duckFilesSize := o.Metrics.duckMetrics.duckFilesSizeMB.Load()

	return MetricsReport{
		NumCPU:             int(o.Metrics.numCPU.Load()),
		RuntimeOS:          o.Metrics.runtimeOS,
		KafkaClientCount:   o.KafkaClientCount(),
		KafkaQueueCap:      o.KafkaQueueCapacity(),
		ProcessorCount:     o.MsgProcessorsCount(),
		ArrowQueueCap:      o.ArrowQueueCapacity(),
		DuckConnCount:      o.DuckConnCount(),
		CustomArrows:       customArrows,
		NormalizerFields:   normFieldCount,
		StartTime:          o.Metrics.startTime.Format(time.RFC3339),
		EndTime:            o.Metrics.endTime.Format(time.RFC3339),
		Records:            formatLargeNumber(float64(recordsProcessed)),
		NormRecords:        formatLargeNumber(float64(normRecordsInserted)),
		DataTransferred:    formatBytes(totalBytes),
		Duration:           formatDuration(duration),
		RecordsPerSec:      formatThroughput(throughput),
		TotalRecordsPerSec: formatThroughput(totalThroughput),
		TransferRate:       formatThroughputBytes(float64(throughputBytes)),
		OutputFiles:        duckFiles,
		OutputFilesMB:      duckFilesSize,
		AvgDurationPerFile: formatDuration(o.Metrics.duckMetrics.duckFileWriteDurationNanosAvg),
	}
}

func (o *Orchestrator[T]) generateUnformatedMetricsReport() TypedMetricsReport {
	var customArrows, normFieldCount *int
	recordsProcessed := o.Metrics.recordsProcessed.Load()
	normRecordsInserted := o.Metrics.normRecordsInserted.Load()
	totalBytes := o.Metrics.totalBytes.Load()
	duration := time.Duration(o.Metrics.totalDuration.Load())
	throughput := float64(o.Metrics.throughput.Load()) / 100
	totalThroughput := float64(o.Metrics.totalThroughput.Load()) / 100
	throughputBytes := o.Metrics.throughputBytes.Load()
	if len(o.opt.normalizerFieldStrings) > 0 {
		normFields := len(o.opt.normalizerFieldStrings)
		normFieldCount = &normFields
	}
	if len(o.opt.customArrow) > 0 {
		customArrowCount := len(o.opt.customArrow)
		customArrows = &customArrowCount
	}
	duckFiles := o.Metrics.duckMetrics.duckFiles.Load()
	duckFilesSize := o.Metrics.duckMetrics.duckFilesSizeMB.Load()
	return TypedMetricsReport{
		NumCPU:             int(o.Metrics.numCPU.Load()),
		RuntimeOS:          o.Metrics.runtimeOS,
		KafkaClientCount:   o.KafkaClientCount(),
		KafkaQueueCap:      o.KafkaQueueCapacity(),
		ProcessorCount:     o.MsgProcessorsCount(),
		ArrowQueueCap:      o.ArrowQueueCapacity(),
		DuckConnCount:      o.DuckConnCount(),
		CustomArrows:       customArrows,
		NormalizerFields:   normFieldCount,
		StartTime:          o.Metrics.startTime.Format(time.RFC3339),
		EndTime:            time.Unix(0, o.Metrics.endTimeUnix.Load()).Format(time.RFC3339),
		Records:            recordsProcessed,
		NormRecords:        normRecordsInserted,
		DataTransferred:    totalBytes,
		Duration:           duration.Nanoseconds(),
		RecordsPerSec:      throughput,
		TotalRecordsPerSec: totalThroughput,
		TransferRate:       formatThroughputBytes(float64(throughputBytes)),
		OutputFiles:        duckFiles,
		OutputFilesMB:      duckFilesSize,
		AvgDurationPerFile: o.Metrics.duckMetrics.duckFileWriteDurationNanosAvg.Seconds(),
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
