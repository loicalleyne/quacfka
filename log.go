package quackfka

type Logger func(string, ...any)

var debugLog Logger = nil

func SetDebugLogger(logger Logger) {
	debugLog = logger
}

var errorLog Logger = nil

func SetErrorLogger(logger Logger) {
	errorLog = logger
}

var fatalLog Logger = nil

func SetFatalLogger(logger Logger) {
	errorLog = logger
}

var benchmarkLog Logger = nil

func SetBenchmarkLogger(logger Logger) {
	benchmarkLog = logger
}
