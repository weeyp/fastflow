package log

import (
	"fmt"
	"log"
	"os"
)

var defLog Logger = &StdoutLogger{}

// SetLogger for testing
func SetLogger(log Logger) {
	defLog = log
}

// StdoutLogger for testing
type StdoutLogger struct {
}

// Debug debug log to stdout
func (s *StdoutLogger) Debug(msg string, fields ...interface{}) {
	log.Println("debug:", msg, fields)
}

// Debugf debug log to stdout format
func (s *StdoutLogger) Debugf(msg string, args ...interface{}) {
	log.Println("debug:", fmt.Sprintf(msg, args...))
}

// Info log to stdout
func (s *StdoutLogger) Info(msg string, fields ...interface{}) {
	log.Println("info:", msg, fields)
}

// Infof log to stdout format
func (s *StdoutLogger) Infof(msg string, args ...interface{}) {
	log.Println("info:", fmt.Sprintf(msg, args...))
}

// Warn log to stdout
func (s *StdoutLogger) Warn(msg string, fields ...interface{}) {
	log.Println("warn:", msg, fields)
}

// Warnf log to stdout format
func (s *StdoutLogger) Warnf(msg string, args ...interface{}) {
	log.Println("warn:", fmt.Sprintf(msg, args...))
}

// Error log to stdout
func (s *StdoutLogger) Error(msg string, fields ...interface{}) {
	log.Println("error:", msg, fields)
}

// Errorf log to stdout format
func (s *StdoutLogger) Errorf(msg string, args ...interface{}) {
	log.Println("error:", fmt.Sprintf(msg, args...))
}

// Fatal log to stdout
func (s *StdoutLogger) Fatal(msg string, fields ...interface{}) {
	log.Println("fatal:", msg, fields)
	os.Exit(1)
}

// Fatalf log to stdout format
func (s *StdoutLogger) Fatalf(msg string, args ...interface{}) {
	log.Println("fatal:", fmt.Sprintf(msg, args...))
	os.Exit(1)
}

// Logger interface
type Logger interface {
	Debug(msg string, fields ...interface{})
	Debugf(msg string, args ...interface{})
	Info(msg string, fields ...interface{})
	Infof(msg string, args ...interface{})
	Warn(msg string, fields ...interface{})
	Warnf(msg string, args ...interface{})
	Error(msg string, fields ...interface{})
	Errorf(msg string, args ...interface{})
	Fatal(msg string, fields ...interface{})
	Fatalf(msg string, args ...interface{})
}

// Debug log to stdout
func Debug(msg string, fields ...interface{}) {
	defLog.Debug(msg, fields...)
}

// Debugf log to stdout format
func Debugf(msg string, args ...interface{}) {
	defLog.Debugf(msg, args...)
}

// Info log to stdout
func Info(msg string, fields ...interface{}) {
	defLog.Info(msg, fields...)
}

// Infof log to stdout format
func Infof(msg string, args ...interface{}) {
	defLog.Infof(msg, args...)
}

// Warn log to stdout
func Warn(msg string, fields ...interface{}) {
	defLog.Warn(msg, fields...)
}

// Warnf log to stdout format
func Warnf(msg string, args ...interface{}) {
	defLog.Warnf(msg, args...)
}

// Error log to stdout
func Error(msg string, fields ...interface{}) {
	defLog.Error(msg, fields...)
}

// Errorf log to stdout format
func Errorf(msg string, args ...interface{}) {
	defLog.Errorf(msg, args...)
}

// Fatal log to stdout
func Fatal(msg string, fields ...interface{}) {
	defLog.Fatal(msg, fields...)
}

// Fatalf log to stdout format
func Fatalf(msg string, args ...interface{}) {
	defLog.Fatalf(msg, args...)
}
