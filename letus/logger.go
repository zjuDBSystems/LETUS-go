package letus

import "fmt"
// DefaultLogger is the logger impl.
type DefaultLogger struct{}

// Debug func
func (d DefaultLogger) Debug(v ...interface{}) {}

// Debugf func
func (d DefaultLogger) Debugf(format string, v ...interface{}) {}

// Info func
func (d DefaultLogger) Info(v ...interface{}) {}

// Infof func
func (d DefaultLogger) Infof(format string, v ...interface{}) {
	fmt.Printf(format, v ...)
}

// Notice func
func (d DefaultLogger) Notice(v ...interface{}) {}

// Noticef func
func (d DefaultLogger) Noticef(format string, v ...interface{}) {}

// Warning func
func (d DefaultLogger) Warning(v ...interface{}) {}

// Warningf func
func (d DefaultLogger) Warningf(format string, v ...interface{}) {}

// Error func
func (d DefaultLogger) Error(v ...interface{}) {}

// Errorf func
func (d DefaultLogger) Errorf(format string, v ...interface{}) {}

// Critical func
func (d DefaultLogger) Critical(v ...interface{}) {}

// Criticalf func
func (d DefaultLogger) Criticalf(format string, v ...interface{}) {}
