package data

import (
	"bytes"
	"errors"
	"fmt"
)

var (
	ErrDataNotFound   = errors.New("data not found")
	ErrDataConflicted = errors.New("data conflicted")
	ErrNoAliveNodes   = errors.New("no alive nodes, stop dispatch")

	ErrMutexAlreadyUnlock = errors.New("mutex is already unlocked")
)

// Errors is a slice of errors
type Errors struct {
	errs []error
}

// Len returns the length of the errors slice
func (e *Errors) Len() int {
	return len(e.errs)
}

// Append appends an error to the errors slice
func (e *Errors) Append(err error) {
	e.errs = append(e.errs, err)
}

// Error
func (e *Errors) Error() string {
	buf := &bytes.Buffer{}
	for i, e := range e.errs {
		buf.WriteString(fmt.Sprintf("err[%d]: %s \n", i, e))
	}
	return buf.String()
}
