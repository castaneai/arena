package arena

import (
	"errors"
	"fmt"
)

type ErrorStatus string

const (
	ErrorStatusUnknown           ErrorStatus = "unknown"
	ErrorStatusNotFound          ErrorStatus = "not_found"
	ErrorStatusResourceExhausted ErrorStatus = "resource_exhausted"
	ErrorStatusInvalidRequest    ErrorStatus = "invalid_request"
)

type Error struct {
	Status ErrorStatus
	err    error
}

func NewError(status ErrorStatus, err error) *Error {
	return &Error{err: err, Status: status}
}

func (e *Error) Error() string {
	return fmt.Errorf("arena error(status: %s): %w", e.Status, e.err).Error()
}

func ErrorHasStatus(target error, status ErrorStatus) bool {
	var e *Error
	if errors.As(target, &e) {
		return e.Status == status
	}
	return false
}
