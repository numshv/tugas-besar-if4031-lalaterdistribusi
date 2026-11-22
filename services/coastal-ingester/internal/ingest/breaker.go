package ingest

import (
	"errors"

	"github.com/failsafe-go/failsafe-go/bulkhead"
	"github.com/failsafe-go/failsafe-go/circuitbreaker"
)

func isBreakerOpen(err error) bool {
	return errors.Is(err, circuitbreaker.ErrOpen) || errors.Is(err, bulkhead.ErrFull)
}
