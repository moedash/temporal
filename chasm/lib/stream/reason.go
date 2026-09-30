package stream

import (
	"fmt"
	"strings"

	"go.temporal.io/api/serviceerror"
)

// Reason tokens a refusal carries in its message prefix. See the package doc.
const (
	ReasonProducerConflict      = "STREAM_PRODUCER_CONFLICT"
	ReasonProducerStaleSequence = "STREAM_PRODUCER_STALE_SEQUENCE"
	ReasonCursorBelowFloor      = "STREAM_CURSOR_BELOW_FLOOR"
	ReasonStreamClosed          = "STREAM_CLOSED"
)

const reasonSeparator = ": "

// Refusal is a FailedPrecondition whose message begins with a reason token, so
// a client maps it to a typed error without matching on the prose after it.
func Refusal(reason string, format string, args ...any) error {
	return serviceerror.NewFailedPrecondition(
		reason + reasonSeparator + fmt.Sprintf(format, args...))
}

// ReasonOf returns the reason token a refusal message begins with, or the
// empty string for a message that carries none.
func ReasonOf(message string) string {
	token, _, found := strings.Cut(message, reasonSeparator)
	if !found {
		return ""
	}
	switch token {
	case ReasonProducerConflict, ReasonProducerStaleSequence, ReasonCursorBelowFloor,
		ReasonStreamClosed:
		return token
	default:
		return ""
	}
}
