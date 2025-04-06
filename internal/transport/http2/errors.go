package http2

import (
	"errors"
	"fmt"

	"golang.org/x/net/http2"
)

func streamError(id uint32, code http2.ErrCode) http2.StreamError {
	return http2.StreamError{StreamID: id, Code: code}
}

// 6.9.1 The Flow Control Window
// "If a sender receives a WINDOW_UPDATE that causes a flow control
// window to exceed this maximum it MUST terminate either the stream
// or the connection, as appropriate. For streams, [...]; for the
// connection, a GOAWAY frame with a FLOW_CONTROL_ERROR code."
type goAwayFlowError struct{}

func (goAwayFlowError) Error() string { return "connection exceeded flow control window size" }

// connError represents an HTTP/2 ConnectionError error code, along
// with a string (for debugging) explaining why.
//
// Errors of this type are only returned by the frame parser functions
// and converted into ConnectionError(Code), after stashing away
// the Reason into the Framer's errDetail field, accessible via
// the (*Framer).ErrorDetail method.
type connError struct {
	Code   http2.ErrCode // the ConnectionError error code
	Reason string        // additional reason
}

func (e connError) Error() string {
	return fmt.Sprintf("http2: connection error: %v: %v", e.Code, e.Reason)
}

type pseudoHeaderError string

func (e pseudoHeaderError) Error() string {
	return fmt.Sprintf("invalid pseudo-header %q", string(e))
}

type duplicatePseudoHeaderError string

func (e duplicatePseudoHeaderError) Error() string {
	return fmt.Sprintf("duplicate pseudo-header %q", string(e))
}

type headerFieldNameError string

func (e headerFieldNameError) Error() string {
	return fmt.Sprintf("invalid header field name %q", string(e))
}

type headerFieldValueError string

func (e headerFieldValueError) Error() string {
	return fmt.Sprintf("invalid header field value for %q", string(e))
}

var (
	errMixPseudoHeaderTypes = errors.New("mix of request and response pseudo headers")
	errPseudoAfterRegular   = errors.New("pseudo header field after regular")
)
