package logstore

import "fmt"

type LogStoreErrType int

const (
	SegmentLimitReached LogStoreErrType = iota
	IndexIsReadOnly
	OSErr
)

type LogStoreErr struct {
	ErrType     LogStoreErrType
	Message     string
	OriginalErr error
}

func NewLogStoreErr(
	errType LogStoreErrType,
	msg string,
	origErr error,
) LogStoreErr {
	return LogStoreErr{
		ErrType:     errType,
		Message:     msg,
		OriginalErr: origErr,
	}
}

func (err LogStoreErr) Error() string {
	return fmt.Sprintf("error:%s\n. Original:%v\n",
		err.Message,
		err.OriginalErr,
	)
}
