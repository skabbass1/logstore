package logstore

type EventType int

const (
	Put = iota
	Get
	Response
	FlushMetaData
	Terminate
)

type Event struct {
	Type         EventType
	Data         []byte
	ResponseChan chan<- Event
	Error        error
}
