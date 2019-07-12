package logstore

import "os"
import "fmt"


type LogSegment struct {
	Offset  int64
	MaxSize int64
	Log     *os.File
}

func NewLogSegment(offset int64, maxSize int64) (*LogSegment, error) {
	name := fmt.Sprintf("%020d.log", offset)
	f, err := os.Create(name)
	if err != nil {
		return nil, err
	}

	return &LogSegment{
		Offset:  offset,
		MaxSize: maxSize,
		Log:     f,
	}, nil
}

func (seg *LogSegment) Append(data []byte) (int, error) {
	size, err := seg.Size()
	if err != nil {
		return -1,  NewLogStoreErr(
			OSErr,
			"unable to get segment size",
			err,
		)
	}

	if int64(len(data)) + size > seg.MaxSize {
		return -1,  NewLogStoreErr(
			SegmentLimitReached,
			"max segment size limit reached",
			nil,
		)
	}

	bytes, err := seg.Log.Write(data)
	if err != nil {
		return -1, NewLogStoreErr(
			OSErr,
			"write to disk failed",
			err,
		)
	}

	return bytes, nil
}

func (seg *LogSegment) Size() (int64, error) {
	fi, err := seg.Log.Stat()
	if err != nil {
		return -1, err
	}

	return fi.Size(), nil
}


func (seg *LogSegment) Close() error {
	return seg.Log.Close()
}
