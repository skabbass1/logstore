package logstore

import (
	"fmt"
	"io"
	"os"
)

type LogSegment struct {
	StartOffset int64
	NextOffset  int64
	Name        string
	MaxSize     int64
	Log         *os.File
	Index       *Index
}

func NewLogSegment(offset int64, maxSize int64) (*LogSegment, error) {
	base := fmt.Sprintf("%020d", offset)
	logName := fmt.Sprintf("%s.log", base)
	f, err := os.Create(logName)
	if err != nil {
		return nil, err
	}

	indexName := fmt.Sprintf("%s.index", base)
	index, err := NewIndex(indexName, int64(4096))
	if err != nil {
		return &LogSegment{}, err
	}

	return &LogSegment{
		StartOffset: offset,
		NextOffset:  offset,
		Name:        base,
		MaxSize:     maxSize,
		Log:         f,
		Index:       index,
	}, nil
}

func (seg *LogSegment) Append(data []byte) (int, error) {
	size, err := seg.Size()
	if err != nil {
		return -1, NewLogStoreErr(
			OSErr,
			"unable to get segment size",
			err,
		)
	}

	if int64(len(data))+size > seg.MaxSize {
		return -1, NewLogStoreErr(
			SegmentLimitReached,
			"max segment size limit reached",
			nil,
		)
	}

	position, _ := seg.Log.Seek(0, 1)
	length, err := seg.Log.Write(data)
	if err != nil {
		return -1, NewLogStoreErr(
			OSErr,
			"write to disk failed",
			err,
		)
	}

	entry := IndexEntry{
		Offset:   seg.NextOffset,
		Position: position,
		Length:   int64(length),
	}

	// TODO: Handle index write failure
	seg.Index.AddEntry(entry)
	seg.NextOffset++

	return length, nil
}

func (seg *LogSegment) Get(offset int64) ([]byte, error) {
	// TODO ensure offset is not greater that what this segment
	// contains
	index, err := seg.Index.GetEntry(offset)
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(fmt.Sprintf("%s.log", seg.Name), os.O_RDONLY, 0655)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	//TODO handle read errors
	buff := make([]byte, index.Length)
	f.Seek(index.Position, io.SeekStart)
	_, err = f.Read(buff)

	return buff, err
}

func (seg *LogSegment) Size() (int64, error) {
	fi, err := seg.Log.Stat()
	if err != nil {
		return -1, err
	}

	return fi.Size(), nil
}

func (seg *LogSegment) Close() {
	seg.Index.Close()
	seg.Log.Close()
}
