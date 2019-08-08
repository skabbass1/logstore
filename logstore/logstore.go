package logstore

import (
	"encoding/binary"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const metafile = "logstore.meta"
const segmentSize = 4096

type MetaData struct {
	NextOffset int64
}

type LogStore struct {
	CurrentSegment *LogSegment
	EventQueue     <-chan Event
	MetaData       MetaData
}

func NewLogStore(queue <-chan Event) (*LogStore, error) {
	metadata, err := readMetaDatafile()
	if err != nil {
		return nil, err
	}

	segment, err := NewLogSegment(metadata.NextOffset, segmentSize, false)
	if err != nil {
		return nil, err
	}

	return &LogStore{
		CurrentSegment: segment,
		EventQueue:     queue,
		MetaData:       metadata,
	}, nil
}

func (store *LogStore) Run() {
	go store.runLoop()
}

func (store *LogStore) runLoop() {
	for {
		event := <-store.EventQueue
		switch {

		case event.Type == Put:
			err := store.append(event.Data)
			event.ResponseChan <- Event{Response, nil, nil, err}

		case event.Type == Get:
			offset, _ := binary.Varint(event.Data)
			data, err := store.get(int64(offset))
			event.ResponseChan <- Event{Response, data, nil, err}

		case event.Type == FlushMetaData:
			go writeMetaData(store.MetaData)

		case event.Type == Terminate:
			store.CurrentSegment.Close()
			return
		default:
			continue
		}
	}
}

func (store *LogStore) append(data []byte) error {
	_, err := store.CurrentSegment.Append(data)
	if err != nil {
		if err.(LogStoreErr).ErrType == SegmentLimitReached {
			store.CurrentSegment.Close()

			segment, err := NewLogSegment(store.CurrentSegment.NextOffset, segmentSize, false)
			if err != nil {
				return err
			}
			store.CurrentSegment = segment

			_, err = store.CurrentSegment.Append(data)
			if err != nil {
				return err
			} else {
				store.MetaData.NextOffset = store.CurrentSegment.NextOffset
				return nil
			}
		} else {
			return err
		}
	}
	store.MetaData.NextOffset = store.CurrentSegment.NextOffset
	return nil
}

func (store *LogStore) get(offset int64) ([]byte, error) {
	if offset < store.CurrentSegment.StartOffset {
		return getFromClosedSegment(offset)
	}
	return store.CurrentSegment.Get(offset)
}

func getFromClosedSegment(offset int64) ([]byte, error) {
	files, _ := filepath.Glob("*.index")
	var values []int
	for _, file := range files {
		n := strings.TrimSuffix(filepath.Base(file), filepath.Ext(file))
		v, _ := strconv.ParseInt(n, 10, 32)
		values = append(values, int(v))
	}
	sort.Ints(values)

	var myoffset int
	for idx, value := range values {
		if int64(value) > offset {
			myoffset = values[idx-1]
			break
		}
	}

	segment, _ := NewLogSegment(int64(myoffset), -1, true)
	result, err := segment.Get(offset)
	segment.Close()
	return result, err

}

func readMetaDatafile() (MetaData, error) {
	_, err := os.Stat(metafile)
	if os.IsNotExist(err) {
		return MetaData{1}, nil
	}

	if err != nil {
		return MetaData{-1}, err
	}

	bytes, err := ioutil.ReadFile(metafile)
	if err != nil {
		return MetaData{-1}, err
	}

	var m MetaData
	json.Unmarshal(bytes, &m)
	return m, nil
}

func writeMetaData(m MetaData) {
	data, _ := json.Marshal(m)
	ioutil.WriteFile(metafile, data, 0644)
}
