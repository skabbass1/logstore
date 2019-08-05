package logstore

import (
	"encoding/binary"
	"encoding/json"
	"io/ioutil"
	"os"
)

const metafile = "logstore.meta"

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

	segment, err := NewLogSegment(metadata.NextOffset, 4096, false)
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
			offset := binary.LittleEndian.Uint64(event.Data)
			data, err := store.get(int64(offset))
			event.ResponseChan <- Event{Response, data, nil, err}

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
	return err
}

func (store *LogStore) get(offset int64) ([]byte, error) {
	return store.CurrentSegment.Get(offset)
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

func writeMetaData() {}
