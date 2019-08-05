package logstore

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"
)

func TestLogStore_New(t *testing.T) {
	eventQueue := make(chan Event, 1000)
	store, err := NewLogStore(eventQueue)
	if err != nil {
		t.Errorf("%v", err)
	}

	if store.MetaData.NextOffset != 1 {
		t.Errorf("Expected metadata next offset to be %d. Got :%d", 1, store.MetaData.NextOffset)
	}

	if store.CurrentSegment.Name != fmt.Sprintf("%020d", 1) {
		t.Errorf("Expected segment name to be %s. Got %s\n", fmt.Sprintf("%020d", 1), store.CurrentSegment.Name)
	}
}

func TestLogStore_Put_Event(t *testing.T) {
	eventQueue := make(chan Event, 1000)
	store, _ := NewLogStore(eventQueue)
	store.Run()

	pchan := make(chan Event, 5)
	messages := [...]TestMessage{
		TestMessage{
			"foo",
			12,
			23.0,
			"bar",
		},
		TestMessage{
			"foo",
			12,
			23.0,
			"bar",
		},
		TestMessage{
			"foo",
			12,
			23.0,
			"bar",
		},
		TestMessage{
			"foo",
			12,
			23.0,
			"bar",
		},
	}

	for _, m := range messages {
		data, _ := json.Marshal(m)
		eventQueue <- Event{Put, data, pchan, nil}
	}

	for _, _ = range messages {
		resp := <-pchan
		if resp.Error != nil {
			t.Errorf("%v", resp.Error)
		}
	}

	eventQueue <- Event{Terminate, nil, nil, nil}
	close(pchan)
	close(eventQueue)
}

func TestLogStore_Get(t *testing.T) {
	eventQueue := make(chan Event, 1000)
	store, _ := NewLogStore(eventQueue)
	store.Run()

	pchan := make(chan Event, 5)
	messages := [...]TestMessage{
		TestMessage{
			"foo",
			12,
			23.0,
			"bar",
		},
		TestMessage{
			"foo",
			12,
			23.0,
			"bar",
		},
		TestMessage{
			"foo",
			12,
			23.0,
			"bar",
		},
		TestMessage{
			"foo",
			12,
			23.0,
			"bar",
		},
	}

	for _, m := range messages {
		data, _ := json.Marshal(m)
		eventQueue <- Event{Put, data, pchan, nil}
	}

	for _, _ = range messages {
		<-pchan
	}
	close(pchan)

	gchan := make(chan Event)
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, 3)
	eventQueue <- Event{Get, b, gchan, nil}

	response := <-gchan
	if response.Error != nil {
		t.Errorf("%v\n", response.Error)
	} else {
		var data TestMessage
		json.Unmarshal(response.Data, &data)
		if data != messages[2] {
			t.Errorf("Expected response to be %v. Got %v\n", messages[2], data)
		}
	}

	eventQueue <- Event{Terminate, nil, pchan, nil}
	close(gchan)
	close(eventQueue)

}
