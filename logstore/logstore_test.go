package logstore

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"
	"time"
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

	removeTestFiles()
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
	removeTestFiles()
}

func TestLogStore_Put_Event_NextSegement(t *testing.T) {
	eventQueue := make(chan Event, 1000)
	store, _ := NewLogStore(eventQueue)
	store.Run()

	pchan := make(chan Event, 1000)
	message := TestMessage{
		"foo",
		12,
		23.0,
		"bar",
	}

	data, _ := json.Marshal(message)
	for i := 1; i <= 1000; i++ {
		eventQueue <- Event{Put, data, pchan, nil}
	}

	for i := 1; i <= 1000; i++ {
		resp := <-pchan
		if resp.Error != nil {
			t.Errorf("%v", resp.Error)
		}
	}

	eventQueue <- Event{Terminate, nil, nil, nil}
	close(pchan)
	close(eventQueue)

	if store.CurrentSegment.NextOffset != 1001 {
		t.Errorf(
			"Expected next offset to be %d. Got %d\n",
			1001,
			store.CurrentSegment.NextOffset,
		)
	}

	if store.CurrentSegment.Name != fmt.Sprintf("%020d", 946) {
		t.Errorf(
			"Expected next segment to be %s. Got %s\n",
			store.CurrentSegment.Name,
			fmt.Sprintf("%020d", 946),
		)
	}

	removeTestFiles()
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
	binary.PutVarint(b, 3)
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
	removeTestFiles()
}

func TestLogStore_Get_Event_Closed_Segment(t *testing.T) {
	eventQueue := make(chan Event, 1000)
	store, _ := NewLogStore(eventQueue)
	store.Run()

	pchan := make(chan Event, 500)

	for i := 1; i <= 500; i++ {
		message := TestMessage{
			"foo",
			i,
			23.0,
			"bar",
		}
		data, _ := json.Marshal(message)
		eventQueue <- Event{Put, data, pchan, nil}
	}

	for i := 1; i <= 500; i++ {
		<-pchan
	}

	gchan := make(chan Event)
	b := make([]byte, 8)
	binary.PutVarint(b, 356)
	eventQueue <- Event{Get, b, gchan, nil}

	response := <-gchan
	if response.Error != nil {
		t.Errorf("%v\n", response.Error)
	} else {
		var data TestMessage
		json.Unmarshal(response.Data, &data)
		expected := TestMessage{"foo", 356, 23.0, "bar"}
		if data != expected {
			t.Errorf("Expected response to be %v. Got %v\n", expected, data)
		}
	}

	eventQueue <- Event{Terminate, nil, nil, nil}
	close(pchan)
	close(gchan)
	close(eventQueue)
	removeTestFiles()
}

func TestLogStore_MetaDataUpdate(t *testing.T) {
	eventQueue := make(chan Event, 1000)
	store, _ := NewLogStore(eventQueue)
	store.Run()

	pchan := make(chan Event, 200)

	for i := 1; i <= 200; i++ {
		message := TestMessage{
			"foo",
			i,
			23.0,
			"bar",
		}
		data, _ := json.Marshal(message)
		eventQueue <- Event{Put, data, pchan, nil}
	}

	for i := 1; i <= 200; i++ {
		<-pchan
	}

	if store.MetaData.NextOffset != 201 {
		t.Errorf("Expected next offset to be %d. Got %d\n", 201, store.MetaData.NextOffset)
	}

	eventQueue <- Event{Terminate, nil, nil, nil}
	close(pchan)
	close(eventQueue)
	removeTestFiles()
}

func TestLogStore_BootFromMetaData(t *testing.T) {
	eventQueue := make(chan Event, 1000)
	store, _ := NewLogStore(eventQueue)
	store.Run()

	pchan := make(chan Event, 200)

	for i := 1; i <= 200; i++ {
		message := TestMessage{
			"foo",
			i,
			23.0,
			"bar",
		}
		data, _ := json.Marshal(message)
		eventQueue <- Event{Put, data, pchan, nil}
	}

	for i := 1; i <= 200; i++ {
		<-pchan
	}

	eventQueue <- Event{FlushMetaData, nil, nil, nil}
	eventQueue <- Event{Terminate, nil, nil, nil}

	// wait for meta file writing go routine to complete
	time.Sleep(100 * time.Millisecond)

	store2, _ := NewLogStore(eventQueue)
	if store2.CurrentSegment.NextOffset != 201 {
		t.Errorf("Expected next offset to be %d. Got %d\n", 201, store.MetaData.NextOffset)
	}

	close(pchan)
	close(eventQueue)
	removeTestFiles()
}
