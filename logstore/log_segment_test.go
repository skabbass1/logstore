package logstore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

type TestMessage struct {
	V1 string
	V2 int
	V3 float64
	V4 string
}

func TestMain(m *testing.M) {
	m.Run()
	logs, _ := filepath.Glob("*.log")
	index, _ := filepath.Glob("*.index")
	files := append(logs, index...)
	for _, f := range files {
		os.Remove(f)
	}
}

func TestLogSegment_NewLogSegment(t *testing.T) {
	segment, err := NewLogSegment(1, 8*1024)
	if err != nil {
		t.Errorf("%v", err)
	}
	defer segment.Close()

	if segment.StartOffset != 1 {
		t.Errorf("Expected offset to be %v but was %v instead", 1, segment.StartOffset)
	}

	expectedIndexSize := 4 * 1024
	if len(*segment.Index.Data) != expectedIndexSize {
		t.Errorf(
			"Expected index size of:%d. Got: %d",
			expectedIndexSize,
			len(*segment.Index.Data),
		)
	}

	expectedIndexName := fmt.Sprintf("%020d.index", 1)
	if segment.Index.Name != expectedIndexName {
		t.Errorf("Expected index name of %s. Got: %s", expectedIndexName, segment.Index.Name)
	}
}

func TestLogSegment_Append(t *testing.T) {
	m1 := TestMessage{
		V1: "GOOG",
		V2: 124,
		V3: 59.0,
		V4: "Note1 Note2 Note3",
	}

	m2 := TestMessage{
		V1: "MSFT",
		V2: 1245,
		V3: 54.1,
		V4: "Note1 Note2 Note3",
	}

	m3 := TestMessage{
		V1: "PYPL",
		V2: 15,
		V3: 54.4,
		V4: "Note1 Note2 Note3",
	}

	b1, _ := json.Marshal(m1)
	b2, _ := json.Marshal(m2)
	b3, _ := json.Marshal(m3)

	segment, _ := NewLogSegment(1, 8*1024)
	len1, err := segment.Append(b1)
	len2, err := segment.Append(b2)
	len3, err := segment.Append(b3)

	if err != nil {
		t.Errorf("%v\n", err)
	}

	segment.Close()

	finfo, _ := os.Lstat(fmt.Sprintf("%s.log", segment.Name))
	expectedSize := len1 + len2 + len3
	if finfo.Size() != int64(expectedSize) {
		t.Errorf("Expected log of size:%d. Got:%d\n", finfo.Size(), expectedSize)
	}

	if segment.NextOffset != 4 {
		t.Errorf("Expected next offset of:%d. Got:%d", 4, segment.NextOffset)
	}

	file, err := os.Open(fmt.Sprintf("%s.index", segment.Name))
	if err != nil {
		t.Errorf("%v\n", err)
	}
	defer file.Close()

	buff := make([]byte, 24*3)
	file.Read(buff)

	entry1 := IndexEntry{}
	entry2 := IndexEntry{}
	entry3 := IndexEntry{}

	entry1.FromBytes(buff[:24])
	entry2.FromBytes(buff[24:48])
	entry3.FromBytes(buff[48:72])

	expected := [...]IndexEntry{
		IndexEntry{1, 0, int64(len1)},
		IndexEntry{2, int64(len1), int64(len2)},
		IndexEntry{3, int64((len1) + (len2)), int64(len3)},
	}

	got := [...]IndexEntry{entry1, entry2, entry3}
	if expected != got {
		t.Errorf("Expected index of:%v. Got: %v\n", expected, got)
	}
}

func TestLogSegment_Append_MaxSizeLimit(t *testing.T) {

	m1 := TestMessage{
		V1: "GOOG",
		V2: 124,
		V3: 59.0,
		V4: "Note1 Note2 Note3",
	}

	m2 := TestMessage{
		V1: "MSFT",
		V2: 1245,
		V3: 54.1,
		V4: "Note1 Note2 Note3",
	}

	b1, _ := json.Marshal(m1)
	b2, _ := json.Marshal(m2)

	segment, _ := NewLogSegment(1, 60)
	defer segment.Close()

	_, err := segment.Append(b1)
	_, err = segment.Append(b2)

	expectedErr := LogStoreErr{
		ErrType:     SegmentLimitReached,
		Message:     "max segment size limit reached",
		OriginalErr: nil,
	}

	if err != expectedErr {
		t.Errorf("Expected err to be:%v. Got:%v\n", err, expectedErr)
	}

}
