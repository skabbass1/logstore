package logstore

import "testing"
import "path/filepath"
import "os"

func TestMain(m *testing.M) {
	m.Run()
	files, _ := filepath.Glob("*.log")
	for _, f := range files {
		os.Remove(f)
	}
}

func TestLogSegment_NewLogSegment(t *testing.T) {
	segment, err := NewLogSegment(1, 4024)
	if segment.Offset != 1 {
		t.Errorf("Expected offset to be %v but was %v instead", 1, segment.Offset)
	}
	if err != nil {
		t.Errorf("%v", err)
	}
}

func TestLogSegment_Append(t *testing.T) {
	segment, err := NewLogSegment(1, 400)
	if err != nil {
		t.Errorf("Failed to create log segment:%v", err)
	}

	segment.Append([]byte("1 1 Message1"))
	segment.Append([]byte("2 2 Message25674"))

	segment.Close()
}
