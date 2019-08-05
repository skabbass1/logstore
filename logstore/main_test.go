package logstore

import (
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