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
	removeTestFiles()
}

func removeTestFiles() {
	logs, _ := filepath.Glob("*.log")
	index, _ := filepath.Glob("*.index")
	meta, _ := filepath.Glob("*.meta")
	files := append(logs, index...)
	files = append(files, meta...)
	for _, f := range files {
		os.Remove(f)
	}
}
