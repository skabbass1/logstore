package logstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"testing"
)

func TestNewMmappedFile(t *testing.T) {
	fpath := "/tmp/test_mapped"

	mf, err := NewMmappedFile(fpath, 50)
	defer mf.Close()

	if err != nil {
		t.Errorf("%v\n", err)
	}

	if len(*mf.Data) != 50 {
		t.Errorf("Expected mapped buffer of len: %d. Got: %d", 50, len(*mf.Data))
	}

	finfo, _ := os.Lstat(fpath)
	if finfo.Size() != 50 {
		t.Errorf("Expected mapped file to be of size:%d. Got :%d", 50, finfo.Size())
	}

	cleanup(fpath)
}

func TestAddItem(t *testing.T) {
	fpath := "/tmp/test_mapped"

	mf, err := NewMmappedFile(fpath, 50)
	defer mf.Close()

	err = mf.AddItem(int64(300), int64(100), int64(150))
	if err != nil {
		t.Errorf("%v", err)
	}

	buf := bytes.NewReader((*mf.Data)[:24])
	var data struct {
		Offset   int64
		Position int64
		Length   int64
	}
	if err := binary.Read(buf, binary.LittleEndian, &data); err != nil {
		t.Errorf("Binary read failed:%v\n", err)
	}

	expected := struct {
		Offset   int64
		Position int64
		Length   int64
	}{

		300,
		100,
		150,
	}

	if expected != data {
		t.Errorf("Expected:%v Got:%v\n", expected, data)
	}

	cleanup(fpath)
}

func TestAddItemResizeFile(t *testing.T) {
	fpath := "/tmp/test_mapped"

	mf, err := NewMmappedFile(fpath, 5)
	defer mf.Close()

	err = mf.AddItem(int64(300), int64(100), int64(150))
	if err != nil {
		t.Errorf("%v", err)
	}

	if len(*mf.Data) != 5*24 {
		t.Errorf("Expected buffer size of %d. Got:%v\n", 5*24, len(*mf.Data))
	}
	buf := bytes.NewReader((*mf.Data)[:24])
	var data struct {
		Offset   int64
		Position int64
		Length   int64
	}
	if err := binary.Read(buf, binary.LittleEndian, &data); err != nil {
		t.Errorf("Binary read failed:%v\n", err)
	}

	expected := struct {
		Offset   int64
		Position int64
		Length   int64
	}{

		300,
		100,
		150,
	}

	if expected != data {
		t.Errorf("Expected:%v Got:%v\n", expected, data)
	}

	cleanup(fpath)
}

func TestGetEntry(t *testing.T) {
	fpath := "/tmp/test_mapped"

	mf, _ := NewMmappedFile(fpath, 1024)
	defer mf.Close()

	mf.AddItem(int64(1), int64(0), int64(150))
	mf.AddItem(int64(2), int64(150), int64(150))
	mf.AddItem(int64(3), int64(300), int64(150))
	mf.AddItem(int64(4), int64(450), int64(150))
	mf.AddItem(int64(5), int64(600), int64(150))

	offset, pos, length := mf.GetEntry(int64(1))
	fmt.Printf("%d - %d - %d", offset, pos, length)

	cleanup(fpath)

}
func cleanup(fpath string) {
	os.Remove(fpath)
}
