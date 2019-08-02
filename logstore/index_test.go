package logstore

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestIndexEntry_ToBytes(t *testing.T) {
	entry := IndexEntry{
		Offset:   1,
		Position: 0,
		Length:   150,
	}
	blob, err := entry.ToBytes()
	if err != nil {
		t.Errorf("conversion to bytes failed:%v\n", err)
	}
	expected := []byte{
		1, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0,
		150, 0, 0, 0, 0, 0, 0, 0}

	if !bytes.Equal(expected, blob) {
		t.Errorf("expected:%v. Got:%v\n", expected, blob)
	}
}

func TestIndexEntry_FromBytes(t *testing.T) {
	entry := IndexEntry{}
	err := entry.FromBytes(
		[]byte{
			1, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			150, 0, 0, 0, 0, 0, 0, 0},
	)
	if err != nil {
		t.Errorf("conversion from  bytes failed:%v\n", err)
	}
	expected := IndexEntry{
		Offset:   1,
		Position: 0,
		Length:   150,
	}
	if expected != entry {
		t.Errorf("expected:%v. Got:%v\n", expected, entry)
	}
}

func TestIndex_NewIndex(t *testing.T) {
	fpath := "/tmp/test_mapped"

	mf, err := NewIndex(fpath, 50)
	defer mf.Close()

	if err != nil {
		t.Errorf("%v\n", err)
	}

	if len(*mf.Data) != 50 {
		t.Errorf(
			"Expected mapped buffer of len: %d. Got: %d",
			50,
			len(*mf.Data),
		)
	}

	finfo, _ := os.Lstat(fpath)
	if finfo.Size() != 50 {
		t.Errorf(
			"Expected mapped file to be of size:%d. Got :%d",
			50,
			finfo.Size(),
		)
	}

	cleanup(fpath)
}

func TestIndex_AddEntry(t *testing.T) {
	fpath := "/tmp/test_mapped"

	mf, err := NewIndex(fpath, 50)
	defer mf.Close()

	expected := IndexEntry{
		Offset:   int64(300),
		Position: int64(100),
		Length:   int64(150),
	}
	err = mf.AddEntry(expected)
	if err != nil {
		t.Errorf("%v", err)
	}

	got := IndexEntry{}
	err = got.FromBytes((*mf.Data)[:IndexItemWidth])

	if expected != got {
		t.Errorf("Expected:%v Got:%v\n", expected, got)
	}

	cleanup(fpath)
}

func TestIndex_AddEntry_Resize(t *testing.T) {
	fpath := "/tmp/test_mapped"

	mf, err := NewIndex(fpath, 5)
	defer mf.Close()

	expected := IndexEntry{300, 100, 150}
	err = mf.AddEntry(expected)
	if err != nil {
		t.Errorf("%v", err)
	}

	if len(*mf.Data) != 5*IndexItemWidth {
		t.Errorf(
			"Expected buffer size of %d. Got:%v\n",
			5*IndexItemWidth,
			len(*mf.Data),
		)
	}

	got := IndexEntry{}
	err = got.FromBytes((*mf.Data)[:IndexItemWidth])
	if err != nil {
		t.Errorf("Binary read failed:%v\n", err)
	}

	if expected != got {
		t.Errorf("Expected:%v Got:%v\n", expected, got)
	}

	cleanup(fpath)
}

func TestIndex_GetEntry(t *testing.T) {
	fpath := "/tmp/test_mapped"

	mf, _ := NewIndex(fpath, 1024)
	defer mf.Close()

	mf.AddEntry(IndexEntry{1, 0, 150})
	mf.AddEntry(IndexEntry{2, 150, 150})
	mf.AddEntry(IndexEntry{3, 300, 150})
	mf.AddEntry(IndexEntry{4, 450, 150})
	mf.AddEntry(IndexEntry{5, 600, 150})

	got1, err := mf.GetEntry(int64(1))
	got3, err := mf.GetEntry(int64(3))
	got5, err := mf.GetEntry(int64(5))
	if err != nil {
		t.Errorf("%v\n", err)
	}

	results := [...]IndexEntry{got1, got3, got5}
	expected := [...]IndexEntry{
		IndexEntry{1, 0, 150},
		IndexEntry{3, 300, 150},
		IndexEntry{5, 600, 150},
	}

	if results != expected {
		t.Errorf("Expected:%v Got:%v\n", expected, results)
	}

	cleanup(fpath)

}

func TestReadOnlyIndex_NewReadOnlyIndex(t *testing.T) {
	fpath := "/tmp/test_mapped"

	mf, _ := NewIndex(fpath, 1024)
	defer mf.Close()

	mf.AddEntry(IndexEntry{1, 0, 150})
	mf.AddEntry(IndexEntry{2, 150, 150})

	roIndex, err := NewReadOnlyIndex(fpath)
	if err != nil {
		t.Errorf("%v\n", err)
	}
	defer roIndex.Close()

	length := len(*roIndex.data)
	if length != 1024 {
		fmt.Errorf("Expected mapped data length to be %d. Got %d\n", 1024, length)
	}

	cleanup(fpath)
}

func TestReadOnlyIndex_GetEntry(t *testing.T) {
	fpath := "/tmp/test_mapped"

	mf, _ := NewIndex(fpath, 1024)
	defer mf.Close()

	entry1 := IndexEntry{1, 0, 150}
	entry2 := IndexEntry{1, 150, 150}
	mf.AddEntry(entry1)
	mf.AddEntry(entry2)

	roIndex, _ := NewReadOnlyIndex(fpath)
	defer roIndex.Close()

	got1, err := roIndex.GetEntry(1)
	got2, err := roIndex.GetEntry(2)

	if err != nil {
		t.Errorf("%v\n", err)
	}

	if entry1 != got1 {
		t.Errorf("Expected entry1 to be %v. Got %v\n", entry1, got1)
	}

	if entry2 != got2 {
		t.Errorf("Expected entry2 to be %v. Got %v\n", entry2, got2)
	}

}

func cleanup(fpath string) {
	os.Remove(fpath)
}
