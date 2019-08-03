package logstore

import (
	"bytes"
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

	mf, err := NewIndex(fpath, 50, false)
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

	mf, err := NewIndex(fpath, 50, false)
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

	mf, err := NewIndex(fpath, 5, false)
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

	mf, _ := NewIndex(fpath, 1024, false)
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

func cleanup(fpath string) {
	os.Remove(fpath)
}
