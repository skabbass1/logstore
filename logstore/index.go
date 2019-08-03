package logstore

import (
	"bytes"
	"encoding/binary"
	"os"

	"golang.org/x/sys/unix"
)

const IndexItemWidth = 24
const perms = 0655

type IndexEntry struct {
	Offset   int64
	Position int64
	Length   int64
}

type Index struct {
	Name       string
	Data       *[]byte
	NextOffset int64
	ReadOnly   bool
}

func (entry *IndexEntry) ToBytes() ([]byte, error) {
	buff := new(bytes.Buffer)
	if err := binary.Write(buff, binary.LittleEndian, entry); err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

func (entry *IndexEntry) FromBytes(data []byte) error {
	reader := bytes.NewReader(data)
	if err := binary.Read(reader, binary.LittleEndian, entry); err != nil {
		return err
	}

	return nil
}

func NewIndex(name string, size int64) (*Index, error) {
	err := createFile(name, size)
	data, err := memMap(name, 0, size)
	if err != nil {
		return nil, err
	}

	return &Index{
		Name: name,
		Data: &data,
	}, err
}

func (m *Index) AddEntry(entry IndexEntry) error {
	if m.NextOffset+IndexItemWidth > int64(len(*m.Data)) {
		newSize := len(*m.Data) * IndexItemWidth
		if err := m.Resize(int64(newSize)); err != nil {
			return err
		}
	}

	packed, err := entry.ToBytes()
	if err != nil {
		return err
	}

	copy((*m.Data)[m.NextOffset:], packed)
	m.NextOffset = m.NextOffset + IndexItemWidth

	return err
}

func (m *Index) GetEntry(offset int64) (IndexEntry, error) {
	first := IndexEntry{}
	if err := first.FromBytes((*m.Data)[:IndexItemWidth]); err != nil {
		return IndexEntry{}, err
	}

	distance := offset - first.Offset
	if distance == 0 {
		return first, nil
	}

	start := IndexItemWidth * distance
	end := start + IndexItemWidth

	entry := IndexEntry{}
	if err := entry.FromBytes((*m.Data)[start:end]); err != nil {
		return IndexEntry{}, err
	}

	return entry, nil
}

func (m *Index) Resize(size int64) error {
	m.Close()

	err := os.Truncate(m.Name, size)
	if err != nil {
		return err
	}
	data, err := memMap(m.Name, 0, size)
	if err != nil {
		return err
	}
	m.Data = &data

	return nil
}

func (m *Index) Close() error {
	unix.Msync(*m.Data, unix.MS_SYNC)
	return unix.Munmap(*m.Data)
}

func createFile(name string, size int64) error {
	f, err := os.Create(name)
	defer f.Close()

	err = f.Truncate(size)
	return err
}

func readOnlyMemMap(name string) ([]byte, error) {
	f, err := os.OpenFile(name, os.O_RDONLY, perms)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	fd := int(f.Fd())

	fi, err := f.Stat()
	size := fi.Size()

	data, err := unix.Mmap(
		fd,
		0,
		int(size),
		unix.PROT_READ,
		unix.MAP_SHARED,
	)

	return data, err

}

func memMap(name string, offset int64, length int64) ([]byte, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, perms)
	defer f.Close()

	if err != nil {
		return nil, err
	}

	fd := int(f.Fd())

	data, err := unix.Mmap(
		fd,
		offset,
		int(length),
		unix.PROT_WRITE|unix.PROT_READ,
		unix.MAP_SHARED,
	)

	return data, err
}
