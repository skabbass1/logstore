package logstore

import (
	"bytes"
	"encoding/binary"
	"os"

	"golang.org/x/sys/unix"
)

const IndexItemWidth = 24

type Mmapper struct {
	Name       string
	Data       *[]byte
	NextOffset int64
}

func NewMmappedFile(name string, size int64) (*Mmapper, error) {
	err := createFile(name, size)
	data, err := memMap(name, 0, size)
	if err != nil {
		return nil, err
	}

	return &Mmapper{
		Name: name,
		Data: &data,
	}, err
}

func (m *Mmapper) AddItem(offset int64, position int64, length int64) error {
	if IndexItemWidth+m.NextOffset > int64(len(*m.Data)) {
		newSize := len(*m.Data) * IndexItemWidth
		if err := m.Resize(int64(newSize)); err != nil {
			return err
		}
	}

	vals := [...]int64{offset, position, length}
	buf := new(bytes.Buffer)

	var err error
	for _, v := range vals {
		err = binary.Write(buf, binary.LittleEndian, v)
	}

	if err != nil {
		return err
	}

	copy((*m.Data)[m.NextOffset:], buf.Bytes())
	m.NextOffset = m.NextOffset + 24

	return err
}

func (m *Mmapper) GetEntry(offset int64) (int64, int64, int64) {
	var data struct {
		Offset   int64
		Position int64
		Length   int64
	}

	reader := bytes.NewReader((*m.Data)[:24])
	binary.Read(reader, binary.LittleEndian, &data)

	distance := offset - data.Offset
	if distance == 0 {
		return data.Offset, data.Position, data.Length
	}

	start := 24 * distance
	end := start + 24

	reader = bytes.NewReader((*m.Data)[start:end])
	binary.Read(reader, binary.LittleEndian, &data)

	return data.Offset, data.Position, data.Length
}

func (m *Mmapper) Resize(size int64) error {
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

func (m *Mmapper) Close() error {
	unix.Msync(*m.Data, unix.MS_SYNC)
	return unix.Munmap(*m.Data)
}

func createFile(name string, size int64) error {
	f, err := os.Create(name)
	defer f.Close()

	err = f.Truncate(size)
	return err
}

func memMap(name string, offset int64, length int64) ([]byte, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0655)
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
