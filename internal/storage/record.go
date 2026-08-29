package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	lengthFieldBytes = 4
	crcFieldBytes    = 4
	headerBytes      = lengthFieldBytes + crcFieldBytes
)

var ErrCorruptRecord = fmt.Errorf("storage: corrupt record (checksum mismatch)")

func EncodeFrame(payload []byte) []byte {
	buf := make([]byte, headerBytes+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(payload))
	copy(buf[headerBytes:], payload)
	return buf
}

func readFrame(r io.Reader) (payload []byte, err error) {
	header := make([]byte, headerBytes)
	n, err := io.ReadFull(r, header)
	if err != nil {
		if n == 0 && err == io.EOF {
			return nil, io.EOF
		}
		return nil, io.ErrUnexpectedEOF
	}
	length := binary.BigEndian.Uint32(header[0:4])
	wantCRC := binary.BigEndian.Uint32(header[4:8])

	payload = make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, io.ErrUnexpectedEOF
	}
	if gotCRC := crc32.ChecksumIEEE(payload); gotCRC != wantCRC {
		return nil, ErrCorruptRecord
	}
	return payload, nil
}
