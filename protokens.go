package main

import (
	"bufio"
	"bytes"
	"io"
)

type TokenReader struct {
	r      *bufio.Reader
	buffer *bytes.Buffer
	next   string
}

func NewTokenReader(r io.Reader) *TokenReader {
	return &TokenReader{
		r:      bufio.NewReader(r),
		buffer: new(bytes.Buffer),
	}
}

func (reader *TokenReader) NextToken() (string, error) {
	for {
		if reader.next != "" {
			out := reader.next
			reader.next = ""
			return out, nil
		}
		b, err := reader.r.ReadByte()
		if err != nil {
			if err == io.EOF && reader.buffer.Len() > 0 {
				return reader.buffer.String(), nil
			}
			return "", err
		}

		if b == ' ' || b == ';' || b == '\n' || b == '\t' || b == '=' {
			if b == ';' || b == '=' {
				reader.next = string(b)
			}
			if reader.buffer.Len() > 0 {
				out := reader.buffer.String()
				reader.buffer.Reset()
				return out, nil
			}
			continue
		}

		reader.buffer.WriteByte(b)
	}
	return "", nil
}
