package mem

import (
	"io"
)

type BufferedReader struct {
	reader      io.Reader
	bufferSize  int
	byteSlice   *[]byte
	buffer      Buffer
	bufferStart int // Inclusive
	bufferEnd   int // Exclusive
	pool        BufferPool
}

func (r *BufferedReader) Read(n int) (BufferSlice, error) {
	if n == 0 {
		return nil, nil
	}
	// Fill up byteSlice first.
	if r.byteSlice == nil {
		r.byteSlice = r.pool.Get(max(n, r.bufferSize))
		r.bufferStart = 0
		r.bufferEnd = 0
		r.buffer = NewBuffer(r.byteSlice, r.pool)
	}
	for r.bufferEnd-r.bufferStart < n && r.bufferEnd < len(*r.byteSlice) {
		n, err := r.reader.Read((*(r.byteSlice))[r.bufferEnd:])
		r.bufferEnd += n
		if err != nil {
			return nil, err
		}
	}
	// If bufferedSlice isn't able to hold the required capacity, allocate a new
	// slice.
	ret := []Buffer{}
	numRead := r.bufferEnd - r.bufferStart
	if r.bufferStart+n >= len(*r.byteSlice) {
		ret = append(ret, r.buffer)
		r.byteSlice = nil
	} else {
		used, remaining := SplitUnsafe(r.buffer, n)
		ret = append(ret, used)
		r.buffer = remaining
		r.bufferStart += n
	}
	if n <= numRead {
		return ret, nil
	}
	// Allocate a new buffer.
	n -= numRead
	r.byteSlice = r.pool.Get(max(n, r.bufferSize))
	r.bufferStart = 0
	r.bufferEnd = 0
	r.buffer = NewBuffer(r.byteSlice, r.pool)
	for r.bufferEnd-r.bufferStart < n {
		n, err := r.reader.Read((*(r.byteSlice))[r.bufferEnd:])
		r.bufferEnd += n
		if err != nil {
			return nil, err
		}
	}
	if r.bufferStart+n >= len(*r.byteSlice) {
		ret = append(ret, r.buffer)
		r.byteSlice = nil
	} else {
		used, remaining := SplitUnsafe(r.buffer, n)
		ret = append(ret, used)
		r.buffer = remaining
		r.bufferStart += n
	}
	return ret, nil
}

func NewBufferReader(bufSize int, pool BufferPool, r io.Reader) BufferedReader {
	return BufferedReader{
		bufferSize: bufSize,
		pool:       pool,
		reader:     r,
	}
}
