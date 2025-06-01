package mem

import (
	"io"
	"sync"
	"sync/atomic"
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

func (r *BufferedReader) Read(n int) (*CompoundBuffer, error) {
	if n == 0 {
		return nil, nil
	}
	lenAvailable := 0
	if r.byteSlice != nil {
		lenAvailable = len(*r.byteSlice) - r.bufferStart
	}
	ret := bufferSlicePool.Get().(*BufferSlice)
	b := *ret
	*ret = b[:0]

	defer ret.Free()
	// Read into the same buffer if it has sufficient capacity.
	if lenAvailable >= n {
		for r.bufferEnd-r.bufferStart < n {
			n, err := r.reader.Read((*(r.byteSlice))[r.bufferEnd:])
			r.bufferEnd += n
			if err != nil {
				return nil, err
			}
		}
		used, remaining := SplitUnsafe(r.buffer, n)
		r.buffer = remaining
		r.bufferStart += n
		*ret = append(*ret, used)
		return NewCompoundBuffer(ret), nil
	}
	// Allocate a new buffer with sufficient capacity.
	numRead := r.bufferEnd - r.bufferStart
	if numRead > 0 {
		used, remaining := SplitUnsafe(r.buffer, numRead)
		remaining.Free()
		*ret = append(*ret, used)
	} else if r.buffer != nil {
		r.buffer.Free()
	}
	n -= numRead
	r.byteSlice = r.pool.Get(max(n, r.bufferSize))
	r.bufferStart = 0
	r.bufferEnd = 0
	r.buffer = NewBuffer(r.byteSlice, r.pool)
	for r.bufferEnd < n {
		remaining := (*r.byteSlice)[r.bufferEnd:]
		n, err := r.reader.Read(remaining)
		r.bufferEnd += n
		if err != nil {
			return nil, err
		}
	}
	used, remaining := SplitUnsafe(r.buffer, n)
	*ret = append(*ret, used)
	r.buffer = remaining
	r.bufferStart += n
	return NewCompoundBuffer(ret), nil
}

// NewBufferReader TODO
func NewBufferReader(bufSize int, pool BufferPool, r io.Reader) BufferedReader {
	return BufferedReader{
		bufferSize: bufSize,
		pool:       pool,
		reader:     r,
	}
}

var (
	compundBufferPool = sync.Pool{New: func() any { return new(CompoundBuffer) }}
	bufferSlicePool   = sync.Pool{New: func() any {
		ret := make(BufferSlice, 2)
		return &ret
	}}
)

// CompoundBuffer TODO
type CompoundBuffer struct {
	refs *atomic.Int32
	bufs *BufferSlice
}

// NewCompoundBuffer TODO
func NewCompoundBuffer(bufs *BufferSlice) *CompoundBuffer {
	if len(*bufs) == 0 {
		return nil
	}
	ret := compundBufferPool.Get().(*CompoundBuffer)
	bufs.Ref()
	ret.bufs = bufs
	ret.refs = refObjectPool.Get().(*atomic.Int32)
	ret.refs.Store(1)
	return ret
}

// Ref TODO
func (cb *CompoundBuffer) Ref() {
	if cb == nil {
		return
	}
	cb.refs.Add(1)
}

// Free TODO
func (cb *CompoundBuffer) Free() {
	if cb == nil {
		return
	}
	val := cb.refs.Add(-1)
	if val > 0 {
		return
	}
	cb.bufs.Free()
	bufferSlicePool.Put(cb.bufs)
	refObjectPool.Put(cb.refs)
	compundBufferPool.Put(cb)
}

// SplitUnsafe TOOD
func (cb *CompoundBuffer) SplitUnsafe(offset int) (*CompoundBuffer, *CompoundBuffer) {
	sumBytes := 0
	rightSlices := bufferSlicePool.Get().(*BufferSlice)
	b := *rightSlices
	*rightSlices = b[:0]
	firstLeft := true

	for i, b := range *cb.bufs {
		if sumBytes >= offset {
			if firstLeft {
				*(cb.bufs) = (*cb.bufs)[:i]
				firstLeft = false
			}
			// completely in right.
			*rightSlices = append(*rightSlices, b)
			continue
		}
		curLen := b.Len()
		if sumBytes+curLen <= offset {
			// completely in left.
			sumBytes += curLen
			continue
		}
		// partially in left, partially in right.
		left, right := b.split(offset - sumBytes)
		(*cb.bufs)[i] = left
		firstLeft = false
		(*cb.bufs) = (*cb.bufs)[:i+1]
		*rightSlices = append(*rightSlices, right)
		sumBytes += curLen
	}
	right := NewCompoundBuffer(rightSlices)
	right.Free()
	return cb, right
}

// ReadOnlyData TODO
func (cb *CompoundBuffer) ReadOnlyData() []byte {
	if cb == nil {
		return nil
	}
	return cb.bufs.Materialize()
}

// ToSlice TODO
func (cb *CompoundBuffer) ToSlice() BufferSlice {
	if cb == nil {
		return nil
	}
	return *cb.bufs
}
