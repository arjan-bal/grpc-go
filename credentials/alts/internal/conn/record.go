/*
 *
 * Copyright 2018 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Package conn contains an implementation of a secure channel created by gRPC
// handshakers.
package conn

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"

	core "google.golang.org/grpc/credentials/alts/internal"
	"google.golang.org/grpc/mem"
)

// ALTSRecordCrypto is the interface for gRPC ALTS record protocol.
type ALTSRecordCrypto interface {
	// Encrypt encrypts the plaintext and computes the tag (if any) of dst
	// and plaintext. dst and plaintext may fully overlap or not at all.
	Encrypt(dst, plaintext []byte) ([]byte, error)
	// EncryptionOverhead returns the tag size (if any) in bytes.
	EncryptionOverhead() int
	// Decrypt decrypts ciphertext and verify the tag (if any). dst and
	// ciphertext may alias exactly or not at all. To reuse ciphertext's
	// storage for the decrypted output, use ciphertext[:0] as dst.
	Decrypt(dst, ciphertext []byte) ([]byte, error)
}

// ALTSRecordFunc is a function type for factory functions that create
// ALTSRecordCrypto instances.
type ALTSRecordFunc func(s core.Side, keyData []byte) (ALTSRecordCrypto, error)

const (
	// MsgLenFieldSize is the byte size of the frame length field of a
	// framed message.
	MsgLenFieldSize = 4
	// The byte size of the message type field of a framed message.
	msgTypeFieldSize = 4
	// The bytes size limit for a ALTS record message.
	altsRecordLengthLimit = 1024 * 1024 // 1 MiB
	// The default bytes size of a ALTS record message.
	altsRecordDefaultLength = 4 * 1024 // 4KiB
	// Message type value included in ALTS record framing.
	altsRecordMsgType = uint32(0x06)
	// The initial write buffer size.
	altsWriteBufferInitialSize = 32 * 1024 // 32KiB
	// The maximum write buffer size. This *must* be multiple of
	// altsRecordDefaultLength.
	altsWriteBufferMaxSize = 512 * 1024 // 512KiB
	// The initial buffer used to read from the network.
	altsReadBufferInitialSize = 32 * 1024 // 32KiB
)

var (
	protocols = make(map[string]ALTSRecordFunc)
)

// RegisterProtocol register a ALTS record encryption protocol.
func RegisterProtocol(protocol string, f ALTSRecordFunc) error {
	if _, ok := protocols[protocol]; ok {
		return fmt.Errorf("protocol %v is already registered", protocol)
	}
	protocols[protocol] = f
	return nil
}

// conn represents a secured connection. It implements the net.Conn interface.
type conn struct {
	net.Conn
	crypto ALTSRecordCrypto
	// buf holds data that has been read from the connection and decrypted,
	// but has not yet been returned by Read.
	buf                *[]byte
	bufBytesRead       int
	bufBytesTotal      int
	payloadLengthLimit int
	// protected holds data read from the network but have not yet been
	// decrypted. This data might not compose a complete frame.
	protected mem.BufferSlice
	// writeBuf is a buffer used to contain encrypted frames before being
	// written to the network.
	writeBuf []byte
	// overhead is the calculated overhead of each frame.
	overhead int
	isClosed bool
}

// NewConn creates a new secure channel instance given the other party role and
// handshaking result. This function takes ownership of "protected".
func NewConn(c net.Conn, side core.Side, recordProtocol string, key []byte, protected []byte) (net.Conn, error) {
	newCrypto := protocols[recordProtocol]
	if newCrypto == nil {
		return nil, fmt.Errorf("negotiated unknown next_protocol %q", recordProtocol)
	}
	crypto, err := newCrypto(side, key)
	if err != nil {
		return nil, fmt.Errorf("protocol %q: %v", recordProtocol, err)
	}
	overhead := MsgLenFieldSize + msgTypeFieldSize + crypto.EncryptionOverhead()
	payloadLengthLimit := altsRecordDefaultLength - overhead

	altsConn := &conn{
		Conn:               c,
		crypto:             crypto,
		payloadLengthLimit: payloadLengthLimit,
		protected:          []mem.Buffer{mem.SliceBuffer(protected)},
		writeBuf:           make([]byte, altsWriteBufferInitialSize),
		overhead:           overhead,
	}
	return altsConn, nil
}

func (p *conn) readBytes(byteCount int) (mem.BufferSlice, error) {
	// Ensure protected contains at least the required number of bytes.
	currentLen := p.protected.Len()
	for currentLen < byteCount {
		newBuf := mem.DefaultBufferPool().Get(altsReadBufferInitialSize)
		bufFillled := 0
		for bufFillled < len(*newBuf) && currentLen < byteCount {
			n, err := p.Conn.Read((*newBuf)[bufFillled:])
			if err != nil {
				return nil, err
			}
			currentLen += n
			bufFillled += n
		}
		bb := mem.NewBuffer(newBuf, mem.DefaultBufferPool())
		if bufFillled < len(*newBuf) {
			used, remaining := mem.SplitUnsafe(bb, bufFillled)
			remaining.Free()
			bb = used
		}
		p.protected = append(p.protected, bb)
	}

	// Gather the exact number of bytes.
	var outBufs mem.BufferSlice
	bytesNeeded := byteCount
	for bytesNeeded > 0 {
		buf := p.protected[0]
		if buf.Len() <= bytesNeeded {
			// Take the entire buf.
			outBufs = append(outBufs, buf)
			p.protected = p.protected[1:]
			bytesNeeded -= buf.Len()
			continue
		}
		// Take a partial slice.
		left, right := mem.SplitUnsafe(buf, bytesNeeded)
		p.protected[0] = right
		outBufs = append(outBufs, left)
		bytesNeeded = 0
		break
	}
	return outBufs, nil
}

func (p *conn) Close() error {
	if !p.isClosed {
		if p.buf != nil {
			mem.DefaultBufferPool().Put(p.buf)
		}
		if p.protected != nil {
			p.protected.Free()
		}
		p.isClosed = true
	}
	return p.Conn.Close()
}

// Read reads and decrypts a frame from the underlying connection, and copies the
// decrypted payload into b. If the size of the payload is greater than len(b),
// Read retains the remaining bytes in an internal buffer, and subsequent calls
// to Read will read from this buffer until it is exhausted.
func (p *conn) Read(b []byte) (n int, err error) {
	if p.isClosed {
		return 0, io.EOF
	}
	if p.bufBytesRead == p.bufBytesTotal {
		headerBufs, err := p.readBytes(MsgLenFieldSize)
		if err != nil {
			return 0, err
		}
		msgLen := int(binary.LittleEndian.Uint32(headerBufs.Materialize()))
		headerBufs.Free()
		if msgLen > altsRecordLengthLimit {
			return 0, fmt.Errorf("received the frame length %d larger than the limit %d", msgLen, altsRecordLengthLimit)
		}

		// read the message type header.
		framedMsgBufs, err := p.readBytes(msgLen)
		if err != nil {
			return 0, err
		}
		framedMsg := mem.DefaultBufferPool().Get(msgLen)
		framedMsgBufs.CopyTo(*framedMsg)
		framedMsgBufs.Free()
		msgType := binary.LittleEndian.Uint32((*framedMsg)[:msgTypeFieldSize])
		if msgType&0xff != altsRecordMsgType {
			return 0, fmt.Errorf("received frame with incorrect message type %v, expected lower byte %v",
				msgType, altsRecordMsgType)
		}

		ciphertext := (*framedMsg)[msgTypeFieldSize:]
		// Decrypt directly into the buffer, avoiding a copy from p.buf if
		// possible.
		if cap(b) >= len(ciphertext) {
			defer mem.DefaultBufferPool().Put(framedMsg)
			dec, err := p.crypto.Decrypt(b[:0], ciphertext)
			if err != nil {
				return 0, err
			}
			return len(dec), nil
		}
		// Decrypt requires that if the dst and ciphertext alias, they
		// must alias exactly. Code here used to use msg[:0], but msg
		// starts MsgLenFieldSize+msgTypeFieldSize bytes earlier than
		// ciphertext, so they alias inexactly. Using ciphertext[:0]
		// arranges the appropriate aliasing without needing to copy
		// ciphertext or use a separate destination buffer. For more info
		// check: https://golang.org/pkg/crypto/cipher/#AEAD.
		dec, err := p.crypto.Decrypt(ciphertext[:0], ciphertext)
		if err != nil {
			mem.DefaultBufferPool().Put(framedMsg)
			return 0, err
		}
		p.buf = framedMsg
		p.bufBytesRead = msgTypeFieldSize
		p.bufBytesTotal = msgTypeFieldSize + len(dec)
	}

	n = copy(b, (*p.buf)[p.bufBytesRead:p.bufBytesTotal])
	p.bufBytesRead += n
	if p.bufBytesRead == p.bufBytesTotal {
		mem.DefaultBufferPool().Put(p.buf)
		p.buf = nil
	}
	return n, nil
}

// Write encrypts, frames, and writes bytes from b to the underlying connection.
func (p *conn) Write(b []byte) (n int, err error) {
	n = len(b)
	// Calculate the output buffer size with framing and encryption overhead.
	numOfFrames := int(math.Ceil(float64(len(b)) / float64(p.payloadLengthLimit)))
	size := len(b) + numOfFrames*p.overhead
	// If writeBuf is too small, increase its size up to the maximum size.
	partialBSize := len(b)
	if size > altsWriteBufferMaxSize {
		size = altsWriteBufferMaxSize
		const numOfFramesInMaxWriteBuf = altsWriteBufferMaxSize / altsRecordDefaultLength
		partialBSize = numOfFramesInMaxWriteBuf * p.payloadLengthLimit
	}
	if len(p.writeBuf) < size {
		p.writeBuf = make([]byte, size)
	}

	for partialBStart := 0; partialBStart < len(b); partialBStart += partialBSize {
		partialBEnd := partialBStart + partialBSize
		if partialBEnd > len(b) {
			partialBEnd = len(b)
		}
		partialB := b[partialBStart:partialBEnd]
		writeBufIndex := 0
		for len(partialB) > 0 {
			payloadLen := len(partialB)
			if payloadLen > p.payloadLengthLimit {
				payloadLen = p.payloadLengthLimit
			}
			buf := partialB[:payloadLen]
			partialB = partialB[payloadLen:]

			// Write buffer contains: length, type, payload, and tag
			// if any.

			// 1. Fill in type field.
			msg := p.writeBuf[writeBufIndex+MsgLenFieldSize:]
			binary.LittleEndian.PutUint32(msg, altsRecordMsgType)

			// 2. Encrypt the payload and create a tag if any.
			msg, err = p.crypto.Encrypt(msg[:msgTypeFieldSize], buf)
			if err != nil {
				return n, err
			}

			// 3. Fill in the size field.
			binary.LittleEndian.PutUint32(p.writeBuf[writeBufIndex:], uint32(len(msg)))

			// 4. Increase writeBufIndex.
			writeBufIndex += len(buf) + p.overhead
		}
		nn, err := p.Conn.Write(p.writeBuf[:writeBufIndex])
		if err != nil {
			// We need to calculate the actual data size that was
			// written. This means we need to remove header,
			// encryption overheads, and any partially-written
			// frame data.
			numOfWrittenFrames := int(math.Floor(float64(nn) / float64(altsRecordDefaultLength)))
			return partialBStart + numOfWrittenFrames*p.payloadLengthLimit, err
		}
	}
	return n, nil
}
