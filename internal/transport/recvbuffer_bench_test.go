/*
 *
 * Copyright 2026 gRPC authors.
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

package transport

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc/internal/envconfig"
	"google.golang.org/grpc/mem"
)

// benchRecvBuffer abstracts over the recvBuffer implementations being
// compared so that the same benchmark bodies can drive both.
type benchRecvBuffer interface {
	put(recvMsg)
	// get blocks until a message is available or ctxDone is closed. It
	// returns false if ctxDone won.
	get(ctxDone <-chan struct{}) (recvMsg, bool)
}

// legacyRecvBuffer is a copy of the previous recvBuffer implementation, where
// the channel holds the head of the queue: put sends to the channel if the
// backlog is empty, and the reader selects on ctxDone and the channel and then
// calls load() to move the next backlog entry into the channel. It reuses
// recvBuffer's fields so that both implementations do identical bookkeeping
// apart from the synchronization strategy.
type legacyRecvBuffer struct {
	recvBuffer
}

func (b *legacyRecvBuffer) put(r recvMsg) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		r.buffer.Free()
		return
	}
	b.err = r.err
	if len(b.backlog) == 0 {
		select {
		case b.c <- r:
			return
		default:
		}
	}
	b.backlog = append(b.backlog, r)
	b.compactBacklogLocked(r)
}

func (b *legacyRecvBuffer) load() {
	b.mu.Lock()
	if len(b.backlog) > 0 {
		select {
		case b.c <- b.backlog[0]:
			if envconfig.EnableReceiveBufferCompaction && b.uncompactedSuffixLen == len(b.backlog) {
				b.uncompactedSuffixLen--
				b.uncompactedBytes -= b.backlog[0].buffer.Len()
			}
			b.backlog[0] = recvMsg{}
			b.backlog = b.backlog[1:]
		default:
		}
	}
	b.mu.Unlock()
}

func (b *legacyRecvBuffer) get(ctxDone <-chan struct{}) (recvMsg, bool) {
	select {
	case <-ctxDone:
		return recvMsg{}, false
	case m := <-b.c:
		b.load()
		return m, true
	}
}

type recvBufferImpl struct {
	name    string
	newFunc func() benchRecvBuffer
}

var recvBufferImpls = []recvBufferImpl{
	{
		name: "impl=legacy",
		newFunc: func() benchRecvBuffer {
			b := &legacyRecvBuffer{}
			b.init(mem.DefaultBufferPool())
			return b
		},
	},
	{
		name: "impl=handoff",
		newFunc: func() benchRecvBuffer {
			b := &recvBuffer{}
			b.init(mem.DefaultBufferPool())
			return b
		},
	},
}

type recvBufferCtx struct {
	name string
	// done returns the ctxDone channel to use and a cleanup func.
	done func() (<-chan struct{}, func())
}

var recvBufferCtxs = []recvBufferCtx{
	{
		// context.Background().Done() is nil, so the select degenerates to a
		// single-channel receive.
		name: "ctx=background",
		done: func() (<-chan struct{}, func()) { return nil, func() {} },
	},
	{
		// Real streams have a cancelable context, so the reader's select
		// involves two channels.
		name: "ctx=cancelable",
		done: func() (<-chan struct{}, func()) {
			ctx, cancel := context.WithCancel(context.Background())
			return ctx.Done(), cancel
		},
	},
}

// benchPayload is large enough (a typical DATA frame) that compaction never
// kicks in, so the benchmarks measure synchronization cost only. SliceBuffer's
// Free is a no-op, so the payload can be shared across messages.
var benchPayload = mem.SliceBuffer(make([]byte, 16*1024))

// runRecvBufferBenchmarks runs body for every combination of implementation
// and context kind.
func runRecvBufferBenchmarks(b *testing.B, body func(b *testing.B, rb benchRecvBuffer, ctxDone <-chan struct{})) {
	for _, c := range recvBufferCtxs {
		for _, impl := range recvBufferImpls {
			b.Run(fmt.Sprintf("%s/%s", c.name, impl.name), func(b *testing.B) {
				ctxDone, cancel := c.done()
				defer cancel()
				b.ReportAllocs()
				body(b, impl.newFunc(), ctxDone)
			})
		}
	}
}

// BenchmarkRecvBufferBatch measures the uncontended fast paths: a single
// goroutine puts batch messages and then reads them back, so the reader never
// blocks. Each op is one put plus one recv.
func BenchmarkRecvBufferBatch(b *testing.B) {
	for _, batch := range []int{1, 16, 256} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			runRecvBufferBenchmarks(b, func(b *testing.B, rb benchRecvBuffer, ctxDone <-chan struct{}) {
				msg := recvMsg{buffer: benchPayload}
				for i := 0; i < b.N; i += batch {
					n := min(batch, b.N-i)
					for range n {
						rb.put(msg)
					}
					for range n {
						if _, ok := rb.get(ctxDone); !ok {
							b.Fatal("get failed")
						}
					}
				}
			})
		})
	}
}

// BenchmarkRecvBufferPingPong measures the handoff to a blocked reader. The
// producer waits for an ack after every put, so the reader is (almost always)
// parked in recv when the next message arrives. Each op is one put, one recv
// and one ack round trip; the ack cost is identical for both implementations.
func BenchmarkRecvBufferPingPong(b *testing.B) {
	runRecvBufferBenchmarks(b, func(b *testing.B, rb benchRecvBuffer, ctxDone <-chan struct{}) {
		msg := recvMsg{buffer: benchPayload}
		ack := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			for range b.N {
				rb.put(msg)
				<-ack
			}
		}()
		for range b.N {
			if _, ok := rb.get(ctxDone); !ok {
				b.Error("get failed")
				return
			}
			ack <- struct{}{}
		}
		<-done
	})
}

// BenchmarkRecvBufferStream measures throughput with a producer and a
// consumer running concurrently and without any coordination, which mixes
// the backlog and blocked-reader paths. Each op is one put plus one recv.
func BenchmarkRecvBufferStream(b *testing.B) {
	runRecvBufferBenchmarks(b, func(b *testing.B, rb benchRecvBuffer, ctxDone <-chan struct{}) {
		msg := recvMsg{buffer: benchPayload}
		done := make(chan struct{})
		go func() {
			defer close(done)
			for range b.N {
				rb.put(msg)
			}
		}()
		for range b.N {
			if _, ok := rb.get(ctxDone); !ok {
				b.Error("get failed")
				return
			}
		}
		<-done
	})
}
