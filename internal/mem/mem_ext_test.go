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

package mem_test

import (
	"testing"

	"google.golang.org/grpc/internal/mem"
)

func TestBufferPool_Zeroing(t *testing.T) {
	tests := []struct {
		name         string
		factory      func() (*mem.BinaryTieredBufferPool, error)
		expectZeroed bool
	}{
		{
			name: "NewBinaryTieredBufferPool",
			factory: func() (*mem.BinaryTieredBufferPool, error) {
				return mem.NewBinaryTieredBufferPool(10)
			},
			expectZeroed: true,
		},
		{
			name: "NewBinaryTieredBufferPoolUninitialized",
			factory: func() (*mem.BinaryTieredBufferPool, error) {
				return mem.NewDirtyBinaryTieredBufferPool(10)
			},
			expectZeroed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool, err := tt.factory()
			if err != nil {
				t.Fatalf("Failed to create pool: %v", err)
			}

			// Try multiple times to ensure we hit the pool reuse path
			reusedDirty := false
			for range 100 {
				// Get a buffer
				size := 1024
				buf := pool.Get(size)

				// Check content
				isZeroed := true
				for _, b := range *buf {
					if b != 0 {
						isZeroed = false
						break
					}
				}

				if tt.expectZeroed && !isZeroed {
					t.Fatalf("Buffer should be zeroed but found dirty bytes")
				}

				if !tt.expectZeroed && !isZeroed {
					reusedDirty = true
				}

				// Fill it with data
				for i := range *buf {
					(*buf)[i] = 0xFF
				}

				// Put it back
				pool.Put(buf)
			}

			if !tt.expectZeroed && !reusedDirty {
				t.Logf("Warning: Could not trigger buffer reuse to verify non-zeroing (GC or sync.Pool behavior)")
			}
		})
	}
}
