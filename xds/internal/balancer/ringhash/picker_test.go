/*
 *
 * Copyright 2021 gRPC authors.
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

package ringhash

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/grpclog"
	igrpclog "google.golang.org/grpc/internal/grpclog"
	"google.golang.org/grpc/internal/testutils"
	"google.golang.org/grpc/resolver"
)

var testSubConns []*testutils.TestSubConn

func init() {
	for i := 0; i < 8; i++ {
		testSubConns = append(testSubConns, testutils.NewTestSubConn(fmt.Sprint(i)))
	}
}

// fakePicker is used to mock pickers from child pickfirst balancers.
type fakePicker struct {
	connectivityState connectivity.State
	subConn           *testutils.TestSubConn
	tfError           error
}

func (p *fakePicker) Pick(balancer.PickInfo) (balancer.PickResult, error) {
	switch p.connectivityState {
	case connectivity.Idle:
		p.subConn.Connect()
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	case connectivity.Connecting:
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	case connectivity.Ready:
		return balancer.PickResult{SubConn: p.subConn}, nil
	default:
		return balancer.PickResult{}, p.tfError
	}
}

func testRingAndEndpointStates(cStats []connectivity.State) (*ring, *resolver.EndpointMap) {
	var items []*ringEntry
	epStates := resolver.NewEndpointMap()
	for i, st := range cStats {
		testSC := testSubConns[i]
		items = append(items, &ringEntry{
			idx:       i,
			hash:      uint64((i + 1) * 10),
			firstAddr: testSC.String(),
		})
		ep := resolver.Endpoint{
			Addresses: []resolver.Address{{Addr: testSC.String()}},
		}
		epState := &endpointState{
			firstAddr: testSC.String(),
			state: balancer.State{
				ConnectivityState: st,
				Picker: &fakePicker{
					connectivityState: st,
					tfError:           fmt.Errorf("%d", i),
					subConn:           testSC,
				},
			},
		}
		epStates.Set(ep, epState)
	}
	return &ring{items: items}, epStates
}

func (s) TestPickerPickFirstTwo(t *testing.T) {
	tests := []struct {
		name               string
		connectivityStates []connectivity.State
		hash               uint64
		wantSC             balancer.SubConn
		wantErr            error
		wantSCToConnect    balancer.SubConn
	}{
		{
			name:               "picked is Ready",
			connectivityStates: []connectivity.State{connectivity.Ready, connectivity.Idle},
			hash:               5,
			wantSC:             testSubConns[0],
		},
		{
			name:               "picked is connecting, queue",
			connectivityStates: []connectivity.State{connectivity.Connecting, connectivity.Idle},
			hash:               5,
			wantErr:            balancer.ErrNoSubConnAvailable,
		},
		{
			name:               "picked is Idle, connect and queue",
			connectivityStates: []connectivity.State{connectivity.Idle, connectivity.Idle},
			hash:               5,
			wantErr:            balancer.ErrNoSubConnAvailable,
			wantSCToConnect:    testSubConns[0],
		},
		{
			name:               "picked is TransientFailure, next is ready, return",
			connectivityStates: []connectivity.State{connectivity.TransientFailure, connectivity.Ready},
			hash:               5,
			wantSC:             testSubConns[1],
		},
		{
			name:               "picked is TransientFailure, next is connecting, queue",
			connectivityStates: []connectivity.State{connectivity.TransientFailure, connectivity.Connecting},
			hash:               5,
			wantErr:            balancer.ErrNoSubConnAvailable,
		},
		{
			name:               "picked is TransientFailure, next is Idle, connect and queue",
			connectivityStates: []connectivity.State{connectivity.TransientFailure, connectivity.Idle},
			hash:               5,
			wantErr:            balancer.ErrNoSubConnAvailable,
			wantSCToConnect:    testSubConns[1],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ring, epStates := testRingAndEndpointStates(tt.connectivityStates)
			p := newPicker(ring, epStates, igrpclog.NewPrefixLogger(grpclog.Component("xds"), "rh_test"))
			got, err := p.Pick(balancer.PickInfo{
				Ctx: SetRequestHash(context.Background(), tt.hash),
			})
			if err != tt.wantErr {
				t.Errorf("Pick() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got.SubConn != tt.wantSC {
				t.Errorf("Pick() got = %v, want picked SubConn: %v", got, tt.wantSC)
			}
			if sc := tt.wantSCToConnect; sc != nil {
				select {
				case <-sc.(*testutils.TestSubConn).ConnectCh:
				case <-time.After(defaultTestShortTimeout):
					t.Errorf("timeout waiting for Connect() from SubConn %v", sc)
				}
			}
		})
	}
}
