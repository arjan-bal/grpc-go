/*
 *
 * Copyright 2024 gRPC authors.
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

// Package pickfirstleaf contains the pick_first load balancing policy which
// will be the universal leaf policy after Dual Stack changes are implemented.
package pickfirstleaf

import (
	"encoding/json"
	"errors"
	"fmt"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/internal"
	internalgrpclog "google.golang.org/grpc/internal/grpclog"
	"google.golang.org/grpc/internal/pretty"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

const (
	subConnListConnecting uint32 = iota
	subConnListConnected
	subConnListClosed
)

func init() {
	balancer.Register(pickfirstBuilder{})
}

var logger = grpclog.Component("pick-first-leaf-lb")

const (
	// Name is the name of the pick_first balancer.
	Name      = "pick_first_leaf"
	logPrefix = "[pick-first-leaf-lb %p] "
)

type pickfirstBuilder struct{}

func (pickfirstBuilder) Build(cc balancer.ClientConn, _ balancer.BuildOptions) balancer.Balancer {
	b := &pickfirstBalancer{
		cc:           cc,
		addressIndex: newIndex(nil),
		subConns:     map[string]*scData{},
	}
	b.logger = internalgrpclog.NewPrefixLogger(logger, fmt.Sprintf(logPrefix, b))
	return b
}

func (pickfirstBuilder) Name() string {
	return Name
}

func (pickfirstBuilder) ParseConfig(js json.RawMessage) (serviceconfig.LoadBalancingConfig, error) {
	var cfg pfConfig
	if err := json.Unmarshal(js, &cfg); err != nil {
		return nil, fmt.Errorf("pickfirst: unable to unmarshal LB policy config: %s, error: %v", string(js), err)
	}
	return cfg, nil
}

type pfConfig struct {
	serviceconfig.LoadBalancingConfig `json:"-"`

	// If set to true, instructs the LB policy to shuffle the order of the list
	// of endpoints received from the name resolver before attempting to
	// connect to them.
	ShuffleAddressList bool `json:"shuffleAddressList"`
}

// scData keeps track of the current state of the subConn.
type scData struct {
	subConn balancer.SubConn
	state   connectivity.State
	addr    *resolver.Address
}

func newScData(b *pickfirstBalancer, addr resolver.Address) (*scData, error) {
	sd := &scData{
		state: connectivity.Idle,
		addr:  &addr,
	}
	sc, err := b.cc.NewSubConn([]resolver.Address{addr}, balancer.NewSubConnOptions{
		StateListener: func(state balancer.SubConnState) {
			// Store the state and delegate.
			sd.state = state.ConnectivityState
			b.updateSubConnState(sd, state)
		},
	})
	if err != nil {
		return nil, err
	}
	sd.subConn = sc
	return sd, nil
}

type pickfirstBalancer struct {
	logger       *internalgrpclog.PrefixLogger
	state        connectivity.State
	cc           balancer.ClientConn
	subConns     map[string]*scData
	addressIndex index
	firstPass    bool
	firstErr     error
	numTf        int
}

func (b *pickfirstBalancer) ResolverError(err error) {
	if b.logger.V(2) {
		b.logger.Infof("Received error from the name resolver: %v", err)
	}
	if b.state == connectivity.Shutdown {
		return
	}
	// The picker will not change since the balancer does not currently
	// report an error.
	if b.state != connectivity.TransientFailure {
		return
	}

	for _, sd := range b.subConns {
		sd.subConn.Shutdown()
	}
	for k := range b.subConns {
		delete(b.subConns, k)
	}
	b.addressIndex.updateEndpointList(nil)
	b.state = connectivity.TransientFailure
	b.cc.UpdateState(balancer.State{
		ConnectivityState: connectivity.TransientFailure,
		Picker:            &picker{err: fmt.Errorf("name resolver error: %v", err)},
	})
}

func (b *pickfirstBalancer) UpdateClientConnState(state balancer.ClientConnState) error {
	if b.state == connectivity.Shutdown {
		return fmt.Errorf("balancer is already closed")
	}
	if len(state.ResolverState.Addresses) == 0 && len(state.ResolverState.Endpoints) == 0 {
		// Cleanup state pertaining to the previous resolver state.
		// Treat an empty address list like an error by calling b.ResolverError.
		b.state = connectivity.TransientFailure
		b.ResolverError(errors.New("produced zero addresses"))
		return balancer.ErrBadResolverState
	}
	// We don't have to guard this block with the env var because ParseConfig
	// already does so.
	cfg, ok := state.BalancerConfig.(pfConfig)
	if state.BalancerConfig != nil && !ok {
		return fmt.Errorf("pickfirst: received illegal BalancerConfig (type %T): %v", state.BalancerConfig, state.BalancerConfig)
	}

	if b.logger.V(2) {
		b.logger.Infof("Received new config %s, resolver state %s", pretty.ToJSON(cfg), pretty.ToJSON(state.ResolverState))
	}

	newEndpoints := state.ResolverState.Endpoints
	if len(newEndpoints) == 0 {
		return fmt.Errorf("addresses are no longer supported, resolvers should produce endpoints instead")
	}

	// Since we have a new set of addresses, we are again at first pass
	b.firstPass = true
	b.firstErr = nil

	newEndpoints = deDupAddresses(newEndpoints)

	// Perform the optional shuffling described in gRFC A62. The shuffling will
	// change the order of endpoints but not touch the order of the addresses
	// within each endpoint. - A61
	if cfg.ShuffleAddressList {
		newEndpoints = append([]resolver.Endpoint{}, newEndpoints...)
		internal.ShuffleAddressListForTesting.(func(int, func(int, int)))(len(newEndpoints), func(i, j int) { newEndpoints[i], newEndpoints[j] = newEndpoints[j], newEndpoints[i] })
	}

	if b.state == connectivity.Ready {
		// If the previous ready subconn exists in new address list,
		// keep this connection and don't create new subconns.
		prevAddr, err := b.addressIndex.currentAddress()
		if err != nil {
			// This error should never happen when the state is READY if the
			// index is managed correctly.
			return fmt.Errorf("address index is in an invalid state: %v", err)
		}
		b.addressIndex.updateEndpointList(newEndpoints)
		if b.addressIndex.seekTo(prevAddr) {
			return nil
		}
		b.addressIndex.reset()
	} else {
		b.addressIndex.updateEndpointList(newEndpoints)
	}
	// Remove old subConns that were not in new address list.
	oldAddrs := map[string]bool{}
	for k := range b.subConns {
		oldAddrs[k] = true
	}

	// Flatten the new endpoint addresses.
	newAddrs := map[string]bool{}
	for _, endpoint := range newEndpoints {
		for _, addr := range endpoint.Addresses {
			newAddrs[addrKey(&addr)] = true
		}
	}

	// Shut them down and remove them.
	for oldAddr := range oldAddrs {
		if _, ok := newAddrs[oldAddr]; ok {
			continue
		}
		b.subConns[oldAddr].subConn.Shutdown()
		delete(b.subConns, oldAddr)
	}

	if len(oldAddrs) == 0 || b.state == connectivity.Ready || b.state == connectivity.Connecting {
		// Start connection attempt at first address.
		b.state = connectivity.Connecting
		b.cc.UpdateState(balancer.State{
			ConnectivityState: connectivity.Connecting,
			Picker:            &picker{err: balancer.ErrNoSubConnAvailable},
		})
		b.requestConnection()
	} else if b.state == connectivity.Idle {
		b.cc.UpdateState(balancer.State{
			ConnectivityState: connectivity.Idle,
			Picker: &idlePicker{
				exitIdle: b.ExitIdle,
			},
		})
	} else if b.state == connectivity.TransientFailure {
		b.requestConnection()
	}
	return nil
}

// UpdateSubConnState is unused as a StateListener is always registered when
// creating SubConns.
func (b *pickfirstBalancer) UpdateSubConnState(subConn balancer.SubConn, state balancer.SubConnState) {
	b.logger.Errorf("UpdateSubConnState(%v, %+v) called unexpectedly", subConn, state)
}

func (b *pickfirstBalancer) Close() {
	for _, sd := range b.subConns {
		sd.subConn.Shutdown()
	}
	for k := range b.subConns {
		delete(b.subConns, k)
	}
	b.state = connectivity.Shutdown
}

// ExitIdle moves the balancer out of idle state. It can be called concurrently
// by the idlePicker and clientConn so access to variables should be synchronized.
func (b *pickfirstBalancer) ExitIdle() {
	// TODO: Synchronize this.
	b.requestConnection()
}

// deDupAddresses ensures that each address belongs to only one endpoint.
func deDupAddresses(endpoints []resolver.Endpoint) []resolver.Endpoint {
	seenAddrs := map[string]bool{}
	newEndpoints := []resolver.Endpoint{}

	for _, ep := range endpoints {
		addrs := []resolver.Address{}
		for _, addr := range ep.Addresses {
			if _, ok := seenAddrs[addrKey(&addr)]; ok {
				continue
			}
			addrs = append(addrs, addr)
		}
		if len(addrs) == 0 {
			continue
		}
		newEndpoints = append(newEndpoints, resolver.Endpoint{
			Addresses:  addrs,
			Attributes: ep.Attributes,
		})
	}
	return newEndpoints
}

// shutdownRemaining shuts down remaining subConns. Called when a subConn
// becomes ready, which means that all other subConn must be shutdown.
func (b *pickfirstBalancer) shutdownRemaining(selected *scData) {
	for _, sd := range b.subConns {
		if sd.subConn != selected.subConn {
			sd.subConn.Shutdown()
		}
	}
	for k := range b.subConns {
		delete(b.subConns, k)
	}
	b.subConns[addrKey(selected.addr)] = selected
}

// requestConnection requests a connection to the next applicable address'
// subcon, creating one if necessary. Schedules a connection to next address in list as well.
// If the current channel has already attempted a connection, we attempt a connection
// to the next address/subconn in our list.  We assume that NewSubConn will never
// return an error.
func (b *pickfirstBalancer) requestConnection() {
	if !b.addressIndex.isValid() || b.state == connectivity.Shutdown {
		return
	}
	curAddr, err := b.addressIndex.currentAddress()
	if err != nil {
		// This should not never happen because we already check for validity and
		// return early above.
		return
	}
	sd, ok := b.subConns[addrKey(curAddr)]
	if !ok {
		sd, err = newScData(b, *curAddr)
		if err != nil {
			// This should never happen.
			b.logger.Errorf("Failed to create a subConn for address %v: %v", curAddr.String(), err)
			b.addressIndex.increment()
			b.requestConnection()
		}
		b.subConns[addrKey(curAddr)] = sd
	}

	switch sd.state {
	case connectivity.Idle:
		sd.subConn.Connect()
	case connectivity.TransientFailure:
		if !b.addressIndex.increment() {
			b.endFirstPass()
		}
		b.requestConnection()
	case connectivity.Ready:
		// Should never happen.
		b.logger.Warningf("Requesting a connection even though we have a READY subconn")
	}
}

func addrKey(addr *resolver.Address) string {
	return fmt.Sprintf("%s-%s", addr.Addr, addr.Attributes)
}

func (b *pickfirstBalancer) updateSubConnState(sd *scData, state balancer.SubConnState) {
	// Previously relevant subconns can still callback with state updates.
	// To prevent pickers from returning these obsolete subconns, this logic
	// is included to check if the current list of active subconns includes this
	// subconn.
	if activeSd, found := b.subConns[addrKey(sd.addr)]; !found || activeSd != sd {
		return
	}
	if state.ConnectivityState == connectivity.Shutdown {
		return
	}

	if state.ConnectivityState == connectivity.Ready {
		b.shutdownRemaining(sd)
		b.addressIndex.seekTo(sd.addr)
		b.state = connectivity.Ready
		b.firstErr = nil
		b.cc.UpdateState(balancer.State{
			ConnectivityState: connectivity.Ready,
			Picker:            &picker{result: balancer.PickResult{SubConn: sd.subConn}},
		})
		return
	}

	// If we are transitioning from READY to IDLE, shutdown and re-connect when
	// prompted.
	if state.ConnectivityState == connectivity.Idle && b.state == connectivity.Ready {
		b.state = connectivity.Idle
		b.addressIndex.reset()
		b.cc.UpdateState(balancer.State{
			ConnectivityState: connectivity.Idle,
			Picker: &idlePicker{
				exitIdle: b.ExitIdle,
			},
		})
		return
	}

	if b.firstPass {
		switch state.ConnectivityState {
		case connectivity.Connecting:
			if b.state == connectivity.Idle {
				b.cc.UpdateState(balancer.State{
					ConnectivityState: connectivity.Connecting,
					Picker:            &picker{err: balancer.ErrNoSubConnAvailable},
				})
			}
		case connectivity.TransientFailure:
			if b.firstErr == nil {
				b.firstErr = state.ConnectionError
			}
			curAddr, err := b.addressIndex.currentAddress()
			if err != nil {
				// This is not expected since we end the first pass when we
				// reach the end of the list.
				b.logger.Errorf("Current index is invalid during first pass: %v", err)
				return
			}
			if activeSd, found := b.subConns[addrKey(curAddr)]; !found || activeSd != sd {
				return
			}
			if b.addressIndex.increment() {
				b.requestConnection()
				return
			}
			// End of the first pass.
			b.endFirstPass()
		}
		return
	}

	// We have finished the first pass, keep re-connecting failing subconns.
	switch state.ConnectivityState {
	case connectivity.TransientFailure:
		b.numTf++
		// We request re-resolution when we've seen the same number of TFs as
		// subconns. It could be that a subconn has seen multiple TFs due to
		// differences in back-off durations, but this is a decent approximation.
		if b.numTf >= len(b.subConns) {
			b.numTf = 0
			b.cc.ResolveNow(resolver.ResolveNowOptions{})
		}
	case connectivity.Idle:
		sd.subConn.Connect()
	}
}

func (b *pickfirstBalancer) endFirstPass() {
	b.firstPass = false
	b.numTf = 0
	b.state = connectivity.TransientFailure
	b.cc.UpdateState(balancer.State{
		ConnectivityState: connectivity.TransientFailure,
		Picker:            &picker{err: b.firstErr},
	})
	// Re-request resolution.
	b.cc.ResolveNow(resolver.ResolveNowOptions{})
	// Start re-connecting all the subconns that are already in IDLE.
	for _, sd := range b.subConns {
		if sd.state == connectivity.Idle {
			sd.subConn.Connect()
		}
	}
}

type picker struct {
	result balancer.PickResult
	err    error
}

func (p *picker) Pick(balancer.PickInfo) (balancer.PickResult, error) {
	return p.result, p.err
}

// idlePicker is used when the SubConn is IDLE and kicks the SubConn into
// CONNECTING when Pick is called.
type idlePicker struct {
	exitIdle func()
}

func (i *idlePicker) Pick(balancer.PickInfo) (balancer.PickResult, error) {
	i.exitIdle()
	return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
}

// index is an Index as in 'i', the pointer to an entry. Not a "search index."
// All updates should be synchronized.
type index struct {
	endpointList []resolver.Endpoint
	endpointIdx  int
	addrIdx      int
}

// newIndex is the constructor for index.
func newIndex(endpointList []resolver.Endpoint) index {
	return index{
		endpointList: endpointList,
	}
}

func (i *index) isValid() bool {
	return i.endpointIdx < len(i.endpointList)
}

// increment moves to the next index in the address list. If at the last address
// in the address list, moves to the next endpoint in the endpoint list.
// This method returns false if it went off the list, true otherwise.
func (i *index) increment() bool {
	if !i.isValid() {
		return false
	}
	ep := i.endpointList[i.endpointIdx]
	i.addrIdx++
	if i.addrIdx >= len(ep.Addresses) {
		i.endpointIdx++
		i.addrIdx = 0
		return i.endpointIdx < len(i.endpointList)
	}
	return false
}

func (i *index) currentAddress() (*resolver.Address, error) {
	if !i.isValid() {
		return nil, fmt.Errorf("index is off the end of the address list")
	}
	return &i.endpointList[i.endpointIdx].Addresses[i.addrIdx], nil
}

func (i *index) reset() {
	i.endpointIdx = 0
	i.addrIdx = 0
}

func (i *index) updateEndpointList(endpointList []resolver.Endpoint) {
	i.endpointList = endpointList
	i.reset()
}

// seekTo returns false if the needle was not found and the current index was left unchanged.
func (i *index) seekTo(needle *resolver.Address) bool {
	for ei, endpoint := range i.endpointList {
		for ai, addr := range endpoint.Addresses {
			if !addr.Attributes.Equal(needle.Attributes) || addr.Addr != needle.Addr {
				continue
			}
			i.endpointIdx = ei
			i.addrIdx = ai
			return true
		}
	}
	return false
}

func (i *index) size() int {
	return len(i.endpointList)
}
