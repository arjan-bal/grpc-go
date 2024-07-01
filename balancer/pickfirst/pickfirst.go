/*
 *
 * Copyright 2017 gRPC authors.
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

// Package pickfirst contains the pick_first load balancing policy.
package pickfirst

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/internal"
	internalgrpclog "google.golang.org/grpc/internal/grpclog"
	"google.golang.org/grpc/internal/pretty"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

func init() {
	balancer.Register(pickfirstBuilder{})
	internal.ShuffleAddressListForTesting = func(n int, swap func(i, j int)) { rand.Shuffle(n, swap) }
}

var logger = grpclog.Component("pick-first-lb")

const (
	// Name is the name of the pick_first balancer.
	Name      = "pick_first"
	logPrefix = "[pick-first-lb %p] "
)

type pickfirstBuilder struct{}

func (pickfirstBuilder) Build(cc balancer.ClientConn, opt balancer.BuildOptions) balancer.Balancer {
	b := &pickfirstBalancer{cc: cc}
	b.logger = internalgrpclog.NewPrefixLogger(logger, fmt.Sprintf(logPrefix, b))
	return b
}

func (pickfirstBuilder) Name() string {
	return Name
}

type pfConfig struct {
	serviceconfig.LoadBalancingConfig `json:"-"`

	// If set to true, instructs the LB policy to shuffle the order of the list
	// of endpoints received from the name resolver before attempting to
	// connect to them.
	ShuffleAddressList bool `json:"shuffleAddressList"`
}

// TODO(arjan-bal): Maybe guard using mutex?
type subConnList struct {
	subConns        []balancer.SubConn
	attemptingIndex int
	b               *pickfirstBalancer
	lastFailure     error
	shuttingDown    bool
}

func newSubConnList(addrs []resolver.Address, b *pickfirstBalancer) *subConnList {
	if b.subConnList != nil {
		if b.logger.V(2) {
			b.logger.Infof("Closing older subConnList")
		}
		b.subConnList.reset()
	}
	sl := &subConnList{}
	b.subConnList = sl
	subConns := sl.subConns
	stateListener := func(subConn balancer.SubConn, state balancer.SubConnState) {
		if b.logger.V(2) {
			b.logger.Infof("Received SubConn state update: %p, %+v", subConn, state)
		}
		if subConn == b.selectedSubConn {
			// As we set the selected subConn only once it's ready, the only
			// possible transitions are to IDLE and SHUTDOWN.
			switch state.ConnectivityState {
			case connectivity.Shutdown:
				return
			case connectivity.Idle:
				// We stay in TransientFailure until we are Ready. Also kick the
				// subConn out of Idle into Connecting. See A62.
				b.goIdle()
				return
			default:
				if b.logger.V(2) {
					b.logger.Infof("Ignoring unexpected transition of selected subConn %p to %v", &subConn, state.ConnectivityState)
				}
			}
			return
		}
		// If this list is already closed, either by a resolver update or we've
		// selected a subConn, ignore the update.
		if sl.shuttingDown {
			if b.logger.V(2) {
				b.logger.Infof("Ignoring state update for non active SubConn %p to %v", &subConn, state.ConnectivityState)
			}
			return
		}
		// We are still in connecting stage.
		// We care only about transient failure or connected.
		switch state.ConnectivityState {
		case connectivity.TransientFailure:
			// If its the initial connection attempt, try to connect to the next subConn.
			if b.logger.V(2) {
				b.logger.Infof("SubConn %p failed to connect due to error: %v", state.ConnectionError)
			}
			sl.attemptingIndex++
			sl.lastFailure = state.ConnectionError
			sl.startConnectingNextSubConn()
			return
		case connectivity.Ready:
			// Cleanup and update the picker to use the subconn.
			b.unsetSelectedSubConn()
			b.selectedSubConn = subConn
			b.state = state.ConnectivityState
			b.cc.UpdateState(balancer.State{
				ConnectivityState: state.ConnectivityState,
				Picker:            &picker{result: balancer.PickResult{SubConn: subConn}},
			})
			sl.reset()
			return
		default:
			if b.logger.V(2) {
				b.logger.Infof("Ignoring update for the subConn %p to state %v", subConn, state.ConnectivityState)
			}
		}
	}

	for _, addr := range addrs {
		var subConn balancer.SubConn
		subConn, err := b.cc.NewSubConn([]resolver.Address{addr}, balancer.NewSubConnOptions{
			StateListener: func(state balancer.SubConnState) {
				stateListener(subConn, state)
			},
		})
		if err != nil {
			if b.logger.V(2) {
				b.logger.Infof("Ignoring failure, could not create a subConn for address %q due to error: %v", addr, err)
			}
			continue
		}
		if b.logger.V(2) {
			b.logger.Infof("Created a subConn for address %q", addr)
		}
		subConns = append(subConns, subConn)
	}
	return sl
}

func (sl *subConnList) reset() {
	if sl == nil {
		return
	}
	if sl.shuttingDown {
		return
	}
	sl.shuttingDown = true
	// Close all the subConns except the selected one. The selected subConn
	// will be closed by the balancer.
	for _, sc := range sl.subConns {
		if sc == sl.b.selectedSubConn {
			continue
		}
		sc.Shutdown()
	}
	sl.subConns = nil
}

func (sl *subConnList) startConnectingNextSubConn() {
	if sl.shuttingDown {
		return
	}
	if sl.attemptingIndex < len(sl.subConns) {
		sl.subConns[sl.attemptingIndex].Connect()
		return
	}
	// Attempted to connect to each subConn once, enter transient failure and
	// keep trying to connect to subConns that enter idle.
	sl.b.state = connectivity.TransientFailure
	sl.b.cc.UpdateState(balancer.State{
		ConnectivityState: connectivity.TransientFailure,
		Picker:            &picker{err: sl.lastFailure},
	})
	// Drop the existing (working) connection, if any.  This may be
	// sub-optimal, but we can't ignore what the control plane told us.
	sl.b.unsetSelectedSubConn()
	// TODO: We need to start connecting to subconns in order, ignoring subConns
	// in transient failure (not completed the backoff)
	// There is no method on subConns to tell their present state, maybe need to
	// wrap subConns and keep track of latest state from the listener.
}

func (pickfirstBuilder) ParseConfig(js json.RawMessage) (serviceconfig.LoadBalancingConfig, error) {
	var cfg pfConfig
	if err := json.Unmarshal(js, &cfg); err != nil {
		return nil, fmt.Errorf("pickfirst: unable to unmarshal LB policy config: %s, error: %v", string(js), err)
	}
	return cfg, nil
}

type pickfirstBalancer struct {
	logger            *internalgrpclog.PrefixLogger
	state             connectivity.State
	cc                balancer.ClientConn
	subConnList       *subConnList
	selectedSubConn   balancer.SubConn
	latestAddressList []resolver.Address
	shuttingDown      bool
}

func (b *pickfirstBalancer) ResolverError(err error) {
	if b.logger.V(2) {
		b.logger.Infof("Received error from the name resolver: %v", err)
	}
	// TODO: Check this condition.
	if b.selectedSubConn == nil {
		b.state = connectivity.TransientFailure
	}

	if b.state != connectivity.TransientFailure {
		// The picker will not change since the balancer does not currently
		// report an error.
		return
	}
	b.cc.UpdateState(balancer.State{
		ConnectivityState: connectivity.TransientFailure,
		Picker:            &picker{err: fmt.Errorf("name resolver error: %v", err)},
	})
}

func (b *pickfirstBalancer) unsetSelectedSubConn() {
	if b.selectedSubConn != nil {
		b.selectedSubConn.Shutdown()
		b.selectedSubConn = nil
	}
}

func (b *pickfirstBalancer) goIdle() {
	b.unsetSelectedSubConn()
	callback := func() {
		err := b.attemptToConnectUsingLatestAddrs()
		if err == nil {
			return
		}
		if errors.Is(err, balancer.ErrBadResolverState) {
			b.cc.ResolveNow(resolver.ResolveNowOptions{})
		}
	}
	b.cc.UpdateState(balancer.State{
		ConnectivityState: connectivity.Idle,
		Picker: &idlePicker{
			callback: callback,
		},
	})

}

type Shuffler interface {
	ShuffleAddressListForTesting(n int, swap func(i, j int))
}

func ShuffleAddressListForTesting(n int, swap func(i, j int)) { rand.Shuffle(n, swap) }

func (b *pickfirstBalancer) attemptToConnectUsingLatestAddrs() error {
	subConnList := newSubConnList(b.latestAddressList, b)
	if len(subConnList.subConns) == 0 {
		b.state = connectivity.TransientFailure
		b.cc.UpdateState(balancer.State{
			ConnectivityState: connectivity.TransientFailure,
			// TODO: Improve this error message.
			Picker: &picker{err: fmt.Errorf("empt address list")},
		})
		b.unsetSelectedSubConn()
		return balancer.ErrBadResolverState
	}
	b.state = connectivity.Idle
	b.cc.UpdateState(balancer.State{
		ConnectivityState: connectivity.Connecting,
		Picker:            &picker{err: balancer.ErrNoSubConnAvailable},
	})
	b.subConnList.startConnectingNextSubConn()
	return nil
}

func (b *pickfirstBalancer) isIdle() bool {
	return b.state == connectivity.Idle && b.subConnList == nil
}

func (b *pickfirstBalancer) UpdateClientConnState(state balancer.ClientConnState) error {
	if len(state.ResolverState.Addresses) == 0 && len(state.ResolverState.Endpoints) == 0 {
		// The resolver reported an empty address list. Treat it like an error by
		// calling b.ResolverError.
		b.unsetSelectedSubConn()
		// Shut down the old subConnList. All addresses were removed, so it is
		// no longer valid.
		b.subConnList.reset()
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

	var addrs []resolver.Address
	if endpoints := state.ResolverState.Endpoints; len(endpoints) != 0 {
		// Perform the optional shuffling described in gRFC A62. The shuffling will
		// change the order of endpoints but not touch the order of the addresses
		// within each endpoint. - A61
		if cfg.ShuffleAddressList {
			endpoints = append([]resolver.Endpoint{}, endpoints...)
			internal.ShuffleAddressListForTesting.(func(int, func(int, int)))(len(endpoints), func(i, j int) { endpoints[i], endpoints[j] = endpoints[j], endpoints[i] })
		}

		// "Flatten the list by concatenating the ordered list of addresses for each
		// of the endpoints, in order." - A61
		for _, endpoint := range endpoints {
			// "In the flattened list, interleave addresses from the two address
			// families, as per RFC-8304 section 4." - A61
			// TODO: support the above language.
			addrs = append(addrs, endpoint.Addresses...)
		}
	} else {
		// Endpoints not set, process addresses until we migrate resolver
		// emissions fully to Endpoints. The top channel does wrap emitted
		// addresses with endpoints, however some balancers such as weighted
		// target do not forward the corresponding correct endpoints down/split
		// endpoints properly. Once all balancers correctly forward endpoints
		// down, can delete this else conditional.
		addrs = state.ResolverState.Addresses
		if cfg.ShuffleAddressList {
			addrs = append([]resolver.Address{}, addrs...)
			rand.Shuffle(len(addrs), func(i, j int) { addrs[i], addrs[j] = addrs[j], addrs[i] })
		}
	}

	b.latestAddressList = addrs
	return b.attemptToConnectUsingLatestAddrs()
}

// UpdateSubConnState is unused as a StateListener is always registered when
// creating SubConns.
func (b *pickfirstBalancer) UpdateSubConnState(subConn balancer.SubConn, state balancer.SubConnState) {
	b.logger.Errorf("UpdateSubConnState(%v, %+v) called unexpectedly", subConn, state)
}

func (b *pickfirstBalancer) Close() {
	b.shuttingDown = true
	b.unsetSelectedSubConn()
	b.subConnList.reset()
}

func (b *pickfirstBalancer) ExitIdle() {
	if b.shuttingDown {
		return
	}
	if !b.isIdle() {
		return
	}
	err := b.attemptToConnectUsingLatestAddrs()
	if err == nil {
		return
	}
	if b.logger.V(2) {
		b.logger.Infof("Error while trying to connect to subConns: %v", err)
	}
	if errors.Is(err, balancer.ErrBadResolverState) {
		// Request a re-resolution.
		b.cc.ResolveNow(resolver.ResolveNowOptions{})
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
	callback func()
}

func (i *idlePicker) Pick(balancer.PickInfo) (balancer.PickResult, error) {
	i.callback()
	return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
}
