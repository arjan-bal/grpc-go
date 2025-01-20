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

// Package ringhash implements the ringhash balancer.
package ringhash

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/balancer/endpointsharding"
	"google.golang.org/grpc/balancer/lazy"
	"google.golang.org/grpc/balancer/pickfirst/pickfirstleaf"
	"google.golang.org/grpc/balancer/weightedroundrobin"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/internal/grpclog"
	"google.golang.org/grpc/internal/pretty"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

// Name is the name of the ring_hash balancer.
const Name = "ring_hash_experimental"

var lazyPickfirstConfig serviceconfig.LoadBalancingConfig

func init() {
	var err error
	lazyPickfirstConfig, err = endpointsharding.ParseConfig(json.RawMessage(fmt.Sprintf(`[{
		"%s": {
			"childPolicy": [{"%s": {}}]
		}
    }]`, lazy.Name, pickfirstleaf.Name)))
	if err != nil {
		logger.Fatal(err)
	}
	balancer.Register(bb{})
}

type bb struct{}

func (bb) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	b := &ringhashBalancer{
		ClientConn:     cc,
		endpointStates: resolver.NewEndpointMap(),
	}
	b.child = endpointsharding.NewBalancerWithoutAutoReconnect(b, opts)
	b.logger = prefixLogger(b)
	b.logger.Infof("Created")
	return b
}

func (bb) Name() string {
	return Name
}

func (bb) ParseConfig(c json.RawMessage) (serviceconfig.LoadBalancingConfig, error) {
	return parseConfig(c)
}

type ringhashBalancer struct {
	balancer.ClientConn
	logger *grpclog.PrefixLogger
	child  balancer.Balancer
	config *LBConfig

	mu                   sync.Mutex
	inhibitChildUpdates  bool
	shouldRegenerateRing bool
	endpointStates       *resolver.EndpointMap // Map from endpoint -> *endpointState
	orderedEndpoints     []resolver.Endpoint

	// ring is always in sync with endpoints. When endpoints change, a new ring
	// is generated. Note that address weights updates also regenerates the
	// ring.
	ring *ring
}

// UpdateState intercepts child balancer state updates. It updates the
// per-endpoint state stored in the ring, and also the aggregated state based on
// the child picker. It also reconciles the endpoint list. It sets
// `b.shouldRegenerateRing` to true if the new endpoints list is different from
// the previous, i.e either of the following is true:
// - an endpoint was added
// - an endpoint was removed
// - an endpoint's weight was updated
func (b *ringhashBalancer) UpdateState(state balancer.State) {
	b.mu.Lock()
	defer b.mu.Unlock()
	childStates := endpointsharding.ChildStatesFromPicker(state.Picker)
	// endpointsSet is the set converted from endpoints, used for quick lookup.
	endpointsSet := resolver.NewEndpointMap()

	for _, childState := range childStates {
		endpoint := childState.Endpoint
		endpointsSet.Set(endpoint, true)
		newWeight := getWeightAttribute(endpoint)
		var es *endpointState
		if val, ok := b.endpointStates.Get(endpoint); !ok {
			es = &endpointState{}
			b.endpointStates.Set(endpoint, es)
			b.shouldRegenerateRing = true
			// Set the first address only during creation time since it's hash
			// is used to create the ring. Even if the address ordering changes
			// in subsequent resolver updates, the endpoint hash should remain
			// the same.
			es.firstAddr = endpoint.Addresses[0].Addr
			es.balancer = childState.Balancer
		} else {
			// We have seen this endpoint before and created a `endpointState`
			// object for it. If the weight associated with the endpoint has
			// changed, update the endpoint state map with the new weight.
			// This will be used when a new ring is created.
			es = val.(*endpointState)
			if oldWeight := es.weight; oldWeight != newWeight {
				b.shouldRegenerateRing = true
			}
		}
		es.weight = newWeight
		es.mu.Lock()
		es.state = childState.State
		es.mu.Unlock()
	}

	for _, endpoint := range b.endpointStates.Keys() {
		if _, ok := endpointsSet.Get(endpoint); ok {
			continue
		}
		// endpoint was removed by resolver.
		b.endpointStates.Delete(endpoint)
		b.shouldRegenerateRing = true
	}

	b.updatePickerLocked()
}

func (b *ringhashBalancer) UpdateClientConnState(ccs balancer.ClientConnState) error {
	if b.logger.V(2) {
		b.logger.Infof("Received update from resolver, balancer config: %+v", pretty.ToJSON(ccs.BalancerConfig))
	}

	newConfig, ok := ccs.BalancerConfig.(*LBConfig)
	if !ok {
		return fmt.Errorf("unexpected balancer config with type: %T", ccs.BalancerConfig)
	}

	b.mu.Lock()
	b.inhibitChildUpdates = true
	// Save the endpoint list. It's used to try IDLE endpoints when previous
	// endpoints have reported TRANSIENT_FAILURE.
	b.orderedEndpoints = ccs.ResolverState.Endpoints
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		b.inhibitChildUpdates = false
		b.updatePickerLocked()
		b.mu.Unlock()
	}()

	// Make pickfirst children use health listeners for outlier detection and
	// health checking to work.
	ccs.ResolverState = pickfirstleaf.EnableHealthListener(ccs.ResolverState)
	childConfig := balancer.ClientConnState{
		BalancerConfig: lazyPickfirstConfig,
		ResolverState:  ccs.ResolverState,
	}
	if err := b.child.UpdateClientConnState(childConfig); err != nil {
		return err
	}

	b.mu.Lock()
	// Ring updates can happen due to the following:
	// 1. Addition or deletion of endpoints: The synchronous picker update from
	//    the child endpointsharding balancer would contain the list of updated
	//    endpoints.  Updates triggered by the child after handling the
	//    `UpdateClientConnState` call will not change the endpoint list.
	// 2. Change in the `LoadBalancerConfig`: Ring config such as max/min ring
	//    size.
	// To avoid extra ring updates, a boolean is used to track the need for a
	// ring update and the update is done only once at the end.
	//
	// If the ring configuration has changed, we need to regenerate the ring
	// while sending a new picker.
	if b.config == nil || b.config.MinRingSize != newConfig.MinRingSize || b.config.MaxRingSize != newConfig.MaxRingSize {
		b.shouldRegenerateRing = true
	}
	b.config = newConfig
	b.mu.Unlock()
	return nil
}

func (b *ringhashBalancer) ResolverError(err error) {
	b.child.ResolverError(err)
}

func (b *ringhashBalancer) UpdateSubConnState(sc balancer.SubConn, state balancer.SubConnState) {
	b.logger.Errorf("UpdateSubConnState(%v, %+v) called unexpectedly", sc, state)
}

func (b *ringhashBalancer) updatePickerLocked() {
	if b.inhibitChildUpdates {
		return
	}
	state := b.aggregatedStateLocked()

	// Start connecting to new endpoints if necessary.
	if state == connectivity.Connecting || state == connectivity.TransientFailure {
		// When overall state is TransientFailure, we need to make sure at least
		// one endpoint is attempting to connect, otherwise this balancer may
		// never get picks if the parent is priority.
		//
		// Because we report Connecting as the overall state when only one
		// endpoint is in TransientFailure, we do the same check for Connecting
		// here.
		//
		// Note that this check also covers deleting endpoints. E.g. if the
		// endpoint attempting to connect is deleted, and the overall state is
		// TF. Since there must be at least one endpoint attempting to connect,
		// we need to trigger one.
		var idleBalancer balancer.ExitIdler
		for _, e := range b.orderedEndpoints {
			val, ok := b.endpointStates.Get(e)
			if !ok {
				b.logger.Errorf("Missing endpoint information in picker update from child balancer: %v", e)
				continue
			}
			es := val.(*endpointState)
			es.mu.RLock()
			connState := es.state.ConnectivityState
			es.mu.RUnlock()
			if connState == connectivity.Connecting {
				idleBalancer = nil
				break
			}
			if idleBalancer == nil && connState == connectivity.Idle {
				idleBalancer = es.balancer
			}
		}
		if idleBalancer != nil {
			idleBalancer.ExitIdle()
		}
	}

	// Update the channel.
	if b.endpointStates.Len() > 0 && b.shouldRegenerateRing {
		// with a non-empty list of endpoints.
		b.ring = newRing(b.endpointStates, b.config.MinRingSize, b.config.MaxRingSize, b.logger)
	}
	b.shouldRegenerateRing = false
	var picker balancer.Picker
	if b.endpointStates.Len() == 0 {
		picker = base.NewErrPicker(errors.New("produced zero addresses"))
	} else {
		picker = newPicker(b.ring, b.logger)
	}
	b.logger.Infof("Pushing new state %v and picker %p", state, picker)
	b.ClientConn.UpdateState(balancer.State{
		ConnectivityState: state,
		Picker:            picker,
	})
}

func (b *ringhashBalancer) Close() {
	b.logger.Infof("Shutdown")
	b.child.Close()
}

func (b *ringhashBalancer) ExitIdle() {
	// ExitIdle implementation is a no-op because connections are either
	// triggers from picks or from child balancer state changes.
}

// aggregatedStateLocked records returns the aggregated child balancers state
// based on the following rules.
//   - If there is at least one endpoint in READY state, report READY.
//   - If there are 2 or more endpoints in TRANSIENT_FAILURE state, report
//     TRANSIENT_FAILURE.
//   - If there is at least one endpoint in CONNECTING state, report CONNECTING.
//   - If there is one endpoint in TRANSIENT_FAILURE and there is more than one
//     endpoint, report state CONNECTING.
//   - If there is at least one endpoint in Idle state, report Idle.
//   - Otherwise, report TRANSIENT_FAILURE.
//
// Note that if there are 1 connecting, 2 transient failure, the overall state
// is transient failure. This is because the second transient failure is a
// fallback of the first failing endpoint, and we want to report transient
// failure to failover to the lower priority.
func (b *ringhashBalancer) aggregatedStateLocked() connectivity.State {
	var nums [5]int
	for _, val := range b.endpointStates.Values() {
		es := val.(*endpointState)
		es.mu.RLock()
		nums[es.state.ConnectivityState]++
		es.mu.RUnlock()
	}

	if nums[connectivity.Ready] > 0 {
		return connectivity.Ready
	}
	if nums[connectivity.TransientFailure] > 1 {
		return connectivity.TransientFailure
	}
	if nums[connectivity.Connecting] > 0 {
		return connectivity.Connecting
	}
	if nums[connectivity.TransientFailure] == 1 && b.endpointStates.Len() > 1 {
		return connectivity.Connecting
	}
	if nums[connectivity.Idle] > 0 {
		return connectivity.Idle
	}
	return connectivity.TransientFailure
}

// getWeightAttribute is a convenience function which returns the value of the
// weight attribute stored in the BalancerAttributes field of addr, using the
// weightedroundrobin package.
//
// When used in the xDS context, the weight attribute is guaranteed to be
// non-zero. But, when used in a non-xDS context, the weight attribute could be
// unset. A Default of 1 is used in the latter case.
func getWeightAttribute(e resolver.Endpoint) uint32 {
	w := weightedroundrobin.GetAddrInfoFromEndpoint(e).Weight
	if w == 0 {
		return 1
	}
	return w
}

type endpointState struct {
	firstAddr string
	weight    uint32
	balancer  balancer.ExitIdler

	mu    sync.RWMutex
	state balancer.State
}
