/*
 *
 * Copyright 2025 gRPC authors.
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

// Package lazy contains load balancer that starts in IDLE instead of
// CONNECTING. Once it starts connecting, it instantiates its delegate.
//
// # Experimental
//
// Notice: This package is EXPERIMENTAL and may be changed or removed in a
// later release.
package lazy

import (
	"encoding/json"
	"fmt"
	"sync"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	"google.golang.org/grpc/internal/balancer/gracefulswitch"
	internalgrpclog "google.golang.org/grpc/internal/grpclog"
)

func init() {
	balancer.Register(builder{})
}

var logger = grpclog.Component("lazy-lb")

const (
	// Name is the name of the lazy balancer.
	Name      = "lazy"
	logPrefix = "[lazy-lb %p] "
)

type builder struct{}

func (builder) Build(cc balancer.ClientConn, bOpts balancer.BuildOptions) balancer.Balancer {
	b := &lazyBalancer{
		cc:           cc,
		buildOptions: bOpts,
	}
	b.logger = internalgrpclog.NewPrefixLogger(logger, fmt.Sprintf(logPrefix, b))
	return b
}

func (builder) Name() string {
	return Name
}

type lazyBalancer struct {
	// The following fields are initialized at build time and read-only after
	// that and therefore do not need to be guarded by a mutex.
	cc           balancer.ClientConn
	buildOptions balancer.BuildOptions
	logger       *internalgrpclog.PrefixLogger

	// The mutex is used to ensure synchronization of calls from the idle picker
	// and the channel.
	mu                     sync.Mutex
	delegate               balancer.Balancer
	latestClientConnState  *balancer.ClientConnState
	latestResolverError    error
	updatedClientConnState bool
}

// Close implements balancer.Balancer.
func (lb *lazyBalancer) Close() {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.delegate != nil {
		lb.delegate.Close()
		lb.delegate = nil
	}
}

// ResolverError implements balancer.Balancer.
func (lb *lazyBalancer) ResolverError(err error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.delegate != nil {
		lb.delegate.ResolverError(err)
		return
	}
	lb.latestResolverError = err
	lb.updateBalancerStateLocked()
}

// UpdateClientConnState implements balancer.Balancer.
func (lb *lazyBalancer) UpdateClientConnState(ccs balancer.ClientConnState) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if childLBCfg, ok := ccs.BalancerConfig.(*lbCfg); !ok {
		lb.logger.Errorf("Got LB config of unexpected type: %v", ccs.BalancerConfig)
		ccs.BalancerConfig = nil
	} else {
		ccs.BalancerConfig = childLBCfg.childLBCfg
	}
	if lb.delegate != nil {
		return lb.delegate.UpdateClientConnState(ccs)
	}

	if childLBCfg, ok := ccs.BalancerConfig.(*lbCfg); !ok {
		lb.logger.Errorf("Got LB config of unexpected type: %v", ccs.BalancerConfig)
		ccs.BalancerConfig = nil
	} else {
		ccs.BalancerConfig = childLBCfg.childLBCfg
	}
	lb.latestClientConnState = &ccs
	lb.latestResolverError = nil
	lb.updateBalancerStateLocked()
	return nil
}

// UpdateSubConnState implements balancer.Balancer.
func (lb *lazyBalancer) UpdateSubConnState(balancer.SubConn, balancer.SubConnState) {
	// UpdateSubConnState is deprecated.
}

func (lb *lazyBalancer) ExitIdle() {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.delegate != nil {
		if d, ok := lb.delegate.(balancer.ExitIdler); ok {
			d.ExitIdle()
		}
		return
	}
	lb.delegate = gracefulswitch.NewBalancer(lb.cc, lb.buildOptions)
	if lb.latestClientConnState != nil {
		if err := lb.UpdateClientConnState(*lb.latestClientConnState); err != nil {
			if err == balancer.ErrBadResolverState {
				lb.cc.ResolveNow(resolver.ResolveNowOptions{})
			} else {
				lb.logger.Warningf("Error from child policy on receiving initial state: %v", err)
			}
		}
		lb.latestClientConnState = nil
	}
	if lb.latestResolverError != nil {
		lb.delegate.ResolverError(lb.latestResolverError)
		lb.latestResolverError = nil
	}
}

func (lb *lazyBalancer) updateBalancerStateLocked() {
	// optimization to avoid extra picker updates.
	if lb.updatedClientConnState {
		return
	}
	lb.cc.UpdateState(balancer.State{
		ConnectivityState: connectivity.Idle,
		Picker:            &idlePicker{exitIdle: sync.OnceFunc(lb.ExitIdle)},
	})
	lb.updatedClientConnState = true
}

type lbCfg struct {
	serviceconfig.LoadBalancingConfig
	childLBCfg serviceconfig.LoadBalancingConfig
}

func (b *builder) ParseConfig(lbConfig json.RawMessage) (serviceconfig.LoadBalancingConfig, error) {
	jsonReprsentation := &struct {
		ChildPolicy     json.RawMessage
		NoAutoReconnect bool
	}{}
	if err := json.Unmarshal(lbConfig, jsonReprsentation); err != nil {
		return nil, err
	}
	childCfg, err := gracefulswitch.ParseConfig(jsonReprsentation.ChildPolicy)
	if err != nil {
		return nil, err
	}
	return &lbCfg{childLBCfg: childCfg}, nil
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
