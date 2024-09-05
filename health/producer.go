package health

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/health/genericproducer"
	"google.golang.org/grpc/internal"
	"google.golang.org/grpc/status"
)

func init() {
	producerBuilderSingleton = &producerBuilder{}
	internal.EnableHealthCheckViaProducer = EnableHealthCheck
}

type producerBuilder struct{}

var producerBuilderSingleton *producerBuilder

type connectivityStateListener struct {
	p *healthServiceProducer
}

func (l *connectivityStateListener) OnStateChange(newState balancer.SubConnState) {
	l.p.mu.Lock()
	defer l.p.mu.Unlock()
	defer func() {
		// Propagate updates down the listener chain.
		l.p.updateStateLocked()
	}()
	if l.p.shutdown {
		return
	}
	prevState := l.p.connectivityState
	l.p.connectivityState = newState
	if prevState.ConnectivityState == newState.ConnectivityState {
		return
	}
	// If connectivity state is not READY.
	if newState.ConnectivityState != connectivity.Ready {
		// Stop the health check if it's running.
		if l.p.currentAttemptMarker != nil {
			// Connection failure, stop health check.
			if l.p.stopClientFn != nil {
				l.p.stopClientFn()
				l.p.stopClientFn = nil
			}
			l.p.currentAttemptMarker = nil
		}
	} else {
		// Start the client health check if not started.
		if l.p.currentAttemptMarker == nil {
			l.p.healthState = balancer.SubConnState{
				ConnectivityState: connectivity.Connecting,
				ConnectionError:   balancer.ErrNoSubConnAvailable,
			}
			l.p.startHealthCheckLocked()
		}
	}
}

// Build constructs and returns a producer and its cleanup function
func (*producerBuilder) Build(cci any) (balancer.Producer, func()) {
	p := &healthServiceProducer{
		cc:                cci.(grpc.ClientConnInterface),
		mu:                sync.Mutex{},
		connectivityState: balancer.SubConnState{ConnectivityState: connectivity.Idle},
		healthState: balancer.SubConnState{
			ConnectivityState: connectivity.Idle,
		},
	}
	return p, sync.OnceFunc(func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.shutdown = true
		p.currentAttemptMarker = nil
		if p.stopClientFn != nil {
			p.stopClientFn()
			p.stopClientFn = nil
		}
		if p.unregisterConnListener != nil {
			p.unregisterConnListener()
			p.unregisterConnListener = nil
		}
		p.healthState = balancer.SubConnState{
			ConnectivityState: connectivity.Ready,
		}
	})
}

type healthServiceProducer struct {
	cc                     grpc.ClientConnInterface
	mu                     sync.Mutex
	connectivityState      balancer.SubConnState
	healthState            balancer.SubConnState
	listener               balancer.StateListener
	unregisterConnListener func()
	opts                   *balancer.HealthCheckOptions
	stopClientFn           func()
	currentAttemptMarker   *struct{}
	shutdown               bool
}

// EnableHealthCheck enabled the health check service client to perform health
// checks for the subchannel.
func EnableHealthCheck(opts balancer.HealthCheckOptions, sc balancer.SubConn) func() {
	pr, closeFn := sc.GetOrBuildProducer(producerBuilderSingleton)
	p := pr.(*healthServiceProducer)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.listener != nil {
		panic("Attempting to start health check multiple times on the same subchannel")
	}
	var closeGenericProducer func()
	p.listener, closeGenericProducer = genericproducer.SwapRootListener(&connectivityStateListener{p: p}, sc)
	ls := &connectivityStateListener{
		p: p,
	}
	sc.RegisterConnectivityListner(ls)
	p.unregisterConnListener = func() {
		sc.UnregisterConnectivityListner(ls)
	}
	p.opts = &opts
	return func() {
		closeFn()
		closeGenericProducer()
	}
}

func (p *healthServiceProducer) updateStateLocked() {
	if p.connectivityState.ConnectivityState == connectivity.Ready {
		p.listener.OnStateChange(p.healthState)
	} else {
		p.listener.OnStateChange(p.connectivityState)
	}
}

func (p *healthServiceProducer) startHealthCheckLocked() {
	marker := &struct{}{}
	p.currentAttemptMarker = marker
	serviceName := p.opts.ServiceName()
	if p.opts.DisableHealthCheckDialOpt || !p.opts.EnableHealthCheck || serviceName == "" {
		p.healthState = balancer.SubConnState{ConnectivityState: connectivity.Ready}
		return
	}
	if p.opts.HealthCheckFunc == nil {
		logger.Error("Health check is requested but health check function is not set.")
		p.healthState = balancer.SubConnState{ConnectivityState: connectivity.Ready}
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.stopClientFn = cancel
	newStream := func(method string) (any, error) {
		return p.cc.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, method)
	}

	setConnectivityState := func(state connectivity.State, err error) {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.currentAttemptMarker != marker {
			return
		}
		p.healthState = balancer.SubConnState{
			ConnectivityState: state,
			ConnectionError:   err,
		}
		p.updateStateLocked()
	}

	go func() {
		err := p.opts.HealthCheckFunc(ctx, newStream, setConnectivityState, serviceName)
		if err == nil {
			return
		}
		if status.Code(err) == codes.Unimplemented {
			logger.Error("Subchannel health check is unimplemented at server side, thus health check is disabled\n")
		} else {
			logger.Errorf("Health checking failed: %v\n", err)
		}
	}()
}
