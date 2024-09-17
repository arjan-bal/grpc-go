package health

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/internal"
	"google.golang.org/grpc/internal/backoff"
	ibackoff "google.golang.org/grpc/internal/backoff"
	"google.golang.org/grpc/status"
)

func init() {
	producerBuilderSingleton = &producerBuilder{}
	internal.EnableHealthCheckViaProducer = ClientSideHealthProducer
}

type producerBuilder struct{}

var producerBuilderSingleton *producerBuilder

// Build constructs and returns a producer and its cleanup function
func (*producerBuilder) Build(cci any) (balancer.Producer, func()) {
	p := &healthServiceProducer{
		cc:   cci.(grpc.ClientConnInterface),
		mu:   sync.Mutex{},
		done: make(chan struct{}),
		healthState: balancer.SubConnState{
			ConnectivityState: connectivity.Connecting,
			ConnectionError:   balancer.ErrNoSubConnAvailable,
		},
		// TODO: Wait for first connectivity update instead of relying on this
		// initial value.
		connectivityState: balancer.SubConnState{
			ConnectivityState: connectivity.Idle,
		},
		cancel: func() {},
	}
	return p, func() {
		p.cancel()
		<-p.done
	}
}

type healthServiceProducer struct {
	cc                grpc.ClientConnInterface
	mu                sync.Mutex
	connectivityState balancer.SubConnState
	healthState       balancer.SubConnState
	cancel            func()
	done              chan (struct{})
	listener          func(balancer.SubConnState)
}

// EnableHealthCheck enables the client side health checking on the subchannel.
// It must be called at most once on a subchannel. Once the health check service
// is enabled, consumers can receive its updates by registering a listener with
// the generic health producer.
func ClientSideHealthProducer(opts *balancer.HealthCheckOptions, oldProducer func(balancer.SubConn, func(balancer.SubConnState)) func()) func(balancer.SubConn, func(balancer.SubConnState)) func() {
	return func(sc balancer.SubConn, oldLis func(balancer.SubConnState)) func() {
		pr, closeFn := sc.GetOrBuildProducer(producerBuilderSingleton)
		p := pr.(*healthServiceProducer)
		if p.listener != nil {
			panic("Attempting to start health check multiple times on the same subchannel")
		}
		p.listener = oldLis
		oldCloseFn := oldProducer(sc, func(scs balancer.SubConnState) {
			p.mu.Lock()
			defer p.mu.Unlock()
			p.connectivityState = scs
			p.updateStateLocked()
		})
		go p.startHealthCheck(opts)
		return func() {
			closeFn()
			oldCloseFn()
		}
	}
}

func (p *healthServiceProducer) updateStateLocked() {
	if p.connectivityState.ConnectivityState != connectivity.Ready {
		p.listener(p.connectivityState)
	}
	p.listener(p.healthState)
}

func (p *healthServiceProducer) startHealthCheck(opts *balancer.HealthCheckOptions) {
	defer close(p.done)
	p.mu.Lock()
	p.healthState = balancer.SubConnState{ConnectivityState: connectivity.Connecting}
	p.updateStateLocked()
	if opts.DisableHealthCheckDialOpt {
		p.healthState = balancer.SubConnState{ConnectivityState: connectivity.Ready}
		p.updateStateLocked()
		p.mu.Unlock()
		return
	}
	serviceName := opts.ServiceName
	if serviceName == "" {
		p.healthState = balancer.SubConnState{ConnectivityState: connectivity.Ready}
		p.updateStateLocked()
		p.mu.Unlock()
		return
	}
	if opts.HealthCheckFunc == nil {
		logger.Error("Health check is requested but health check function is not set.")
		p.healthState = balancer.SubConnState{ConnectivityState: connectivity.Ready}
		p.updateStateLocked()
		p.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	newStream := func(method string) (any, error) {
		return p.cc.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, method)
	}

	setConnectivityState := func(state connectivity.State, err error) {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.healthState = balancer.SubConnState{
			ConnectivityState: state,
			ConnectionError:   err,
		}
		p.updateStateLocked()
	}

	runStream := func() error {
		// TODO: Make the client return backoff reset true of it managed to connect.
		err := opts.HealthCheckFunc(ctx, newStream, setConnectivityState, serviceName)
		if err == nil {
			return nil
		}
		// Unimplemented, do not retry.
		if status.Code(err) == codes.Unimplemented {
			logger.Error("Subchannel health check is unimplemented at server side, thus health check is disabled\n")
			return err
		}

		// Retry for all other errors.
		if code := status.Code(err); code != codes.Unavailable && code != codes.Canceled {
			// TODO: Unavailable and Canceled should also ideally log an error,
			// but for now we receive them when shutting down the ClientConn
			// (Unavailable if the stream hasn't started yet, and Canceled if it
			// happens mid-stream).  Once we can determine the state or ensure
			// the producer is stopped before the stream ends, we can log an
			// error when it's not a natural shutdown.
			logger.Error("Received unexpected stream error:", err)
		}
		if code := status.Code(err); code == codes.Unavailable {
			return fmt.Errorf("Stream closed")
		}
		return nil
	}
	p.mu.Unlock()
	backoff.RunF(ctx, runStream, ibackoff.DefaultExponential.Backoff)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.healthState = balancer.SubConnState{
		ConnectivityState: connectivity.Shutdown,
	}
	p.updateStateLocked()
}
