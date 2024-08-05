package health

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
)

func init() {
	producerBuilderSingleton = &producerBuilder{}
	balancer.HealthCheckStartFunc = StartHealtCheck
}

type producerBuilder struct{}

var producerBuilderSingleton *producerBuilder

// Build constructs and returns a producer and its cleanup function
func (*producerBuilder) Build(cci any) (balancer.Producer, func()) {
	p := &producer{
		cc: cci.(grpc.ClientConnInterface),
		mu: sync.Mutex{},
	}
	return p, sync.OnceFunc(func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.stopped = true
	})
}

type producer struct {
	cc        grpc.ClientConnInterface
	mu        sync.Mutex
	started   bool
	stopped   bool
	state     connectivity.State
	err       error
	scheduler func(func())
}

func StartHealtCheck(ctx context.Context, sc balancer.SubConn, enableHealthCheck bool, serviceName string, listener balancer.HealthListener) func() {
	pr, close := sc.GetOrBuildProducer(producerBuilderSingleton)
	p := pr.(*producer)
	p.mu.Lock()
	if p.started || p.stopped {
		p.mu.Unlock()
		return close
	}
	p.started = true
	p.state = connectivity.Connecting
	p.mu.Unlock()

	if !enableHealthCheck {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.stopped {
			return close
		}
		p.state = connectivity.Ready
		p.err = nil
		listener.OnStateChange(p.state, nil)
		return close
	}

	newStream := func(method string) (any, error) {
		return p.cc.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, method)
	}

	setConnectivityState := func(state connectivity.State, err error) {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.state = state
		p.err = err
		listener.OnStateChange(state, err)
	}

	go func() {
		err := clientHealthCheck(ctx, newStream, setConnectivityState, serviceName)
		if err == nil {
			return
		}
		if status.Code(err) == codes.Unimplemented {
			fmt.Printf("Subchannel health check is unimplemented at server side, thus health check is disabled\n")
		} else {
			fmt.Printf("Health checking failed: %v\n", err)
		}
	}()
	return close
}
