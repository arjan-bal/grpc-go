package health

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/internal"
	"google.golang.org/grpc/internal/backoff"
	"google.golang.org/grpc/status"
)

func init() {
	producerBuilderSingleton = &producerBuilder{}
	internal.RegisterClientHealthCheckListener = RegisterClientSideHealthCheckListener
}

type producerBuilder struct{}

var producerBuilderSingleton *producerBuilder

// Build constructs and returns a producer and its cleanup function
func (*producerBuilder) Build(cci any) (balancer.Producer, func()) {
	p := &healthServiceProducer{
		cc:   cci.(grpc.ClientConnInterface),
		done: make(chan struct{}),
		opts: internal.HealthCheckOptions{},
	}
	return p, func() {
		fmt.Println("Health producer is closing")
		if p.cancel == nil {
			return
		}
		p.cancel()
		<-p.done
	}
}

type healthServiceProducer struct {
	cc       grpc.ClientConnInterface
	cancel   func()
	done     chan (struct{})
	listener func(balancer.SubConnState)
	opts     internal.HealthCheckOptions
}

// RegisterClientSideHealthCheckListener accepts a listener to provide server
// health state via the health service.
func RegisterClientSideHealthCheckListener(sc balancer.SubConn, opts internal.HealthCheckOptions, listener func(balancer.SubConnState)) {
	pr, _ := sc.GetOrBuildProducer(producerBuilderSingleton)
	p := pr.(*healthServiceProducer)
	if p.listener != nil {
		panic("Attempting to start health check multiple times on the same subchannel")
	}
	p.listener = listener
	p.opts = opts
	go p.startHealthCheck()
	return
}

func (p *healthServiceProducer) startHealthCheck() {
	fmt.Println("Starting health checks")
	defer close(p.done)
	p.cancel = func() {}
	opts := p.opts
	serviceName := opts.HealthServiceName
	if opts.HealthCheckFunc == nil {
		logger.Error("Health check is requested but health check function is not set.")
		p.listener(balancer.SubConnState{ConnectivityState: connectivity.Ready})
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	newStream := func(method string) (any, error) {
		return p.cc.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, method)
	}

	setConnectivityState := func(state connectivity.State, err error) {
		fmt.Println("Gor health check update", state)
		p.listener(balancer.SubConnState{
			ConnectivityState: state,
			ConnectionError:   err,
		})
	}

	runStream := func() error {
		// TODO: Make the client return backoff reset true of it managed to connect.
		fmt.Println("Health check starting")
		err := opts.HealthCheckFunc(ctx, newStream, setConnectivityState, serviceName)
		if err == nil {
			return nil
		}
		fmt.Println("Health check returned error:", err)
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
	backoff.RunF(ctx, runStream, backoff.DefaultExponential.Backoff)
}
