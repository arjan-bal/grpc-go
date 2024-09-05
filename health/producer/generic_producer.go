package producer

import (
	"context"
	"sync"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/internal/grpcsync"
)

var logger = grpclog.Component("health_producer")

func init() {
	producerBuilderSingleton = &producerBuilder{}
}

type producerBuilder struct{}

var producerBuilderSingleton *producerBuilder

type broadcastingListner struct {
	p         *producer
	listeners map[balancer.StateListener]bool
}

func (l *broadcastingListner) OnStateChange(scs balancer.SubConnState) {
	l.p.serializer.TrySchedule(func(ctx context.Context) {
		l.p.healthState = scs
		for lis := range l.listeners {
			lis.OnStateChange(scs)
		}
	})
}

// Build constructs and returns a producer and its cleanup function
func (*producerBuilder) Build(cci any) (balancer.Producer, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	p := &producer{
		cci: cci,
		healthState: balancer.SubConnState{
			ConnectivityState: connectivity.Ready,
		},
		serializer: grpcsync.NewCallbackSerializer(ctx),
	}
	p.broadcastingListener = &broadcastingListner{
		p:         p,
		listeners: make(map[balancer.StateListener]bool),
	}
	p.rootListener = p.broadcastingListener
	return p, sync.OnceFunc(func() {
		p.serializer.TrySchedule(func(ctx context.Context) {
			if len(p.broadcastingListener.listeners) > 0 {
				logger.Errorf("Health Producer closing with %d listeners remaining in list", len(p.broadcastingListener.listeners))
			}
			p.broadcastingListener.listeners = nil
		})
		cancel()
		<-p.serializer.Done()
	})
}

type producer struct {
	cci                  any // grpc.ClientConnInterface
	opts                 *balancer.HealthCheckOptions
	healthState          balancer.SubConnState
	serializer           *grpcsync.CallbackSerializer
	rootListener         balancer.StateListener
	broadcastingListener *broadcastingListner
}

func RegisterListener(l balancer.StateListener, sc balancer.SubConn) func() {
	pr, closeFn := sc.GetOrBuildProducer(producerBuilderSingleton)
	p := pr.(*producer)
	unregister := func() {
		p.unregisterListener(l)
		closeFn()
	}
	p.serializer.TrySchedule(func(ctx context.Context) {
		p.broadcastingListener.listeners[l] = true
		l.OnStateChange(p.healthState)
	})
	return unregister
}

// Adds a Sender to beginning of the chain, gives the next sender in the chain to send
// updates.
func SwapRootListener(newListener balancer.StateListener, sc balancer.SubConn) (balancer.StateListener, func()) {
	pr, closeFn := sc.GetOrBuildProducer(producerBuilderSingleton)
	p := pr.(*producer)
	senderCh := make(chan balancer.StateListener, 1)
	p.serializer.ScheduleOr(func(ctx context.Context) {
		oldSender := p.rootListener
		p.rootListener = newListener
		senderCh <- oldSender
	}, func() {
		close(senderCh)
	})
	oldSender := <-senderCh
	// Send an update on the root listener to allow the new producer to set
	// update the state present in listener down the chain if required.
	p.serializer.TrySchedule(func(ctx context.Context) {
		p.rootListener.OnStateChange(balancer.SubConnState{
			ConnectivityState: connectivity.Ready,
		})
	})
	return oldSender, closeFn
}

func (p *producer) unregisterListener(l balancer.StateListener) {
	p.serializer.TrySchedule(func(ctx context.Context) {
		delete(p.broadcastingListener.listeners, l)
	})
}
