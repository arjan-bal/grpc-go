// Package genericproducer provides a balancer.Producer that is used to publish
// and subscribe to health state updates.
package genericproducer

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
	listeners map[balancer.StateListener]int
}

func (l *broadcastingListner) OnStateChange(scs balancer.SubConnState) {
	l.p.serializer.TrySchedule(func(_ context.Context) {
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
	p.connectivityListener = &connectivityListener{p: p}
	p.broadcastingListener = &broadcastingListner{
		p:         p,
		listeners: make(map[balancer.StateListener]int),
	}
	p.rootListener = p.broadcastingListener
	return p, sync.OnceFunc(func() {
		cancel()
		<-p.serializer.Done()
		if len(p.broadcastingListener.listeners) > 0 {
			logger.Errorf("Health Producer closing with %d listeners remaining in list", len(p.broadcastingListener.listeners))
		}
		p.broadcastingListener.listeners = nil
		if p.sc != nil {
			p.sc.UnregisterConnectivityListner(p.connectivityListener)
			p.connectivityListener = nil
		}
	})
}

type producer struct {
	cci                  any // grpc.ClientConnInterface
	healthState          balancer.SubConnState
	serializer           *grpcsync.CallbackSerializer
	rootListener         balancer.StateListener
	broadcastingListener *broadcastingListner
	connectivityListener *connectivityListener
	sc                   balancer.SubConn
}

type connectivityListener struct {
	p                 *producer
	connectivityState balancer.SubConnState
}

func (l *connectivityListener) OnStateChange(state balancer.SubConnState) {
	l.p.serializer.TrySchedule(func(_ context.Context) {
		l.connectivityState = state
		l.p.rootListener.OnStateChange(state)
	})
}

// RegisterListener allows health consumers to start listening for health
// updates till the ClientConn is closed. It is not guaranteed for listeners to
// get an update when the subchannel transitions to SHUTDOWN.
// It returns a function to unregister the listener and manage ref counting.
// Listeners must be unregestered when they are no longer required. The listener
// will get called with the present connectivity state before receiving any
// other updates. If a listener is registered multiple times, it will receive each
// update only once. The function returned by all the registration requests must be
// called to finally unregister the listener. The listener will asynchronously
// get all the updates that were queued before the unregisteration request came.
func RegisterListener(l balancer.StateListener, sc balancer.SubConn) func() {
	pr, closeFn := sc.GetOrBuildProducer(producerBuilderSingleton)
	p := pr.(*producer)
	unregister := func() {
		p.unregisterListener(l)
		closeFn()
	}
	p.serializer.TrySchedule(func(_ context.Context) {
		if p.sc == nil {
			p.sc = sc
			sc.RegisterConnectivityListner(p.connectivityListener)
		}
		p.broadcastingListener.listeners[l]++
		l.OnStateChange(p.healthState)
	})
	return unregister
}

// SwapRootListener sets the given listener as the root of the listener chain.
// It returns the previous root of the chain. The new root will be called with
// the present connectivity state before any other updates are sent.
// A cleanup function is also returned which must be called before the calling
// producer/LB policy shuts down.
func SwapRootListener(newListener balancer.StateListener, sc balancer.SubConn) (balancer.StateListener, func()) {
	// closeFn can be called synchronously when consumer listeners are getting notified
	// from the serializer.If the refcount falls to 0, it can use the producer
	// to be closed. This requires the serializer queue to exit. So we can't
	// let closeFn execute serially and block the serializer queue.
	// We can queue closeFn to run on the serializer, but if its running on the
	// serializer, the serializer can't exit! We run this in a separate thread.
	pr, closeFn := sc.GetOrBuildProducer(producerBuilderSingleton)
	p := pr.(*producer)
	senderCh := make(chan balancer.StateListener, 1)
	p.serializer.ScheduleOr(func(_ context.Context) {
		oldSender := p.rootListener
		p.rootListener = newListener
		senderCh <- oldSender
	}, func() {
		close(senderCh)
	})
	oldSender := <-senderCh
	// Send an update on the root listener to allow the new producer to set
	// update the state present in listener down the chain if required.
	p.serializer.TrySchedule(func(_ context.Context) {
		p.rootListener.OnStateChange(p.connectivityListener.connectivityState)
	})
	return oldSender, func() {
		go closeFn()
	}
}

func (p *producer) unregisterListener(l balancer.StateListener) {
	p.serializer.TrySchedule(func(_ context.Context) {
		p.broadcastingListener.listeners[l]--
		if p.broadcastingListener.listeners[l] == 0 {
			delete(p.broadcastingListener.listeners, l)
		}
	})
}
