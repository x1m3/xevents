package xevents

import (
	"github.com/x1m3/priorityQueue"
	"errors"
	"sync"
	"time"
)

var ErrTopicWithoutListener = errors.New("listener not found for topic")

// This is the interface that any event must satisfy. Simply, decorate your event struct with this 2 methods.
type Event interface {
	// Returns the name of the event
	Topic() string
	// Returns a priority. Greatest priority will be processed before lower if there are more than one event in
	// the queue at at the moment to handle it.
	Priority() int
}


// This is the internal structure of the items stored in the queue. It is a sugar interface to adapt priorities to
// the priorityqueue.Interface
type qEvent struct {
	Event     Event
	CreatedOn time.Time
}

func (e *qEvent) HigherPriorityThan(o priorityQueue.Interface) bool {
	return e.Event.Priority() > o.(*qEvent).Event.Priority()
}

// A worker maintains a list of function handlers that will be executed when an event is received. There is also a list
// of listening channels to send an event.
// Every worker starts a new goroutine that will be responsible of handling events of a specified type. This is add some
// granularity to the event dispatcher, so a slow event processor is not going to slow down other event processors.
type worker struct {
	handlers   []func(Event)
	channels   []chan Event
	signalChan chan *qEvent
}

func newWorker() *worker {
	w := &worker{
		handlers:   make([]func(Event), 0),
		channels:   make([]chan Event, 0),
		// The channel is buffered to avoid delays on publish operations. This way, a temporarily slow worker is not
		// going to stop other workers
		signalChan: make(chan *qEvent, 1024),
	}
	w.run()
	return w
}
// A worker is waiting for an event via signalChan and any time it gets one, it runs al the handlers and write the event
// over all listening channels
func (w *worker) run() {
	go func() {
		for {
			qevent := <-w.signalChan // Wait to receive something
			for _, fn := range w.handlers {
				fn(qevent.Event)
			}
		}
	}()
}

type Handler struct {
	// Publish, Register Callbacks and poping from the queue must be protected by a mutex to avoid race conditions.
	sync.Mutex
	subscribers map[string]*worker
	queue       *priorityQueue.Queue
	signalChan  chan struct{}
}

// It creates a new channel an spawns a goroutine that will be listening for enqueuing signals. This goroutine acts as
// orchestator. Any time it is notified that something is on the queue, it gets the next event an passes it to
// the worker responsible of this type of events.
func New() *Handler {
	h := &Handler{
		subscribers: make(map[string]*worker),
		signalChan:  make(chan struct{}, 1024),
		queue: priorityQueue.New(),
	}
	go func() {
		for {
			// Wait for a signal
			<-h.signalChan
			// Dequeue event from the queue
			h.Lock()
			qevent := h.queue.Pop().(*qEvent)
			h.Unlock()
			// Send event to the worker that manage this kind of event
			h.subscribers[qevent.Event.Topic()].signalChan <- qevent
		}
	}()

	return h
}

// Publish an event. It will launch an error if there is no listener register to this event.
func (h *Handler) Publish(e Event) error {
	h.Lock()
	if err := h.guardTopicExists(e.Topic()); err != nil {
		h.Unlock()
		return err
	}
	h.queue.Push(&qEvent{Event: e, CreatedOn: time.Now()})
	h.signalChan <- struct {}{}
	h.Unlock()
	return nil
}

// Registers a callback function that accepts an event.
func (h *Handler) RegisterCallback(topic string, callback func(Event)) {
	h.Lock()
	if nil != h.guardTopicExists(topic) {
		h.subscribers[topic] = newWorker()
	}
	h.subscribers[topic].handlers = append(h.subscribers[topic].handlers, callback)
	h.Unlock()
}
// Not implemented
func (h *Handler) RegisterChannel(topic string, cbChan chan Event) error { return nil }

func (h *Handler) guardTopicExists(topic string) error {
	if _, found := h.subscribers[topic]; !found {
		return ErrTopicWithoutListener
	}
	return nil
}
