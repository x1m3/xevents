package xevents

import (
	"errors"
	"reflect"
	"sync"
)

var ErrTopicWithoutListener = errors.New("listener not found for topic")

const SyncMode = false
const BackgroundMode = true

type pubSub struct {
	sync.RWMutex
	subscribers map[string]*worker
}

// New returns a new Event handler ready to be used.
func New() *pubSub {
	p := &pubSub{
		subscribers: make(map[string]*worker),
	}
	return p
}

// Publish enqueues an event.
// It will launch an error if there is no listener registered to this event.
// All the subscribers will receive the event and their respective function callback
// will be executed.
func (p *pubSub) Publish(e interface{}) error {

	topic := reflect.TypeOf(e).String()

	p.RLock()
	defer p.RUnlock()

	if _, found := p.subscribers[topic]; !found {
		return ErrTopicWithoutListener
	}
	p.subscribers[topic].push(e)
	return nil
}

// Subscribe links a callback function with an event type based on the type name of the event.
// The callback function will be executed everytime that a new event of this type is published.
func (p *pubSub) Subscribe(topic interface{}, async bool, callback func(interface{})) {
	topicName := reflect.TypeOf(topic).String()
	p.Lock()
	if _, found := p.subscribers[topicName]; found {
		p.subscribers[topicName] = newWorker(async)
		p.subscribers[topicName].addFnHandler(callback)
	}
	p.Unlock()
}

// Clear close all pending goroutines and clears the list of subscribers. You should call
// clear specially when your application is shutdown to ensure that all pending events are
// processed.
func (p *pubSub) Clear() {
	p.Lock()
	defer p.Unlock()

	for _, worker := range p.subscribers {
		worker.clear()
	}

	p.subscribers = make(map[string]*worker)
}

type worker struct {
	sync.RWMutex
	async  bool
	handlers []func(interface{})
	events   chan interface{}
}

func newWorker(async bool) *worker {
	w := &worker{
		handlers: make([]func(interface{}), 0),
		async: async,
	}
	if async {
		w.events = make(chan interface{}, 1024)
		w.runInBackground()
	}
	return w
}

func (w *worker) runInBackground() {
	go func() {
		for event := range w.events {
			w.RLock()
			for _, fn := range w.handlers {
				fn(event)
			}
			w.RUnlock()
		}
	}()
}

func (w *worker) push(event interface{}) {
	if w.async {
		w.events <- event
	}else {
		w.RLock()
		defer w.RUnlock()
		for _, fn := range w.handlers {
			fn(event)
		}
	}
}

func (w *worker) clear() {
	if w.async {
		close(w.events)
	}
}


func (w *worker) addFnHandler(fn func(interface{})) {
	w.Lock()
	w.handlers = append(w.handlers, fn)
	w.Unlock()
}

