package xevents

import (
	"testing"
	"github.com/davecgh/go-spew/spew"
	"time"
)

type exampleEvent struct {
	number   int
	sentence string
}

func (e *exampleEvent) Topic() string { return "example" }
func (e *exampleEvent) Priority() int { return 1 }

func TestNew(t *testing.T) {
	h := New()
	if h == nil {
		t.Error("New() cannot return nil")
	}
}

func Test_PublishCallbackSimple(t *testing.T) {
	h := New()
	err := h.Publish(&exampleEvent{number: 1, sentence: "el perro de san roque no tiene rabo"})
	if err == nil {
		t.Error("Expecting an error, because no subscriber has been registered.")
	}
	for i := 0; i < 1000; i++ {
		h.Subscribe("example", func(e Event) {
			spew.Dump(e)
			if got, expected := e.(*exampleEvent).sentence, "el perro de san roque no tiene rabo"; got != expected {
				t.Errorf("Bad event sentence. Got <%s>, expecting <%s>", got, expected)
			}
			if got, expected := e.(*exampleEvent).number, 1; got != expected {
				t.Errorf("Bad event number. Got <%d>, expecting <%d>", got, expected)
			}
		})
	}
	err = h.Publish(&exampleEvent{number: 1, sentence: "el perro de san roque no tiene rabo"})
	if err != nil {
		t.Errorf("Got and error <%s>. Expecting nil", err)
	}

	time.Sleep(2 * time.Second)
}
