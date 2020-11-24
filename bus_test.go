package bus

import (
	"reflect"
	"testing"
)

func TestBusPublish(t *testing.T) {
	makeSubs := func(topics ...string) map[Topic]chan Message {
		ret := make(map[Topic]chan Message)
		for _, topic := range topics {
			ret[topic] = make(chan Message, 1)
		}
		return ret
	}

	tests := []struct {
		name  string
		subs  map[Topic]chan Message
		topic Topic
	}{
		{"one subscriber", makeSubs("topic1"), "topic1"},
		{"two subscribers", makeSubs("topic1", "topic2"), "topic1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bus := New()

			for topic, ch := range test.subs {
				bus.Subscribe(topic, ch)
			}

			bus.Publish(test.topic, nil)

			for topic, ch := range test.subs {
				if topic == test.topic {
					if len(ch) != 1 {
						t.Errorf("Wanted topic to be delivered to channel")
					} else {
						<-ch
					}
				} else {
					if len(ch) != 0 {
						t.Errorf("Channel got message it wasn't subscribed to")
					}
				}
			}

			bus.Close()
			for _, ch := range test.subs {
				select {
				case _, open := <-ch:
					if open {
						t.Errorf("Expected channel to be closed")
					}
				default:
					t.Errorf("Expected channel to be closed")
				}
			}
		})
	}
}

func TestBusClose(t *testing.T) {
	makeTopics := func(shared bool, topics ...string) map[Topic]chan Message {
		ret := make(map[Topic]chan Message)
		nextChannel := make(chan Message)
		for _, topic := range topics {
			ret[topic] = nextChannel
			if !shared {
				nextChannel = make(chan Message)
			}
		}
		return ret
	}

	tests := []struct {
		name   string
		topics map[Topic]chan Message
	}{
		{"one channel, one topic", makeTopics(false, "topic1")},
		{"one channel, two topics", makeTopics(true, "topic1", "topic2")},
		{"two channels, two topics", makeTopics(false, "topic1", "topic2")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bus := &Bus{
				index:         make(indexType),
				subscriptions: make(subscriptionsType),
			}

			for topic, ch := range test.topics {
				errCh := make(chan error, 1)
				bus.subscribe(subscription{topic: topic, ch: ch, errCh: errCh})
				<-errCh
			}

			err := bus.close()
			if err != nil {
				t.Errorf("Unexpected error %v", err)
				return
			}

			for _, ch := range test.topics {
				_, open := <-ch
				if open {
					t.Errorf("Expected channel to be closed")
					return
				}
			}
		})
	}
}

func TestBusSubscribe(t *testing.T) {
	ch := make(chan Message)
	errCh := make(chan error, 1)
	topic := "topic"

	bus := &Bus{
		index:         make(indexType),
		subscriptions: make(subscriptionsType),
	}
	bus.subscribe(subscription{topic: topic, ch: ch, errCh: errCh})
	gotErr := <-errCh
	if gotErr != nil {
		t.Errorf("Unexpected error %v", gotErr)
		return
	}

	wantIndex := indexType{ch: {topic}}
	wantSubscriptions := subscriptionsType{
		topic: {
			ch: nil,
		},
	}

	if !reflect.DeepEqual(wantIndex, bus.index) {
		t.Errorf("Wanted index %v got %v", wantIndex, bus.index)
	}

	if !reflect.DeepEqual(wantSubscriptions, bus.subscriptions) {
		t.Errorf("Wanted subscriptions %v got %v", wantSubscriptions, bus.subscriptions)
	}

	errCh = make(chan error, 1)
	bus.subscribe(subscription{ch: ch, errCh: errCh, unsubscribe: true})
	gotErr = <-errCh
	if gotErr != nil {
		t.Errorf("Unexpected error %v", gotErr)
		return
	}

	wantIndex = indexType{}
	wantSubscriptions = subscriptionsType{}

	if !reflect.DeepEqual(wantIndex, bus.index) {
		t.Errorf("Wanted index %v got %v", wantIndex, bus.index)
	}

	if !reflect.DeepEqual(wantSubscriptions, bus.subscriptions) {
		t.Errorf("Wanted subscriptions %v got %v", wantSubscriptions, bus.subscriptions)
	}
}
