package bus

import "github.com/joeshaw/gengen/generic"

type Topic generic.T

type Message generic.T

type publish struct {
	msg   Message
	topic Topic
	errCh chan<- error
}

type subscription struct {
	unsubscribe bool
	topic       Topic
	ch          chan<- Message
	errCh       chan<- error
}

type Bus struct {
	publishCh   chan<- publish
	subscribeCh chan<- subscription
	closeCh     chan<- chan<- error

	index         indexType
	subscriptions subscriptionsType
}

type indexType map[chan<- Message][]Topic
type subscribersType map[chan<- Message]interface{}
type subscriptionsType map[Topic]subscribersType

func New() *Bus {
	publishCh := make(chan publish)
	subscribeCh := make(chan subscription)
	closeCh := make(chan chan<- error)

	b := &Bus{
		index:         make(indexType),
		subscriptions: make(subscriptionsType),

		publishCh:   publishCh,
		subscribeCh: subscribeCh,
		closeCh:     closeCh,
	}

	go b.run(publishCh, subscribeCh, closeCh)
	return b
}

func (b *Bus) close() error {
	closed := make(map[chan<- Message]bool)
	for _, topic := range b.subscriptions {
		for ch := range topic {
			// Channels can be subscribed to more than one topic, only
			// close them once
			if _, found := closed[ch]; !found {
				close(ch)
				closed[ch] = true
			}
		}
	}
	return nil
}

func (b *Bus) publish(p publish) {
	if topic, found := b.subscriptions[p.topic]; found {
		for ch := range topic {
			ch <- p.msg
		}
		p.errCh <- nil
		close(p.errCh)
	}
}

func (b *Bus) subscribe(subscription subscription) {
	if subscription.unsubscribe {
		for _, topic := range b.index[subscription.ch] {
			delete(b.subscriptions[topic], subscription.ch)
			if len(b.subscriptions[topic]) == 0 {
				delete(b.subscriptions, topic)
			}
		}
		delete(b.index, subscription.ch)
		close(subscription.ch)
		subscription.errCh <- nil
	} else {
		b.index[subscription.ch] = append(b.index[subscription.ch], subscription.topic)
		topic, found := b.subscriptions[subscription.topic]
		if !found {
			topic = make(subscribersType)
			b.subscriptions[subscription.topic] = topic
		}
		topic[subscription.ch] = nil
		subscription.errCh <- nil
	}
	close(subscription.errCh)
}

func (b *Bus) run(publishCh <-chan publish, subscribeCh <-chan subscription, closeCh <-chan chan<- error) {
	for {
		select {
		case publish := <-publishCh:
			b.publish(publish)
		case subscription := <-subscribeCh:
			b.subscribe(subscription)
		case errCh := <-closeCh:
			errCh <- b.close()
			close(errCh)
			return
		}
	}
}

// Publish a message to the message bus
func (b *Bus) Publish(topic Topic, msg Message) error {
	errCh := make(chan error)
	b.publishCh <- publish{topic: topic, msg: msg, errCh: errCh}
	return <-errCh
}

// Subscribe a message channel to messages for the topic
func (b *Bus) Subscribe(topic Topic, ch chan<- Message) error {
	errCh := make(chan error)
	b.subscribeCh <- subscription{topic: topic, ch: ch, errCh: errCh}
	return <-errCh
}

// Unsubscribe a channel from all topics
func (b *Bus) Unsubscribe(ch chan<- Message) error {
	errCh := make(chan error)
	b.subscribeCh <- subscription{unsubscribe: true, ch: ch, errCh: errCh}
	return <-errCh
}

func (b *Bus) Close() error {
	errCh := make(chan error)
	b.closeCh <- errCh
	return <-errCh
}
