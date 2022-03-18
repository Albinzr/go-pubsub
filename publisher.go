package pubsub

import (
	"sync"
	"time"
)

// Publisher The publisher related meta data
type Publisher struct {
	subscribers Subscribers
	sLock       sync.RWMutex

	topics map[string]Subscribers
	tLock  sync.RWMutex
}

// NewPublisher Create new publisher
func NewPublisher() *Publisher {
	return &Publisher{
		subscribers: Subscribers{},
		sLock:       sync.RWMutex{},
		topics:      map[string]Subscribers{},
		tLock:       sync.RWMutex{},
	}
}

// Attach Create a new subscriber and register it into our main publisher
func (p *Publisher) Attach() (*Subscriber, error) {
	s, err := NewSubscriber()

	if err != nil {
		return nil, err
	}

	p.sLock.Lock()
	p.subscribers[s.GetID()] = s
	p.sLock.Unlock()

	return s, nil
}

// Subscribe subscribes the specified subscriber "s" to the specified list of topic(s)
func (p *Publisher) Subscribe(s *Subscriber, topics ...string) {
	p.tLock.Lock()
	defer p.tLock.Unlock()

	for _, topic := range topics {
		if nil == p.topics[topic] {
			p.topics[topic] = Subscribers{}
		}
		s.AddTopic(topic)
		p.topics[topic][s.id] = s
	}

}

// Unsubscribe Unsubscribe the specified subscriber from the specified topic(s)
func (p *Publisher) Unsubscribe(s *Subscriber, topics ...string) {
	for _, topic := range topics {
		p.tLock.Lock()
		if p.topics[topic] == nil {
			p.tLock.Unlock()
			continue
		}
		delete(p.topics[topic], s.id)
		p.tLock.Unlock()
		s.RemoveTopic(topic)
	}
}

// Detach remove the specified subscriber from the publisher
func (p *Publisher) Detach(s *Subscriber) {
	s.destroy()
	p.sLock.Lock()
	p.Unsubscribe(s, s.GetTopics()...)
	delete(p.subscribers, s.id)
	defer p.sLock.Unlock()
}

// Broadcast broadcast the specified payload to all the topic(s) subscribers
func (p *Publisher) Broadcast(payload interface{}, topics ...string) {
	for _, topic := range topics {
		if p.Subscribers(topic) < 1 {
			continue
		}
		p.tLock.RLock()
		for _, s := range p.topics[topic] {
			m := &Message{
				topic:     topic,
				payload:   payload,
				createdAt: time.Now().UnixNano(),
			}
			go (func(s *Subscriber) {
				s.Signal(m)
			})(s)
		}
		p.tLock.RUnlock()
	}
}

// Subscribers Get the subscribers count
func (p *Publisher) Subscribers(topic string) int {
	p.tLock.RLock()
	defer p.tLock.RUnlock()
	return len(p.topics[topic])
}

// GetTopics Returns a slice of topics
func (p *Publisher) GetTopics() []string {
	p.tLock.RLock()
	publisherTopics := p.topics
	p.tLock.RUnlock()

	topics := []string{}
	for topic := range publisherTopics {
		topics = append(topics, topic)
	}

	return topics
}
