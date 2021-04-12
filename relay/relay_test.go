package relay

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/p2p"
	"reflect"
	"testing"
)

type messageProcessorRegistererStub struct {
	peerID                      core.PeerID
	registeredMessageProcessors map[string]p2p.MessageProcessor
	createdTopics               []string
	createTopicCalled           bool
	electionWasCalled           bool

	lastSendTopicName string
	lastSendData      []byte
	lastSendPeerID    core.PeerID
}

func (p *messageProcessorRegistererStub) RegisterMessageProcessor(topic string, handler p2p.MessageProcessor) error {
	if p.registeredMessageProcessors == nil {
		p.registeredMessageProcessors = make(map[string]p2p.MessageProcessor)
	}

	p.registeredMessageProcessors[topic] = handler
	return nil
}

func (p *messageProcessorRegistererStub) HasTopic(name string) bool {
	for _, topic := range p.createdTopics {
		if topic == name {
			return true
		}
	}
	return false
}

func (p *messageProcessorRegistererStub) CreateTopic(name string, _ bool) error {
	p.createdTopics = append(p.createdTopics, name)
	p.createTopicCalled = true
	return nil
}

func (p *messageProcessorRegistererStub) Addresses() []string {
	return nil
}

func (p *messageProcessorRegistererStub) ID() core.PeerID {
	return "myself"
}

func (p *messageProcessorRegistererStub) Broadcast(string, []byte) {
}

func (p *messageProcessorRegistererStub) SendToConnectedPeer(topic string, buff []byte, peerID core.PeerID) error {
	p.lastSendTopicName = topic
	p.lastSendData = buff
	p.lastSendPeerID = peerID

	return nil
}

func (p *messageProcessorRegistererStub) Close() error {
	return nil
}

// P2PMessageMock -
type P2PMessageMock struct {
	FromField      []byte
	DataField      []byte
	SeqNoField     []byte
	TopicField     string
	SignatureField []byte
	KeyField       []byte
	PeerField      core.PeerID
	PayloadField   []byte
	TimestampField int64
}

// From -
func (msg *P2PMessageMock) From() []byte {
	return msg.FromField
}

// Data -
func (msg *P2PMessageMock) Data() []byte {
	return msg.DataField
}

// SeqNo -
func (msg *P2PMessageMock) SeqNo() []byte {
	return msg.SeqNoField
}

// Topics -
func (msg *P2PMessageMock) Topic() string {
	return msg.TopicField
}

// Signature -
func (msg *P2PMessageMock) Signature() []byte {
	return msg.SignatureField
}

// Key -
func (msg *P2PMessageMock) Key() []byte {
	return msg.KeyField
}

// Peer -
func (msg *P2PMessageMock) Peer() core.PeerID {
	return msg.PeerField
}

// Timestamp -
func (msg *P2PMessageMock) Timestamp() int64 {
	return msg.TimestampField
}

// Payload -
func (msg *P2PMessageMock) Payload() []byte {
	return msg.PayloadField
}

// IsInterfaceNil returns true if there is no value under the interface
func (msg *P2PMessageMock) IsInterfaceNil() bool {
	return msg == nil
}

type mockTimeProvider struct{ currentTime int64 }

func (p *mockTimeProvider) UnixNow() int64 {
	return p.currentTime
}

func TestConstructor(t *testing.T) {
	topics := []string{ActionsTopicName, StateTopicName}

	for _, topic := range topics {
		t.Run(fmt.Sprintf("will create %q if it does not exist", topic), func(t *testing.T) {
			messenger := &messageProcessorRegistererStub{createdTopics: []string{}}
			_, err := NewRelay(messenger, nil)
			if err != nil {
				t.Fatal(err)
			}

			if !messenger.HasTopic(ActionsTopicName) {
				t.Errorf("Expected to create topic %q", ActionsTopicName)
			}
		})
	}
}

func TestPrivateTopic(t *testing.T) {
	messenger := &messageProcessorRegistererStub{}
	relay, err := NewRelay(messenger, nil)
	if err != nil {
		t.Fatal(err)
	}

	privateMessageProcessor := messenger.registeredMessageProcessors[StateTopicName]
	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	expected := Peers{"first", "second"}
	err = enc.Encode(expected)
	if err != nil {
		t.Fatal(err)
	}

	message := buildPrivateMessage("other", data.Bytes())
	_ = privateMessageProcessor.ProcessReceivedMessage(message, "peer_near_me")

	if !reflect.DeepEqual(relay.peers, expected) {
		t.Errorf("Expected peers to be %v, but there are %v", expected, relay.peers)
	}
}

func TestJoinedAction(t *testing.T) {
	t.Run("when there there more peers than yourself will broadcast to private", func(t *testing.T) {
		messenger := &messageProcessorRegistererStub{}
		_, err := NewRelay(messenger, nil)
		if err != nil {
			t.Fatal(err)
		}

		privateMessageProcessor := messenger.registeredMessageProcessors[StateTopicName]
		var data bytes.Buffer
		enc := gob.NewEncoder(&data)
		existingPeers := Peers{"first", "second"}
		privateData := existingPeers
		err = enc.Encode(privateData)
		if err != nil {
			t.Fatal(err)
		}

		message := buildPrivateMessage("existing_peer", data.Bytes())
		_ = privateMessageProcessor.ProcessReceivedMessage(message, "peer_near_me")

		actionsMessageProcessor := messenger.registeredMessageProcessors[ActionsTopicName]
		_ = actionsMessageProcessor.ProcessReceivedMessage(buildJoinedMessage("other", 42), "peer_near_me")

		expected := Peers{"first", "other", "second"}

		dec := gob.NewDecoder(bytes.NewReader(messenger.lastSendData))
		var got Peers
		err = dec.Decode(&got)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(expected, got) {
			t.Errorf("Expected %v, got %v", expected, got)
		}
	})
	t.Run("when you just joined will not broadcast to private", func(t *testing.T) {
		messenger := &messageProcessorRegistererStub{}
		_, err := NewRelay(messenger, nil)
		if err != nil {
			t.Fatal(err)
		}

		actionsMessageProcessor := messenger.registeredMessageProcessors[ActionsTopicName]
		_ = actionsMessageProcessor.ProcessReceivedMessage(buildJoinedMessage(messenger.ID(), 42), messenger.ID())

		if messenger.lastSendPeerID != "" {
			t.Error("Should not broadcast for self")
		}
	})
}

func TestLeaderSelection(t *testing.T) {
	t.Run("will not select a leader if there are no peers", func(t *testing.T) {
		messenger := &messageProcessorRegistererStub{}
		relay, err := NewRelay(messenger, nil)
		if err != nil {
			t.Fatal(err)
		}

		if relay.leader() != "" {
			t.Error("Expected no leader")
		}
	})
	t.Run("will select leader based on time", func(t *testing.T) {
		messenger := &messageProcessorRegistererStub{}
		relay, err := NewRelay(messenger, &mockTimeProvider{120})
		if err != nil {
			t.Fatal(err)
		}

		actionsMessageProcessor := messenger.registeredMessageProcessors[ActionsTopicName]
		expected := core.PeerID("first")
		_ = actionsMessageProcessor.ProcessReceivedMessage(buildJoinedMessage("second", 2), "peer_near_me")
		_ = actionsMessageProcessor.ProcessReceivedMessage(buildJoinedMessage(expected, 1), "peer_near_me")

		if relay.leader() != expected {
			t.Errorf("Expect leader to be %v, but was %v", expected, relay.leader())
		}
	})
}

func buildJoinedMessage(peerID core.PeerID, timestamp int64) p2p.MessageP2P {
	return &P2PMessageMock{
		TopicField:     ActionsTopicName,
		PeerField:      peerID,
		TimestampField: timestamp,
		DataField:      []byte(JoinedAction),
	}
}

func buildPrivateMessage(peerID core.PeerID, data []byte) p2p.MessageP2P {
	return &P2PMessageMock{
		TopicField: StateTopicName,
		PeerField:  peerID,
		DataField:  data,
	}
}
