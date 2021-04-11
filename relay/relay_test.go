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

func (p *messageProcessorRegistererStub) Broadcast(topicName string, data []byte) {
	if topicName == ActionsTopicName && string(data) == ElectionAction {
		p.electionWasCalled = true
	}
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

func TestConstructor(t *testing.T) {
	topics := []string{ActionsTopicName, PrivateTopicName}

	for _, topic := range topics {
		t.Run(fmt.Sprintf("will create %q if it does not exist", topic), func(t *testing.T) {
			messenger := &messageProcessorRegistererStub{createdTopics: []string{}}
			_, err := NewRelay(messenger)
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
	relay, err := NewRelay(messenger)
	if err != nil {
		t.Fatal(err)
	}

	privateMessageProcessor := messenger.registeredMessageProcessors[PrivateTopicName]
	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	expected := PrivateData{
		Peers: PeerJoinedTimestamps{
			PeerTimestamp{"first", 1},
			PeerTimestamp{"second", 2},
		},
		LeaderIndex: 1,
	}
	err = enc.Encode(expected)
	if err != nil {
		t.Fatal(err)
	}

	message := buildPrivateMessage("other", data.Bytes())
	_ = privateMessageProcessor.ProcessReceivedMessage(message, "peer_near_me")

	if !reflect.DeepEqual(relay.peers, expected.Peers) {
		t.Errorf("Expected peers to be %v, but there are %v", expected.Peers, relay.peers)
	}

	if relay.leaderIndex != expected.LeaderIndex {
		t.Errorf("Expected leader index to be %d, but it was %d", expected.LeaderIndex, relay.leaderIndex)
	}
}

func TestJoinedAction(t *testing.T) {
	t.Run("when there there more peers than yourself will broadcast to private", func(t *testing.T) {
		messenger := &messageProcessorRegistererStub{}
		_, err := NewRelay(messenger)
		if err != nil {
			t.Fatal(err)
		}

		privateMessageProcessor := messenger.registeredMessageProcessors[PrivateTopicName]
		var data bytes.Buffer
		enc := gob.NewEncoder(&data)
		existingPeers := PeerJoinedTimestamps{
			PeerTimestamp{"first", 1},
			PeerTimestamp{"second", 2},
		}
		privateData := PrivateData{
			Peers:       existingPeers,
			LeaderIndex: 0,
		}
		err = enc.Encode(privateData)
		if err != nil {
			t.Fatal(err)
		}

		message := buildPrivateMessage("existing_peer", data.Bytes())
		_ = privateMessageProcessor.ProcessReceivedMessage(message, "peer_near_me")

		actionsMessageProcessor := messenger.registeredMessageProcessors[ActionsTopicName]
		_ = actionsMessageProcessor.ProcessReceivedMessage(buildJoinedMessage("other", 42), "peer_near_me")
		expected := append(existingPeers, PeerTimestamp{"other", 42})

		dec := gob.NewDecoder(bytes.NewReader(messenger.lastSendData))
		var got PrivateData
		err = dec.Decode(&got)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(expected, got.Peers) {
			t.Errorf("Expected %v, got %v", expected, got)
		}
	})
	t.Run("when you just joined will not broadcast to private", func(t *testing.T) {
		messenger := &messageProcessorRegistererStub{}
		_, err := NewRelay(messenger)
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
	cases := []struct {
		description    string
		messages       []p2p.MessageP2P
		expectedLeader core.PeerID
	}{
		{
			description:    "the first joined will be the leader after the first election",
			messages:       []p2p.MessageP2P{buildJoinedMessage("second", 2), buildJoinedMessage("first", 1)},
			expectedLeader: "first",
		},
		{
			description: "will choose the next peer based on Timestamp on successive election",
			messages: []p2p.MessageP2P{
				buildJoinedMessage("second", 2),
				buildJoinedMessage("first", 1),
				buildElectionMessage(),
			},
			expectedLeader: "second",
		},
		{
			description: "will choose the first peer again on successive election",
			messages: []p2p.MessageP2P{
				buildJoinedMessage("second", 2),
				buildJoinedMessage("first", 1),
				buildElectionMessage(),
				buildElectionMessage(),
			},
			expectedLeader: "first",
		},
		{
			description:    "will skip election if it has no peers",
			messages:       []p2p.MessageP2P{buildElectionMessage()},
			expectedLeader: "",
		},
	}

	for _, tt := range cases {
		t.Run(tt.description, func(t *testing.T) {
			messenger := &messageProcessorRegistererStub{}
			relay, err := NewRelay(messenger)
			if err != nil {
				t.Fatal(err)
			}

			actionsMessageProcessor := messenger.registeredMessageProcessors[ActionsTopicName]
			for _, message := range tt.messages {
				_ = actionsMessageProcessor.ProcessReceivedMessage(message, "peer_near_me")
			}

			if relay.leader() != tt.expectedLeader {
				t.Errorf("Expect leader to be %v, but was %v", tt.expectedLeader, relay.leader())
			}
		})
	}
}

func TestCallElection(t *testing.T) {
	t.Run("it will broadcast on election topic if it is the current leader", func(t *testing.T) {
		messenger := &messageProcessorRegistererStub{}
		relay, err := NewRelay(messenger)
		if err != nil {
			t.Fatal(err)
		}

		joinedMessageProcessor := messenger.registeredMessageProcessors[ActionsTopicName]
		_ = joinedMessageProcessor.ProcessReceivedMessage(buildJoinedMessage(messenger.ID(), 0), "peer_near_me")
		electionMessageProcessor := messenger.registeredMessageProcessors[PrivateTopicName]
		_ = electionMessageProcessor.ProcessReceivedMessage(buildElectionMessage(), "peer_near_me")

		relay.CallElection()

		if !messenger.electionWasCalled {
			t.Error("Expected a message to be broadcasted on the election topic")
		}
	})
	t.Run("it will not broadcast on the election topic if it's not the leader", func(t *testing.T) {
		messenger := &messageProcessorRegistererStub{}
		relay, err := NewRelay(messenger)
		if err != nil {
			t.Fatal(err)
		}

		joinedMessageProcessor := messenger.registeredMessageProcessors[ActionsTopicName]
		_ = joinedMessageProcessor.ProcessReceivedMessage(buildJoinedMessage("other", 0), "peer_near_me")
		electionMessageProcessor := messenger.registeredMessageProcessors[PrivateTopicName]
		_ = electionMessageProcessor.ProcessReceivedMessage(buildElectionMessage(), "peer_near_me")

		relay.CallElection()

		if messenger.electionWasCalled {
			t.Error("Expected a message not to be broadcasted on the election topic")
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

func buildElectionMessage() p2p.MessageP2P {
	return &P2PMessageMock{
		TopicField: ActionsTopicName,
		DataField:  []byte(ElectionAction),
	}
}

func buildPrivateMessage(peerID core.PeerID, data []byte) p2p.MessageP2P {
	return &P2PMessageMock{
		TopicField: PrivateTopicName,
		PeerField:  peerID,
		DataField:  data,
	}
}
