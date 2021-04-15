package relay

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/p2p"
	"github.com/ElrondNetwork/elrond-go/p2p/mock"
	"reflect"
	"testing"
	"time"
)

func TestConstructor(t *testing.T) {
	topics := []string{ActionsTopicName, PrivateTopicName}

	for _, topic := range topics {
		t.Run(fmt.Sprintf("will create %q if it does not exist", topic), func(t *testing.T) {
			messenger := &messageProcessorRegistererStub{createdTopics: []string{}}
			_, err := NewRelay(messenger, nil, nil, nil)
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
	relay, err := NewRelay(messenger, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	privateMessageProcessor := messenger.registeredMessageProcessors[PrivateTopicName]
	expected := Peers{"first", "second"}
	message := buildPrivateMessage("other", expected)
	_ = privateMessageProcessor.ProcessReceivedMessage(message, "peer_near_me")

	if !reflect.DeepEqual(relay.peers, expected) {
		t.Errorf("Expected peers to be %v, but there are %v", expected, relay.peers)
	}
}

func TestJoinedAction(t *testing.T) {
	t.Run("when there there more peers than yourself will broadcast to private", func(t *testing.T) {
		messenger := &messageProcessorRegistererStub{}
		_, err := NewRelay(messenger, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		privateMessageProcessor := messenger.registeredMessageProcessors[PrivateTopicName]
		existingPeers := Peers{"first", "second"}
		message := buildPrivateMessage("existing_peer", existingPeers)
		_ = privateMessageProcessor.ProcessReceivedMessage(message, "peer_near_me")

		actionsMessageProcessor := messenger.registeredMessageProcessors[ActionsTopicName]
		_ = actionsMessageProcessor.ProcessReceivedMessage(buildJoinedMessage("other"), "peer_near_me")

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
		_, err := NewRelay(messenger, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		actionsMessageProcessor := messenger.registeredMessageProcessors[ActionsTopicName]
		_ = actionsMessageProcessor.ProcessReceivedMessage(buildJoinedMessage(messenger.ID()), messenger.ID())

		if messenger.lastSendPeerID != "" {
			t.Error("Should not broadcast for self")
		}
	})
}

func TestJoin(t *testing.T) {
	join := func(relay *Relay, timeout time.Duration) context.CancelFunc {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		_ = relay.Join(ctx)
		return cancel
	}

	t.Run("will broadcast joined action", func(t *testing.T) {
		messenger := &messageProcessorRegistererStub{}
		ethSafe := &safeStub{transactions: map[uint64]DepositTransactions{0: {}}}
		relay, _ := NewRelay(messenger, ethSafe, nil, nil)
		relay.timer = &timerStub{sleepDelay: 1 * time.Millisecond}

		cancel := join(relay, 5*time.Millisecond)
		defer cancel()

		if !messenger.joinedWasCalled {
			t.Errorf("Expected broadcast %q on topic %q", JoinedAction, ActionsTopicName)
		}
	})
	t.Run("will keep processing blocks that deposit transactions", func(t *testing.T) {
		messenger := &messageProcessorRegistererStub{}
		ethSafe := &safeStub{transactions: map[uint64]DepositTransactions{}}
		relay, _ := NewRelay(messenger, ethSafe, nil, nil)
		relay.timer = &timerStub{sleepDelay: 1 * time.Millisecond}

		cancel := join(relay, 3*time.Millisecond)
		defer cancel()

		if ethSafe.blocksProcessed < 10 {
			t.Errorf("Expected at least 10 blocks to be processed, but there where %v", ethSafe.blocksProcessed)
		}
	})
	t.Run("will propose a deposit transaction when leader", func(t *testing.T) {
		messenger := &messageProcessorRegistererStub{peerID: "first"}
		expected := DepositTransactions{DepositTransaction{}}
		ethSafe := &safeStub{transactions: map[uint64]DepositTransactions{0: expected}}
		elrondBridgeContract := &bridgeContractStub{}
		relay, _ := NewRelay(messenger, ethSafe, nil, elrondBridgeContract)
		relay.timer = &timerStub{sleepDelay: 1 * time.Millisecond, now: 0}

		privateMessageProcessor := messenger.registeredMessageProcessors[PrivateTopicName]
		_ = privateMessageProcessor.ProcessReceivedMessage(buildPrivateMessage("other", Peers{"first", "second"}), "peer_near_me")

		cancel := join(relay, 10*time.Millisecond)
		defer cancel()

		if !reflect.DeepEqual(elrondBridgeContract.proposedTransactions, expected) {
			t.Errorf("Expected %v transactions to be proposed, and get %v", expected, elrondBridgeContract.proposedTransactions)
		}
	})
	t.Run("will monitor for proposals if not leader", func(t *testing.T) {
		messenger := &messageProcessorRegistererStub{peerID: "first"}
		expected := DepositTransaction{}
		ethSafe := &safeStub{transactions: map[uint64]DepositTransactions{0: {expected}}}
		elrondBridgeContract := &bridgeContractStub{}
		relay, _ := NewRelay(messenger, ethSafe, nil, elrondBridgeContract)
		relay.timer = &timerStub{sleepDelay: 1 * time.Millisecond, now: int64(Timeout.Seconds()) + 1}

		privateMessageProcessor := messenger.registeredMessageProcessors[PrivateTopicName]
		_ = privateMessageProcessor.ProcessReceivedMessage(buildPrivateMessage("other", Peers{"first", "second"}), "peer_near_me")

		cancel := join(relay, 10*time.Millisecond)
		defer cancel()

		if !reflect.DeepEqual(elrondBridgeContract.lastTransactionCheckedForProposal, &expected) {
			t.Error("Expected relay that is not a leader to wait for a proposal")
		}
	})
	t.Run("will monitor for signatures if leader", func(t *testing.T) {
		messenger := &messageProcessorRegistererStub{peerID: "first"}
		ethSafe := &safeStub{transactions: map[uint64]DepositTransactions{0: {DepositTransaction{}}}}
		elrondBridgeContract := &bridgeContractStub{}
		relay, _ := NewRelay(messenger, ethSafe, nil, elrondBridgeContract)
		relay.timer = &timerStub{sleepDelay: 1 * time.Millisecond}

		privateMessageProcessor := messenger.registeredMessageProcessors[PrivateTopicName]
		_ = privateMessageProcessor.ProcessReceivedMessage(buildPrivateMessage("other", Peers{"first", "second"}), "peer_near_me")

		cancel := join(relay, 5*time.Millisecond)
		defer cancel()

		expectedSignersCountCallCount := uint(2)
		got := elrondBridgeContract.signersCalledCount

		if got < expectedSignersCountCallCount {
			t.Errorf("Should have asked called SignersCount at least %d times but was called %d time", expectedSignersCountCallCount, got)
		}
	})
	t.Run("will call execute when number of signatures is reached and leader", func(t *testing.T) {
		messenger := &messageProcessorRegistererStub{peerID: "first"}
		expected := DepositTransaction{}
		ethSafe := &safeStub{transactions: map[uint64]DepositTransactions{0: {expected}}}
		elrondBridgeContract := &bridgeContractStub{signersCount: 3, lastExecutedTransaction: nil}
		relay, _ := NewRelay(messenger, ethSafe, nil, elrondBridgeContract)
		relay.timer = &timerStub{sleepDelay: 1 * time.Millisecond}

		actionsMessageProcessor := messenger.registeredMessageProcessors[PrivateTopicName]
		_ = actionsMessageProcessor.ProcessReceivedMessage(buildPrivateMessage("second", Peers{"first", "second", "other"}), "peer_near_me")

		cancel := join(relay, 10*time.Millisecond)
		defer cancel()

		if !reflect.DeepEqual(&expected, elrondBridgeContract.lastExecutedTransaction) {
			t.Errorf("Expected execute to be called with %v, but was called with %v", expected, elrondBridgeContract.lastExecutedTransaction)
		}
	})
	t.Run("will not get the next block while deposit transactions need processing", func(t *testing.T) {
		messenger := &messageProcessorRegistererStub{peerID: "first"}
		ethSafe := &safeStub{transactions: map[uint64]DepositTransactions{0: {DepositTransaction{}}}}
		elrondBridgeContract := &bridgeContractStub{signersCount: 0}
		relay, _ := NewRelay(messenger, ethSafe, nil, elrondBridgeContract)
		relay.timer = &timerStub{sleepDelay: 1 * time.Millisecond}

		actionsMessageProcessor := messenger.registeredMessageProcessors[ActionsTopicName]
		_ = actionsMessageProcessor.ProcessReceivedMessage(buildJoinedMessage("first"), "peer_near_me")
		_ = actionsMessageProcessor.ProcessReceivedMessage(buildJoinedMessage("second"), "peer_near_me")

		cancel := join(relay, 5*time.Millisecond)
		defer cancel()

		if ethSafe.blocksProcessed != 1 {
			t.Errorf("Expected to process only the first block, but %v blocks where processed", ethSafe.blocksProcessed)
		}
	})
}

func buildJoinedMessage(peerID core.PeerID) p2p.MessageP2P {
	return &mock.P2PMessageMock{
		TopicField: ActionsTopicName,
		PeerField:  peerID,
		DataField:  []byte(JoinedAction),
	}
}

func buildPrivateMessage(peerID core.PeerID, peers Peers) p2p.MessageP2P {
	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	err := enc.Encode(peers)
	if err != nil {
		panic(err)
	}

	return &mock.P2PMessageMock{
		TopicField: PrivateTopicName,
		PeerField:  peerID,
		DataField:  data.Bytes(),
	}
}

/* MOCKS/STUBS */

type bridgeContractStub struct {
	proposedTransactions              DepositTransactions
	signCountCall                     uint
	lastExecutedTransaction           *DepositTransaction
	lastTransactionCheckedForProposal *DepositTransaction
	wasProposalMade                   bool
	signersCalledCount                uint
	signersCount                      uint
}

func (c *bridgeContractStub) Propose(transaction *DepositTransaction) {
	if c.proposedTransactions == nil {
		c.proposedTransactions = DepositTransactions{}
	}
	c.proposedTransactions = append(c.proposedTransactions, *transaction)
}

func (c *bridgeContractStub) WasProposalMadeFor(t *DepositTransaction) bool {
	c.lastTransactionCheckedForProposal = t
	return c.wasProposalMade
}

func (c *bridgeContractStub) Sign(*DepositTransaction) {
	c.signCountCall++
}

func (c *bridgeContractStub) SignersCount(_ *DepositTransaction) uint {
	c.signersCalledCount++
	return c.signersCount
}

func (c *bridgeContractStub) Execute(t *DepositTransaction) {
	c.lastExecutedTransaction = t
}

type safeStub struct {
	transactions    map[uint64]DepositTransactions
	blocksProcessed uint64
}

func (s *safeStub) GetDepositTransactionsFrom(blockIndex uint64) DepositTransactions {
	s.blocksProcessed++
	return s.transactions[blockIndex]
}

type timerStub struct {
	sleepDelay time.Duration
	now        int64
}

func (s *timerStub) sleep(time.Duration) {
	time.Sleep(s.sleepDelay)
}

func (s *timerStub) nowUnix() int64 {
	return s.now
}

type messageProcessorRegistererStub struct {
	peerID                      core.PeerID
	registeredMessageProcessors map[string]p2p.MessageProcessor
	createdTopics               []string
	createTopicCalled           bool
	joinedWasCalled             bool

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
	return p.peerID
}

func (p *messageProcessorRegistererStub) Broadcast(topic string, data []byte) {
	if topic == ActionsTopicName && string(data) == JoinedAction {
		p.joinedWasCalled = true
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
