package relay

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/p2p"
	"math"
	"math/rand"
	"time"
)

const (
	ActionsTopicName = "actions/1"
	JoinedAction     = "joined"

	PrivateTopicName = "private/1"

	Timeout             = 30 * time.Second
	MinSignaturePercent = 67
)

type Peers []core.PeerID

type State int

const (
	Init              State = 0
	Join              State = 1
	ReadBlock         State = 2
	Propose           State = 3
	WaitForSignatures State = 4
	Execute           State = 5
)

type Relay struct {
	peers     Peers
	messenger NetMessenger
	timer     Timer

	ethSafe              Safe
	elrondSafe           Safe
	elrondBridgeContract BridgeContract

	ethBlockIndex        uint64
	depositTransactions  DepositTransactions
	proposedTransaction  *DepositTransaction
	executingTransaction *DepositTransaction

	initialState State
	currentState State
}

type Timer interface {
	sleep(d time.Duration)
	nowUnix() int64
}

type defaultTimer struct{}

func (s *defaultTimer) sleep(d time.Duration) {
	time.Sleep(d)
}

func (s *defaultTimer) nowUnix() int64 {
	return time.Now().Unix()
}

type Safe interface {
	GetDepositTransactionsFrom(uint64) DepositTransactions
}

type BridgeContract interface {
	Propose(*DepositTransaction)
	WasProposalMadeFor(*DepositTransaction) bool
	Sign(*DepositTransaction)
	Execute(*DepositTransaction)
	SignersCount(*DepositTransaction) uint
}

type DepositTransaction struct{}

type DepositTransactions []DepositTransaction

type NetMessenger interface {
	ID() core.PeerID
	Addresses() []string
	RegisterMessageProcessor(string, p2p.MessageProcessor) error
	HasTopic(name string) bool
	CreateTopic(name string, createChannelForTopic bool) error
	Broadcast(topic string, buff []byte)
	SendToConnectedPeer(topic string, buff []byte, peerID core.PeerID) error
	Close() error
}

func NewRelay(messenger NetMessenger, ethSafe, elrondSafe Safe, elrondBridgeContract BridgeContract) (*Relay, error) {
	self := &Relay{
		peers:     make(Peers, 0),
		messenger: messenger,
		timer:     &defaultTimer{},

		ethSafe:              ethSafe,
		elrondSafe:           elrondSafe,
		elrondBridgeContract: elrondBridgeContract,

		initialState: Init,
	}

	topics := []string{ActionsTopicName, PrivateTopicName}
	for _, topic := range topics {
		if !messenger.HasTopic(topic) {
			err := messenger.CreateTopic(topic, true)
			if err != nil {
				return nil, err
			}
		}

		fmt.Printf("Registered on topic %q\n", topic)
		err := messenger.RegisterMessageProcessor(topic, self)
		if err != nil {
			return nil, err
		}
	}

	return self, nil
}

func (r *Relay) ProcessReceivedMessage(message p2p.MessageP2P, _ core.PeerID) error {
	fmt.Printf("Got message on topic %q\n", message.Topic())

	switch message.Topic() {
	case ActionsTopicName:
		fmt.Printf("Action: %q\n", string(message.Data()))
		switch string(message.Data()) {
		case JoinedAction:
			r.addPeer(message.Peer())
			err := r.broadcastTopology(message.Peer())
			if err != nil {
				fmt.Println(err)
			}
		}
	case PrivateTopicName:
		err := r.setTopology(message.Data())
		if err != nil {
			// TODO: log error
			fmt.Println(err)
		}
	}

	return nil
}

func (r *Relay) IsInterfaceNil() bool {
	return r == nil
}

func (r *Relay) Join(ctx context.Context) error {
	ch := make(chan State, 1)
	ch <- r.initialState

	for {
		select {
		case state := <-ch:
			switch state {
			case Init:
				go r.init(ch)
			case Join:
				go r.join(ch)
			case ReadBlock:
				go r.readBlock(ch)
			case Propose:
				go r.propose(ch)
			case WaitForSignatures:
				go r.waitForSignatures(ch)
			case Execute:
				go r.execute(ch)
			}
		case <-ctx.Done():
			return r.Close()
		}
	}
}

func (r *Relay) Close() error {
	return r.messenger.Close()
}

// Messenger

func (r *Relay) addPeer(peerID core.PeerID) {
	// TODO: account for peers that rejoin
	if len(r.peers) == 0 || r.peers[len(r.peers)-1] < peerID {
		r.peers = append(r.peers, peerID)
		return
	}

	// TODO: can optimize via binary search
	for index, peer := range r.peers {
		if peer > peerID {
			r.peers = append(r.peers, "")
			copy(r.peers[index+1:], r.peers[index:])
			r.peers[index] = peerID
			break
		}
	}
}

func (r *Relay) setTopology(data []byte) error {
	// TODO: ignore if peers are already set
	if len(r.peers) > 1 {
		// ignore this call if we already have peers
		// TODO: find a better way here
		return nil
	}

	dec := gob.NewDecoder(bytes.NewReader(data))
	var topology Peers
	err := dec.Decode(&topology)
	if err != nil {
		return err
	}
	r.peers = topology

	return nil
}

func (r *Relay) broadcastTopology(toPeer core.PeerID) error {
	if len(r.peers) == 1 && r.peers[0] == r.messenger.ID() {
		return nil
	}

	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	err := enc.Encode(r.peers)
	if err != nil {
		return err
	}

	err = r.messenger.SendToConnectedPeer(PrivateTopicName, data.Bytes(), toPeer)
	if err != nil {
		return err
	}

	return nil
}

// State

func (r *Relay) init(ch chan State) {
	r.timer.sleep(10 * time.Second)
	fmt.Println(r.messenger.Addresses())
	ch <- Join
}

func (r *Relay) join(ch chan State) {
	rand.Seed(time.Now().UnixNano())
	v := rand.Intn(5)
	r.timer.sleep(time.Duration(v) * time.Second)
	r.messenger.Broadcast(ActionsTopicName, []byte(JoinedAction))
	ch <- ReadBlock
}

func (r *Relay) readBlock(ch chan State) {
	r.depositTransactions = r.ethSafe.GetDepositTransactionsFrom(r.ethBlockIndex)

	if len(r.depositTransactions) > 0 {
		ch <- Propose
	} else {
		r.ethBlockIndex++
		ch <- ReadBlock
	}
}

func (r *Relay) propose(ch chan State) {
	if r.amITheLeader() {
		r.proposedTransaction = &r.depositTransactions[0]
		r.elrondBridgeContract.Propose(r.proposedTransaction)
		r.depositTransactions = r.depositTransactions[1:]
		ch <- WaitForSignatures
	} else {
		r.timer.sleep(Timeout)
		if r.elrondBridgeContract.WasProposalMadeFor(&r.depositTransactions[0]) {
			// sign
		} else {
			ch <- Propose
		}
	}
}

func (r *Relay) waitForSignatures(ch chan State) {
	r.timer.sleep(1 * time.Second)
	count := r.elrondBridgeContract.SignersCount(r.proposedTransaction)
	minCountRequired := math.Ceil(float64(len(r.peers)) * MinSignaturePercent / 100)

	if count >= uint(minCountRequired) && count > 0 {
		r.executingTransaction = r.proposedTransaction
		r.proposedTransaction = nil
		ch <- Execute
	} else {
		ch <- WaitForSignatures
	}
}

func (r *Relay) execute(ch chan State) {
	r.elrondBridgeContract.Execute(r.executingTransaction)
	r.executingTransaction = nil
	r.ethBlockIndex++
	ch <- ReadBlock
}

// Helpers

func (r *Relay) amITheLeader() bool {
	if len(r.peers) == 0 {
		return false
	} else {
		numberOfPeers := int64(len(r.peers))
		index := (r.timer.nowUnix() / int64(Timeout.Seconds())) % numberOfPeers

		return r.peers[index] == r.messenger.ID()
	}
}
