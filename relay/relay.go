package relay

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/ElrondNetwork/elrond-go/core"
	"github.com/ElrondNetwork/elrond-go/p2p"
	"math/rand"
	"time"
)

const (
	ActionsTopicName = "actions/1"
	JoinedAction     = "joined"

	StateTopicName = "private/1"

	Timeout = int64(30)
)

type Peers []core.PeerID

type Relay struct {
	peers        Peers
	messenger    NetMessenger
	timeProvider TimeProvider
}

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

type TimeProvider interface {
	UnixNow() int64
}

type defaultTimeProvider struct{}

func (p *defaultTimeProvider) UnixNow() int64 {
	return time.Now().Unix()
}

func NewRelay(messenger NetMessenger, timeProvider TimeProvider) (*Relay, error) {
	if timeProvider == nil {
		timeProvider = &defaultTimeProvider{}
	}

	self := &Relay{
		peers:        make(Peers, 0),
		messenger:    messenger,
		timeProvider: timeProvider,
	}

	topics := []string{ActionsTopicName, StateTopicName}
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
			err := r.broadcastState(message.Peer())
			if err != nil {
				fmt.Println(err)
			}
		}
	case StateTopicName:
		err := r.parseState(message.Data())
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

func (r *Relay) Join() {
	time.Sleep(10 * time.Second)
	fmt.Println(r.messenger.Addresses())

	rand.Seed(time.Now().UnixNano())
	v := rand.Intn(5)
	time.Sleep(time.Duration(v) * time.Second)
	r.messenger.Broadcast(ActionsTopicName, []byte(JoinedAction))

	// start the loop
	/*
		- process block
		- do we have deposit transactions?
		- are they actionable? (have they been bridged)
			- no: nothing to do
			- yes: am I the leader?
				- no: wait for 4 seconds for a signature request (on timeout try the next leader)
				- yes: (monitor) propose && ask for signature (txhash
			- monitor for execution confirmation the received txhash from leader
	*/
}

func (r *Relay) Close() error {
	return r.messenger.Close()
}

func (r *Relay) amITheLeader() bool {
	return r.messenger.ID() == r.leader()
}

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

func (r *Relay) parseState(data []byte) error {
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

func (r *Relay) broadcastState(toPeer core.PeerID) error {
	if len(r.peers) == 1 && r.peers[0] == r.messenger.ID() {
		return nil
	}

	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	err := enc.Encode(r.peers)
	if err != nil {
		return err
	}

	err = r.messenger.SendToConnectedPeer(StateTopicName, data.Bytes(), toPeer)
	if err != nil {
		return err
	}

	return nil
}

func (r *Relay) leader() core.PeerID {
	if len(r.peers) == 0 {
		return ""
	} else {
		unixTime := r.timeProvider.UnixNow()
		numberOfPeers := uint64(len(r.peers))
		index := uint64(unixTime/Timeout) % numberOfPeers

		return r.peers[index]
	}
}
