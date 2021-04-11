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
	ElectionAction   = "election"

	PrivateTopicName = "private/1"
)

type PeerTimestamp struct {
	PeerID    core.PeerID
	Timestamp int64
}
type PeerJoinedTimestamps []PeerTimestamp

type PrivateData struct {
	Peers       PeerJoinedTimestamps
	LeaderIndex uint
}

type Relay struct {
	peers       PeerJoinedTimestamps
	messenger   NetMessenger
	leaderIndex uint
}

type NetMessenger interface {
	ID() core.PeerID
	RegisterMessageProcessor(string, p2p.MessageProcessor) error
	HasTopic(name string) bool
	CreateTopic(name string, createChannelForTopic bool) error
	Addresses() []string
	Broadcast(topic string, buff []byte)
	SendToConnectedPeer(topic string, buff []byte, peerID core.PeerID) error
	Close() error
}

func NewRelay(messenger NetMessenger) (*Relay, error) {
	self := &Relay{
		peers:       make(PeerJoinedTimestamps, 0),
		messenger:   messenger,
		leaderIndex: 0,
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
			r.addPeer(message.Peer(), message.Timestamp())
			err := r.broadcastPrivate(message.Peer())
			if err != nil {
				fmt.Println(err)
			}
		case ElectionAction:
			r.election(message.Peer())
		}
	case PrivateTopicName:
		err := r.processPrivate(message.Data())
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
	time.Sleep(5 * time.Second)
	fmt.Println(r.messenger.Addresses())

	rand.Seed(time.Now().UnixNano())
	v := rand.Intn(5)
	time.Sleep(time.Duration(v) * time.Second)
	r.messenger.Broadcast(ActionsTopicName, []byte(JoinedAction))
}

func (r *Relay) CallElection() {
	if r.IAmLeader() {
		r.messenger.Broadcast(ActionsTopicName, []byte(ElectionAction))
	}
}

func (r *Relay) IAmLeader() bool {
	return r.messenger.ID() == r.leader()
}

func (r *Relay) Close() error {
	return r.messenger.Close()
}

func (r *Relay) election(source core.PeerID) {
	if len(r.peers) == 0 {
		return
	}

	if source != r.leader() {
		// don't know this leader
		return
	}

	// TODO: need to ignore election calls from others that the leader
	if r.leaderIndex+1 >= uint(len(r.peers)) {
		r.leaderIndex = 0
	} else {
		r.leaderIndex++
	}

	fmt.Printf("The newly appointed leader is %q\n", r.leader().Pretty())
}

func (r *Relay) addPeer(peerID core.PeerID, timestamp int64) {
	// TODO: account for peers that rejoin
	peerToBeAdded := PeerTimestamp{PeerID: peerID, Timestamp: timestamp}

	if len(r.peers) == 0 || r.peers[len(r.peers)-1].Timestamp < timestamp {
		r.peers = append(r.peers, peerToBeAdded)
		return
	}

	// TODO: can optimize via binary search
	for index, peer := range r.peers {
		if peer.Timestamp > timestamp {
			r.peers = append(r.peers, PeerTimestamp{})
			copy(r.peers[index+1:], r.peers[index:])
			r.peers[index] = peerToBeAdded
			break
		}
	}
}

func (r *Relay) processPrivate(data []byte) error {
	if len(r.peers) > 1 {
		// ignore this call if we already have peers
		// TODO: find a better way here
		return nil
	}

	dec := gob.NewDecoder(bytes.NewReader(data))
	var privateData PrivateData
	err := dec.Decode(&privateData)
	if err != nil {
		return err
	}
	r.peers = privateData.Peers
	r.leaderIndex = privateData.LeaderIndex

	return nil
}

func (r *Relay) broadcastPrivate(toPeer core.PeerID) error {
	if len(r.peers) == 1 && r.peers[0].PeerID == r.messenger.ID() {
		return nil
	}

	// TODO: a little chatty if all peers send to the newly joined one
	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	err := enc.Encode(PrivateData{
		Peers:       r.peers,
		LeaderIndex: r.leaderIndex,
	})
	if err != nil {
		return err
	}

	err = r.messenger.SendToConnectedPeer(PrivateTopicName, data.Bytes(), toPeer)
	if err != nil {
		return err
	}

	return nil
}

func (r *Relay) leader() core.PeerID {
	if len(r.peers) == 0 {
		return ""
	} else {
		return r.peers[r.leaderIndex].PeerID
	}
}
