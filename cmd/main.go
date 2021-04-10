package main

import (
	"fmt"
	"github.com/ElrondNetwork/elrond-go/config"
	"github.com/ElrondNetwork/elrond-go/core"
	factoryMarshalizer "github.com/ElrondNetwork/elrond-go/marshal/factory"
	"github.com/ElrondNetwork/elrond-go/p2p"
	"github.com/ElrondNetwork/elrond-go/p2p/libp2p"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const topic_name = "eth_block_processing/1"

type PrintMessageProcessor struct {
	peerID core.PeerID
}

func (p *PrintMessageProcessor) ProcessReceivedMessage(message p2p.MessageP2P, fromConnectedPeer core.PeerID) error {
	if fromConnectedPeer == p.peerID {
		return nil
	}

	fmt.Printf("Received %q from %v\n", string(message.Data()), fromConnectedPeer.Pretty())
	return nil
}

func (p *PrintMessageProcessor) IsInterfaceNil() bool {
	return p == nil
}

func main() {
	internalMarshalizer, err := factoryMarshalizer.NewMarshalizer("gogo protobuf")
	if err != nil {
		panic(err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	nodeConfig := config.NodeConfig{
		Port:                       "10000",
		Seed:                       "seed",
		MaximumExpectedPeerCount:   0,
		ThresholdMinConnectedPeers: 0,
	}
	peerDiscoveryConfig := config.KadDhtPeerDiscoveryConfig{
		Enabled:                          true,
		RefreshIntervalInSec:             5,
		ProtocolID:                       "/erd/relay/1.0.0",
		InitialPeerList:                  []string{},
		BucketSize:                       0,
		RoutingTableRefreshIntervalInSec: 300,
	}

	p2pConfig := config.P2PConfig{
		Node:                nodeConfig,
		KadDhtPeerDiscovery: peerDiscoveryConfig,
		Sharding: config.ShardingConfig{
			TargetPeerCount:         0,
			MaxIntraShardValidators: 0,
			MaxCrossShardValidators: 0,
			MaxIntraShardObservers:  0,
			MaxCrossShardObservers:  0,
			Type:                    "NilListSharder",
		},
	}

	args := libp2p.ArgsNetworkMessenger{
		Marshalizer:   internalMarshalizer,
		ListenAddress: libp2p.ListenAddrWithIp4AndTcp,
		P2pConfig:     p2pConfig,
		SyncTimer:     &libp2p.LocalSyncTimer{},
	}

	messenger, err := libp2p.NewNetworkMessenger(args)
	if err != nil {
		panic(err)
	}

	err = messenger.Bootstrap()
	if err != nil {
		panic(err)
	}

	if messenger.HasTopic(topic_name) {
		fmt.Printf("Topic %q already exists\n", topic_name)
	} else {
		err := messenger.CreateTopic(topic_name, true)
		if err != nil {
			panic(err)
		}
	}

	err = messenger.RegisterMessageProcessor(topic_name, &PrintMessageProcessor{peerID: messenger.ID()})
	if err != nil {
		panic(err)
	}

	mainLoop(messenger, sigs)
}

func mainLoop(messenger p2p.Messenger, stop chan os.Signal) {
	fmt.Println(messenger.Addresses())
	for {
		select {
		case <-stop:
			return
		case <-time.After(5 * time.Second):
			fmt.Println(messenger.ConnectedAddresses())
			messenger.Broadcast(topic_name, []byte(fmt.Sprintf("Hello from %v.", messenger.ID())))
		}
	}
}
