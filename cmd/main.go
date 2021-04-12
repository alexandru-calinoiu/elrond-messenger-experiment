package main

import (
	"github.com/ElrondNetwork/elrond-go/config"
	factoryMarshalizer "github.com/ElrondNetwork/elrond-go/marshal/factory"
	"github.com/ElrondNetwork/elrond-go/p2p/libp2p"
	"github.com/alexandru-calinoiu/messenger-experiment/relay"
	"os"
	"os/signal"
	"syscall"
	"time"
)

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

	relayNode, err := relay.NewRelay(messenger)
	if err != nil {
		panic(err)
	}

	go relayNode.Join()

	mainLoop(relayNode, sigs)
}

func mainLoop(relayNode *relay.Relay, stop chan os.Signal) {
	for {
		select {
		case <-stop:
			err := relayNode.Close()
			if err != nil {
				panic(err)
			}

			return
		case <-time.After(10 * time.Second):
			// why
		}
	}
}
