package edu.yu.cs.com3800.stage4;

import java.net.InetSocketAddress;
import java.util.Map;

public class GatewayPeerServerImpl extends ZooKeeperPeerServerImpl {

    public GatewayPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long, InetSocketAddress> peerIDtoAddress) {
        super(myPort, peerEpoch, id, peerIDtoAddress);
        setPeerState(ServerState.OBSERVER);
        //TODO: fix the busy wait:
        while(this.getCurrentLeader() == null){} //do nothing
        //now that there is a leader, begin processing requests
    }


}
