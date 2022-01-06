package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.LoggingServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.logging.Logger;

public class GatewayPeerServerImpl extends ZooKeeperPeerServerImpl implements LoggingServer {
    static GatewayServer gs;
    public GatewayPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long, InetSocketAddress> peerIDtoAddress) throws IOException {
        super(myPort, peerEpoch, id, peerIDtoAddress);
        setPeerState(ServerState.OBSERVER);
        setCurrentLeader(null);
        gs = new GatewayServer(myPort, this);
        gs.start();
        Logger log = initializeLogging(this.getClass().getCanonicalName()+"-on-port-"+myPort);
        log.info("started GatewayServer");
    }


}
