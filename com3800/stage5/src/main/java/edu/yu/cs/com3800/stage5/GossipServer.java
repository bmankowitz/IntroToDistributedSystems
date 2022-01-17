package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GossipServer extends Thread implements LoggingServer {
    public static final boolean printGossipInfo = false;
    public static int GOSSIP_TIME = 1500;
    public static final int GOSSIP_FAIL_TIME = GOSSIP_TIME * 10;
    public static final int GOSSIP_FAILURE_CLEANUP_TIME = GOSSIP_FAIL_TIME * 2;
    ZooKeeperPeerServerImpl server;
    boolean shutdown = false;
    Logger log;
    Random random = new Random();
    long lastUpdate = System.currentTimeMillis();

    public GossipServer(ZooKeeperPeerServerImpl server) {
        this.server = server;
        setDaemon(true);
        try {
            log = initializeLogging(this.getClass().getCanonicalName() + "-on-server-port-"+server.getUdpPort());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private void checkEachServerFailed(){
        ArrayList<Long> servers = new ArrayList<>(server.peerIDtoAddress.keySet());;
        servers.forEach((id) -> {
            if (server.isPeerDead(id)) {
                if (server.gossipTable.get(id) != null &&
                        (System.currentTimeMillis() - server.gossipTable.get(id).getLastUpdatedTime() >= GOSSIP_FAILURE_CLEANUP_TIME)) {
                    server.gossipTable.remove(id);
                    //TODO: change this back to info
                    log.log(Level.SEVERE, "Removed server {0} from gossipTable", id);
                }
            } else if (System.currentTimeMillis() - server.gossipTable.get(id).getLastUpdatedTime() >= GOSSIP_FAIL_TIME) {
                server.reportFailedPeer(id);
            }
        });
    }
    private void gossipToRandomServer() throws IOException {
        log.log(Level.INFO, "Incrementing current heartbeat to {0}", server.gossipHeartbeat.incrementAndGet());
        //TODO: this may cause cascading failures
        server.updateLocalGossipCounter(server.getMyAddress());

        ArrayList<Long> potentialGossipServers = new ArrayList<>(server.peerIDtoAddress.keySet());
        potentialGossipServers.remove(server.getServerId());
        potentialGossipServers.removeIf(server::isPeerDead);
        if (!potentialGossipServers.isEmpty()) {
            long serverIdToSendGossip = potentialGossipServers.get(random.nextInt(potentialGossipServers.size()));
            while (server.isPeerDead(serverIdToSendGossip)) {
                serverIdToSendGossip = potentialGossipServers.get(random.nextInt(potentialGossipServers.size()));
            }
            server.sendGossip(serverIdToSendGossip);
            log.log(Level.INFO, "Sent gossip to server {0}", serverIdToSendGossip);
        }
        lastUpdate = System.currentTimeMillis();
    }
    public void checkForIncomingGossip() throws IOException, ClassNotFoundException {
        Message incomingGossip = server.getNextGossipMessage(1000000);
        if (incomingGossip != null && incomingGossip.getMessageType().equals(Message.MessageType.GOSSIP)) {
            log.log(Level.FINEST, "Received gossip message {0}", incomingGossip);
            long senderId = server.getPeerIdByAddress(new InetSocketAddress(incomingGossip.getSenderHost(), incomingGossip.getSenderPort()));
            ConcurrentHashMap<Long, GossipArchive.GossipLine> otherGossipTable = GossipArchive.getMapFromBytes(incomingGossip.getMessageContents());
            server.incorporateGossip(senderId, otherGossipTable);
        }
    }
    @Override
    public void run() {
        while(!this.isInterrupted() && !shutdown) {
            try {
                checkEachServerFailed();
                if (System.currentTimeMillis() - lastUpdate >= GOSSIP_TIME) {
                    gossipToRandomServer();
                }
                checkForIncomingGossip();
                if (shutdown || isInterrupted()) {
                    log.log(Level.SEVERE, "Interrupted while gossiping");
                } else if (server.hasCurrentLeader && server.isPeerDead(server.getCurrentLeader().getProposedLeaderID())) {
                    //the current leader failed. Go back to election
                    log.severe("Leader Failed");
                    server.setCurrentLeaderFailed();
                }
            } catch (Exception e) {
                log.severe(Util.getStackTrace(e));
                e.printStackTrace();
                shutdown();
                throw new RuntimeException(e);
            }
        }
        log.log(Level.SEVERE, "Shutting down ZooKeeperPeerServer");
    }

    public void shutdown() {
        shutdown = true;
    }

}
