package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GossipServer extends Thread implements LoggingServer {
    public static int GOSSIP_TIME = 350;
    public static final int GOSSIP_FAIL_TIME = GOSSIP_TIME * 10;
    public static final int GOSSIP_FAILURE_CLEANUP_TIME = GOSSIP_FAIL_TIME * 2;
    ZooKeeperPeerServerImpl server;
    boolean shutdown = false;
    Logger log;
    Random random = new Random();
    long lastUpdate = System.currentTimeMillis();
    //gossip table: id to heartbeat time
    public final ConcurrentHashMap<Long, GossipArchive.GossipLine> gossipTable;
    public final AtomicLong gossipHeartbeat;
    public final GossipArchive gossipArchive;
    public final boolean displayPrintlnGossip = false;

    public GossipServer(ZooKeeperPeerServerImpl server) {
        this.server = server;
        setDaemon(true);
        setName("SID:"+server.getServerId()+"-GossipServerUdpPort"+server.getUdpPort());
        try {
            log = initializeLogging(this.getClass().getCanonicalName() + "-on-server-port-"+server.getUdpPort());
        } catch (IOException e) {
            e.printStackTrace();
        }
        gossipTable = new ConcurrentHashMap<>();
        gossipHeartbeat = new AtomicLong(0);
        gossipArchive = new GossipArchive();
        server.getPeerIDtoAddress().keySet().forEach((peerId) -> {
            //Peers are assumed to be alive when first created. They will only fail after T_fail seconds
            gossipTable.put(peerId, new GossipArchive.GossipLine(server.getServerId(), gossipHeartbeat.get(), System.currentTimeMillis(), false));
        });
        //if self wasn't included, add it now:
        gossipTable.put(server.getServerId(), new GossipArchive.GossipLine(server.getServerId(), gossipHeartbeat.get(), System.currentTimeMillis(), false));
    }
    private void checkEachServerFailed(){
        ArrayList<Long> servers = new ArrayList<>(server.getPeerIDtoAddress().keySet());;
        servers.forEach((id) -> {
            if (server.isPeerDead(id)) {
                if (gossipTable.get(id) != null &&
                        (System.currentTimeMillis() - gossipTable.get(id).getLastUpdatedTime() >= GOSSIP_FAILURE_CLEANUP_TIME)) {
                    gossipTable.remove(id);
                    //TODO: change this back to info
                    log.log(Level.SEVERE, "Removed server {0} from gossipTable", id);
                }
            } else if (System.currentTimeMillis() - gossipTable.get(id).getLastUpdatedTime() >= GOSSIP_FAIL_TIME) {
                reportFailedPeer(id);
            }
        });
    }
    private void gossipToRandomServer() throws IOException {
        log.log(Level.INFO, "Incrementing current heartbeat to {0}", gossipHeartbeat.incrementAndGet());
        //TODO: this may cause cascading failures
        updateLocalGossipCounter(server.getMyAddress());

        ArrayList<Long> potentialGossipServers = new ArrayList<>(server.getPeerIDtoAddress().keySet());
        potentialGossipServers.remove(server.getServerId());
        potentialGossipServers.removeIf(server::isPeerDead);
        if (!potentialGossipServers.isEmpty()) {
            long serverIdToSendGossip = potentialGossipServers.get(random.nextInt(potentialGossipServers.size()));
            while (server.isPeerDead(serverIdToSendGossip)) {
                serverIdToSendGossip = potentialGossipServers.get(random.nextInt(potentialGossipServers.size()));
            }
            sendGossip(serverIdToSendGossip);
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
            incorporateGossip(senderId, otherGossipTable);
        }
    }
    public String getGossipArchive(){
        //TODO: also create new log file
        String gossipArchiveSnapshot = gossipArchive.getArchive();
        try{
            String fileName = "Server"+server.getServerId()+"GossipArchiveAt"+ LocalDateTime.now();
            File file = new File(fileName);
            file.createNewFile();
            FileWriter logFile = new FileWriter(fileName);
            logFile.write(gossipArchiveSnapshot);
            logFile.close();
        } catch (IOException e) {
            e.printStackTrace();
            log.warning("Error occured creating gossip log file");
        }
        return gossipArchiveSnapshot;
    }
    public void incorporateGossip(long senderId, ConcurrentHashMap<Long, GossipArchive.GossipLine> otherGossip){
        otherGossip.forEach((otherId, otherGossipLine) ->{
            if((gossipTable.get(otherId) == null) || (otherGossipLine.lastHeartbeat > this.gossipTable.get(otherId).getLastHeartbeat())) {
                otherGossipLine.setLastUpdatedTime(System.currentTimeMillis());
                if(gossipTable.put(otherId, otherGossipLine) == null){
                    log.log(Level.WARNING, "Inserted new server {0} into gossip table", otherId);
                }
                String logString = server.getServerId() + ": updated " + otherId + "'s heartbeat sequence to "
                        + otherGossipLine.lastHeartbeat + " at node time " + System.currentTimeMillis();
                //double logging per requirements:
                log.log(Level.FINER, logString);
                if(displayPrintlnGossip) System.out.println(logString);
            }
            else log.log(Level.FINER, server.getServerId() + ": skipped updating " + otherId + "'s heartbeat because " +
                    otherGossipLine.lastHeartbeat + "is lower. Node time " + System.currentTimeMillis());
        });
        gossipArchive.addToArchive(senderId, System.currentTimeMillis(), gossipTable);
    }
    public void updateLocalGossipCounter(InetSocketAddress serverAddress){
        long serverId = server.getPeerIdByAddress(serverAddress);
        long newHeartbeat = gossipHeartbeat.get();
        if(gossipTable.get(serverId) == null || newHeartbeat > gossipTable.get(serverId).getLastHeartbeat()){
            log.log(Level.FINER, "Updating local gossip table. Changing server {0} from {1} to {2}",
                    new Object[]{serverId, gossipTable.get(serverId), newHeartbeat});
            GossipArchive.GossipLine gs = new GossipArchive.GossipLine(serverId, newHeartbeat, System.currentTimeMillis(), false);
            gossipTable.put(serverId, gs);
        }
        else log.log(Level.FINEST, "ignoring heartbeat {0} from server {1}", new Object[]{newHeartbeat, serverId});
    }
    public void sendGossip(long gossipDestinationId) throws IOException {
        ConcurrentHashMap<Long, GossipArchive.GossipLine> gossipToSend = new ConcurrentHashMap<>(gossipTable);
        gossipTable.keySet().forEach((serverId) -> {
            if(isPeerDead(serverId)) gossipToSend.remove(serverId);
        });
        byte[] bytes = GossipArchive.getBytesFromMap(gossipToSend);
        //Gossip messages are sent by UDP not TCP because it does not deal with client work. See
        server.sendMessage(Message.MessageType.GOSSIP, bytes, server.getPeerIDtoAddress().get(gossipDestinationId));
    }

    public boolean isPeerDead(InetSocketAddress address) {
        Optional<Map.Entry<Long, InetSocketAddress>> getId = server.getPeerIDtoAddress().entrySet()
                .stream().filter(entry -> entry.getValue().equals(address)).findFirst();
        if(getId.isEmpty()) return true;
        else return isPeerDead(getId.get().getKey());
    }

    public void reportFailedPeer(long peerID) {
        log.log(Level.WARNING, "{0}: no heartbeat from server {1} - server failed", new Object[]{this.server.getServerId(), peerID});
        //double-logging per requirements
        System.out.println(server.getServerId()+": no heartbeat from server "+peerID+" - server failed");
        gossipTable.get(peerID).setFailed(true);
        server.peerIDtoStatus.remove(peerID);
    }

    public boolean isPeerDead(long peerID) {
        //if the peer doesn't exist on the list, it is failed (or never existed)
        if(gossipTable.get(peerID) == null || gossipTable.get(peerID).isFailed()){
            return true;
        }
        else if (System.currentTimeMillis() - gossipTable.get(peerID).getLastUpdatedTime() >= GOSSIP_FAIL_TIME) {
            reportFailedPeer(peerID);
            return true;
        }
        return false;
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
