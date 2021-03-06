package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RoundRobinLeader extends Thread implements LoggingServer {
    //Used to generate unique IDs for every (work) request
    static final AtomicLong requestIDGenerator = new AtomicLong(0);
    private Logger logger;
    int nextServer = 0;
    private final ArrayList<InetSocketAddress> workerServers;
    private final ZooKeeperPeerServerImpl server;

    public RoundRobinLeader(ZooKeeperPeerServerImpl server, LinkedBlockingQueue<Message> incomingMessageQueue) {
        this.server = server;
        Map<Long, InetSocketAddress> peerIDtoAddress = server.getPeerIDtoAddress();
        HashSet<Long> observers = new HashSet<>();
        peerIDtoAddress.forEach((id, x) -> {
            if(server.isObserver(id)){
                observers.add(id);
            }
        });
        observers.forEach(peerIDtoAddress::remove);
        workerServers = new ArrayList<>(peerIDtoAddress.values());
        setDaemon(true);
        setName("RoundRobinLeader-port-" + server.getUdpPort());

    }

    public void shutdown() {
        interrupt();
    }
    public synchronized InetSocketAddress getTCPAddressOfNextServer(Message msg) throws IOException {
        if(this.logger == null){
            this.logger = initializeLogging(RoundRobinLeader.class.getCanonicalName() + "-on-server-with-udpPort-"+server.getUdpPort());
            logger.log(Level.INFO, "Logging started. Next server {0}", nextServer);
        }
        //probably assume that all messages have been sanitized. iterate through each node and deliver message:
        //something like:
        if(nextServer >= workerServers.size()) nextServer = 0;
        logger.log(Level.INFO, "Next server {0}", nextServer);
        if(msg.getMessageType() != Message.MessageType.WORK) throw new RuntimeException("UNEXPECTED MESSAGE TYPE");
        if(msg.getRequestID() == -1L) {
            msg = new Message(msg.getMessageType(), msg.getMessageContents(), msg.getSenderHost(), msg.getSenderPort(), msg.getReceiverHost(), msg.getReceiverPort(), requestIDGenerator.getAndIncrement());
            logger.log(Level.FINE, "Processed unassigned message and created new id: {0}", msg);
        }
        logger.log(Level.INFO, "Forwarding message {0} to {1}", new Object[]{msg,workerServers.get(nextServer)});
        InetSocketAddress nextServerAddress = workerServers.get(nextServer);
        nextServer++;
        return new InetSocketAddress(nextServerAddress.getHostName(), nextServerAddress.getPort()+2);
    }
}
