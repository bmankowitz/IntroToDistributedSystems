package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RoundRobinLeader extends Thread implements LoggingServer {
    private Logger logger;
    private final LinkedBlockingQueue<Message> messageQueue;
    int nextServer = 0;
    private final ArrayList<InetSocketAddress> workerServers;
    private ZooKeeperPeerServerImpl server;
    private Map<Long, InetSocketAddress> requestIDtoAddress = new HashMap<>();

    public RoundRobinLeader(ZooKeeperPeerServerImpl server, LinkedBlockingQueue<Message> messageQueue) {
        this.server = server;
        Map<Long, InetSocketAddress> peerIDtoAddress = server.getPeerIDtoAddress();
        this.messageQueue = messageQueue;
        workerServers = new ArrayList<>(peerIDtoAddress.values());
        setDaemon(true);
        setName("RoundRobinLeader-port-");
    }

    public void shutdown() {
        interrupt();
    }

    @Override
    public void run() {
        while (!this.isInterrupted()) {
            try {
                if(this.logger == null){
                    this.logger = initializeLogging(RoundRobinLeader.class.getCanonicalName() + "-on-server-with-udpPort-");
                    logger.log(Level.INFO, "Logging started. Next server {0}", nextServer);
                }
                //probably assume that all messages have been sanitized. iterate through each node and deliver message:
                //something like:
                if(nextServer >= workerServers.size()) nextServer = 0;
                //TODO: get the actual message
                logger.log(Level.INFO, "Next server {0}", nextServer);
                Message msg = messageQueue.take();
                if(msg.getMessageType() != Message.MessageType.WORK) throw new RuntimeException("UNEXPECTED MESSAGE TYPE");
                if(msg.getRequestID() == -1L) throw new RuntimeException("This message should have a request ID");
                requestIDtoAddress.put(msg.getRequestID(), new InetSocketAddress(msg.getSenderHost(), msg.getSenderPort()));
                logger.log(Level.INFO, "Received message {0}. Sending to {1}", new Object[]{msg,workerServers.get(nextServer)});
                server.sendMessage(Message.MessageType.WORK, msg.getRequestID(), msg.getMessageContents(), workerServers.get(nextServer));
                nextServer++;
            }
            catch (IOException | InterruptedException e) {
                this.logger.log(Level.WARNING,"Exception trying to process workItem", e);
                throw new RuntimeException();
            }
        }
        this.logger.log(Level.SEVERE,"Exiting RoundRobinLeader.run()");
    }

}
