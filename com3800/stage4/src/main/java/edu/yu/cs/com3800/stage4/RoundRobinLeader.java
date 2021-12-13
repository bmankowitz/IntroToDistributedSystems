package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RoundRobinLeader extends Thread implements LoggingServer {
    //Used to generate unique IDs for every (work) request
    static final AtomicLong requestIDGenerator = new AtomicLong(0);
    private Logger logger;
    private final LinkedBlockingQueue<Message> incomingMessageQueue;
    int nextServer = 0;
    private final ArrayList<InetSocketAddress> workerServers;
    private final ZooKeeperPeerServerImpl server;
    private final Map<Long, InetSocketAddress> requestIDtoAddress = new HashMap<>();

    public RoundRobinLeader(ZooKeeperPeerServerImpl server, LinkedBlockingQueue<Message> incomingMessageQueue) {
        this.server = server;
        Map<Long, InetSocketAddress> peerIDtoAddress = server.getPeerIDtoAddress();
        server.observerIds.forEach(x -> peerIDtoAddress.remove(x));
        this.incomingMessageQueue = incomingMessageQueue;
        workerServers = new ArrayList<>(peerIDtoAddress.values());
        setDaemon(true);
        setName("RoundRobinLeader-port-");

    }

    public void shutdown() {
        interrupt();
    }
    public synchronized InetSocketAddress getTCPAddressOfNextServer(Message msg) throws IOException {
        //todo: implement
        if(this.logger == null){
            this.logger = initializeLogging(RoundRobinLeader.class.getCanonicalName() + "-on-server-with-udpPort-");
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
        requestIDtoAddress.put(msg.getRequestID(), new InetSocketAddress(msg.getSenderHost(), msg.getSenderPort()+2));
        logger.log(Level.INFO, "Received message {0}. Returning value to caller:to {1}", new Object[]{msg,workerServers.get(nextServer)});
        InetSocketAddress nextServerAddress = workerServers.get(nextServer);
        nextServer++;
        return new InetSocketAddress(nextServerAddress.getHostName(), nextServerAddress.getPort()+2);
    }
    @Override @Deprecated
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
                logger.log(Level.INFO, "Next server {0}", nextServer);
                Message msg = incomingMessageQueue.take();
                if(msg.getMessageType() != Message.MessageType.WORK) throw new RuntimeException("UNEXPECTED MESSAGE TYPE");
                if(msg.getRequestID() == -1L) {
                    msg = new Message(msg.getMessageType(), msg.getMessageContents(), msg.getSenderHost(), msg.getSenderPort(), msg.getReceiverHost(), msg.getReceiverPort(), requestIDGenerator.getAndIncrement());
                    logger.log(Level.FINE, "Processed unassigned message and created new id: {0}", msg);
                }
                requestIDtoAddress.put(msg.getRequestID(), new InetSocketAddress(msg.getSenderHost(), msg.getSenderPort()));
                logger.log(Level.INFO, "Received message {0}. Sending to {1}", new Object[]{msg,workerServers.get(nextServer)});
                //TODO: convert to TCP
                server.sendMessage(Message.MessageType.WORK, msg.getRequestID(), msg.getMessageContents(), workerServers.get(nextServer));
                nextServer++;
            } catch (IOException e) {
                this.logger.log(Level.SEVERE,"Exception trying to process workItem", e);
                throw new RuntimeException();
            } catch (InterruptedException e) {
                this.logger.log(Level.WARNING,"Received Interrupt in main loop. Exiting...");
            }
        }
        this.logger.log(Level.SEVERE,"Exiting RoundRobinLeader.run()");
    }

}
