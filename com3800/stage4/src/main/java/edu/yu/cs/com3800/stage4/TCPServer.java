package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TCPServer extends Thread implements LoggingServer, Callable<Message> {
    private Logger logger;
    private final InetSocketAddress address;
    private final RoundRobinLeader scheduler;
    private final LinkedBlockingQueue<Message> roundRobinQueue;
    private final InetSocketAddress connectionAddress;
    private final MessageType msgType;
    private final Message initialMessage;
    private final boolean isHost;
    static final ConcurrentHashMap<Long, Message> idToMessage = new ConcurrentHashMap<>();

    enum MessageType{MASTER_CONNECTING_TO_GATEWAY, WORKER_CONNECTING_TO_MASTER}

    /**
     * @param server the ZooKeeperPeerImpl that this is attached to
     * @param msgType The type of message. THe message origin and destination
     * @param connectionAddress the address to connect to. If this is a client, this field is the host
     */
    public TCPServer(ZooKeeperPeerServerImpl server, MessageType msgType, InetSocketAddress connectionAddress, boolean isHost, Message msg){
        this.address = new InetSocketAddress(server.getMyAddress().getHostString(), server.getUdpPort() + 2);
        roundRobinQueue = new LinkedBlockingQueue<>();
        scheduler = new RoundRobinLeader(server, roundRobinQueue);
        this.msgType = msgType;
        this.connectionAddress = connectionAddress;
        this.isHost = isHost;
        this.initialMessage = msg;
    }
    public static void sendTCPMessage(InetSocketAddress target, MessageType msgType, Message msg){
        new TCPServer(null, msgType, target, true, msg).start();
    }
    public static void acceptTCPMessage(InetSocketAddress host, MessageType msgType){
        new TCPServer(null, msgType, host, false, null).start();
    }

    private Message retrieveAndAssignID(InputStream is){
        //read the request from the gateway:
        byte[] request = Util.readAllBytesFromNetwork(is);
        Message msg = new Message(request);
        //assign requestID
        long requestID = RoundRobinLeader.requestIDGenerator.getAndIncrement();
        msg = new Message(msg.getMessageType(), msg.getMessageContents(), msg.getSenderHost(),
                msg.getSenderPort(), msg.getReceiverHost(), msg.getReceiverPort(), requestID);
        logger.log(Level.FINE, "Processed unassigned message and created new id: {0}", msg);
        return msg;
    }

    @Override
    public Message call() {
        while (!this.isInterrupted()) {
            try {
                if(logger == null){
                    logger = initializeLogging(TCPServer.class.getCanonicalName() + "-on-server-with-TcpPort-" + address.getPort());
                }
                //Now that we have the connection, what to do?
                //if this is supposed to be a host:
                if(isHost){
                    ServerSocket serverSocket = new ServerSocket(address.getPort());
                    logger.log(Level.INFO, "Created new serversocket on port {0}", address.getPort());
                    Socket client = serverSocket.accept();
                    //if I'm the host, I need to initially send data.
                    //send the message:
                    OutputStream os = client.getOutputStream();
                    os.write(initialMessage.getNetworkPayload());
                    //Now I need to wait for a reply. This is a blocking call
                    byte[] response = Util.readAllBytesFromNetwork(client.getInputStream());
                    //Done! cleanup and return result
                    os.close();
                    client.close();
                    return new Message(response);
                }
                else {
                    //I am a client
                    if (msgType == MessageType.MASTER_CONNECTING_TO_GATEWAY) {
                        //TODO: Probably smarter to switch who is a "client" and "server"
                        Socket gatewaySocket = new Socket();
                        gatewaySocket.connect(connectionAddress);
                        //read the request from the gateway:
                        Message msg = retrieveAndAssignID(gatewaySocket.getInputStream());
                        //send task to scheduler
                        roundRobinQueue.put(msg);
                        //RoundRobin will schedule task and assign to a worker. The worker will communicate with the scheduler
                        //on a separate connection. Block until it is ready.
                        while (!idToMessage.containsKey(msg.getRequestID())) {};
                        //now that we have the result, send it back to the gateway:
                        OutputStream os = gatewaySocket.getOutputStream();
                        byte[] result = idToMessage.get(msg).getNetworkPayload();
                        os.write(result);
                        //the Gateway should close the connection
                    } else if (msgType == MessageType.WORKER_CONNECTING_TO_MASTER) {
                        //TODO: Probably smarter to switch who is a "client" and "server"
                        Socket gatewaySocket = new Socket();
                        gatewaySocket.connect(connectionAddress);
                        //need a way for the worker to do work locally on javarunner
                        //perhaps this should be embedded locally?
                        //or passed as a parameter
                        //something like: new TCPServer(..., JavaRunnerFollower follower)
                        //also need to return the value
                        return null;
                    }
                }
            } catch (IOException e) {
                this.logger.log(Level.WARNING,"Received IO exception in main loop. Exiting...");
            } catch (InterruptedException e) {
                this.logger.log(Level.WARNING,"Received interrupted exception in main loop. Exiting...");
            }
        }
        this.logger.log(Level.SEVERE,"Exiting RoundRobinLeader.run()");
        return null;
    }
}
