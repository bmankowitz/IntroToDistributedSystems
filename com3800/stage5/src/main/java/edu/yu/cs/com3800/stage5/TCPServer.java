package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.io.IOException;
import java.io.InputStream;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TCPServer extends Thread implements LoggingServer, Callable<Message> {
    private Logger logger;
    private RoundRobinLeader scheduler;
    private JavaRunnerFollower javaRunnerFollower;
    private final LinkedBlockingQueue<Message> tcpMessageQueue;
    private final InetSocketAddress connectionAddress;
    private final InetSocketAddress myAddress;
    private final ServerType serverType;
    private final ZooKeeperPeerServerImpl server;
    private Socket outerSocket;
    private ServerSocket serverSocket;
    private boolean shutdown = false;

    enum ServerType {CONNECTOR, SCHEDULER, WORKER}

    /**
     * @param server the ZooKeeperPeerImpl that this is attached to
     * @param connectionAddress the address to connect to. If this is a client, this field is the host
     */
    public TCPServer(ZooKeeperPeerServerImpl server, InetSocketAddress connectionAddress, ServerType serverType, Message initialMessage) throws IOException, InterruptedException {
        this.setDaemon(true);
        this.connectionAddress = connectionAddress;
        this.serverType = serverType;
        this.server = server;
        this.myAddress = new InetSocketAddress(server.getMyAddress().getHostName(),server.getMyAddress().getPort()+2);
        tcpMessageQueue = new LinkedBlockingQueue<>();
        if(initialMessage != null) tcpMessageQueue.put(initialMessage);
        setName(TCPServer.class.getCanonicalName() + "-on-server-with-TcpPort-" + myAddress.getPort());
        if(logger == null) logger = initializeLogging(TCPServer.class.getCanonicalName() + "-on-server-with-TcpPort-" + myAddress.getPort());
        LinkedBlockingQueue<Message> messageQueue;
        //ensure there is only one JavaRunner/RoundRobinScheduler per server. They should only be created here!
        if(serverType == ServerType.SCHEDULER){
            messageQueue = new LinkedBlockingQueue<>();
            scheduler = new RoundRobinLeader(server, messageQueue);
            scheduler.start();
        } else if(serverType == ServerType.WORKER){
            //create JavaRunnerFollower
            messageQueue = new LinkedBlockingQueue<>();
            javaRunnerFollower = new JavaRunnerFollower(server, messageQueue);
            javaRunnerFollower.start();
        } 
    }
    public Socket startTcpServer(InetSocketAddress address) throws IOException{
        logger.log(Level.INFO, "Creating new server socket on port {0}...", address.getPort());
        serverSocket = new ServerSocket(address.getPort());
        logger.log(Level.INFO, "Created new server socket on port {0}. Accepting....", address.getPort());
        Socket client = serverSocket.accept();
        logger.log(Level.INFO, "Accepted connection from {0} on port {1}.", new Object[]{client, address.getPort()});
//        // ---- GOSSIP STUFF -----
//        InetSocketAddress socketAddress = new InetSocketAddress(client.getLocalAddress().getHostName(), client.getLocalPort() - 2);
//        server.gs.updateLocalGossipCounter(socketAddress);
//        // ---- GOSSIP STUFF -----
        return client;
    }

    public Socket connectTcpServer(InetSocketAddress address) throws IOException{
        logger.log(Level.INFO, "Connecting to TCPServer on port {0}....", address.getPort());
        Socket client = new Socket(address.getHostString(), address.getPort());
        logger.log(Level.INFO, "Connected to TCPServer on port {0}", address.getPort());
//        // ---- GOSSIP STUFF -----
//        InetSocketAddress socketAddress = new InetSocketAddress(client.getLocalAddress().getHostName(), client.getLocalPort());
//        server.updateLocalGossipCounter(socketAddress);
//        // ---- GOSSIP STUFF -----
        return client;
    }
    public boolean sendMessage(Socket socket, byte[] data) throws IOException {
        logger.log(Level.FINE, "Sending message to {0}: {1}.", new Object[]{socket, data, new Message(data)});
        try {
            socket.getOutputStream().write(data);
            logger.log(Level.FINE, "Sent message to {0}: {1}.", new Object[]{socket, data, new Message(data)});
            return true;
        }catch (SocketException e){
            logger.log(Level.WARNING, "SocketException {0} failed to send to {1}: {2}.", new Object[]{e, socket, data, new Message(data)});
            return false;
        }
    }
    public byte[] receiveMessage(Socket socket) throws IOException, InterruptedException {
        final byte[] received = modifiedReadAllBytesFromNetwork(socket);
        if(received == null){
            //There was a problem with the peer, most likely failed.
            logger.log(Level.WARNING, "Problem with {0}. It most likely failed", socket);
            return null;
        }
        logger.log(Level.FINE, "Received message from {0}: {1}..\n Parsed as Message: {2}",
                new Object[]{socket, received, new Message(received)});
        // ---- GOSSIP STUFF -----
        Message msg = new Message(received);
        int port = msg.getSenderPort();
        //if the last digit is a two, then this is the TCPServer. Check the main server's address
        if(((msg.getSenderPort() % 1000) % 100) % 10 == 2){
            //subtracting 2 from port to get address of the main ZooKeeperPeerServer
            port = msg.getSenderPort() -2;
        }
        InetSocketAddress socketAddress = new InetSocketAddress(msg.getSenderHost(), port);
        server.gs.updateLocalGossipCounter(socketAddress);
        // ---- GOSSIP STUFF -----
        return received;
    }
    public void closeConnection(Socket socket) throws IOException {
        logger.log(Level.INFO, "shutting down socket {0}", socket);
        socket.close();
        if(socket.isClosed()) logger.log(Level.INFO, "Successfully shut down socket {0}", socket);
    }

    public void shutdown(){
        logger.log(Level.WARNING, "Received TCPServer shutdown request");
        if(this.javaRunnerFollower != null) javaRunnerFollower.shutdown();
        if(this.scheduler != null) scheduler.shutdown();
        shutdown = true;
        this.interrupt();
    }
    public byte[] modifiedReadAllBytesFromNetwork(Socket socket) throws InterruptedException {
        InputStream in = null;
        try {
            in = socket.getInputStream();
            while (in.available() == 0) {
//                try {
                    //Due to inconsistencies between remote/local address, assume a peer is failed only if both are failed.
                    //TODO: find a better system
                    InetSocketAddress socketAddressLocal = new InetSocketAddress(socket.getLocalAddress().getHostName(), socket.getLocalPort()-2);
                    InetSocketAddress socketAddressRemote = new InetSocketAddress(socket.getLocalAddress().getHostName(), socket.getPort()-2);
                    if(server.isPeerDead(socketAddressLocal) && server.isPeerDead(socketAddressRemote)){
                        logger.log(Level.WARNING, "Trying to read data from failed peer {0}", socket);
                        return null;
                    }
                    sleep(1000);
                    if(isInterrupted() || shutdown) throw new InterruptedException("Was interrupted");
//                }
            }
        }
        catch(IOException ignored){}
        assert in != null;
        return Util.readAllBytes(in);
    }

    @Override
    public Message call() {
        while (!this.isInterrupted() && !shutdown) {
            try {
                //Now that we have the connection, what to do?
                if(serverType == ServerType.CONNECTOR){
                    //this "server" connects to a host, sends data, receives data, and then closes immediately
                    if(outerSocket == null){
                        outerSocket = startTcpServer(connectionAddress);
                        logger.log(Level.INFO, "Started new server on socket: {0}", outerSocket);
                    }
                    else{
                        logger.log(Level.INFO, "Existing server appears to exist: {0}. Accepting", outerSocket);
                        outerSocket = serverSocket.accept();
                    }
                    sendMessage(outerSocket, tcpMessageQueue.take().getNetworkPayload());
                    byte[] response = receiveMessage(outerSocket);
                }
                else{
                    //I am a "client"
                    //This server should stay alive until closed by the client, at which point it should restart. This
                    //server should ONLY shut down if interrupted
                    logger.log(Level.FINE, "Trying to start server on: {0}", connectionAddress);
                    if(outerSocket == null){
                        outerSocket = startTcpServer(connectionAddress);
                        logger.log(Level.INFO, "Started new server on socket: {0}", outerSocket);
                    }
                    else{
                        logger.log(Level.INFO, "Existing server appears to exist: {0}. Accepting", outerSocket);
                        outerSocket = serverSocket.accept();
                    }
                    //get the request (ie block until a request comes in):
                    byte[] request = null;
                    while(request == null) {
                        request= receiveMessage(outerSocket);
                    }
                    Message msg = new Message(request);
                    //need to add the message to the queue, and get back a response:
                    //Since at this point, everything is synchronous anyway, might as well create a synchronous
                    //method that returns the result.
                    String result = "";
                    if(serverType == ServerType.WORKER){
                        //this is a worker. Assign the value and return it later
                        result = javaRunnerFollower.processWorkItem(msg);
                    } else if (serverType == ServerType.SCHEDULER){
                        //if a server is found to have failed, try asking the next one:
                        byte[] rawResponse = null;
                        InetSocketAddress nextFollowerAddress = null;
                        while(rawResponse== null){
                            try {
                                //This is the RoundRobinLeader. This will connect to a specified worker, which will accept,
                                //process the data, and send back the result. The scheduler is required to close the connection.
                                nextFollowerAddress = scheduler.getTCPAddressOfNextServer(msg);
                                logger.log(Level.INFO, "Attempting to send work to worker id {0} at address {1}",
                                        new Object[]{server.getPeerIdByAddress(nextFollowerAddress), nextFollowerAddress});
                                Socket innerSocket = connectTcpServer(nextFollowerAddress);
                                sendMessage(innerSocket, msg.getNetworkPayload());
                                rawResponse = receiveMessage(innerSocket);
                                if (rawResponse != null) {
                                    Message response = new Message(rawResponse);
                                    this.sendMessage(outerSocket, response.getNetworkPayload());
                                } else logger.log(Level.WARNING, "Sent message to failed node. Trying again:");
                            } catch(ConnectException e){
                                logger.log(Level.WARNING, "Unable to connect to address {0}. Trying next address", nextFollowerAddress);
                            }

                        }

                    }
                    logger.log(Level.FINE, "Received result: {0}", result);
                    Message completedWork = new Message(Message.MessageType.COMPLETED_WORK,
                            result.getBytes(StandardCharsets.UTF_8),this.myAddress.getHostString(), this.myAddress.getPort(),
                            outerSocket.getInetAddress().getHostName(), outerSocket.getPort(), msg.getRequestID());
                    if(shutdown || isInterrupted()){
                        logger.log(Level.SEVERE, "was interrupted before sending completed work...");
                        break;
                    }
                    //this eliminates queuing. A server constantly tries to send result to the leader. If the leader is
                    //dead, wait until a new leader is elected. Proactively send work, no need to wait for a request.
                    while(!sendMessage(outerSocket, completedWork.getNetworkPayload())){
                        logger.log(Level.WARNING, "Failed to send message. Leader is likely dead");
                        while(!server.hasCurrentLeader || server.isPeerDead(server.getCurrentLeader().getProposedLeaderID())){
                            Thread.sleep(500);
                            logger.log(Level.INFO, "Leader is not available. Waiting until leader elected...");
                        }
                    }
                }

            } catch (IOException e) {
                this.logger.log(Level.WARNING,"Received IO exception in main loop. Trying again...");
                e.printStackTrace();
            } catch (InterruptedException e) {
                this.logger.log(Level.WARNING,"Received InterruptedException in main loop.");
                break;
            }
        }
        this.logger.log(Level.SEVERE,"Exiting TCPServer.run()");
        return null;
    }
}
