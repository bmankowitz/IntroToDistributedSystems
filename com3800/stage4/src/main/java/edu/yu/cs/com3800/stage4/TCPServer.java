package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TCPServer extends Thread implements LoggingServer, Callable<Message> {
    private Logger logger;
    private RoundRobinLeader scheduler;
    private JavaRunnerFollower javaRunnerFollower;
    private final LinkedBlockingQueue<Message> tcpMessageQueue;
    private final InetSocketAddress connectionAddress;
    static final ConcurrentHashMap<Long, Message> idToMessage = new ConcurrentHashMap<>();
    private final InetSocketAddress myAddress;
    private final ServerType serverType;
    private final ZooKeeperPeerServerImpl server;
    private Socket lastSocket;
    private Socket socket;
    private Socket connectTcpServer;

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
        //todo ensure this is correct
        this.myAddress = new InetSocketAddress(server.getMyAddress().getHostName(),server.getMyAddress().getPort()+2);
        tcpMessageQueue = new LinkedBlockingQueue<>();
        if(initialMessage != null) tcpMessageQueue.put(initialMessage);
        logger = initializeLogging(TCPServer.class.getCanonicalName() + "-on-server-with-TcpPort-" + myAddress.getPort());
        //todo: ensure there is only one JavaRunner/RoundRoubinScheduler per server. They should only be created here!
        LinkedBlockingQueue<Message> messageQueue;
        if(serverType == ServerType.SCHEDULER){
            messageQueue = new LinkedBlockingQueue<>();
            scheduler = new RoundRobinLeader(server, messageQueue);
            scheduler.start();
        } else if(serverType == ServerType.WORKER){
            //create javarunner
            messageQueue = new LinkedBlockingQueue<>();
            javaRunnerFollower = new JavaRunnerFollower(server, messageQueue);
            javaRunnerFollower.start();
        } 
    }
    public Socket startTcpServer(InetSocketAddress address) throws IOException{
        if(lastSocket !=null && lastSocket.getLocalPort()==address.getPort()) return lastSocket;
        ServerSocket serverSocket = new ServerSocket(address.getPort());
        logger.log(Level.INFO, "Created new server socket on port {0}", address.getPort());
        Socket client = serverSocket.accept();
        lastSocket = client;
        return client;
    }

    public Socket connectTcpServer(InetSocketAddress address) throws IOException{
        if(lastSocket !=null && lastSocket.getLocalPort()==address.getPort()) return lastSocket;
        logger.log(Level.INFO, "Connecting to TCPServer on port {0}....", address.getPort());
        Socket client = new Socket(address.getHostString(), address.getPort());
        logger.log(Level.INFO, "Connected to TCPServer on port {0}", address.getPort());
        lastSocket = client;
        return client;
    }
    public void sendMessage(Socket socket, byte[] data) throws IOException {
        if(socket == null) socket = lastSocket;
        logger.log(Level.FINE, "Sending message to {0}: {1}. \n Parsed as Message", new Object[]{socket, data, new Message(data)});
        socket.getOutputStream().write(data);
    }
    public byte[] receiveMessage(Socket socket) throws IOException {
        if(socket == null) socket = lastSocket;
        final byte[] received = Util.readAllBytesFromNetwork(socket.getInputStream());
        logger.log(Level.FINE, "Received message from {0}: {1}..\n Parsed as Message: {2}",
                new Object[]{socket, received, new Message(received)});
        return received;
    }
    public void closeConnection(Socket socket) throws IOException {
        if(socket == null) socket = lastSocket;
        logger.log(Level.INFO, "shutting down socket {0}", socket);
        socket.close();
        if(socket.isClosed()) logger.log(Level.INFO, "Successfully shut down socket {0}", socket);
    }
    public synchronized void submitWork(Message msg) throws InterruptedException {
        tcpMessageQueue.put(msg);
    }
    public void shutdown(){
        this.interrupt();
    }

    @Override
    public Message call() {
        while (!this.isInterrupted()) {
            try {
                if(logger == null){
                    logger = initializeLogging(TCPServer.class.getCanonicalName() + "-on-server-with-TcpPort-" + myAddress.getPort());
                }
                //Now that we have the connection, what to do?
                if(serverType == ServerType.CONNECTOR){
                    //connect to host
//                    if(connectTcpServer == null || connectTcpServer.isClosed() || connectTcpServer.getLocalPort() != connectionAddress.getPort())
                        connectTcpServer = connectTcpServer(connectionAddress);
                    //send over our request:
                    sendMessage(connectTcpServer, tcpMessageQueue.take().getNetworkPayload());
                    //response:
                    byte[] response = receiveMessage(connectTcpServer);
                    logger.log(Level.FINE, "Received response: {0}. Parsed as Message: {1}", new Object[]{response, new Message(response)});
                    //close the connection. TODO: make general-purpose?
                    closeConnection(connectTcpServer);
                    return new Message(response);
                }
                else{
                    //I am a client
                    logger.log(Level.FINE, "Initial startup. Trying to start server: {0}", connectionAddress);
                    if(socket == null || socket.isClosed() || socket.getLocalPort() != connectionAddress.getPort())
                        socket = startTcpServer(connectionAddress);
                    logger.log(Level.INFO, "started server on socket: {0}", socket);
                    //get the request:
                    byte[] request = receiveMessage(socket);
                    Message msg = new Message(request);
                    logger.log(Level.FINE, "Received from {0} message: {1}", new Object[]{socket, msg});
                    //need to add the message to the queue, and get back a response:
                    //todo: Since at this point, everything is synchronous anyway, might as well create a synchronous
                    // method that return the result.
                    String result = "";
                    if(serverType == ServerType.WORKER){
                        result = javaRunnerFollower.processItem(msg);
                    } else if (serverType == ServerType.SCHEDULER){
                        //this will create a new TCP connection to a worker. Eventually this worker will get a result,
                        //and return it here
                        InetSocketAddress nextFollowerAddress = scheduler.getTCPAddressOfNextServer(msg);
                        TCPServer tcpServer = new TCPServer(this.server, nextFollowerAddress, ServerType.CONNECTOR, msg);
                        logger.log(Level.FINE, "Sent request to {0}", nextFollowerAddress);
                        Message response = tcpServer.call();
                        this.sendMessage(socket, response.getNetworkPayload());
                        //closeConnection(server);
//                        return response;
                    }
                    logger.log(Level.FINE, "Received result: {0}", result);
                    Message completedWork = new Message(Message.MessageType.COMPLETED_WORK,
                            result.getBytes(StandardCharsets.UTF_8),this.myAddress.getHostString(), this.myAddress.getPort(),
                            socket.getInetAddress().getHostName(), socket.getPort(), msg.getRequestID());
                    sendMessage(socket, completedWork.getNetworkPayload());
                    //return completedWork;
                }

            } catch (IOException | InterruptedException e) {
                this.logger.log(Level.WARNING,"Received IO exception in main loop. Exiting...");
                interrupt();
                break;
            }
        }
        this.logger.log(Level.SEVERE,"Exiting TCPServer.run()");
        return null;
    }
}
