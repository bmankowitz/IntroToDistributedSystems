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
    private Socket outerSocket;
    private Socket innerSocket;
    private ServerSocket serverSocket;

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
        setName(TCPServer.class.getCanonicalName() + "-on-server-with-TcpPort-" + myAddress.getPort());
        if(logger == null) logger = initializeLogging(TCPServer.class.getCanonicalName() + "-on-server-with-TcpPort-" + myAddress.getPort());
        //todo: ensure there is only one JavaRunner/RoundRobinScheduler per server. They should only be created here!
        LinkedBlockingQueue<Message> messageQueue;
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
//        if(lastSocket !=null && lastSocket.getLocalPort()==address.getPort()) return lastSocket;
        logger.log(Level.INFO, "Creating new server socket on port {0}...", address.getPort());
        serverSocket = new ServerSocket(address.getPort());
        logger.log(Level.INFO, "Created new server socket on port {0}. Accepting....", address.getPort());
        Socket client = serverSocket.accept();
        logger.log(Level.INFO, "Accepted connection from {0} on port {1}.", new Object[]{client, address.getPort()});
//        lastSocket = client;
        return client;
    }

    public Socket connectTcpServer(InetSocketAddress address) throws IOException{
//        if(lastSocket !=null && lastSocket.getLocalPort()==address.getPort()) return lastSocket;
        logger.log(Level.INFO, "Connecting to TCPServer on port {0}....", address.getPort());
        Socket client = new Socket(address.getHostString(), address.getPort());
        logger.log(Level.INFO, "Connected to TCPServer on port {0}", address.getPort());
//        lastSocket = client;
        return client;
    }
    public void sendMessage(Socket socket, byte[] data) throws IOException {
//        if(socket == null) socket = lastSocket;
        logger.log(Level.FINE, "Sending message to {0}: {1}. \n Parsed as Message", new Object[]{socket, data, new Message(data)});
        socket.getOutputStream().write(data);
    }
    public byte[] receiveMessage(Socket socket) throws IOException {
//        if(socket == null) socket = lastSocket;
        final byte[] received = Util.readAllBytesFromNetwork(socket.getInputStream());
        logger.log(Level.FINE, "Received message from {0}: {1}..\n Parsed as Message: {2}",
                new Object[]{socket, received, new Message(received)});
        return received;
    }
    public void closeConnection(Socket socket) throws IOException {
//        if(socket == null) socket = lastSocket;
        logger.log(Level.INFO, "shutting down socket {0}", socket);
        socket.close();
        //todo: busy-wait is bad
        if(socket.isClosed()) logger.log(Level.INFO, "Successfully shut down socket {0}", socket);
    }
    public synchronized void submitWork(Message msg) throws InterruptedException {
        tcpMessageQueue.put(msg);
    }
    public void shutdown(){
        if(this.javaRunnerFollower != null) javaRunnerFollower.shutdown();
        if(this.scheduler != null) scheduler.shutdown();
        this.interrupt();
    }

    @Override
    public Message call() {
        while (!this.isInterrupted()) {
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
                    //return new Message(response);
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
                    byte[] request = receiveMessage(outerSocket);
                    Message msg = new Message(request);
                    //need to add the message to the queue, and get back a response:
                    //Since at this point, everything is synchronous anyway, might as well create a synchronous
                    //method that returns the result.
                    String result = "";
                    if(serverType == ServerType.WORKER){
                        //this is a worker. Assign the value and return it later
                        result = javaRunnerFollower.processItem(msg);
                    } else if (serverType == ServerType.SCHEDULER){
                        //This is the RoundRobinLeader. This will connect to a specified worker, which will accept,
                        //process the data, and send back the result. The scheduler is required to close the connection.
                        InetSocketAddress nextFollowerAddress = scheduler.getTCPAddressOfNextServer(msg);
                        innerSocket = connectTcpServer(nextFollowerAddress);
                        sendMessage(innerSocket, msg.getNetworkPayload());
                        Message response = new Message(receiveMessage(innerSocket));
                        this.sendMessage(outerSocket, response.getNetworkPayload());
                    }
                    logger.log(Level.FINE, "Received result: {0}", result);
                    Message completedWork = new Message(Message.MessageType.COMPLETED_WORK,
                            result.getBytes(StandardCharsets.UTF_8),this.myAddress.getHostString(), this.myAddress.getPort(),
                            outerSocket.getInetAddress().getHostName(), outerSocket.getPort(), msg.getRequestID());
                    sendMessage(outerSocket, completedWork.getNetworkPayload());
                    //return completedWork;
                }

            } catch (IOException e) {
                this.logger.log(Level.WARNING,"Received IO exception in main loop. Trying again...");
                e.printStackTrace();
            } catch (InterruptedException e) {
                this.logger.log(Level.WARNING,"Received InterruptedException in main loop.");
                interrupt();
                break;
            }
        }

        this.logger.log(Level.SEVERE,"Exiting TCPServer.run()");
        return null;
    }
}
