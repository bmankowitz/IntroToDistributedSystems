package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.SimpleServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GatewayServer implements SimpleServer, LoggingServer{
    static ZooKeeperPeerServerImpl gateway;
    static final HashMap<Long, String> requestsToSend = new HashMap<>(); //requestId -> requestString
    static final HashMap<Long, String> incompleteRequests = new HashMap<>(); //requestId -> requestString
    static Logger log;
    HttpServer server;

    public static class JavaRunnerHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            StringBuilder response;
            Message responseMessage = null;
            log.info("Visitor to context: /compileandrun using " + httpExchange.getRequestMethod());
            if(httpExchange.getRequestHeaders().get("Content-type") == null ||
                    !httpExchange.getRequestHeaders().get("Content-type").get(0).equals("text/x-java-source")){

                response = new StringBuilder();
                log.warning("Response: 400. Error bad content type. Needs to be text/x-java-source, not " +
                        httpExchange.getRequestHeaders().get("Content-type"));
                httpExchange.sendResponseHeaders(400, 0);
            }
            else {
                //This means the headers are now valid. Need to get the actual request
                InputStream is = httpExchange.getRequestBody();
                byte[] request = is.readAllBytes();
                is.close();

                String requestString = new String(request);
                log.info("Received the following request (code to compile):" + requestString);

                //valid request. Enqueue
                long requestId = RoundRobinLeader.requestIDGenerator.getAndIncrement();
                incompleteRequests.put(requestId, requestString);
                requestsToSend.put(requestId, requestString);
                log.log(Level.INFO, "added new request {0}. incompleteRequests: {1}, requestsToSend: {2}",
                        new Object[]{requestId, incompleteRequests, requestsToSend});
                //Now to run through the leader:
                responseMessage = sendNextRequestAndFormat(httpExchange);
                //if we get here, the code compiled and gave a result:
                log.info("ResponseCode: 200. Code compiled successfully and returned.");
                //double check that the leader is still alive:
                while(gateway.isPeerDead(gateway.getLeaderAddress())){
                    //todo check this
                    log.log(Level.WARNING, "Leader is marked failed. Discarding response and starting again.");
                    responseMessage = sendNextRequestAndFormat(httpExchange);
                    //if(response == null) return;
                }
                response = new StringBuilder(new String(responseMessage.getMessageContents()));
                httpExchange.sendResponseHeaders(200, response.length());
            }
            //if we got this far, the request has been completed:
            incompleteRequests.remove(responseMessage.getRequestID());
            //Sending back the result
            OutputStream os = httpExchange.getResponseBody();
            os.write(response.toString().getBytes());
            os.close();
        }

        private Message sendNextRequestAndFormat(HttpExchange httpExchange) throws IOException {
            byte[] result = waitUntilLeaderReadyAndSendRequest();
            return new Message(result);
        }

        private static byte[] waitUntilLeaderReadyAndSendRequest() {
            while (gateway.getLeaderAddress() == null) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                log.log(Level.WARNING, "The leader is not available or does not exist. Waiting ...");
            }
            byte[] returnValue = null;
            while (returnValue == null) {
                //now that the leader is back online (or never left), enqueue the messages that were sent to the old leader and lost.
                incompleteRequests.forEach(requestsToSend::putIfAbsent);
                //sending the first request
                Long minID = requestsToSend.keySet().stream().min(Long::compareTo).get();
                String minIdRequest = requestsToSend.remove(minID);
                //todo: change log level
                log.log(Level.WARNING, "Leader is available. Sending out request {0}. incompleteRequests: {1}, requestsToSend: {2}",
                        new Object[]{minID, incompleteRequests, requestsToSend});

                int leaderPort = gateway.getLeaderAddress().getPort();
                returnValue = sendMessage(minID, minIdRequest, leaderPort, gateway);
            }
            return returnValue;
        }
        private static byte[] sendMessage(Long requestID, String code, int leaderPort, ZooKeeperPeerServerImpl gatewayServer) {
            try {
                Message msg = new Message(Message.MessageType.WORK, code.getBytes(),
                        gatewayServer.getAddress().getHostString(),
                        gatewayServer.getAddress().getPort(), "localhost", leaderPort, requestID);
                Thread.sleep(500);
                byte[] response = null;
                Socket lastSocket = null;
                    InetSocketAddress connectionAddress = new InetSocketAddress("localhost", leaderPort + 2);
                    TCPServer tcpGatewayServer = new TCPServer(gatewayServer, connectionAddress, TCPServer.ServerType.CONNECTOR, null);
                    lastSocket = tcpGatewayServer.connectTcpServer(connectionAddress);
                    tcpGatewayServer.sendMessage(lastSocket, msg.getNetworkPayload());
                    response = tcpGatewayServer.receiveMessage(lastSocket);
                    tcpGatewayServer.closeConnection(lastSocket);

                return response;
            } catch(InterruptedException | IOException e){
                log.warning(edu.yu.cs.com3800.Util.getStackTrace(e));
                //returning null as a sentinel value that something went wrong. This is checked by the caller
                return null;
            }
        }
    }


    public GatewayServer(int port, ZooKeeperPeerServerImpl gatewayPeer) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/compileandrun", new JavaRunnerHandler());
        Executor executor = Executors.newFixedThreadPool(4);
        server.setExecutor(executor);
        GatewayServer.gateway = gatewayPeer;
        log = gateway.initializeLogging(this.getClass().getCanonicalName());
        log.info("Created gateway server on port " +port);
    }

    /**
     * start the server
     */
    @Override
    public void start() {
        server.start();
        log.info("Ready for connections");
    }

    /**
     * stop the server
     */
    @Override
    public void stop() {
        log.info("Stopping server");
        server.stop(0);

        log.info("Server stopped");
    }

    public static void main(String[] args) {
        int port = 9000;
        if(args.length >0) { port = Integer.parseInt(args[0]);}
        SimpleServer myserver = null;
        try {
            myserver = new GatewayServer(port, gateway);
            myserver.start();
        } catch(Exception e) {
            System.err.println(e.getMessage());
            assert myserver != null;
            myserver.stop();
        }
    }
}
