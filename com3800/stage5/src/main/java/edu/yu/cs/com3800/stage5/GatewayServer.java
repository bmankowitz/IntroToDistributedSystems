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
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GatewayServer implements SimpleServer, LoggingServer{
    static ZooKeeperPeerServerImpl gateway;
    static final LinkedList<String> queuedRequests = new LinkedList<>();
    static Logger log;
    HttpServer server;

    public static class JavaRunnerHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            StringBuilder response;
            log.info("Visitor to context: /compileandrun using " + httpExchange.getRequestMethod());
            if(httpExchange.getRequestHeaders().get("Content-type") == null ||
                    !httpExchange.getRequestHeaders().get("Content-type").get(0).equals("text/x-java-source")){

                response = new StringBuilder();
                log.warning("Response: 400. Error bad content type. Needs to be text/x-java-source, not " +
                        httpExchange.getRequestHeaders().get("Content-type"));
                httpExchange.sendResponseHeaders(400, 0);
            }
            else {
                //This means the headers are now valid. Need to create a new InputStream to pass to the JavaRunner.
                //Can't use the same one otherwise it might get clobbered.
                InputStream is = httpExchange.getRequestBody();
                byte[] request = is.readAllBytes();
                is.close();

                String requestString = new String(request);
                log.info("Received the following request (code to compile):" + requestString);

                //valid request. Enqueue
                queuedRequests.add(requestString);
                //Now to run through the leader:
                response = sendNextRequestAndFormat(httpExchange);
                if (response == null) return;
                //if we get here, the code compiled and gave a result:
                log.info("ResponseCode: 200. Code compiled successfully and returned.");
                //double check that the leader is still alive:
                if(gateway.isPeerDead(gateway.getLeaderAddress())){
                    //todo check this
                    log.log(Level.WARNING, "Leader is marked failed. Discarding response and starting again.");
                    queuedRequests.addFirst(requestString);
                    response = sendNextRequestAndFormat(httpExchange);
                    if(response == null) return;
                }
                httpExchange.sendResponseHeaders(200, response.length());
            }
            //Sending back the result
            OutputStream os = httpExchange.getResponseBody();
            os.write(response.toString().getBytes());
            os.close();
        }

        private StringBuilder sendNextRequestAndFormat(HttpExchange httpExchange) throws IOException {
            StringBuilder response;
            try {
                String nextRequest = waitUntilLeaderReadyAndGetRequest();
                int leaderPort = gateway.getLeaderAddress().getPort();
                Message msg = new Message(sendMessage(nextRequest, leaderPort, gateway));
                response = new StringBuilder(new String(msg.getMessageContents()));
            } catch (Exception e) {
                //There was some sort of exception. Need to create stack trace:
                response = new StringBuilder();
                response.append(e.getMessage());
                response.append("\n");
                response.append(edu.yu.cs.com3800.Util.getStackTrace(e));
                log.warning("ResponseCode: 400. Code generated the following error(s): " +response);

                //Sending the error back to client:
                httpExchange.sendResponseHeaders(400, response.length());
                OutputStream os = httpExchange.getResponseBody();
                os.write(response.toString().getBytes());
                os.close();
                return null;
            }
            return response;
        }

        private static String waitUntilLeaderReadyAndGetRequest(){
            while(gateway.getLeaderAddress() == null){
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                log.log(Level.WARNING, "The leader is not available or does not exist. Waiting ...");
            }
            return queuedRequests.poll();
        }
        private byte[] sendMessage(String code, int leaderPort, ZooKeeperPeerServerImpl gatewayServer) {
            try {
                Message msg = new Message(Message.MessageType.WORK, code.getBytes(),
                        gatewayServer.getAddress().getHostString(),
                        gatewayServer.getAddress().getPort(), "localhost", leaderPort);
                //if(lastLeaderPort != leaderPort){
                Thread.sleep(500);
                Socket lastSocket;
                InetSocketAddress connectionAddress = new InetSocketAddress("localhost", leaderPort + 2);
                TCPServer tcpGatewayServer = new TCPServer(gatewayServer, connectionAddress, TCPServer.ServerType.CONNECTOR, null);
                lastSocket = tcpGatewayServer.connectTcpServer(connectionAddress);
                tcpGatewayServer.sendMessage(lastSocket, msg.getNetworkPayload());
                byte[] response = tcpGatewayServer.receiveMessage(lastSocket);
                tcpGatewayServer.closeConnection(lastSocket);
                return response;
            } catch(InterruptedException | IOException e){
                log.warning(edu.yu.cs.com3800.Util.getStackTrace(e));
                throw new RuntimeException(e);
            }
        }
    }


    public GatewayServer(int port, ZooKeeperPeerServerImpl gatewayPeer) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/compileandrun", new JavaRunnerHandler());
        server.setExecutor(null);
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
