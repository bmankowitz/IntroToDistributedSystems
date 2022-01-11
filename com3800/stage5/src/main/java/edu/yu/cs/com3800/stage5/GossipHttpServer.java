package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.SimpleServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GossipHttpServer implements SimpleServer, LoggingServer {
    HttpServer server;
    ZooKeeperPeerServerImpl hostPeerServer;
    static Logger log;
    class GossipArchiveHttpHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            String response;
            log.info("Visitor to context: /getgossipinfo using " + httpExchange.getRequestMethod());
            //This means the headers are now valid. Need to create a new InputStream to pass to the JavaRunner.
            //Can't use the same one otherwise it might get clobbered.
            InputStream is = httpExchange.getRequestBody();
            byte[] request = is.readAllBytes();
            String requestString = new String(request);
            log.info("Received the following request:" + requestString);
            //Now to get the archive:
            response = hostPeerServer.getGossipArchive();
            log.info("ResponseCode: 200");
            httpExchange.sendResponseHeaders(200, response.length());
            //Sending back the result
            OutputStream os = httpExchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }
    class CurrentServerStatusHttpHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            String response;
            log.info("Visitor to context: /getserverstatus using " + httpExchange.getRequestMethod());
            //This means the headers are now valid. Need to create a new InputStream to pass to the JavaRunner.
            //Can't use the same one otherwise it might get clobbered.
            InputStream is = httpExchange.getRequestBody();
            byte[] request = is.readAllBytes();
            String requestString = new String(request);
            log.info("Received the following request:" + requestString);
            //Now to get the archive:
            response = hostPeerServer.peerIDtoStatus.toString();
            log.info("ResponseCode: 200");
            httpExchange.sendResponseHeaders(200, response.length());
            //Sending back the result
            OutputStream os = httpExchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }

    public GossipHttpServer(ZooKeeperPeerServerImpl hostPeerServer) throws IOException {
        //add 1 to udp port to get http port
        this.hostPeerServer = hostPeerServer;
        log = initializeLogging(this.getClass().getCanonicalName() + "-on-port-" + hostPeerServer.getUdpPort() + 1);
        server = HttpServer.create(new InetSocketAddress(hostPeerServer.getUdpPort()+1), 0);
        server.createContext("/getgossipinfo", new GossipArchiveHttpHandler());
        server.createContext("/getserverstatus", new CurrentServerStatusHttpHandler());
        server.setExecutor(null);
        log.log(Level.INFO, "Created Gossip HTTP Server");
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

}
