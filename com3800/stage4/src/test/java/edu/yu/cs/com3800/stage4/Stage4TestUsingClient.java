package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class Stage4TestUsingClient {

    private final String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
    private HashMap<Long, InetSocketAddress> peerIDtoAddress;
    private ArrayList<ZooKeeperPeerServerImpl> servers;
    private final int myPort = 9999;
    private final InetSocketAddress myAddress = new InetSocketAddress("localhost", this.myPort);
    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
    private ZooKeeperPeerServerImpl gs;

    @Before
    public void setUp() throws Exception {
        //step 1: create sender & sending queue
        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();
        int senderPort = 8002;

        //create IDs and addresses
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(3);
        peerIDtoAddress.put(1L, new InetSocketAddress("localhost", 8010));
        peerIDtoAddress.put(2555555L, new InetSocketAddress("localhost", 8020));
        peerIDtoAddress.put(3L, new InetSocketAddress("localhost", 8030));
        peerIDtoAddress.put(4L, new InetSocketAddress("localhost", 8040));
        peerIDtoAddress.put(5L, new InetSocketAddress("localhost", 8050));
        peerIDtoAddress.put(6L, new InetSocketAddress("localhost", 8060));
        peerIDtoAddress.put(7L, new InetSocketAddress("localhost", 8070));
        peerIDtoAddress.put(8L, new InetSocketAddress("localhost", 8080));
        //gateway:
        peerIDtoAddress.put(44L, new InetSocketAddress("localhost", 8000));

        //create servers
        servers = new ArrayList<>(3);
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
            map.remove(entry.getKey());
            //hardcoding 44L as the gateway address:
            ZooKeeperPeerServerImpl server;
            if(entry.getKey() == 44L) server = new GatewayPeerServerImpl(entry.getValue().getPort(), 22, entry.getKey(), map);
            else server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 22, entry.getKey(), map);
            servers.add(server);
            new Thread(server, "Server on port " + server.getMyAddress().getPort()).start();
        }
        //wait for threads elect leader:
        servers.get(1).lookForLeader();
        try {
            gs = servers.stream()
                    .filter(x -> x.getPeerState() == ZooKeeperPeerServer.ServerState.OBSERVER).findFirst().get();
        } catch(Exception e) {
            System.err.println(e.getMessage());
        }
    }

    @After
    public void tearDown() throws Exception {
        servers.forEach(ZooKeeperPeerServerImpl::shutdown);
    }
    //==================================================
    //===============  STAGE 4 TESTS  ==================
    //==================================================


    //================= HTTP =============
    @Test
    public void singleRequestValidCode() throws IOException, InterruptedException {
        String code = "public class Test { public Test(){} public String run(){ return \"hello world!\";}}";
        String[] ret = sendHTTPRequest("text/x-java-source", new HashMap<>(), code, "/compileandrun", "POST", gs.getUdpPort());
        System.out.println("Expected response:\n 200)");
        System.out.println("Actual response:\n "+ ret[1]);
        System.out.println("Expected response:\n contains(\"hello world!\") ");
        System.out.println("Actual response:\n "+ ret[1]);
        assertEquals("200", ret[0]);
        assertTrue(ret[1].contains("hello world!"));
    }

    @Test
    public void singleRequestIncorrectCode() throws InterruptedException, IOException {
        String[] ret = sendHTTPRequest("text/x-java-source", new HashMap<>(), "uncompilable code", "/compileandrun", "POST", gs.getUdpPort());
        System.out.println("Expected response:\n 200)");
        System.out.println("Actual response:\n "+ ret[1]);
        System.out.println("Expected response:\n contains(\"No class name found in code\") ");
        System.out.println("Actual response:\n "+ ret[1]);
        assertEquals("200", ret[0]);
        assertTrue(ret[1].contains("No class name found in code"));
    }

    @Test
    public void electLeaderAndEvaluateMultipleCorrectCode() throws InterruptedException, IOException {
        int iterations = 3;
        //send message to leader
        for (int i = 0; i < iterations; i++) {
            String[] ret = sendHTTPRequest("text/x-java-source", new HashMap<>(), validClass, "/compileandrun", "POST", gs.getUdpPort());
            assertEquals("200", ret[0]);
            assertTrue(ret[1].contains("Hello world!"));
        }
    }
    @Test
    public void electLeaderAndEvaluateMultipleIncorrectCode() throws InterruptedException, IOException {
        int iterations = 3;
        //send message to leader
        for (int i = 0; i < iterations; i++) {
            String[] ret = sendHTTPRequest("text/x-java-source", new HashMap<>(), "validClass", "/compileandrun", "POST", gs.getUdpPort());
            assertEquals("200", ret[0]);
            assertTrue(ret[1].contains("No class name found in code"));
        }
    }
    @Test
    public void electLeaderAndEvaluateActualNodeCountCorrectCode() throws InterruptedException, IOException {
        int iterations = servers.size();
        for (int i = 0; i < iterations; i++) {
            String[] ret = sendHTTPRequest("text/x-java-source", new HashMap<>(), validClass, "/compileandrun", "POST", gs.getUdpPort());
            assertEquals("200", ret[0]);
            assertTrue(ret[1].contains("Hello world!"));
        }
    }
    @Test
    public void electLeaderAndEvaluateActualNodeCountIncorrectCode() throws InterruptedException, IOException {
        int iterations = servers.size();
        for (int i = 0; i < iterations; i++) {
            String[] ret = sendHTTPRequest("text/x-java-source", new HashMap<>(), "validClass", "/compileandrun", "POST", gs.getUdpPort());
            assertEquals("200", ret[0]);
            assertTrue(ret[1].contains("No class name found in code"));
        }
    }

    @Test
    public void electLeaderAndEvaluateMoreThanNodeCountCorrectCode() throws InterruptedException, IOException {
        int iterations = servers.size()*3;
        for (int i = 0; i < iterations; i++) {
            String[] ret = sendHTTPRequest("text/x-java-source", new HashMap<>(), validClass, "/compileandrun", "POST", gs.getUdpPort());
            assertEquals("200", ret[0]);
            assertTrue(ret[1].contains("Hello world!"));
        }
    }

    @Test
    public void electLeaderAndEvaluateMoreThanNodeCountIncorrectCode() throws InterruptedException, IOException {
        int iterations = servers.size()*3;
        for (int i = 0; i < iterations; i++) {
            String[] ret = sendHTTPRequest("text/x-java-source", new HashMap<>(), "validClass", "/compileandrun", "POST", gs.getUdpPort());
            assertEquals("200", ret[0]);
            assertTrue(ret[1].contains("No class name found in code"));
        }
    }






    public String[] sendHTTPRequest(String contentType, Map<String, String> params, String body, String context,
                                    String method, int port) throws IOException {
        HttpURLConnection server = null;
        String response;
        int responseCode;
        try {
            URL url = new URL("http://localhost:" + port + context);
            server = (HttpURLConnection) url.openConnection();
            server.setRequestProperty("Content-type", contentType);
            server.setRequestMethod(method);
            server.setDoOutput(true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] data = body.getBytes(StandardCharsets.UTF_8);
        InputStream errorInputStream = null;
        InputStream normalInputStream = null;
        OutputStream outputStream = null;
        byte[] responseByte = null;
        byte[] errorResponse = null;


        outputStream = server.getOutputStream();
        outputStream.write(data);
        try {
            normalInputStream = server.getInputStream();
            responseByte = normalInputStream.readAllBytes();
            response = new String(responseByte);
            responseCode = server.getResponseCode();
            return new String[]{String.valueOf(responseCode), response};
        } catch (IOException e){
            errorInputStream = server.getErrorStream();
            errorResponse = errorInputStream.readAllBytes();
            response = new String(errorResponse);
            responseCode = server.getResponseCode();
            return new String[]{String.valueOf(responseCode), response};
        }
    }
}