package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.Map.Entry;

public class Stage4TestRawNodes {

    private final String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
    private HashMap<Long, InetSocketAddress> peerIDtoAddress;
    private ArrayList<ZooKeeperPeerServerImpl> servers;
    private final int myPort = 9999;
    private TCPServer tcpGatewayServer;
//    private Socket lastSocket = null;
    private final int lastLeaderPort = -1;
    private final InetSocketAddress myAddress = new InetSocketAddress("localhost", this.myPort);
    private LinkedBlockingQueue<Message> incomingMessages;
    private final ExecutorService executorService = Executors.newFixedThreadPool(8);

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        incomingMessages = new LinkedBlockingQueue<>();
        ZooKeeperPeerServerImpl gatewayPeerServer = new GatewayPeerServerImpl(myPort, 224, 22L, new HashMap<>());
        tcpGatewayServer = new TCPServer(gatewayPeerServer, null, TCPServer.ServerType.CONNECTOR, null);
        //create IDs and addresses
        peerIDtoAddress = new HashMap<>();
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
        servers = new ArrayList<>();
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
            map.remove(entry.getKey());
            //hard-coding 44L as the gateway address:
            ZooKeeperPeerServerImpl server;
            if(entry.getKey() == 44L) server = new GatewayPeerServerImpl(entry.getValue().getPort(), 22, entry.getKey(), map);
            else server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 22, entry.getKey(), map);
            servers.add(server);
            new Thread(server, "Server on port " + server.getMyAddress().getPort()).start();
        }

        //wait for threads to start
        try {
            Thread.sleep(500);
        }
        catch (Exception ignored) {
        }
    }

    @After
    public void tearDown() throws Exception {
        servers.forEach(ZooKeeperPeerServerImpl::shutdown);
        peerIDtoAddress.clear();
        servers.clear();
        Thread.sleep(5000);

    }
    //==================================================
    //===============  STAGE 4 TESTS  ==================
    //==================================================

    @Test
    public void sendBroadcast() {
        servers.get(0).sendBroadcast(Message.MessageType.WORK, "test".getBytes(StandardCharsets.UTF_8));
    }
    @Test
    public void sendMessage() {
        servers.get(0).sendMessage(Message.MessageType.WORK, "test".getBytes(StandardCharsets.UTF_8),
                servers.get(1).getMyAddress());
//        servers.get(1);
    }

    @Test
    public void sendElectionMessage() {
        ElectionNotification elec = new ElectionNotification(123, ZooKeeperPeerServer.ServerState.FOLLOWING, -234, 43);
        byte[] elecBytes = ZooKeeperPeerServerImpl.buildMsgContent(elec);
        Message msg = new Message(Message.MessageType.ELECTION, elecBytes, "localhost", 23, "localhost", 25);
        servers.get(0).sendMessage(Message.MessageType.ELECTION, elecBytes,
                servers.get(1).getMyAddress());
        Assert.assertEquals(elec, ZooKeeperPeerServerImpl.getNotificationFromMessage(msg));
    }
    @Test
    public void leaderElectionSuccess() throws Exception {
        Vote v = servers.get(0).lookForLeader();
        servers.forEach(server ->{
                    System.out.println("Checking server id "+ server.getServerId());
                    Assert.assertEquals(2555555L, server.getCurrentLeader().getProposedLeaderID());
                });
        //done.
    }
    @Test
    public void lowerEpochIsUpdated() {
        HashMap<Long, InetSocketAddress> peerIDtoAddress1 = new HashMap<>(3);
        peerIDtoAddress1.put(100010L, new InetSocketAddress("localhost", 8110));
        peerIDtoAddress1.put(100020L, new InetSocketAddress("localhost", 8120));
        peerIDtoAddress1.put(100030L, new InetSocketAddress("localhost", 8130));
        peerIDtoAddress1.put(100040L, new InetSocketAddress("localhost", 8140));
        peerIDtoAddress1.put(100050L, new InetSocketAddress("localhost", 8150));
        ArrayList<ZooKeeperPeerServerImpl> servers1 = new ArrayList<>();

        //create servers
        for(Entry<Long, InetSocketAddress> entry : peerIDtoAddress1.entrySet()){
            ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), entry.getKey(),
                    entry.getKey(), peerIDtoAddress1);
            servers1.add(server);
            new Thread(server, "Server on port " + server.getMyAddress().getPort()).start();
        }

        //wait for threads to start
        try {
            Thread.sleep(1000);
        }
        catch (Exception ignored) {
        }
        //Vote v = servers1.get(0).lookForLeader();
        servers1.forEach(server ->{
                Assert.assertEquals("Proposed Leader ID Mismatch", 100050L, server.getCurrentLeader().getProposedLeaderID());
                Assert.assertEquals("Proposed Leader Epoch Mismatch",100050L, server.getPeerEpoch());
        });
    }

    @Test
    public void electLeaderAndEvaluateSingleIncorrectCode() throws InterruptedException {
        Vote v = servers.get(0).lookForLeader();
        //send message to leader
        sendMessage("uncompilable code", servers.get(0).getLeaderAddress().getPort());
        Assert.assertTrue(nextResponse().contains("No class name found in code"));
    }
    @Test
    public void electLeaderAndEvaluateSingleCorrectCode() throws InterruptedException {
        Vote v = servers.get(0).lookForLeader();
        //send message to leader
        sendMessage(validClass, servers.get(0).getLeaderAddress().getPort());
        Assert.assertTrue(nextResponse().contains("Hello world!"));
    }
    @Test
    public void electLeaderAndEvaluateMultipleCorrectCode() throws InterruptedException {
        int iterations = 3;
        Vote v = servers.get(0).lookForLeader();
        //send message to leader
        for (int i = 0; i < iterations; i++) {
            sendMessage(validClass, servers.get(0).getLeaderAddress().getPort());
        }
        executorService.shutdown();
        executorService.awaitTermination(2000, TimeUnit.MILLISECONDS);
        for(Message msg : incomingMessages){
            iterations--;
            String resp = new String(msg.getMessageContents());
            Assert.assertTrue(resp.contains("Hello world!"));
        }
    }
    @Test
    public void electLeaderAndEvaluateMultipleIncorrectCode() throws InterruptedException {
        int iterations = 3;
        Vote v = servers.get(0).lookForLeader();
        //send message to leader
        for (int i = 0; i < iterations; i++) {
            sendMessage("validClass", servers.get(0).getLeaderAddress().getPort());
        }
        executorService.shutdown();
        executorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
        for(Message msg : incomingMessages){
            iterations--;
            String resp = new String(msg.getMessageContents());
            Assert.assertTrue(resp.contains("No class name found in code"));
        }
        Assert.assertEquals(0, iterations);
    }
    @Test
    public void electLeaderAndEvaluateActualNodeCountCorrectCode() throws InterruptedException {
        int iterations = servers.size();
        Vote v = servers.get(0).lookForLeader();
        //send message to leader
        for (int i = 0; i < iterations; i++) {
            sendMessage(validClass, servers.get(0).getLeaderAddress().getPort());
        }
        executorService.shutdown();
        executorService.awaitTermination(20000, TimeUnit.MILLISECONDS);
        servers.forEach(ZooKeeperPeerServerImpl::shutdown);
        for(Message msg : incomingMessages){
            iterations--;
            String resp = new String(msg.getMessageContents());
            Assert.assertTrue(resp.contains("Hello world!"));
        }
        Assert.assertEquals(0, iterations);
    }
    @Test
    public void electLeaderAndEvaluateActualNodeCountIncorrectCode() throws InterruptedException {
        int iterations = servers.size();
        Vote v = servers.get(0).lookForLeader();
        //send message to leader
        for (int i = 0; i < iterations; i++) {
            sendMessage("validClass", servers.get(0).getLeaderAddress().getPort());
        }
        executorService.shutdown();
        executorService.awaitTermination(15000, TimeUnit.MILLISECONDS);
        for(Message msg : incomingMessages){
            iterations--;
            String resp = new String(msg.getMessageContents());
            Assert.assertTrue(resp.contains("No class name found in code"));
        }
        Assert.assertEquals(0, iterations);
    }

    @Test
    public void electLeaderAndEvaluateMoreThanNodeCountCorrectCode() throws InterruptedException {
        int iterations = servers.size()*3;
        Vote v = servers.get(0).lookForLeader();
        //send message to leader
        for (int i = 0; i < iterations; i++) {
            sendMessage("validClass", servers.get(0).getLeaderAddress().getPort());
        }
        executorService.shutdown();
        executorService.awaitTermination(45000, TimeUnit.MILLISECONDS);
        for(Message msg : incomingMessages){
            iterations--;
            String resp = new String(msg.getMessageContents());
            Assert.assertTrue(resp.contains("No class name found in code"));
            Assert.assertFalse(resp.contains("Request ID: -1"));
        }
        Assert.assertEquals(0, iterations);
    }
    @Test
    public void ensureAsyncFasterThanSync() throws InterruptedException {
        int iterations = servers.size()*2;
        Vote v = servers.get(0).lookForLeader();
        //send message to leader
        long startAsync = System.currentTimeMillis();
        for (int i = 0; i < iterations; i++) {
            sendMessage("validClass", servers.get(0).getLeaderAddress().getPort());
        }
        executorService.shutdown();
        executorService.awaitTermination(45000, TimeUnit.MILLISECONDS);
        long endAsync = System.currentTimeMillis();
        final long startSync = System.currentTimeMillis();
        final long[] endSync = {0};
        new Thread(() -> {
            while((System.currentTimeMillis()- startSync <= 45000 && endSync[0] == 0 )){}
            if(endSync[0] == 0) {
                Assert.fail("Synchronous timed out");
                servers.forEach(ZooKeeperPeerServerImpl::shutdown);
            }
        }).start();
        for (int i = 0; i < iterations; i++) {
            sendMessageSynchronous("validClass", servers.get(0).getLeaderAddress().getPort());
        }
        endSync[0] = System.currentTimeMillis();
        iterations *= 2;
        for(Message msg : incomingMessages){
            iterations--;
            String resp = new String(msg.getMessageContents());
            Assert.assertTrue(resp.contains("No class name found in code"));
            Assert.assertFalse(resp.contains("Request ID: -1"));
        }
        Assert.assertEquals(0, iterations);
        long asyncTime = endAsync - startAsync;
        long syncTime = endSync[0] - startSync;
        Assert.assertTrue(asyncTime < syncTime);
        System.out.println("Async: "+asyncTime);
        System.out.println("Sync: "+syncTime);
    }

    @Test
    public void electLeaderAndEvaluateMoreThanNodeCountIncorrectCode() throws InterruptedException {
        int iterations = servers.size()*3;
        Vote v = servers.get(0).lookForLeader();
        //send message to leader
        for (int i = 0; i < iterations; i++) {
            sendMessage("validClass", servers.get(0).getLeaderAddress().getPort());
        }
        //wait for processing to complete. Note that the requests are asynchronous and nonblocking, but it takes time
        //to evaluate all of them
        executorService.shutdown();
        executorService.awaitTermination(45000, TimeUnit.MILLISECONDS);
        for(Message msg : incomingMessages){
            iterations--;
            String resp = new String(msg.getMessageContents());
            Assert.assertTrue(resp.contains("No class name found in code"));
        }
        Assert.assertEquals(0, iterations);
    }
    @Test
    public void electLeaderAndSendCorrectCodeDirectlyToWorker() throws InterruptedException {
        //Per piazza (https://piazza.com/class/ksjkv7bd6th1jf?cid=114_f1)
        // A message directly to a worker node should return the result, even though in
        // future iterations it should only respond to the leader
        Vote v = servers.get(0).lookForLeader();
        //send message to random server
        sendMessage(validClass, servers.get(4).getUdpPort());
        Assert.assertTrue(nextResponse().contains("Hello world!"));
        servers.forEach(ZooKeeperPeerServerImpl::shutdown);
    }
    @Test
    public void electLeaderAndSendIncorrectCodeDirectlyToWorker() throws InterruptedException {
        //Per piazza (https://piazza.com/class/ksjkv7bd6th1jf?cid=114_f1)
        // A message directly to a worker node should return the result, even though in
        // future iterations it should only respond to the leader
        Vote v = servers.get(0).lookForLeader();
        //send message to random server
        sendMessage("validClass", servers.get(4).getUdpPort());
        Assert.assertTrue(nextResponse().contains("No class name found in code"));
        servers.forEach(ZooKeeperPeerServerImpl::shutdown);
    }
    @Test
    public void electLeaderAndShutdownKillsAllThreads() throws InterruptedException {
        //Per piazza (https://piazza.com/class/ksjkv7bd6th1jf?cid=114_f1)
        // A message directly to a worker node should return the result, even though in
        // future iterations it should only respond to the leader
        Vote v = servers.get(0).lookForLeader();
        //send message to leader
        servers.forEach(ZooKeeperPeerServerImpl::shutdown);
        Thread.sleep(400);
        servers.forEach(server ->
                Assert.assertFalse(server.isAlive()));
    }
    private void sendMessage(String code, int leaderPort) {
        //making this Asynchronous to test better:
        Runnable r1 = () -> {
            try {
                Message msg = new Message(Message.MessageType.WORK, code.getBytes(), this.myAddress.getHostString(), this.myPort, "localhost", leaderPort);
                //if(lastLeaderPort != leaderPort){
                Thread.sleep(500);
                Socket lastSocket;
                TCPServer tcpGatewayServer = this.tcpGatewayServer;
                lastSocket = tcpGatewayServer.connectTcpServer(new InetSocketAddress("localhost", leaderPort + 2));
                //lastLeaderPort = leaderPort;
                //}
                tcpGatewayServer.sendMessage(lastSocket, msg.getNetworkPayload());
                byte[] response = tcpGatewayServer.receiveMessage(lastSocket);
                tcpGatewayServer.closeConnection(lastSocket);
                incomingMessages.put(new Message(response));
            } catch(InterruptedException | IOException e){
                throw new RuntimeException();
            }
        };
        executorService.submit(r1);
    }
    @SuppressWarnings("SameParameterValue")
    private void sendMessageSynchronous(String code, int leaderPort) {
            try {
                Message msg = new Message(Message.MessageType.WORK, code.getBytes(), this.myAddress.getHostString(), this.myPort, "localhost", leaderPort);
                //if(lastLeaderPort != leaderPort){
                Thread.sleep(500);
                Socket lastSocket;
                TCPServer tcpGatewayServer = this.tcpGatewayServer;
                lastSocket = tcpGatewayServer.connectTcpServer(new InetSocketAddress("localhost", leaderPort + 2));
                //lastLeaderPort = leaderPort;
                //}
                tcpGatewayServer.sendMessage(lastSocket, msg.getNetworkPayload());
                byte[] response = tcpGatewayServer.receiveMessage(lastSocket);
                tcpGatewayServer.closeConnection(lastSocket);
                incomingMessages.put(new Message(response));
            } catch(InterruptedException | IOException e){
                throw new RuntimeException();
            }
    }

    private String nextResponse() throws InterruptedException {
        Message msg = this.incomingMessages.take();
        return new String(msg.getMessageContents());
    }
}