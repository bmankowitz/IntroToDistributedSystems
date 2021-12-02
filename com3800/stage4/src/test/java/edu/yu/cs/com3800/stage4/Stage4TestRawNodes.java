package edu.yu.cs.com3800.stage4 ;

import edu.yu.cs.com3800.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class Stage4TestRawNodes {

    private final String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
    private HashMap<Long, InetSocketAddress> peerIDtoAddress;
    private ArrayList<ZooKeeperPeerServerImpl> servers;
    private final int myPort = 9999;
    private final InetSocketAddress myAddress = new InetSocketAddress("localhost", this.myPort);
    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
    private UDPMessageSender sender;
    private UDPMessageReceiver receiver;

    @Before
    public void setUp() throws Exception {
        //step 1: create sender & sending queue
        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();
        int senderPort = 8002;
        sender = new UDPMessageSender(this.outgoingMessages, senderPort);
        receiver = new UDPMessageReceiver(this.incomingMessages, myAddress, this.myPort, null);
        Util.startAsDaemon(sender, "Sender thread");
        Util.startAsDaemon(receiver, "Receiver thread");

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
        //wait for threads to start
        try {
            Thread.sleep(500);
        }
        catch (Exception e) {
        }
    }

    @After
    public void tearDown() throws Exception {
        servers.forEach(ZooKeeperPeerServerImpl::shutdown);
        sender.shutdown();
        receiver.shutdown();
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
    public void lowerEpochIsUpdated() throws InterruptedException {
        HashMap<Long, InetSocketAddress> peerIDtoAddress1 = new HashMap<>(3);
        peerIDtoAddress1.put(1L, new InetSocketAddress("localhost", 8110));
        peerIDtoAddress1.put(2L, new InetSocketAddress("localhost", 8120));
        peerIDtoAddress1.put(3L, new InetSocketAddress("localhost", 8130));
        ArrayList<ZooKeeperPeerServerImpl> servers1 = new ArrayList<>();

        //create servers
        for (long i = 1L; i <= peerIDtoAddress1.size(); i++) {
            ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(peerIDtoAddress1.get(i).getPort(), 22 + i,
                    i, peerIDtoAddress1);
            servers1.add(server);
            new Thread(server, "Server on port " + server.getMyAddress().getPort()).start();
        }

        //wait for threads to start
        try {
            Thread.sleep(1000);
        }
        catch (Exception e) {
        }
        Vote v = servers1.get(0).lookForLeader();
        servers1.forEach(server ->{
                Assert.assertEquals(3L, server.getCurrentLeader().getProposedLeaderID());
                Assert.assertEquals(25, server.getPeerEpoch());
        });
    }
    @Test(expected = RuntimeException.class)
    public void singleServerTimesOut() throws InterruptedException {
        HashMap<Long, InetSocketAddress> peerIDtoAddress1 = new HashMap<>(3);
        peerIDtoAddress1.put(1L, new InetSocketAddress("localhost", 8110));
        ArrayList<ZooKeeperPeerServerImpl> servers1 = new ArrayList<>();
        ZooKeeperLeaderElection.maxNotificationInterval = 30;
        //create servers
        for (long i = 1L; i <= peerIDtoAddress1.size(); i++) {
            ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(peerIDtoAddress1.get(i).getPort(), 22 + i,
                    i, peerIDtoAddress1);
            servers1.add(server);
            new Thread(server, "Server on port " + server.getMyAddress().getPort()).start();
        }
        //wait for threads to start
        try {
            Thread.sleep(500);
        }
        catch (Exception e) {
        }
        Vote v = servers1.get(0).lookForLeader();
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
        Thread.sleep(1500);
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
        Thread.sleep(1500);
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
        Thread.sleep(3500);
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
        Thread.sleep(1500);
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
        Thread.sleep(1500);
        for(Message msg : incomingMessages){
            iterations--;
            String resp = new String(msg.getMessageContents());
            Assert.assertTrue(resp.contains("No class name found in code"));
        }
        Assert.assertEquals(0, iterations);
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
        Thread.sleep(1500);
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
    private void sendMessage(String code, int leaderPort) throws InterruptedException {
        Message msg = new Message(Message.MessageType.WORK, code.getBytes(), this.myAddress.getHostString(), this.myPort, "localhost", leaderPort);
        this.outgoingMessages.put(msg);
    }
    private String nextResponse() throws InterruptedException {
        Message msg = this.incomingMessages.take();
        return new String(msg.getMessageContents());
    }
}