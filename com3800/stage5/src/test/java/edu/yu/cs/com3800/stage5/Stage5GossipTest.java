package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.ZooKeeperPeerServer;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import static junit.framework.TestCase.assertEquals;

public class Stage5GossipTest {

    private final String validClass = "package edu.yu.cs.fall2019.com3800.stage1;public class HelloWorld{public String run(){return \"Hello world!\";    }}";
    private HashMap<Long, InetSocketAddress> peerIDtoAddress;
    private ArrayList<ZooKeeperPeerServerImpl> servers;
    private final int myPort = 9999;
    private final InetSocketAddress myAddress = new InetSocketAddress("localhost", this.myPort);
    private ZooKeeperPeerServerImpl gs;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        //step 1: create sender & sending queue
        LinkedBlockingQueue<Message> outgoingMessages = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<Message> incomingMessages = new LinkedBlockingQueue<>();
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
            //hard-coding 44L as the gateway address:
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
    public void tearDown() {
        servers.forEach(ZooKeeperPeerServerImpl::shutdown);
    }

    @Test
    public void gossipTimerIsUpdated() throws IOException, InterruptedException {
        long firstHeartbeat = servers.get(0).gossipHeartbeat.get();
        Thread.sleep(GossipServer.GOSSIP_TIME + GossipServer.GOSSIP_TIME/2L);
        long secondHeartbeat = servers.get(0).gossipHeartbeat.get();
        Assert.assertTrue(secondHeartbeat > firstHeartbeat);
    }
    @Test
    public void nodesDoNotArbitrarilyDie() throws IOException, InterruptedException {
        ConcurrentHashMap<Long, GossipArchive.GossipLine> startingGossip, endingGossip;
        startingGossip = new ConcurrentHashMap<>(servers.get(0).gossipTable);
        Thread.sleep(500000);
        endingGossip = servers.get(0).gossipTable;
        Assert.assertEquals(startingGossip.keySet(), endingGossip.keySet());
        startingGossip.forEach((id, gossipLine) ->{
            Assert.assertTrue(endingGossip.containsKey(id));
            Assert.assertFalse(gossipLine.isFailed());
            Assert.assertFalse(endingGossip.get(id).isFailed());
        });
    }
    @Test
    public void deadNodeDetectedAfterFullCleanup() throws IOException, InterruptedException {
        ConcurrentHashMap<Long, GossipArchive.GossipLine> startingGossip, endingGossip;
        startingGossip = new ConcurrentHashMap<>(servers.get(1).gossipTable);
        ZooKeeperPeerServerImpl serverToShutdown = servers.remove(0);
        long idToShutdown = serverToShutdown.getServerId();
        serverToShutdown.shutdown();
        Thread.sleep(GossipServer.GOSSIP_FAILURE_CLEANUP_TIME * 2L);
        servers.forEach(zooKeeperPeerServer -> {
            //should be null because failed nodes are removed after GOSSIP_FAILURE_CLEANUP_TIME
            Assert.assertNull(zooKeeperPeerServer.gossipTable.get(idToShutdown));
        });
    }
    @Test
    public void deadNodeMarkedBeforeDeleted() throws IOException, InterruptedException {
        ConcurrentHashMap<Long, GossipArchive.GossipLine> startingGossip, endingGossip;
        startingGossip = new ConcurrentHashMap<>(servers.get(1).gossipTable);
        ZooKeeperPeerServerImpl serverToShutdown = servers.remove(0);
        long idToShutdown = serverToShutdown.getServerId();
        serverToShutdown.shutdown();
        Thread.sleep(GossipServer.GOSSIP_FAIL_TIME + 3L * GossipServer.GOSSIP_TIME);
        servers.forEach(zooKeeperPeerServer -> {
            Assert.assertNotNull(zooKeeperPeerServer.gossipTable.get(idToShutdown));
            Assert.assertTrue(zooKeeperPeerServer.isPeerDead(idToShutdown));
            Assert.assertTrue(zooKeeperPeerServer.gossipTable.get(idToShutdown).isFailed());
        });
        Thread.sleep(GossipServer.GOSSIP_FAILURE_CLEANUP_TIME + 3L* GossipServer.GOSSIP_TIME);
        servers.forEach(zooKeeperPeerServer -> {
            Assert.assertNull(zooKeeperPeerServer.gossipTable.get(idToShutdown));
            Assert.assertTrue(zooKeeperPeerServer.isPeerDead(idToShutdown));
        });
    }

    @Test
    public void gossipHttpTest() throws IOException, InterruptedException {
        ZooKeeperPeerServerImpl zs = servers.get(0);
        Thread.sleep(GossipServer.GOSSIP_TIME* 10L);
        String[] response = sendHTTPRequest("", new HashMap<>(), "", "/getgossipinfo", "GET", zs.getUdpPort()+1);
        Assert.assertEquals("200", response[0]);
        servers.forEach(server ->{
            Assert.assertTrue(response[1].contains("server "+server.getServerId()));
        });
        Assert.assertFalse(response[1].contains("failed=true"));
        //TODO: assert log file is created
    }

    @Test
    public void clusterWorksAfterDeadWorkerCleanedUp() throws InterruptedException, IOException {
        //TODO: test all HTTP endpoints (getgossipinfo, getserverstatus)
        ZooKeeperPeerServerImpl serverToShutdown = servers.remove(0);
        long idToShutdown = serverToShutdown.getServerId();
        serverToShutdown.shutdown();
        Thread.sleep(GossipServer.GOSSIP_FAILURE_CLEANUP_TIME * 2L + GossipServer.GOSSIP_TIME);
        int iterations = servers.size() + 2;
        for (int i = 0; i < iterations; i++) {
            System.out.println("Iteration: " + i);
            String[] ret = sendHTTPRequest("text/x-java-source", new HashMap<>(), validClass, "/compileandrun", "POST", gs.getUdpPort());
            assertEquals("200", ret[0]);
            assertEquals("Hello world!", ret[1]);
        }
    }

    @Test
    public void clusterWorksAfterDeadWorkerMarkedNotYetCleaned() throws InterruptedException, IOException {
        ZooKeeperPeerServerImpl serverToShutdown = servers.remove(0);
        long idToShutdown = serverToShutdown.getServerId();
        serverToShutdown.shutdown();
        Thread.sleep(GossipServer.GOSSIP_FAIL_TIME * 2L + GossipServer.GOSSIP_TIME);
        int iterations = servers.size() + 2;
        for (int i = 0; i < iterations; i++) {
            String[] ret = sendHTTPRequest("text/x-java-source", new HashMap<>(), validClass, "/compileandrun", "POST", gs.getUdpPort());
            assertEquals("200", ret[0]);
            assertEquals("Hello world!", ret[1]);
        }
    }
    @Test
    public void clusterWorksAfterWorkerDeadNotYetMarked() throws InterruptedException, IOException {
        //ZooKeeperPeerServerImpl.GOSSIP_TIME = 350;
        ZooKeeperPeerServerImpl serverToShutdown = servers.remove(0);
        long idToShutdown = serverToShutdown.getServerId();
        serverToShutdown.shutdown();
        int iterations = servers.size() + 2;
        for (int i = 0; i < iterations; i++) {
            String[] ret = sendHTTPRequest("text/x-java-source", new HashMap<>(), validClass, "/compileandrun", "POST", gs.getUdpPort());
            assertEquals("200", ret[0]);
            assertEquals("Hello world!", ret[1]);
        }
    }
    @Test
    public void clusterWorksAfterTwoDeadWorkersCleanedUp() throws InterruptedException, IOException {
        //TODO: test all HTTP endpoints (getgossipinfo, getserverstatus)
        ArrayList<ZooKeeperPeerServerImpl> serversToShutDown = new ArrayList<>();
        //these are both workers
        serversToShutDown.add(servers.remove(0));
        serversToShutDown.add(servers.remove(5));
        serversToShutDown.forEach(ZooKeeperPeerServerImpl::shutdown);
        Thread.sleep(GossipServer.GOSSIP_FAILURE_CLEANUP_TIME * 2L + GossipServer.GOSSIP_TIME);
        int iterations = servers.size() + 4;
        for (int i = 0; i < iterations; i++) {
            System.out.println("Iteration: " + i);
            String[] ret = sendHTTPRequest("text/x-java-source", new HashMap<>(), validClass, "/compileandrun", "POST", gs.getUdpPort());
            assertEquals("200", ret[0]);
            assertEquals("Hello world!", ret[1]);
        }
    }

    @Test
    public void clusterWorksAfterTwoDeadWorkerMarkedNotYetCleaned() throws InterruptedException, IOException {
        ArrayList<ZooKeeperPeerServerImpl> serversToShutDown = new ArrayList<>();
        //these are both workers
        serversToShutDown.add(servers.remove(0));
        serversToShutDown.add(servers.remove(5));
        serversToShutDown.forEach(ZooKeeperPeerServerImpl::shutdown);
        Thread.sleep(GossipServer.GOSSIP_FAIL_TIME * 2L + GossipServer.GOSSIP_TIME);
        int iterations = servers.size() + 4;
        for (int i = 0; i < iterations; i++) {
            System.out.println("Iteration: " + i);
            String[] ret = sendHTTPRequest("text/x-java-source", new HashMap<>(), validClass, "/compileandrun", "POST", gs.getUdpPort());
            assertEquals("200", ret[0]);
            assertEquals("Hello world!", ret[1]);
        }
    }
    @Test
    public void clusterWorksAfterTwoWorkersDeadNotYetMarked() throws InterruptedException, IOException {
        ArrayList<ZooKeeperPeerServerImpl> serversToShutDown = new ArrayList<>();
        //these are both workers
        serversToShutDown.add(servers.remove(0));
        serversToShutDown.add(servers.remove(5));
        serversToShutDown.forEach(ZooKeeperPeerServerImpl::shutdown);
        int iterations = servers.size() + 4;
        for (int i = 0; i < iterations; i++) {
            System.out.println("Iteration: " + i);
            String[] ret = sendHTTPRequest("text/x-java-source", new HashMap<>(), validClass, "/compileandrun", "POST", gs.getUdpPort());
            assertEquals("200", ret[0]);
            assertEquals("Hello world!", ret[1]);
        }
    }
    @Test
    public void clusterWorksAfterDeadLeaderCleanedUp() throws InterruptedException, IOException {
        //TODO: test all HTTP endpoints (getgossipinfo, getserverstatus)
        ZooKeeperPeerServerImpl serverToShutdown = servers.remove(3);
        long idToShutdown = serverToShutdown.getServerId();
        serverToShutdown.shutdown();
        Thread.sleep(GossipServer.GOSSIP_FAILURE_CLEANUP_TIME * 2L + GossipServer.GOSSIP_TIME);
        int iterations = servers.size() + 2;
        for (int i = 0; i < iterations; i++) {
            System.out.println("Iteration: " + i);
            String[] ret = sendHTTPRequest("text/x-java-source", new HashMap<>(), validClass, "/compileandrun", "POST", gs.getUdpPort());
            assertEquals("200", ret[0]);
            assertEquals("Hello world!", ret[1]);
        }
    }

    @Test
    public void clusterWorksAfterDeadLeaderMarkedNotYetCleaned() throws InterruptedException, IOException {
        ZooKeeperPeerServerImpl serverToShutdown = servers.remove(3);
        long idToShutdown = serverToShutdown.getServerId();
        serverToShutdown.shutdown();
        Thread.sleep(GossipServer.GOSSIP_FAIL_TIME * 2L + GossipServer.GOSSIP_TIME);
        int iterations = servers.size() + 2;
        for (int i = 0; i < iterations; i++) {
            String[] ret = sendHTTPRequest("text/x-java-source", new HashMap<>(), validClass, "/compileandrun", "POST", gs.getUdpPort());
            assertEquals("200", ret[0]);
            assertEquals("Hello world!", ret[1]);
        }
    }
    @Test
    public void clusterWorksAfterLeaderDeadNotYetMarked() throws InterruptedException, IOException {
        //ZooKeeperPeerServerImpl.GOSSIP_TIME = 350;
        ZooKeeperPeerServerImpl serverToShutdown = servers.remove(3);
        long idToShutdown = serverToShutdown.getServerId();
        serverToShutdown.shutdown();
        int iterations = servers.size() + 2;
        for (int i = 0; i < iterations; i++) {
            String[] ret = sendHTTPRequest("text/x-java-source", new HashMap<>(), validClass, "/compileandrun", "POST", gs.getUdpPort());
            assertEquals("200", ret[0]);
            assertEquals("Hello world!", ret[1]);
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