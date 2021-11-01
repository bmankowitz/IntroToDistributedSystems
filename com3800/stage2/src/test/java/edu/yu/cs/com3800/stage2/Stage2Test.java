package edu.yu.cs.com3800.stage2;

import edu.yu.cs.com3800.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Stage2Test {

    private ArrayList<ZooKeeperPeerServerImpl> servers;

    @Before
    public void setUp() throws Exception {
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

        //create servers
        servers = new ArrayList<>(3);
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
            map.remove(entry.getKey());
            ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 22, entry.getKey(), map);
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
    }

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
    public void leaderElectionSuccess() throws InterruptedException {
        Vote v = servers.get(0).lookForLeader();
        servers.forEach(server ->
                Assert.assertEquals(2555555L, server.getCurrentLeader().getProposedLeaderID()));
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
}