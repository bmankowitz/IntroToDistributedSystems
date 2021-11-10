package edu.yu.cs.com3800.stage2;

import edu.yu.cs.com3800.ElectionNotification;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Vote;
import edu.yu.cs.com3800.ZooKeeperPeerServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Stage2Test {

    private HashMap<Long, InetSocketAddress> peerIDtoAddress;
    private ArrayList<ZooKeeperPeerServerImpl> servers;

    @Before
    public void setUp() throws Exception {
        //create IDs and addresses
        peerIDtoAddress = new HashMap<>(3);
        peerIDtoAddress.put(1L, new InetSocketAddress("localhost", 8010));
        peerIDtoAddress.put(2L, new InetSocketAddress("localhost", 8020));
        peerIDtoAddress.put(3L, new InetSocketAddress("localhost", 8030));
//        peerIDtoAddress.put(4L, new InetSocketAddress("localhost", 8040));
//        peerIDtoAddress.put(5L, new InetSocketAddress("localhost", 8050));
//        peerIDtoAddress.put(6L, new InetSocketAddress("localhost", 8060));
//        peerIDtoAddress.put(7L, new InetSocketAddress("localhost", 8070));
//        peerIDtoAddress.put(8L, new InetSocketAddress("localhost", 8080));

        //create servers
        servers = new ArrayList<>(3);
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
            map.remove(entry.getKey());
            ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map);
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
//        servers.get(1);
    }
    @Test
    public void sendMessage() {
        servers.get(0).sendMessage(Message.MessageType.WORK, "test".getBytes(StandardCharsets.UTF_8),
                servers.get(1).getMyAddress());
//        servers.get(1);
    }
    @Test
    public void sendElectionMessage() {
        ElectionNotification electionNotification =
                new ElectionNotification(123, ZooKeeperPeerServer.ServerState.LOOKING,
                        21,-433);
//        servers.get(0).sendMessage(Message.MessageType.WORK, new Byte[](electionNotification),
//                servers.get(1).getMyAddress());
//        servers.get(1);
    }
}