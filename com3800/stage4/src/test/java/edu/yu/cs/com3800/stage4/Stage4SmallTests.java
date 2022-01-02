package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.Message;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

@SuppressWarnings({"unchecked", "RedundantThrows"})
public class Stage4SmallTests {

    private HashMap<Long, InetSocketAddress> peerIDtoAddress;
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
        //gateway:
        peerIDtoAddress.put(44L, new InetSocketAddress("localhost", 8000));

        //create servers
        servers = new ArrayList<>();
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
        catch (Exception ignored) {
        }
    }

    @Test
    public void tcpTest() throws Exception {
        ZooKeeperPeerServerImpl peerServer = new ZooKeeperPeerServerImpl(9230, 22, 1L, null);
        TCPServer server = new TCPServer(peerServer, new InetSocketAddress("localhost", 2221), null, null);
        Message msg = new Message(Message.MessageType.WORK, "test".getBytes(StandardCharsets.UTF_8),
                "localhost", 8022, "localhost", 8022);
        Callable<Socket> start = () ->{
            Socket s = server.startTcpServer(new InetSocketAddress(8022));
            String received = new String (server.receiveMessage(s));
            System.out.println(received);
            Assert.assertEquals("hello test2", received);
            return s;
        };
        Callable<Socket> connect = () -> {
            Socket s = server.connectTcpServer(new InetSocketAddress(8022));
            server.sendMessage(s, "hello test2".getBytes(StandardCharsets.UTF_8));
            server.closeConnection(s);
            return s;
        };
        ExecutorService x = Executors.newCachedThreadPool();
        Future<Socket> f1 = x.submit(start);
        Future<Socket> f2 = x.submit(connect);
        Socket s1 = f1.get();
        Socket s2 = f2.get();
        peerServer.shutdown();
    }
    @Test
    public void tcpServerThreadTest() throws InterruptedException, IOException {
        Message msg = new Message(Message.MessageType.WORK, "test".getBytes(StandardCharsets.UTF_8),
                "localhost", 8032, "localhost", 8034);
        Thread.sleep(409);

        TCPServer start = new TCPServer(servers.get(0), servers.get(0).getAddress(), TCPServer.ServerType.CONNECTOR, msg);
        TCPServer connect = new TCPServer(servers.get(1), servers.get(0).getAddress(), TCPServer.ServerType.WORKER, msg);
        ExecutorService executor = Executors.newCachedThreadPool();
        executor.submit((Callable<Message>) start);
        executor.submit((Callable<Message>) connect);
        Thread.sleep(1500);
    }
}
