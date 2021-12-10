package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Stage4SmallTests {
    @Test
    public void tcpTest() throws Exception {
        Message msg = new Message(Message.MessageType.WORK, "test".getBytes(StandardCharsets.UTF_8),
                "localhost", 8032, "localhost", 8034);
        Callable<Socket> start = () ->{
            Socket s = TCPServer.startTcpServer(new InetSocketAddress(8022));
            System.out.println(new String(TCPServer.receiveMessage(s)));
            return s;
        };
        Callable<Socket> connect = () -> {
            Socket s = TCPServer.connectTcpServer(new InetSocketAddress(8022));
            TCPServer.sendMessage(s, "hello test2".getBytes(StandardCharsets.UTF_8));
            TCPServer.closeConnection(s);
            return s;
        };
        ExecutorService x = Executors.newCachedThreadPool();
        Future<Socket> f1 = x.submit(start);
        Future<Socket> f2 = x.submit(connect);
        Socket s1 = f1.get();
        Socket s2 = f2.get();
    }
}
