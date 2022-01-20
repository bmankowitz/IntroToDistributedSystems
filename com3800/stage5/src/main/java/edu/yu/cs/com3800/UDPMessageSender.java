package edu.yu.cs.com3800;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

@SuppressWarnings("ALL")
public class UDPMessageSender extends Thread implements LoggingServer {
    private LinkedBlockingQueue<Message> outgoingMessages;
    private Logger logger;
    private int serverUdpPort;

    public UDPMessageSender(LinkedBlockingQueue<Message> outgoingMessages, int serverUdpPort) {
        this.outgoingMessages = outgoingMessages;
        setDaemon(true);
        this.serverUdpPort = serverUdpPort;
        setName("UDPMessageSender-port-" + this.serverUdpPort);
    }

    public void shutdown() {
        interrupt();
    }

    @Override
    public void run() {
        while (!this.isInterrupted()) {
            try {
                if(this.logger == null){
                    this.logger = initializeLogging(UDPMessageSender.class.getCanonicalName() + "-on-server-with-udpPort-" + this.serverUdpPort);
                }
                Message messageToSend = this.outgoingMessages.take();
                if (messageToSend != null) {
                    DatagramSocket socket = new DatagramSocket();
                    byte[] payload = messageToSend.getNetworkPayload();
                    DatagramPacket sendPacket = new DatagramPacket(payload, payload.length, new InetSocketAddress(messageToSend.getReceiverHost(), messageToSend.getReceiverPort()));
                    socket.send(sendPacket);
                    socket.close();
                    this.logger.finer("Message sent:\n" + messageToSend.toString());
                }
            }
            catch (IOException e) {
                this.logger.log(Level.WARNING,"failed to send packet", e);
            } catch (InterruptedException e) {
                this.logger.log(Level.SEVERE,"UDPMessageSender was interrupted");
            }
        }
        this.logger.log(Level.SEVERE,"Exiting UDPMessageSender.run()");
    }
}