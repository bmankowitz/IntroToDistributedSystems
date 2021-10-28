package edu.yu.cs.com3800.stage2;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;


public class ZooKeeperPeerServerImpl extends Thread implements ZooKeeperPeerServer{
    private final InetSocketAddress myAddress;
    private final int myPort;
    private ServerState state;
    private volatile boolean shutdown;
    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
    private Long id;
    private long peerEpoch;
    private volatile Vote currentLeader;
    private Map<Long,InetSocketAddress> peerIDtoAddress;

    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;

    /**
     * @param myPort port
     * @param peerEpoch This appears to simply be the starting epoch, though unclear why it is "peer" epoch instead of epoch
     * @param id Each server has a (hopefully) unique id to differentiate itself from other servers
     * @param peerIDtoAddress Mapping from peer server id to peer server socket address
     */
    public ZooKeeperPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long,InetSocketAddress> peerIDtoAddress){
        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();
        this.myAddress = new InetSocketAddress("localhost",myPort);
        this.myPort = myPort;
        this.peerIDtoAddress = peerIDtoAddress;
        this.id = id;
        this.peerEpoch = peerEpoch;
        //code here...
        //TODO: what is the initial states:?
        state = ServerState.LOOKING;
        currentLeader = new Vote(id, peerEpoch);
    }

    @Override
    public void run() {
        try{
            //step 1: create and run thread that sends broadcast messages
            senderWorker = new UDPMessageSender(this.outgoingMessages,this.myPort);
            senderWorker.start();
            //step 2: create and run thread that listens for messages sent to this server
            receiverWorker = new UDPMessageReceiver(this.incomingMessages,this.myAddress,this.myPort,null);
            receiverWorker.start();
        }catch(IOException e){
            e.printStackTrace();
            return;
        }
        //step 3: process received messages
        while(true) {
            try {
                Message msg = this.incomingMessages.take();
                System.out.println("@" + myAddress.getPort() + ": RECEIVED message from client at " + msg.getSenderHost() +
                        ":" + msg.getSenderPort() + ". Message: " + new String(msg.getMessageContents()));
            }
            catch (Exception e) {
                e.printStackTrace();
                return;
            }
        }
    }

    public InetSocketAddress getMyAddress() {
        return getAddress();
    }


    @Override
    public void shutdown(){
        this.shutdown = true;
        this.senderWorker.shutdown();
        this.receiverWorker.shutdown();
    }

    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        currentLeader = v;
    }

    @Override
    public Vote getCurrentLeader() {
        return currentLeader;
    }

    @Override
    public void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {
        //based off of sendBroadcast:
        Message msg = new Message(type, messageContents, myAddress.getHostString(), myPort, target.getHostString(), target.getPort());
        this.outgoingMessages.offer(msg);
    }

    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        for(InetSocketAddress peer : peerIDtoAddress.values()) {
            //no need to send messages to myself
            if(peer.equals(myAddress)) continue;
            Message msg = new Message(type, messageContents,this.myAddress.getHostString(),this.myPort,peer.getHostString(),peer.getPort());
            this.outgoingMessages.offer(msg);
        }
    }

    /**
     * @return The current ServerState of THIS server, not of any peer
     */
    @Override
    public ServerState getPeerState() {
        return state;
    }

    /**
     * @param newState The state to set THIS server to, should not affect peers
     */
    @Override
    public void setPeerState(ServerState newState) {
        state = newState;
    }

    @Override
    public Long getServerId() {
        return id;
    }

    @Override
    public long getPeerEpoch() {
        return peerEpoch;
    }

    /**
     * @return returns the address of the current server
     */
    @Override
    public InetSocketAddress getAddress() {
        return myAddress;
    }

    @Override
    public int getUdpPort() {
        return myAddress.getPort();
    }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        return peerIDtoAddress.get(peerId);
    }

    /**
     * For stage 2, this method returns the size of the peerIDtoAddress map. At this stage,
     * there is no capability to dynamically add or remove peers, and peers are assumed to
     * always be alive
     * @return
     */
    @Override
    public int getQuorumSize() {
        return peerIDtoAddress.size();
    }
}
