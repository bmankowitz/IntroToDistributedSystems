package edu.yu.cs.com3800.stage2;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState.*;


public class ZooKeeperPeerServerImpl extends Thread implements ZooKeeperPeerServer, LoggingServer{
    private final InetSocketAddress myAddress;
    private final int myPort;
    private ServerState state;
    private volatile boolean shutdown;
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final Long id;
    private long peerEpoch;
    private volatile Vote currentLeader;
    private final Map<Long,InetSocketAddress> peerIDtoAddress;
    private final Map<Long,ElectionNotification> peerIDtoVote = new HashMap<>();
    private Logger log;
    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;


    public ZooKeeperPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long,InetSocketAddress> peerIDtoAddress) {
        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();
        this.myAddress = new InetSocketAddress("localhost",myPort);
        this.myPort = myPort;
        this.peerIDtoAddress = peerIDtoAddress;
        this.id = id;
        this.peerEpoch = peerEpoch;
        try {
            log = initializeLogging("ZKPeer" + id, false);
        } catch (Exception e) {e.printStackTrace();}
        //code here...
        //TODO: what is the initial states:?
        state = ServerState.LOOKING;
        currentLeader = new Vote(this.id, this.peerEpoch);
    }

    public synchronized Vote lookForLeader() throws InterruptedException {
        //send initial notifications to other peers to get things started
        sendNotifications();
        peerIDtoVote.put(this.id, createElectionNotificationFromVote(currentLeader));
        //Loop, exchanging notifications with other servers until we find a leader
        SEARCH: while (getPeerState() == LOOKING && !shutdown) {
            //Remove next notification from queue, timing out after 2 times the termination time
            ElectionNotification vote = getNotificationFromMessage(incomingMessages.poll(ZooKeeperLeaderElection.maxNotificationInterval, TimeUnit.MILLISECONDS));
            //if no notifications received ....
            if(vote == null){
                // resend notifications to prompt a reply from others ...
                sendNotifications();
                // and implement exponential back-off when notifications not received
                vote = getNotificationFromMessage(incomingMessages.poll(ZooKeeperLeaderElection.maxNotificationInterval * 2L, TimeUnit.MILLISECONDS));
                // if there are still no notification, abort as this is the second time iterating 2*maxNotificationInterval:
                if(vote == null){
                    log.severe("Aborting - did not receive any messages within 2*maxNotificationInterval");
                    throw new RuntimeException("Unable to receive vote within 2*maxNotificationInterval");
                }
            }
            //if/when we get a message and it's from a valid server and for a valid server..
            log.log(Level.WARNING, "evaluating vote {0} from {1}", new Object[]{vote, vote.getSenderID()});
            if(vote.getPeerEpoch() < this.peerEpoch){
                log.log(Level.WARNING, "ignoring vote {0} from {1} -- supplied epoch is less than current epoch {2}",
                        new Object[]{vote, vote.getSenderID(), this.peerEpoch});
                continue SEARCH;
            }
            peerIDtoVote.put(vote.getSenderID(), vote);
            log.log(Level.WARNING, "vote array after inserting/updating vote from id {0}: {1}",
                    new Object[]{vote.getSenderID(), peerIDtoVote});
            //switch on the state of the sender:
            OUTER: switch (vote.getState()) {
                case LOOKING: //if the sender is also looking
                    //if the received message has a vote for a leader which supersedes mine, change my vote and tell all my peers what my new vote is.
                    //keep track of the votes I received and who I received them from.
                    if(supersedesCurrentVote(vote.getSenderID(),vote.getPeerEpoch())){
                        log.log(Level.WARNING, "received vote {0} supersedes current vote: {1}", new Vote[]{vote, currentLeader});
                        setCurrentLeader(createElectionNotificationFromVote(vote));
                        sendBroadcast(Message.MessageType.ELECTION, buildMsgContent(createElectionNotificationFromVote(currentLeader)));
                        log.log(Level.WARNING, "broadcast new vote {0}", currentLeader);
                    }
                    ////if I have enough votes to declare my currently proposed leader as the leader:
                    if(haveEnoughVotes(peerIDtoVote, currentLeader)){
                        //first check if there are any new votes for a higher ranked possible leader before I declare a leader. If so, continue in my election loop
                        log.log(Level.WARNING, "Found enough votes. Double checking");
                        for (Message msg : incomingMessages) {
                            ElectionNotification latestVote = getNotificationFromMessage(msg);
                            if (supersedesCurrentVote(latestVote.getProposedLeaderID(), latestVote.getPeerEpoch())) {
                                //found higher vote. break and go back to while loop:
                                log.log(Level.WARNING, "found higher vote. Restarting loop");
                                break OUTER;
                            }
                        }
                        //If not, set my own state to either LEADING (if I won the election) or FOLLOWING (if someone lese won the election) and exit the election
                        acceptElectionWinner(createElectionNotificationFromVote(currentLeader));
                        break SEARCH;
                    }
                    break;
                case FOLLOWING:
                    //FALLTHROUGH:
                case LEADING: //if the sender is following a leader already or thinks it is the leader

                    //IF: see if the sender's vote allows me to reach a conclusion based on the election epoch that I'm in, i.e. it gives the majority to the vote of the FOLLOWING or LEADING peer whose vote I just received.
                    //if so, accept the election winner.
                    if(haveEnoughVotes(peerIDtoVote, vote)){
                        acceptElectionWinner(vote);
                        //As, once someone declares a winner, we are done. We are not worried about / accounting for misbehaving peers.
                        break SEARCH;
                    }
                    //ELSE: if n is from a LATER election epoch
                    else if (vote.getPeerEpoch() > this.getPeerEpoch()){
                        //IF a quorum from that epoch are voting for the same peer as the vote of the FOLLOWING or LEADING peer whose vote I just received.
                        if(haveEnoughVotes(peerIDtoVote, vote)){
                            //THEN accept their leader, and update my epoch to be their epoch
                            setCurrentLeader(vote); //this sets the epoch
                            acceptElectionWinner(vote);
                        }
                    }
                    //ELSE:
                    else{
                        //keep looping on the election loop.
                        break;
                    }
                    break;
            }
        }
        return currentLeader;
    }

    private void sendNotifications() {
        log.log(Level.FINE, "Sending initial EN notifications from port {0}",this.getUdpPort());
        //send our initial vote to peers. They will reply with their own vote:
        sendBroadcast(Message.MessageType.ELECTION, buildMsgContent(createElectionNotificationFromVote(currentLeader)));
    }

    private Vote acceptElectionWinner(ElectionNotification n) {
        //set my state to either LEADING or FOLLOWING
        //clear out the incoming queue before returning
        log.log(Level.SEVERE, "Elected leader {0}", n);
        setCurrentLeader(n);
        if(this.id == currentLeader.getProposedLeaderID()) setPeerState(LEADING);
        else setPeerState(FOLLOWING);
        incomingMessages.clear();
        return n;
    }

    /*
     * We return true if one of the following three cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     */
    protected boolean supersedesCurrentVote(long newId, long newEpoch) {
        return (newEpoch > this.currentLeader.getPeerEpoch())
                || ((newEpoch == this.currentLeader.getPeerEpoch())
                      && (newId > this.currentLeader.getProposedLeaderID()));
    }
    protected ElectionNotification createElectionNotificationFromVote(Vote vote) {
        return new ElectionNotification(vote.getProposedLeaderID(), this.state, this.id, vote.getPeerEpoch());
    }

    /**
     * Termination predicate. Given a set of votes, determines if have sufficient support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one current vote
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification> votes, Vote proposal) {
        //is the number of votes for the proposal > the size of my peer server???s quorum?
        log.log(Level.WARNING, "Checking support for vote {0}", proposal);
        AtomicInteger voteCount = new AtomicInteger();
        votes.forEach((voterId,electionNotification) -> {
            if(electionNotification.getProposedLeaderID() == proposal.getProposedLeaderID()
                    && electionNotification.getPeerEpoch() == proposal.getPeerEpoch())
                voteCount.getAndIncrement();
        });
        log.log(Level.WARNING, "Proposal {0} has {1} votes. Passed: {2}. Votes: {3}",
                new Object[]{proposal, voteCount.get(),voteCount.get() >= getQuorumSize(), votes});
        return voteCount.get() >= getQuorumSize();
    }

    public static ElectionNotification getNotificationFromMessage(Message received) {
        if(received == null) return null;
        //ElectionNotifications should be exactly 26 bytes:
        byte[] data = received.getMessageContents();
        ByteBuffer msgBytes = ByteBuffer.wrap(data);
        long leader = msgBytes.getLong();
        char stateChar = msgBytes.getChar();
        long senderID = msgBytes.getLong();
        long peerEpoch = msgBytes.getLong();
        return new ElectionNotification(leader, ServerState.getServerState(stateChar), senderID, peerEpoch);
    }

    public static byte[] buildMsgContent(ElectionNotification notification) {
        /*
        *   Order from Message.java:
        *
        * if(getMessageType() == MessageType.ELECTION){
                ByteBuffer msgBytes = ByteBuffer.wrap(getMessageContents());
                long leader = msgBytes.getLong();
                char stateChar = msgBytes.getChar();
                long senderID = msgBytes.getLong();
                long peerEpoch = msgBytes.getLong();

         */
        //The byte array will be a total of 2 bytes (char) + 8 bytes (long) * 3 = 26 bytes
        byte[] array = new byte[26];
        long leader = notification.getProposedLeaderID();
        char stateChar = notification.getState().getChar();
        long senderID = notification.getSenderID();
        long peerEpoch = notification.getPeerEpoch();
        ByteBuffer msgBytes = ByteBuffer.wrap(array);
        msgBytes.putLong(leader);
        msgBytes.putChar(stateChar);
        msgBytes.putLong(senderID);
        msgBytes.putLong(peerEpoch);
        return msgBytes.array();
    }

    @Override
    public void run() {
        try{
            //step 1: create and run thread that sends broadcast messages
            senderWorker = new UDPMessageSender(this.outgoingMessages,this.myPort);
            senderWorker.setDaemon(true);
            senderWorker.start();
            //step 2: create and run thread that listens for messages sent to this server
            receiverWorker = new UDPMessageReceiver(this.incomingMessages,this.myAddress,this.myPort,this);
            receiverWorker.setDaemon(true);
            receiverWorker.start();
        }catch(IOException e){
            e.printStackTrace();
            return;
        }
        //step 3: process received messages
        while(true) {
            try {
                if(lookForLeader() != null) return;
            } catch (Exception e) {
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
    public void setCurrentLeader(Vote v) {
        currentLeader = new Vote(v.getProposedLeaderID(), v.getPeerEpoch());
        this.peerEpoch = v.getPeerEpoch();
        peerIDtoVote.put(this.id, createElectionNotificationFromVote(v));
    }

    @Override
    public Vote getCurrentLeader() {
        return currentLeader;
    }

    @Override
    public void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target)
            throws IllegalArgumentException {
        //based off of sendBroadcast:
        Message msg = new Message(type, messageContents,
                myAddress.getHostString(), myPort, target.getHostString(), target.getPort());
        this.outgoingMessages.offer(msg);
    }

    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        for(InetSocketAddress peer : peerIDtoAddress.values()) {
            //no need to send messages to myself
            if(peer.equals(myAddress)) continue;
            Message msg = new Message(type, messageContents,
                    this.myAddress.getHostString(), this.myPort, peer.getHostString(), peer.getPort());
            this.outgoingMessages.offer(msg);
        }
    }

    @Override
    public ServerState getPeerState() {
        return state;
    }

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
        //if the peer address map doesn't include itself, add 1:
        if(peerIDtoAddress.containsKey(id)) return peerIDtoAddress.size()/2 + 1;
        else return (peerIDtoAddress.size() + 1)/2 +1;
    }
}
