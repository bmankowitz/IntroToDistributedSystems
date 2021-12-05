package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState.*;


public class ZooKeeperPeerServerImpl extends Thread implements ZooKeeperPeerServer, LoggingServer{
    private final InetSocketAddress myAddress;
    private final int myPort;
    private JavaRunnerFollower javaRunnerFollower;
    private RoundRobinLeader roundRobinLeader;
    private ServerState state;
    private volatile boolean shutdown;
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final LinkedBlockingQueue<Message> javaRunnerWorkItems;
    private final LinkedBlockingQueue<Message> roundRobinWork;
    private final Long id;
    private long peerEpoch;
    private volatile Vote currentLeader;
    public final Map<Long,InetSocketAddress> peerIDtoAddress;
    public final Map<Long,ElectionNotification> peerIDtoVote = new HashMap<>();
    private Logger log;
    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;


    public ZooKeeperPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long,InetSocketAddress> peerIDtoAddress) {
        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();
        this.javaRunnerWorkItems = new LinkedBlockingQueue<Message>();
        this.roundRobinWork = new LinkedBlockingQueue<Message>();
        this.myAddress = new InetSocketAddress("localhost",myPort);
        this.myPort = myPort;
        this.peerIDtoAddress = peerIDtoAddress;
        this.id = id;
        this.peerEpoch = peerEpoch;
        try {
            log = initializeLogging("ZKPeer" + id, false);
        } catch (Exception e) {e.printStackTrace();}
        state = ServerState.LOOKING;
        currentLeader = new Vote(this.id, this.peerEpoch);
    }

    public Map<Long, InetSocketAddress> getPeerIDtoAddress() {
        return peerIDtoAddress;
    }

    public synchronized Vote lookForLeader() throws InterruptedException {
        //send initial notifications to other peers to get things started
        sendNotifications();
        peerIDtoVote.put(this.id, createElectionNotificationFromVote(currentLeader));
        //Loop, exchanging notifications with other servers until we find a leader
        //NOTE: Even if we are an observer, we still need to "participate" in voting to determine the master.
        LeaderSearch: while ((getPeerState() == LOOKING || getPeerState() == OBSERVER) && !shutdown) {
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
            //if/when we get a message
            log.log(Level.INFO, "evaluating vote {0} from {1}", new Object[]{vote, vote.getSenderID()});
            if(vote.getPeerEpoch() < this.peerEpoch){
                log.log(Level.WARNING, "ignoring vote {0} from {1} -- supplied epoch is less than current epoch {2}",
                        new Object[]{vote, vote.getSenderID(), this.peerEpoch});
                continue LeaderSearch;
            }
            peerIDtoVote.put(vote.getSenderID(), vote);
            log.log(Level.INFO, "vote array after inserting/updating vote from id {0}: {1}",
                    new Object[]{vote.getSenderID(), peerIDtoVote});
            //switch on the state of the sender:
            MessageProcessing: switch (vote.getState()) {
                case LOOKING: //if the sender is also looking
                    //if the received message has a vote for a leader which supersedes mine, change my vote and tell all my peers what my new vote is.
                    //keep track of the votes I received and who I received them from.
                    //TODO: implement general case: If the vote is for an observer, that vote should be superseded
                    //until then, use the specific case of I am an observer and voting for myself
                    if(supersedesCurrentVote(vote.getProposedLeaderID(),vote.getPeerEpoch()) ){
                        log.log(Level.INFO, "received vote {0} supersedes current vote: {1}", new Vote[]{vote, currentLeader});
                        setCurrentLeader(createElectionNotificationFromVote(vote));
                        if(this.getPeerState() == OBSERVER){
                            log.log(Level.INFO, "Skipping vote broadcast - this is an OBSERVER");
                        } else {
                            sendBroadcast(Message.MessageType.ELECTION, buildMsgContent(createElectionNotificationFromVote(currentLeader)));
                            log.log(Level.INFO, "broadcast new vote {0}", currentLeader);
                        }
                    }
                    ////if I have enough votes to declare my currently proposed leader as the leader:
                    if(haveEnoughVotes(peerIDtoVote, currentLeader)){
                        //first check if there are any new votes for a higher ranked possible leader before I declare a leader. If so, continue in my election loop
                        log.log(Level.INFO, "Found enough votes. Double checking");
                        for (Message msg : incomingMessages) {
                            ElectionNotification latestVote = getNotificationFromMessage(msg);
                            if (supersedesCurrentVote(latestVote.getProposedLeaderID(), latestVote.getPeerEpoch())) {
                                //found higher vote. break and go back to while loop:
                                log.log(Level.WARNING, "found higher vote. Restarting loop");
                                break MessageProcessing;
                            }
                        }
                        //If not, set my own state to either LEADING (if I won the election) or FOLLOWING (if someone lese won the election) and exit the election
                        acceptElectionWinner(createElectionNotificationFromVote(currentLeader));
                        break LeaderSearch;
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
                        break LeaderSearch;
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
                case OBSERVER:
                    log.log(Level.INFO, "Received vote from observer. Discarding: {0}", vote);
                    break;
            }
        }
        return currentLeader;
    }
    public void startWorkProcessingThreads(){
        Message msg;
        if((msg = incomingMessages.peek()) != null && msg.getMessageType() == Message.MessageType.WORK){
            log.log(Level.INFO, "Received new work message. Forwarding to JavaRunnerWorkQueue");
            incomingMessages.remove(msg);
            javaRunnerWorkItems.add(msg);
        }
        switch (this.state){
            case LEADING:
                if(roundRobinLeader != null){
                    log.log(Level.INFO, "State: LEADING; Leader already started");
                    break;
                }
                log.log(Level.INFO, "State: LEADING; Starting RoundRobinLeader");
                roundRobinLeader = new RoundRobinLeader(this, roundRobinWork);
                roundRobinLeader.start();
                break;
            case FOLLOWING:
                if(javaRunnerFollower != null){
                    log.log(Level.INFO, "State: FOLLOWING; JavaRunnerFollower already started");
                    break;
                }
                log.log(Level.INFO, "State: Following; Starting JavaRunnerFollower");
                javaRunnerFollower = new JavaRunnerFollower(this, this.javaRunnerWorkItems);
                javaRunnerFollower.start();
                break;
            default:
                log.log(Level.SEVERE, "Tried to process work when not LEADING/FOLLOWING");
        }
    }

    private void sendNotifications() {
        log.log(Level.FINE, "Sending initial EN notifications from port {0}",this.getUdpPort());
        //send our initial vote to peers. They will reply with their own vote:
        sendBroadcast(Message.MessageType.ELECTION, buildMsgContent(createElectionNotificationFromVote(currentLeader)));
    }

    private Vote acceptElectionWinner(ElectionNotification n) throws InterruptedException {
        //set my state to either LEADING or FOLLOWING
        //clear out the incoming queue before returning
        log.log(Level.INFO, "Elected leader {0}", n);
        setCurrentLeader(n);
        if(this.id == currentLeader.getProposedLeaderID()) setPeerState(LEADING);
        else setPeerState(FOLLOWING);
        incomingMessages.clear();
        Thread.sleep(ZooKeeperLeaderElection.finalizeWait);
        //After sleeping the requisite sleep time, start whatever threads (Javarunner/RoundRobinLeader) are necessary
        startWorkProcessingThreads();
        return n;
    }

    /*
     * We return true if one of the following three cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     * // ADDITION: IF this is an observer voting for itself, should fail
     * // TODO: generalize this to include any vote for an observer
     */
    protected boolean supersedesCurrentVote(long newId, long newEpoch) {
        final boolean newEpochIsHigher = newEpoch > this.currentLeader.getPeerEpoch();
        final boolean serverIdIsHigher = newId > this.currentLeader.getProposedLeaderID();
        final boolean iAmObserverVotingForSelf =
                (this.getPeerState() == OBSERVER) && (this.getCurrentLeader().getProposedLeaderID() == this.id);
        final boolean supersedesCurrentVote = newEpochIsHigher || (newEpoch == this.getPeerEpoch() && serverIdIsHigher)
                || iAmObserverVotingForSelf;
        log.log(Level.FINER, "newID: {0}, newEpoch: {1}, currentLeader: {2}", new Object[]{newId, newEpoch, currentLeader});
        log.log(Level.FINER, "Higher epoch: {0}, higher ID: {1}, self-voting observer: {2}, Supersedes  Vote: {3}",
                new Object[]{newEpochIsHigher, serverIdIsHigher, iAmObserverVotingForSelf, supersedesCurrentVote});
        return supersedesCurrentVote;
    }
    protected ElectionNotification createElectionNotificationFromVote(Vote vote) {
        return new ElectionNotification(vote.getProposedLeaderID(), this.state, this.id, vote.getPeerEpoch());
    }

    /**
     * Termination predicate. Given a set of votes, determines if have sufficient support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one current vote
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification> votes, Vote proposal) {
        //FIXME TODO SOMETHIGN VERY FISHY GOING ON HERE WHERE SOMETIMES ACCEPTING WITHOUT QUORUM
        //is the number of votes for the proposal > the size of my peer server’s quorum?
        log.log(Level.FINE, "Checking support for vote {0}", proposal);
        AtomicInteger voteCount = new AtomicInteger();
        votes.forEach((voterId,electionNotification) -> {
            if(electionNotification.getProposedLeaderID() == proposal.getProposedLeaderID()
                    && electionNotification.getPeerEpoch() == proposal.getPeerEpoch())
                voteCount.getAndIncrement();
        });
        log.log(Level.INFO, "Proposal {0} has {1} votes. Passed: {2}. Votes: {3}",
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
        //get initial states:
        boolean isLooking = this.getPeerState() == LOOKING;
        boolean isObserverInitialState = this.getPeerState() == OBSERVER && this.getCurrentLeader().getProposedLeaderID() == this.id;
        while(!this.isInterrupted()) {
            try {
                //since we don't need fault tolerance yet, just do leader search if we are LOOKING
                //OR if we are an OBSERVER but the current leader is myself
                if (isLooking || isObserverInitialState) lookForLeader();
                processMessages();
                //return;
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
        }
    }

    private void processMessages() throws InterruptedException {
        Message msg = incomingMessages.take();
        switch (msg.getMessageType()){
            case ELECTION:
                //there should be no elections after the initial one (as of stage 4). If so, assume this is an error
                // and ignore it.
                log.log(Level.WARNING, "Received unexpected ELECTION message after election: {0}", msg);
                break;
            case WORK:
                //we need to create a request ID so we know where to send responses. Note that this will override the
                //original id if it exists.
                //TODO: redo logic
                if(msg.getRequestID() == -1L) {
                    msg = new Message(msg.getMessageType(), msg.getMessageContents(), msg.getSenderHost(), msg.getSenderPort(),
                            msg.getReceiverHost(), msg.getReceiverPort(), RoundRobinLeader.requestIDGenerator.getAndIncrement());
                    RoundRobinLeader.requestIdtoAddress.put(msg.getRequestID(), new InetSocketAddress(msg.getSenderHost(), msg.getSenderPort()));
                }
                //in Stage 4, we do not need to deal with direct requests.

                if(this.state == LEADING){
                    //we are the leader. Give work item to roundRobinLeader to schedule
                    roundRobinWork.put(msg);
                }
                else if(this.state == FOLLOWING){
                    //we are a follower. In stage 3, we just deal with the task directly
                    javaRunnerWorkItems.put(msg);
                }
                break;
            case COMPLETED_WORK:
                //need to send the completed work item to the requester.
                //If I am the requester, process it here. Otherwise pass it on:
                InetSocketAddress destination = RoundRobinLeader.requestIdtoAddress.get(msg.getRequestID());
                if(destination.equals(getMyAddress())){
                    log.log(Level.INFO, "Tried to send completed work item {0} to self: {1}", new Object[]{msg, destination});

                }
                else {
                    sendMessage(Message.MessageType.COMPLETED_WORK, msg.getRequestID(), msg.getMessageContents(), destination);
                    log.log(Level.INFO, "Sent completed work item {0} to destination {1}", new Object[]{msg, destination});
                }
                break;
            default:
                log.log(Level.WARNING, "Received unexpected message. Ignoring: {0}", msg);
        }
    }

    public InetSocketAddress getMyAddress() {
        return getAddress();
    }
    public InetSocketAddress getLeaderAddress(){
        return peerIDtoAddress.get(currentLeader.getProposedLeaderID());
    }


    @Override
    public void shutdown(){
        this.shutdown = true;
        this.senderWorker.shutdown();
        this.receiverWorker.shutdown();
        if(javaRunnerFollower != null) javaRunnerFollower.shutdown();
        if(roundRobinLeader != null) roundRobinLeader.shutdown();
        interrupt();
    }

    @Override
    public void setCurrentLeader(Vote v) {
        log.log(Level.FINE, "Changing leader from {0} to {1}",new Object[]{currentLeader, v});
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
        sendMessage(type, -1L, messageContents, target);
    }

    public void sendMessage(Message.MessageType type, Long requestID, byte[] messageContents, InetSocketAddress target)
            throws IllegalArgumentException {
        //based off of sendBroadcast:
        Message msg = new Message(type, messageContents,
                myAddress.getHostString(), myPort, target.getHostString(), target.getPort(), requestID);
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
        log.log(Level.INFO, "Changing server state from {0} to {1}", new Object[]{state,newState});
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
     * For stage 4, this method returns the number of non-observer nodes divided by two plus 1. At this stage,
     * there is no capability to dynamically add or remove peers, and peers are assumed to
     * always be alive
     * @return minimum quorum size
     */
    @Override
    public int getQuorumSize() {
        final AtomicInteger observerCount = new AtomicInteger();
        peerIDtoVote.values().forEach(x -> {if(x.getState() == OBSERVER) observerCount.getAndIncrement();});
        int nonObserverNodes = peerIDtoAddress.size() - observerCount.get();
        //if the peer vote map doesn't include itself, add 1:
        if(!peerIDtoVote.containsKey(id)) nonObserverNodes++;
        //in order to have a quorum, there must be a majority of voting servers
        //for even numbers: 4/2 + 1 = 3
        //for odd numbers: integer division: 5/2 + 1 = 2 + 1 = 3
        return nonObserverNodes/2 + 1;
    }
}