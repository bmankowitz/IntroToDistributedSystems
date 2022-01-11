package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;

import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState.*;


public class ZooKeeperPeerServerImpl extends Thread implements ZooKeeperPeerServer, LoggingServer{
    private final InetSocketAddress myAddress;
    private final int myPort;
    private volatile ServerState state;
    private volatile boolean shutdown;
    private final LinkedBlockingQueue<Message> outgoingMessages;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final Long id;
    private long peerEpoch;
    private volatile Vote currentLeader;
    public final Map<Long,InetSocketAddress> peerIDtoAddress;
    public final Map<Long,ElectionNotification> peerIDtoVote = new HashMap<>();
    public final Map<Long, ServerState> peerIDtoStatus;
    private Logger log;
    public final ExecutorService executorService = Executors.newFixedThreadPool(8);
    private TCPServer tcpServer;
    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;
    public final Set<Long> observerIds = new HashSet<>();
    //------ GOSSIP STUFF (TODO: MIGRATE) -------
    public static int GOSSIP_TIME = 350;
    public static final int GOSSIP_FAIL_TIME = GOSSIP_TIME * 10;
    public static final int GOSSIP_FAILURE_CLEANUP_TIME = GOSSIP_FAIL_TIME * 2;
    //gossip table: id to heartbeat time
    public final ConcurrentHashMap<Long, GossipArchive.GossipLine> gossipTable;
    public final AtomicLong gossipHeartbeat;
    public final GossipArchive gossipArchive;
    //------ GOSSIP STUFF -----------------------


    public ZooKeeperPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long,InetSocketAddress> peerIDtoAddress) {
        //-------- GOSSIP STUFF --------
        gossipTable = new ConcurrentHashMap<>();
        gossipHeartbeat = new AtomicLong(0);
        gossipArchive = new GossipArchive();
        peerIDtoAddress.keySet().forEach((peerId) -> {
            //Peers are assumed to be alive when first created. They will only fail after T_fail seconds
                gossipTable.put(peerId, new GossipArchive.GossipLine(id, gossipHeartbeat.get(), System.currentTimeMillis(), false));
                });
        //if self wasn't included, add it now:
        gossipTable.put(id, new GossipArchive.GossipLine(id, gossipHeartbeat.get(), System.currentTimeMillis(), false));
        peerIDtoStatus = new HashMap<>();
        //-------- GOSSIP STUFF --------
        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();
        this.myAddress = new InetSocketAddress("localhost",myPort);
        this.myPort = myPort;
        this.peerIDtoAddress = peerIDtoAddress;
        this.id = id;
        this.peerEpoch = peerEpoch;
        try {
            log = initializeLogging(ZooKeeperPeerServerImpl.class.getCanonicalName() + "-on-port-"+getUdpPort(),
                    false);
        } catch (Exception e) {e.printStackTrace();}
        state = ServerState.LOOKING;
        currentLeader = new Vote(this.id, this.peerEpoch);
    }

    public Map<Long, InetSocketAddress> getPeerIDtoAddress() {
        return peerIDtoAddress;
    }
    private Message getNextElectionMessage(long maxWaitMs){
        long startTime = System.currentTimeMillis();
        while(System.currentTimeMillis() - startTime < maxWaitMs) {
            Optional<Message> msg = incomingMessages.stream().filter(x -> x.getMessageType() == Message.MessageType.ELECTION).findFirst();
            if (msg.isPresent()){
                incomingMessages.remove(msg.get());
                return msg.get();
            }
        }
        return null;
    }

    public synchronized Vote lookForLeader() throws InterruptedException {
        int maxNotificationTime = ZooKeeperLeaderElection.maxNotificationInterval;
        //send initial notifications to other peers to get things started
        sendNotifications();
        peerIDtoVote.put(this.id, createElectionNotificationFromVote(currentLeader));
        //Loop, exchanging notifications with other servers until we find a leader
        //NOTE: Even if we are an observer, we still need to "participate" in voting to determine the master.
        LeaderSearch: while ((getPeerState() == LOOKING || getPeerState() == OBSERVER) && !shutdown && !isInterrupted()) {
            //Remove next notification from queue, timing out after 2 times the termination time
            Message message;
            while((message = getNextElectionMessage(maxNotificationTime)) == null){
                // resend notifications to prompt a reply from others ...
                sendNotifications();
                // and implement exponential back-off when notifications not received
                maxNotificationTime *= 2;
                log.log(Level.WARNING, "Did not receive election notification in after waiting {0}ms, " +
                        "increasing wait to {1}ms", new Object[]{maxNotificationTime/2, maxNotificationTime});
            }

            ElectionNotification vote = getNotificationFromMessage(message);
            //if no notifications received ....

            //if/when we get a message
            log.log(Level.INFO, "evaluating vote {0} from {1}", new Object[]{vote, vote.getSenderID()});
            if(vote.getPeerEpoch() < this.peerEpoch){
                log.log(Level.WARNING, "ignoring vote {0} from {1} -- supplied epoch is less than current epoch {2}",
                        new Object[]{vote, vote.getSenderID(), this.peerEpoch});
                //noinspection UnnecessaryLabelOnContinueStatement
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
                    //If the vote is for an observer, that vote should be superseded
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
                    //set the status:
                    peerIDtoStatus.put(vote.getSenderID(), vote.getState());

                    //IF: if the sender's vote allows me to reach a conclusion based on the election epoch that I'm in, i.e. it gives the majority to the vote of the FOLLOWING or LEADING peer whose vote I just received.
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
                    log.log(Level.WARNING, "Received vote from observer. Discarding: {0}", vote);
                    break;
            }
        }
        return currentLeader;
    }

    private void sendNotifications() {
        log.log(Level.FINE, "Sending initial EN notifications from port {0}",this.getUdpPort());
        //send our initial vote to peers. They will reply with their own vote:
        //if this is an observer, send an invalid vote:
        if(getPeerState() == OBSERVER){
            log.log(Level.INFO, "OBSERVER sending initial notification. Sending vote (-1,-1)");
            sendBroadcast(Message.MessageType.ELECTION,
                    buildMsgContent(createElectionNotificationFromVote(new Vote(-1, -1))));
        }
        else {
            sendBroadcast(Message.MessageType.ELECTION, buildMsgContent(createElectionNotificationFromVote(currentLeader)));
        }
    }

    private void acceptElectionWinner(ElectionNotification n) throws InterruptedException {
        //set my state to either LEADING or FOLLOWING
        //clear out the incoming queue before returning
        log.log(Level.INFO, "Elected leader {0}", n);
        setCurrentLeader(n);
        if(this.id == currentLeader.getProposedLeaderID()) setPeerState(LEADING);
        else if(this.getPeerState() == OBSERVER){/* Do nothing - OBSERVER should never change state*/ }
        else setPeerState(FOLLOWING);
        //per stage 5: quick way to distribute current server status is to send an extra message.
        sendBroadcast(Message.MessageType.ELECTION, buildMsgContent(createElectionNotificationFromVote(currentLeader)));

        incomingMessages.clear();
        Thread.sleep(ZooKeeperLeaderElection.finalizeWait);
        //After sleeping the requisite sleep time, start whatever threads (TCPServer) are necessary
        startWorkProcessingThreads();
    }

    /*
     * We return true if one of the following three cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     *    ADDITION: If this is an observer voting for itself, should fail
     */
    protected boolean supersedesCurrentVote(long newId, long newEpoch) {
        if(currentLeader == null) return true;
        final boolean newEpochIsHigher = newEpoch > getCurrentLeader().getPeerEpoch();
        final boolean serverIdIsHigher = newId > getCurrentLeader().getProposedLeaderID();
        final boolean iAmObserverVotingForSelf =
                (this.getPeerState() == OBSERVER) && (getCurrentLeader().getProposedLeaderID() == this.id);
        final boolean supersedesCurrentVote = newEpochIsHigher || (newEpoch == this.getPeerEpoch() && serverIdIsHigher)
                || iAmObserverVotingForSelf;
        log.log(Level.FINER, "newID: {0}, newEpoch: {1}, currentLeader: {2}", new Object[]{newId, newEpoch, currentLeader});
        log.log(Level.FINER, "Higher epoch: {0}, higher ID: {1}, self-voting observer: {2}, Supersedes  Vote: {3}",
                new Object[]{newEpochIsHigher, serverIdIsHigher, iAmObserverVotingForSelf, supersedesCurrentVote});
        return supersedesCurrentVote;
    }
    protected ElectionNotification createElectionNotificationFromVote(Vote vote) {
        if(vote == null) return null;
        return new ElectionNotification(vote.getProposedLeaderID(), this.state, this.id, vote.getPeerEpoch());
    }

    /**
     * Termination predicate. Given a set of votes, determines if there is sufficient support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one current vote
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification> votes, Vote proposal) {
        log.log(Level.FINE, "Checking support for vote {0}", proposal);
        AtomicInteger voteCount = new AtomicInteger();
        votes.forEach((voterId,electionNotification) -> {
            if(electionNotification == null) return;
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

    public static synchronized byte[] buildMsgContent(ElectionNotification notification) {
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

        while(!this.isInterrupted() && !shutdown) {
            try {
                boolean isLooking = this.getPeerState() == LOOKING;
                boolean isObserverInitialState = this.getPeerState() == OBSERVER
                        && (this.getCurrentLeader() == null ||this.getCurrentLeader().getProposedLeaderID() == this.id);
                //since we don't need fault-tolerance yet, just do leader search if we are LOOKING
                //OR if we are an OBSERVER but the current leader is myself
                if (isLooking || isObserverInitialState) lookForLeader();
                processGossip();
            } catch (Exception e) {
                log.severe(Util.getStackTrace(e));
                shutdown();
                throw new RuntimeException(e);
            }
        }
        log.log(Level.SEVERE, "Shutting down ZooKeeperPeerServer");
    }
    public void startWorkProcessingThreads() {
        try {
            if (tcpServer != null) {
                log.log(Level.INFO, "Skipping new TCPServer creation: already exists: {0}", tcpServer);
                return;
            }
            InetSocketAddress address = new InetSocketAddress(myAddress.getHostName(), getUdpPort()+2);
            switch (this.state) {
                case LEADING:
                    log.log(Level.INFO, "State: LEADING; Starting TCPServer");
                    tcpServer = new TCPServer(this, address, TCPServer.ServerType.SCHEDULER, null);
                    executorService.submit((Callable<Message>) tcpServer);
                    break;
                case FOLLOWING:
                    log.log(Level.INFO, "State: Following; Starting TCPServer");
                    tcpServer = new TCPServer(this, address, TCPServer.ServerType.WORKER, null);
                    executorService.submit((Callable<Message>) tcpServer);
                    break;
                case OBSERVER:
                    log.log(Level.INFO, "State: OBSERVER; Starting TCPServer");
                    tcpServer = new TCPServer(this, address, TCPServer.ServerType.CONNECTOR, null);
                    executorService.submit((Callable<Message>) tcpServer);
                    break;
                default:
                    log.log(Level.SEVERE, "Tried to process work when not LEADING/FOLLOWING");
            }
        } catch(IOException | InterruptedException e) {
            e.printStackTrace();
            log.log(Level.SEVERE, "Unexpected error: {0}", e);
        }

    }

    public InetSocketAddress getMyAddress() {
        return getAddress();
    }
    public InetSocketAddress getLeaderAddress(){
        if(getCurrentLeader() == null) return null;
        return peerIDtoAddress.get(getCurrentLeader().getProposedLeaderID());
    }


    @Override
    public void shutdown(){
        this.shutdown = true;
        this.senderWorker.shutdown();
        this.receiverWorker.shutdown();
        if(this.tcpServer != null) this.tcpServer.shutdown();
        executorService.shutdownNow();
        interrupt();
    }

    @Override
    public void setCurrentLeader(Vote v) {
        log.log(Level.FINE, "Changing leader from {0} to {1}",new Object[]{currentLeader, v});
        if(v == null) currentLeader = null;
        else{
            currentLeader = new Vote(v.getProposedLeaderID(), v.getPeerEpoch());
            this.peerEpoch = v.getPeerEpoch();
        }
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
    public synchronized void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        for(InetSocketAddress peer : peerIDtoAddress.values()) {
            //no need to send messages to myself
            if(peer.equals(myAddress)) continue;
            //no need to send messages to dead peers
            if(isPeerDead(peer)) continue;
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
        //per stage 5 requirements, double logging:
        System.out.println(id+": switching from " +getPeerState()+ " to " +newState);
        log.log(Level.INFO, "Changing server state from {0} to {1}", new Object[]{state,newState});
        state = newState;
        peerIDtoStatus.put(id, newState);
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
     * Returns the number of non-observer alive nodes divided by two plus 1 (to ensure majority).
     * @return minimum quorum size
     */
    @Override
    public int getQuorumSize() {
        final AtomicInteger nonVotingMembers = new AtomicInteger();
        peerIDtoVote.keySet().forEach(x -> {
            if(isObserver(x)){
                log.log(Level.FINE, "Ignoring observer {0}", x);
                nonVotingMembers.getAndIncrement();
                observerIds.add(id);
            } else if(isPeerDead(x)){
                log.log(Level.FINE, "Ignoring dead peer {0}", x);
                nonVotingMembers.getAndIncrement();
            }
        });
        int nonObserverNodes = peerIDtoAddress.size() - nonVotingMembers.get();
        //if the peer vote map doesn't include itself, add 1:
        if(!peerIDtoVote.containsKey(id)) nonObserverNodes++;
        //in order to have a quorum, there must be a majority of voting servers
        //for even numbers: 4/2 + 1 = 3
        //for odd numbers: integer division: 5/2 + 1 = 2 + 1 = 3
        return nonObserverNodes/2 + 1;
    }
    public boolean isObserver(long id){
        return peerIDtoVote.get(id) == null;
    }
    public long getPeerIdByAddress(InetSocketAddress address){
        AtomicLong returnId = new AtomicLong(-1);
        getPeerIDtoAddress().forEach((peerId, peerAddress) -> {
            if(peerAddress.equals(address)) returnId.set(peerId);
        });
        return returnId.get();
    }

    // ------------- GOSSIP STUFF ------------------
    @Override
    public boolean isPeerDead(InetSocketAddress address) {
        Optional<Map.Entry<Long, InetSocketAddress>> getId = peerIDtoAddress.entrySet()
                .stream().filter(entry -> entry.getValue().equals(address)).findFirst();
        if(getId.isEmpty()) return true;
        else return isPeerDead(getId.get().getKey());
    }
    @Override
    public void reportFailedPeer(long peerID) {
        log.log(Level.WARNING, "{0}: no heartbeat from server {1} - server failed", new Object[]{this.id, peerID});
        //double-logging per requirements
        System.out.println(id+": no heartbeat from server "+peerID+" - server failed");
        gossipTable.get(peerID).setFailed(true);
        peerIDtoStatus.remove(peerID);
    }

    @Override
    public boolean isPeerDead(long peerID) {
        //Use sentinel value of -1 to indicate failed peer
        //if the peer doesn't exist on the list, it is failed
        return (gossipTable.get(peerID) == null || gossipTable.get(peerID).isFailed());
    }
    public String getGossipArchive(){
        //TODO: also create new log file
        String gossipArchiveSnapshot = gossipArchive.getArchive();
        try{
            String fileName = "Server"+id+"GossipArchiveAt"+ LocalDateTime.now();
            FileWriter logFile = new FileWriter(fileName);
            logFile.write(gossipArchiveSnapshot);
            logFile.close();
        } catch (IOException e) {
            e.printStackTrace();
            log.warning("Error occured creating gossip log file");
        }
        return gossipArchiveSnapshot;
    }
    public void incorporateGossip(long senderId, ConcurrentHashMap<Long, GossipArchive.GossipLine> otherGossip){
        otherGossip.forEach((otherId, otherGossipLine) ->{
            if(gossipTable.get(otherId) == null || gossipTable.get(otherId).lastHeartbeat < otherGossipLine.lastHeartbeat) {
                otherGossipLine.setLastUpdatedTime(System.currentTimeMillis());
                gossipTable.put(otherId, otherGossipLine);
                String logString = id + ": updated " + otherId + "'s heartbeat sequence to "
                        + otherGossipLine.lastHeartbeat + " at node time " + System.currentTimeMillis();
                //double logging per requirements:
                log.log(Level.FINER, logString);
                System.out.println(logString);
            }
            else log.log(Level.FINER, id + ": skipped updating " + otherId + "'s heartbeat because " +
                                            otherGossipLine.lastHeartbeat + "is lower. Node time " + System.currentTimeMillis());
        });
        gossipArchive.addToArchive(senderId, System.currentTimeMillis(), gossipTable);
    }
    public void updateLocalGossipCounter(InetSocketAddress serverAddress){
        long serverId = getPeerIdByAddress(serverAddress);
        long newHeartbeat = gossipHeartbeat.get();
        if(gossipTable.get(serverId) == null || newHeartbeat > gossipTable.get(serverId).getLastHeartbeat()){
            log.log(Level.FINER, "Updating local gossip table. Changing server {0} from {1} to {2}",
                    new Object[]{serverId, gossipTable.get(serverId), newHeartbeat});
            GossipArchive.GossipLine gs = new GossipArchive.GossipLine(serverId, newHeartbeat, System.currentTimeMillis(), false);
            gossipTable.put(serverId, gs);
        }
        else log.log(Level.FINEST, "ignoring heartbeat {0} from server {1}", new Object[]{newHeartbeat, serverId});
    }
    public void sendGossip(long gossipDestinationId) throws IOException {
        byte[] bytes = GossipArchive.getBytesFromMap(this.gossipTable);
        //Gossip messages are send by UDP not TCP because it does not deal with client work. See
        sendMessage(Message.MessageType.GOSSIP, bytes, peerIDtoAddress.get(gossipDestinationId));
    }
    public void processGossip() throws InterruptedException, IOException, ClassNotFoundException {
        //TODO: migrate some of this
        Random random = new Random();
        long lastUpdate = System.currentTimeMillis();
        //the reason we stopped doing this when the leader is dead is because then we need to go back to leader election
        //quick hack: just put it after the loop.
        //TODO: This really needs to run in a separate thread
        while (!isInterrupted() && !shutdown && !isPeerDead(getCurrentLeader().getProposedLeaderID())){
            if(System.currentTimeMillis() - lastUpdate >= GOSSIP_TIME){
                log.log(Level.INFO, "Incrementing current heartbeat to {0}", gossipHeartbeat.incrementAndGet());
                lastUpdate = System.currentTimeMillis();
                ArrayList<Long> validServers = new ArrayList<>(peerIDtoAddress.keySet());
                Set<Long> idsToSkip = new HashSet<>();
                validServers.forEach((id) ->{
                    if(isPeerDead(id)){
                        idsToSkip.add(id);
                        if(gossipTable.get(id) != null &&
                                System.currentTimeMillis() - gossipTable.get(id).getLastUpdatedTime() >= GOSSIP_FAILURE_CLEANUP_TIME){
                            gossipTable.remove(id);
                            log.log(Level.INFO, "Removed server {0} from gossipTable", id);
                        }
                    }
                    else if(System.currentTimeMillis() - gossipTable.get(id).getLastUpdatedTime() >= GOSSIP_FAIL_TIME){
                        reportFailedPeer(id);
                        idsToSkip.add(id);
                    }
                });
                validServers.removeAll(idsToSkip);
                if(!validServers.isEmpty()) {
                    long serverIdToSendGossip = validServers.get(random.nextInt(validServers.size()));
                    sendGossip(serverIdToSendGossip);
                }
            }
            Message incomingGossip = incomingMessages.poll();
            if(incomingGossip != null && incomingGossip.getMessageType().equals(Message.MessageType.GOSSIP)){
                log.log(Level.INFO, "Received gossip message {0}", incomingGossip);
                long senderId = getPeerIdByAddress(new InetSocketAddress(incomingGossip.getSenderHost(), incomingGossip.getSenderPort()));
                ConcurrentHashMap<Long, GossipArchive.GossipLine> otherGossipTable = GossipArchive.getMapFromBytes(incomingGossip.getMessageContents());
                incorporateGossip(senderId, otherGossipTable);
            }
        }
        if(shutdown || isInterrupted()){
            log.log(Level.SEVERE, "Interrupted while gossiping");
        }
        else{
            //the current leader failed. Go back to election
            lookForLeader();
        }
    }
    // ------------- GOSSIP STUFF ------------------
}
