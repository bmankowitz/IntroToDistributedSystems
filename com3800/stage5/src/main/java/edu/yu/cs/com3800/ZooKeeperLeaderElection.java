package edu.yu.cs.com3800;


import edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

@SuppressWarnings("ALL")
public class ZooKeeperLeaderElection {
    /**
     * time to wait once we believe we've reached the end of leader election.
     */
    public final static int finalizeWait = 200;

    /**
     * Upper bound on the amount of time between two consecutive notification checks.
     * This impacts the amount of time to get the system up again after long partitions. Currently 60 seconds.
     */
    public static int maxNotificationInterval = 10000;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final ZooKeeperPeerServer myPeerServer;
    private long proposedLeader;
    private long proposedEpoch;

    public ZooKeeperLeaderElection(ZooKeeperPeerServer server, LinkedBlockingQueue<Message> incomingMessages) {
        this.incomingMessages = incomingMessages;
        this.myPeerServer = server;
    }

    public static ElectionNotification getNotificationFromMessage(Message received) {
        return ZooKeeperPeerServerImpl.getNotificationFromMessage(received);
    }

    public static byte[] buildMsgContent(ElectionNotification notification) {
        return ZooKeeperPeerServerImpl.buildMsgContent(notification);
    }

    private synchronized Vote getCurrentVote() {
        return new Vote(this.proposedLeader, this.proposedEpoch);
    }

    public synchronized Vote lookForLeader() {
        //the logic has been migrated to ZooKeeperPeerServerImpl
        return null;
    }

    private void sendNotifications() {
    }

    private Vote acceptElectionWinner(ElectionNotification n) {
        //the logic has been migrated to ZooKeeperPeerServerImpl

        //set my state to either LEADING or FOLLOWING
        //clear out the incoming queue before returning
        return null;
    }

    /*
     * We return true if one of the following three cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     */
    protected boolean supersedesCurrentVote(long newId, long newEpoch) {
        return (newEpoch > this.proposedEpoch) || ((newEpoch == this.proposedEpoch) && (newId > this.proposedLeader));
    }

    /**
     * Termination predicate. Given a set of votes, determines if have sufficient support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one current vote
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification> votes, Vote proposal) {
        //the logic has been migrated to ZooKeeperPeerServerImpl

        //is the number of votes for the proposal > the size of my peer server’s quorum?
        return false;
    }
}
