package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Vote;
import edu.yu.cs.com3800.ZooKeeperPeerServer;

import java.io.IOException;
import java.net.InetSocketAddress;

public class GatewayPeerServerImpl implements ZooKeeperPeerServer {
    @Override
    public void shutdown() {

    }

    @Override
    public void setCurrentLeader(Vote v) throws IOException {

    }

    @Override
    public Vote getCurrentLeader() {
        return null;
    }

    @Override
    public void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {

    }

    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {

    }

    @Override
    public ServerState getPeerState() {
        return null;
    }

    @Override
    public void setPeerState(ServerState newState) {

    }

    @Override
    public Long getServerId() {
        return null;
    }

    @Override
    public long getPeerEpoch() {
        return 0;
    }

    @Override
    public InetSocketAddress getAddress() {
        return null;
    }

    @Override
    public int getUdpPort() {
        return 0;
    }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        return null;
    }

    @Override
    public int getQuorumSize() {
        return 0;
    }
}
