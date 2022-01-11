package edu.yu.cs.com3800.stage5;

import java.io.*;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class GossipArchive {
    //------------- GOSSIP STUFF --------------------
    public class Pair{
        public final long id;
        public final long heartbeat;
        public Pair(long id, long heartbeat){
            this.id = id;
            this.heartbeat = heartbeat;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Pair pair = (Pair) o;
            return id == pair.id && heartbeat == pair.heartbeat;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, heartbeat);
        }

        @Override
        public String toString() {
            return "("+id+", " +heartbeat+")";
        }
    }
    public static class GossipLine implements Serializable{
        long id;
        long lastHeartbeat;
        long lastUpdatedTime;
        boolean failed;

        public long getId() {
            return id;
        }
        public void setId(long id) {
            this.id = id;
        }
        public long getLastHeartbeat() {
            return lastHeartbeat;
        }
        public void setLastHeartbeat(long lastHeartbeat) {
            this.lastHeartbeat = lastHeartbeat;
        }
        public long getLastUpdatedTime() {
            return lastUpdatedTime;
        }
        public void setLastUpdatedTime(long lastUpdatedTime) {
            this.lastUpdatedTime = lastUpdatedTime;
        }
        public boolean isFailed() {
            return failed;
        }
        public void setFailed(boolean failed) {
            this.failed = failed;
        }
        @Override
        public String toString() {
            return "GossipLine{" +
                    "id=" + id +
                    ", lastHeartbeat=" + lastHeartbeat +
                    ", lastUpdatedTime=" + lastUpdatedTime +
                    ", failed=" + failed +
                    '}';
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GossipLine that = (GossipLine) o;
            return id == that.id && lastHeartbeat == that.lastHeartbeat && lastUpdatedTime == that.lastUpdatedTime && failed == that.failed;
        }
        @Override
        public int hashCode() {
            return Objects.hash(id, lastHeartbeat, lastUpdatedTime, failed);
        }

        public GossipLine(long id, long lastHeartbeat, long lastUpdatedTime, boolean failed) {
            this.id = id;
            this.lastHeartbeat = lastHeartbeat;
            this.lastUpdatedTime = lastUpdatedTime;
            this.failed = failed;
        }
    }
    //Util type methods:
    public static ConcurrentHashMap<Long, GossipLine> getMapFromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
        //TODO: move away from serialization apparently it is "too fragile"
        InputStream inputStream = new ByteArrayInputStream(bytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        ConcurrentHashMap<Long, GossipLine> result = (ConcurrentHashMap<Long, GossipLine>) objectInputStream.readObject();
        inputStream.close();
        objectInputStream.close();
        return result;
    }
    public static byte[] getBytesFromMap(ConcurrentHashMap<Long, GossipLine> map) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
        objectOutputStream.writeObject(map);
        objectOutputStream.flush();
        byte[] bytes = outputStream.toByteArray();
        return bytes;
    }



    //UTIL-----
    ConcurrentHashMap<Pair, ConcurrentHashMap<Long, GossipLine>> receivedHeartbeatToGossipTable = new ConcurrentHashMap<>();
        public void addToArchive(long senderId, long heartbeat, ConcurrentHashMap<Long, GossipLine> gossipTable){
            receivedHeartbeatToGossipTable.put(new Pair(senderId, heartbeat), gossipTable);
        }
        public String getArchive(){
            final StringBuilder stringBuilder = new StringBuilder();
            receivedHeartbeatToGossipTable.forEach((idHeartbeatPair, gossipTable) -> {
                stringBuilder.append("Gossip received from ").append(idHeartbeatPair.id).append(" at time ")
                        .append(idHeartbeatPair.heartbeat).append(": ").append("\n");
                gossipTable.forEach((id, gossipLine) ->{
                    stringBuilder.append("\t").append("server ").append(id).append(" \t->\t lastHeartbeat=")
                            .append(gossipLine.lastHeartbeat).append(", lastUpdatedTime=").append(gossipLine.lastUpdatedTime)
                            .append(", failed=").append(gossipLine.failed).append("\n");
                });
            });
            return stringBuilder.toString();
        }
    }
    //------------- GOSSIP STUFF --------------------

