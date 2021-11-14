package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JavaRunnerFollower extends Thread implements LoggingServer {
    private final String serverUdpPort;
    private final LinkedBlockingQueue<Message> workQueue;
    private ZooKeeperPeerServerImpl server;
    private Logger logger;


    public JavaRunnerFollower(ZooKeeperPeerServerImpl server, LinkedBlockingQueue<Message> workQueue) {
        setDaemon(true);
        this.server = server;
        this.serverUdpPort = "" + server.getUdpPort();
        this.workQueue = workQueue;
        setName("JavaRunnerFollower-port-" + this.serverUdpPort);
    }

    public void shutdown() {
        interrupt();
    }

    @Override
    public void run() {
        while (!this.isInterrupted()) {
            try {
                if(this.logger == null){
                    this.logger = initializeLogging(JavaRunnerFollower.class.getCanonicalName() + "-on-server-with-udpPort-" + this.serverUdpPort);
                }
                //TODO: probably want to switch to take for performance reasons. Just make sure it is interruptible
                Message workItem = this.workQueue.poll();
                if (workItem != null) {
                    logger.log(Level.INFO, "Received work item: {0}", workItem);
                    processWorkItem(workItem);
                    logger.log(Level.INFO, "Processed work item: {0}", workItem);
                }
            }
            catch (IOException e) {
                this.logger.log(Level.WARNING,"Exception trying to process workItem", e);
            }
        }
        this.logger.log(Level.SEVERE,"Exiting JavaRunnerFollower.run()");
    }

    private void processWorkItem(Message workItem) {
        //TODO: should just be a copy/paste from stage1
    }
}
