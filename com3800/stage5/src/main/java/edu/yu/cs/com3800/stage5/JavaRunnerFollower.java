package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JavaRunnerFollower extends Thread implements LoggingServer {
    private final String serverUdpPort;
    private final LinkedBlockingQueue<Message> workQueue;
    private final ZooKeeperPeerServerImpl server;
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

                Message workItem = this.workQueue.take();
                logger.log(Level.INFO, "Received work item: {0}", workItem);
                String result = processWorkItem(workItem);
                Message resultMessage = new Message(Message.MessageType.COMPLETED_WORK,
                        result.getBytes(StandardCharsets.UTF_8), server.getAddress().getHostString(),
                        server.getUdpPort(), server.getLeaderAddress().getHostString(),
                        server.getLeaderAddress().getPort(), workItem.getRequestID());
                server.sendMessage(Message.MessageType.COMPLETED_WORK, resultMessage.getRequestID(),
                        resultMessage.getMessageContents(), server.getLeaderAddress());
                logger.log(Level.INFO, "Processed work item: {0}", workItem);
            }
            catch (IOException e) {
                this.logger.log(Level.WARNING,"Exception trying to process workItem", e);
            } catch (InterruptedException e) {
                //e.printStackTrace();
            }
        }
        if(logger != null) logger.log(Level.SEVERE,"Exiting JavaRunnerFollower.run()");
    }

    public synchronized String processWorkItem(Message workItem) throws IOException {
        logger.log(Level.INFO, "Received the following request (code to compile):{0}", new String(workItem.getMessageContents()));
        InputStream is = new ByteArrayInputStream(workItem.getMessageContents());
        StringBuilder response;

        //Now to run through the JavaRunner:
        JavaRunner javaRunner = new JavaRunner();
        try {
            response = new StringBuilder(javaRunner.compileAndRun(is));
        } catch (Exception e) {
            //There was some sort of exception. Need to create stack trace:
            response = new StringBuilder();
            response.append(e.getMessage());
            response.append("\n");
            response.append(Util.getStackTrace(e));
            logger.info("Code generated the following error(s): " + response);

            //Sending the error back to client:
            return response.toString();
        }
        //if we get here, the code compiled and gave a result:
        //TODO: Change back to info
        logger.warning("Code compiled successfully and returned: " + response);
        return response.toString();
    }
}
