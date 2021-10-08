package edu.yu.cs.com3800.stage1;

import edu.yu.cs.com3800.SimpleServer;

import java.io.IOException;

public class SimpleServerImpl implements SimpleServer {
    public SimpleServerImpl(int port) throws IOException {

    }

    /**
     * start the server
     */
    @Override
    public void start() {

    }

    /**
     * stop the server
     */
    @Override
    public void stop() {

    }

    public static void main(String[] args)
    {
        int port = 9000;
        if(args.length >0)
        {
            port = Integer.parseInt(args[0]);
        }
        SimpleServer myserver = null;
        try
        {
            myserver = new SimpleServerImpl(port);
            myserver.start();
        }
        catch(Exception e)
        {
            System.err.println(e.getMessage());
            myserver.stop();
        }
    }
}
