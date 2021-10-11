package edu.yu.cs.com3800.stage1;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.SimpleServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.logging.*;

public class SimpleServerImpl implements SimpleServer {
    HttpServer server;
    static Logger log;
    static FileHandler fileHandler;
    static ConsoleHandler consoleHandler;
    static class JavaRunnerHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            StringBuilder response;
            log.info("Visitor to context: /compileandrun using " + httpExchange.getRequestMethod());
            if(httpExchange.getRequestHeaders().get("Content-type") == null ||
                    !httpExchange.getRequestHeaders().get("Content-type").get(0).equals("text/x-java-source")){

                response = new StringBuilder();
                log.warning("Response: 400. Error bad content type. Needs to be text/x-java-source, not " +
                        httpExchange.getRequestHeaders().get("Content-type"));
                httpExchange.sendResponseHeaders(400, 0);
            }
            else {
                //This means the headers are now valid. Need to create a new InputStream to pass to the JavaRunner.
                //Can't use the same one otherwise it might get clobbered.
                InputStream is = httpExchange.getRequestBody();
                byte[] request = is.readAllBytes();
                InputStream duplicate = new ByteArrayInputStream(request);
                is.close();

                String requestString = new String(request);
                log.info("Received the following request (code to compile):" + requestString);

                //Now to run through the javarunner:
                JavaRunner javaRunner = new JavaRunner();
                try {
                    response = new StringBuilder(javaRunner.compileAndRun(duplicate));
                } catch (Exception e) {
                    //There was some sort of exception. Need to create stack trace:
                    response = new StringBuilder();
                    response.append(e.getMessage());
                    response.append("\n");
                    response.append(main.java.edu.yu.cs.com3800.Util.getStackTrace(e));
                    log.warning("ResponseCode: 400. Code generated the following error(s): " +response);

                    //Sending the error back to client:
                    httpExchange.sendResponseHeaders(400, response.length());
                    OutputStream os = httpExchange.getResponseBody();
                    os.write(response.toString().getBytes());
                    os.close();
                    return;
                }
                //if we get here, the code compiled and gave a result:
                log.info("ResponseCode: 200. Code compiled successfully and returned: ");
                httpExchange.sendResponseHeaders(200, response.length());
            }
            //Sending back the result
            OutputStream os = httpExchange.getResponseBody();
            os.write(response.toString().getBytes());
            os.close();
        }
    }

    public SimpleServerImpl(int port) throws IOException {
        //set up logger. I did it this way because I couldn't get the config file working properly:
        System.setProperty("java.util.logging.FileHandler.formatter", "java.util.logging.SimpleFormatter");
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%4$s] [%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS] %2$s:  %5$s%6$s%n");
        log = Logger.getLogger("server");
        for (Handler handler : log.getHandlers()) {  log.removeHandler(handler);}
        fileHandler = new FileHandler("server.log", false);
        fileHandler.setFormatter(new SimpleFormatter());
        consoleHandler = new ConsoleHandler();
        consoleHandler.setLevel(Level.FINER);
        log.addHandler(fileHandler);
        log.addHandler(consoleHandler);
        log.setUseParentHandlers(false);
        log.setLevel(Level.ALL);
        log.config("The server logs to both the console and a file. The file is name server.log");
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/compileandrun", new JavaRunnerHandler());
        server.setExecutor(null);
        log.info("Created server on port " +port);

    }

    /**
     * start the server
     */
    @Override
    public void start() {
        server.start();
        log.info("Ready for connections");
    }

    /**
     * stop the server
     */
    @Override
    public void stop() {
        log.info("Stopping server");
        server.stop(0);
        log.info("Server stopped");
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
