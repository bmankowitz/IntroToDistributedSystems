package edu.yu.cs.com3800;

import java.io.IOException;
import java.util.logging.*;

public interface LoggingServer {
    public default Logger initializeLogging(String fileNamePreface, boolean disableParentHandlers) throws IOException {
        //TODO: Remove this:
        disableParentHandlers = false;
        //TODO: Implement this: 2) set up that newly created logger to write out to its own unique log file under a
        // directory that has a name whose suffix was created with the following
        // DateTimeFormatter pattern: "yyyy-MM-dd-kk_mm"
        System.setProperty("java.util.logging.FileHandler.formatter", "java.util.logging.SimpleFormatter");
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%4$s] [%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS] %2$s:  %5$s%6$s%n");
        Logger log = Logger.getLogger(fileNamePreface);
        for (Handler handler : log.getHandlers()) {  log.removeHandler(handler);}
        FileHandler fileHandler = new FileHandler(fileNamePreface+ ".log", false);
        fileHandler.setFormatter(new SimpleFormatter());
        ConsoleHandler consoleHandler = new ConsoleHandler();
        consoleHandler.setLevel(Level.FINER);
        log.addHandler(fileHandler);
        if(!disableParentHandlers) log.addHandler(consoleHandler);
        log.setUseParentHandlers(false);
        log.setLevel(Level.ALL);
        return log;
    }
    default String prependThreadName(String msg){
        return "==" + Thread.currentThread().getName() + "== : " + msg;
    }
}
