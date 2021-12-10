package edu.yu.cs.com3800;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.*;

public interface LoggingServer {
    public default Logger initializeLogging(String fileNamePreface, boolean disableParentHandlers) throws IOException {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd-kk_mm");
        String folderName = "Stage4Logs_" + dtf.format(LocalDateTime.now());
        File folder = new File(folderName);
        folder.mkdir();
        System.setProperty("java.util.logging.FileHandler.formatter", "java.util.logging.SimpleFormatter");
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%4$s] [%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS.%1$tL] %2$s:  %5$s%6$s%n");
        Logger log = Logger.getLogger(fileNamePreface);
        for (Handler handler : log.getHandlers()) {  log.removeHandler(handler);}
        FileHandler fileHandler = new FileHandler(folderName+"/"+fileNamePreface+ ".log", false);
        fileHandler.setFormatter(new SimpleFormatter());
        ConsoleHandler consoleHandler = new ConsoleHandler();
        consoleHandler.setLevel(Level.FINER);
        log.addHandler(fileHandler);
        if(!disableParentHandlers) log.addHandler(consoleHandler);
        log.setUseParentHandlers(false);
        log.setLevel(Level.ALL);
        return log;
    }
    default Logger initializeLogging(String fileNamePreface) throws IOException{
        //TODO: CHANGE the method to disable parent handlers!!
        return initializeLogging(fileNamePreface, false);
//        return initializeLogging(fileNamePreface, true);
    }
}
