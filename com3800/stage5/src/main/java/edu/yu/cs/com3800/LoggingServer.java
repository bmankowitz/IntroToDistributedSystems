package edu.yu.cs.com3800;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.logging.*;

public interface LoggingServer {
    @SuppressWarnings("ResultOfMethodCallIgnored")
    default Logger initializeLogging(String fileNamePreface, boolean disableParentHandlers) throws IOException {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd-kk_mm");
        String folderName = "Stage5Logs_" + dtf.format(LocalDateTime.now());
        File folder = new File(folderName);
        folder.mkdir();
        System.setProperty("java.util.logging.FileHandler.formatter", "java.util.logging.SimpleFormatter");
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%4$s] [%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS.%1$tL] %2$s:  %5$s%6$s%n");
        Logger log = Logger.getLogger(fileNamePreface);
        for (Handler handler : log.getHandlers()) {  log.removeHandler(handler);}
        FileHandler fileHandler = new FileHandler(folderName+"/"+fileNamePreface+ ".log", false);
        fileHandler.setFormatter(new SimpleFormatter());
        ConsoleHandler consoleHandler = new ConsoleHandler();
        consoleHandler.setFormatter(getCustomFormatter());
        //----- SET DESIRED LOGGING LEVEL ----
        consoleHandler.setLevel(Level.WARNING);
        fileHandler.setLevel(Level.FINEST);
        //------------------------------------
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

    //Below taken from https://stackoverflow.com/questions/6889057/printing-thread-name-using-java-util-logging

    static Optional<Thread> getThread(long threadId) {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getId() == threadId)
                .findFirst();
    }
    private static String getSimpleName(String fullName){
        return fullName.substring(fullName.lastIndexOf(".") + 1);
    }

    private static Formatter getCustomFormatter() {
        return new Formatter() {

            @Override
            public String format(LogRecord record) {

                var dateTime = ZonedDateTime.ofInstant(record.getInstant(), ZoneId.systemDefault());

                int threadId = record.getThreadID();
                String threadName = getThread(threadId)
                        .map(Thread::getName)
                        .orElse("Thread with ID " + threadId);

                // See also: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/Formatter.html
                var formatString = "[%2$s] [%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS.%1$tL] [%3$s: %4$s.%5$s]: %6$s%n";
                //original:
                //var formatString = "%1$tF %1$tT %2$-7s [%3$s] %4$s.%5$s: %6$s %n%7$s";
                StringBuilder sb = new StringBuilder();
                sb.append("[").append(record.getLevel()).append("] [")
                        .append(dateTime).append("] [")
                        .append(threadName).append(": ")
                        .append(getSimpleName(record.getSourceClassName())).append(".")
                        .append(record.getSourceMethodName()).append("]: ")
                        .append(formatMessage(record)).append("\n");
                return sb.toString();
//                return String.format(
//                        formatString,
//                        dateTime,
//                        record.getLevel().getName(),
//                        threadName,
//                        getSimpleName(record.getSourceClassName()),
//                        record.getSourceMethodName(),
//                        record.getMessage(),
//                        stackTraceToString(record)
//                );
            }
        };
    }

    private static String stackTraceToString(LogRecord record) {
        final String throwableAsString;
        if (record.getThrown() != null) {
            var stringWriter = new StringWriter();
            var printWriter = new PrintWriter(stringWriter);
            printWriter.println();
            record.getThrown().printStackTrace(printWriter);
            printWriter.close();
            throwableAsString = stringWriter.toString();
        } else {
            throwableAsString = "";
        }
        return throwableAsString;
    }
}
