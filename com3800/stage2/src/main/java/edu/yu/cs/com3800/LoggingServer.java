package edu.yu.cs.com3800;

import java.io.IOException;
import java.util.logging.Logger;

public interface LoggingServer {
    public default Logger initializeLogging(String fileNamePreface, boolean disableParentHandlers) throws IOException {
        //@FIXME: implement
        return null;
    }
}
