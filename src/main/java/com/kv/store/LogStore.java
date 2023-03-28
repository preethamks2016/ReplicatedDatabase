package com.kv.store;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface LogStore {
    void WriteToIndex (Log log, int index) throws IOException;
    void WriteAtEnd(Log log) throws IOException;

    Optional<Log> getLastLogEntry() throws IOException;

    List<Log> readAllLogs() throws IOException;
    Log ReadAtIndex(int index) throws IOException;
}
