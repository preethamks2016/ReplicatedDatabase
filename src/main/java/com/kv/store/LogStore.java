package com.kv.store;

import java.io.IOException;
import java.util.List;

public interface LogStore {
    void WriteToIndex (Log log, int index) throws IOException;
    void WriteAtEnd(Log log) throws IOException;
    List<Log> readAllLogs() throws IOException;
}
