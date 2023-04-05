package com.kv.store;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface LogStore {
    void setTerm(int newTerm) throws IOException;

    int getCurrentTerm() throws IOException;

    void resetVotedFor() throws IOException;

    void setVotedFor(int votedFor) throws IOException;

    Optional<Integer> getVotedFor() throws IOException;

    void WriteToIndex (Log log, int index) throws IOException;
    Optional<Log> getLastLogEntry() throws IOException;

    List<Log> readAllLogs() throws IOException;

    Optional<Log> ReadAtIndex(int index) throws IOException;

    long getEOFOffset() throws IOException;

    void markEnding(int currentIndex) throws IOException;

    int getCommitIndex();

    void setCommitIndex(int commitIndex);
}
