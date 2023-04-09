package com.kv.store;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

public class LogStoreImpl implements LogStore {

    private final String fileName;
    private final RandomAccessFile file;

    private final RandomAccessFile metadataFile;
    ReentrantLock lock;

    private Integer currentTerm = null;

    private int commitIndex;

    private final int votedForOffset = 1*Integer.BYTES;

    private  Log finalLog = null;


    public LogStoreImpl(String fileName, String metadataFileName) throws IOException {
        this.fileName = fileName;
        this.file = new RandomAccessFile(fileName, "rw");
        this.metadataFile = new RandomAccessFile(metadataFileName, "rw");
        setInitialTerm();
        resetVotedFor();
        commitIndex = -1;
        lock = new ReentrantLock();
    }

    @Override
    public void resetVotedFor() throws IOException {
        setVotedFor(-1);
    }

    @Override
    public void setVotedFor(int votedFor) throws IOException {
        metadataFile.seek(votedForOffset);
        metadataFile.writeInt(votedFor);
        metadataFile.getChannel().force(true);
    }

    @Override
    public Optional<Integer> getVotedFor() throws IOException {
        metadataFile.seek(votedForOffset);
        int votedFor = metadataFile.readInt();
        return (votedFor != -1) ? Optional.of(votedFor) : Optional.empty();
    }



    private void setInitialTerm() throws IOException {
        // setting initial term to 0
        setTerm(0);
    }

    @Override
    public void setTerm(int newTerm) throws IOException {
        metadataFile.seek(0);
        metadataFile.writeInt(newTerm);
        metadataFile.getChannel().force(true);
        currentTerm = newTerm;
        resetVotedFor();
    }
    @Override
    public int getCurrentTerm() throws IOException {
        if (currentTerm != null) return currentTerm;
        else {
            metadataFile.seek(0);
            currentTerm = metadataFile.readInt();
        }
        return currentTerm;
    }

    @Override
    public void WriteToIndex(Log log, int index) throws IOException {
        //finalLog = null;
        long newOffset = (long) index * Log.SIZE;
        file.seek(newOffset);
        //if (newOffset == file.length()) finalLog = log;
        file.write(log.toBytes());
        file.getChannel().force(true);
    }

    @Override
    public Optional<Log> getLastLogEntry() throws IOException {
        long offset = getEOFOffset();
        if (offset == 0)
            return Optional.empty();
        else {
            //if (finalLog != null) return Optional.of(finalLog);
            offset -= Log.SIZE;
            file.seek(offset);
            byte[] buffer = new byte[Log.SIZE];
            file.readFully(buffer);
            ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
            int index = byteBuffer.getInt();
            int term = byteBuffer.getInt();
            int key = byteBuffer.getInt();
            int value = byteBuffer.getInt();
            return Optional.of(new Log(index, term, key, value));
        }
    }

    public List<Log> readAllLogs() throws IOException {
        List<Log> logs = new ArrayList<>();
        byte[] buffer = new byte[Log.SIZE];
        long offset = 0;
        while (offset < getEOFOffset()) {
            file.seek(offset);
            file.readFully(buffer);
            ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
            int index = byteBuffer.getInt();
            int term = byteBuffer.getInt();
            int key = byteBuffer.getInt();
            int value = byteBuffer.getInt();
            logs.add(new Log(index, term, key, value));
            offset += Log.SIZE;
        }
        return logs;
    }

    public Optional<Log> ReadAtIndex(int index) throws IOException {
        long offset = (long) index * Log.SIZE;
        if (offset >= file.length()) {
            return Optional.empty(); // index out of bounds
        }
        file.seek(offset);
        byte[] buffer = new byte[Log.SIZE];
        file.readFully(buffer);
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
        int logIndex = byteBuffer.getInt();
        if (logIndex == 0 && index !=0) return Optional.empty(); // empty slot
        int term = byteBuffer.getInt();
        int key = byteBuffer.getInt();
        int value = byteBuffer.getInt();
        return Optional.of(new Log(logIndex, term, key, value));
    }

    public void close() throws IOException {
        file.close();
    }

    @Override
    public long getEOFOffset() throws IOException {
        long offset = 0;
        int idx = 0;
        while (offset < file.length()) {
            Optional<Log> optionalLog = ReadAtIndex(idx);
            if (optionalLog.isPresent() && optionalLog.get().getIndex() == -1) return offset;
            offset += Log.SIZE;
            idx++;
        }
        return offset;
    }

    @Override
    public void markEnding(int currentIndex) throws IOException {
        long offset = (long) currentIndex * Log.SIZE;
        int idx = currentIndex;
        while (offset < file.length()) {
            WriteToIndex(new Log(-1, -1,-1,-1), idx);
            offset += Log.SIZE;
            idx++;
        }
    }

    @Override
    public int getCommitIndex() {
        return commitIndex;
    }

    @Override
    public void setCommitIndex(int commitIndex) {
        this.commitIndex = commitIndex;
    }
}
