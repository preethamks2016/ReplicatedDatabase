package com.kv.store;

import com.kvs.Kvservice;

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

    ReentrantLock lock;


    public LogStoreImpl(String fileName) throws IOException {
        this.fileName = fileName;
        this.file = new RandomAccessFile(fileName, "rw");
        lock = new ReentrantLock();
        long offset = file.length();
    }

    @Override
    public void WriteToIndex(Log log, int index) throws IOException {
        long offset = index * Log.SIZE;
        if (offset != file.getFilePointer()) {
            file.seek(offset);
        }
        file.write(log.toBytes());
        file.getChannel().force(true);
    }

    @Override
    public void WriteAtEnd(Log log) throws IOException {
        long offset = file.length();
        file.seek(offset);
        file.write(log.toBytes());
        file.getChannel().force(true);
    }

    @Override
    public Optional<Log> getLastLogEntry() throws IOException {
        long offset = file.length();
        if (offset == 0)
            return Optional.empty();
        else {
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
        while (offset < file.length()) {
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

    public Log ReadAtIndex(int index) throws IOException {
        long offset = index * Log.SIZE;
        if (offset >= file.length()) {
            return null; // index out of bounds
        }
        file.seek(offset);
        byte[] buffer = new byte[Log.SIZE];
        file.readFully(buffer);
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
        index = byteBuffer.getInt();
        int term = byteBuffer.getInt();
        int key = byteBuffer.getInt();
        int value = byteBuffer.getInt();
        return new Log(index, term, key, value);
    }

    public void close() throws IOException {
        file.close();
    }
}
