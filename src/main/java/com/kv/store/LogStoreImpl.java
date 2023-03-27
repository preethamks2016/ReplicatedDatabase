package com.kv.store;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class LogStoreImpl implements LogStore {

    private final String fileName;
    private final RandomAccessFile file;

    public LogStoreImpl(String fileName) throws IOException {
        this.fileName = fileName;
        this.file = new RandomAccessFile(fileName, "rw");
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

    public List<Log> readAllLogs() throws IOException {
        List<Log> logs = new ArrayList<>();
        byte[] buffer = new byte[Log.SIZE];
        long offset = 0;
        while (offset < file.length()) {
            file.seek(offset);
            file.readFully(buffer);
            ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
            int term = byteBuffer.getInt();
            int key = byteBuffer.getInt();
            int value = byteBuffer.getInt();
            logs.add(new Log(term, key, value));
            offset += Log.SIZE;
        }
        return logs;
    }

    public void close() throws IOException {
        file.close();
    }
}
