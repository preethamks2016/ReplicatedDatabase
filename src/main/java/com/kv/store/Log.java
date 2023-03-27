package com.kv.store;

import java.nio.ByteBuffer;

public class Log {
    public static final int SIZE = Integer.BYTES * 3;
    int term;
    int key;
    int value;

    public Log(int term, int key, int value) {
        this.term = term;
        this.key = key;
        this.value = value;
    }

    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(SIZE);
        buffer.putInt(term);
        buffer.putInt(key);
        buffer.putInt(value);
        return buffer.array();
    }
}
