package com.kv.store;

import lombok.Getter;

import java.nio.ByteBuffer;

@Getter
public class Log {
    public static final int SIZE = Integer.BYTES * 4;

    int index;
    int term;
    int key;
    int value;

    public Log(int index, int term, int key, int value) {
        this.index = index;
        this.term = term;
        this.key = key;
        this.value = value;
    }

    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(SIZE);
        buffer.putInt(index);
        buffer.putInt(term);
        buffer.putInt(key);
        buffer.putInt(value);
        return buffer.array();
    }
}
