package com.kv.store;

import java.io.IOException;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {
        System.out.println("Hello world!");
        KVStore kvStore = new KVStoreImpl();
        kvStore.put(1, 5);
        System.out.println(kvStore.get(1));

        LogStore logStore = new LogStoreImpl("log.txt");
        Log log1 = new Log(0,1,2);
        Log log2 = new Log(2,3,4);
        Log log3 = new Log(2,6,5);
        logStore.WriteToIndex(log1, 0);
        logStore.WriteToIndex(log2, 1);
        logStore.WriteToIndex(log3, 2);

        List<Log> logs = logStore.readAllLogs();
        System.out.println(logs.size());

    }
}