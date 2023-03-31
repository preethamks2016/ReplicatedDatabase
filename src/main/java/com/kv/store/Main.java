package com.kv.store;

import com.kv.service.grpc.KVSClient;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("Hello world!");
        KVStore kvStore = new KVStoreImpl();
        kvStore.put(1, 5);
        kvStore.put(2, 3);
        kvStore.put(3, 7);
//        System.out.println(kvStore.get(1));


//        Log log1 = new Log(0,0,1,2);
//        Log log2 = new Log(1,2,3,4);
//        Log log3 = new Log(2,2,6,5);
//        logStore.WriteToIndex(log1, 0);
//        logStore.WriteToIndex(log2, 1);
//        logStore.WriteToIndex(log3, 2);

        ManagedChannel channel = ManagedChannelBuilder.forTarget("127.0.0.1:5051")
                .usePlaintext()
                .build();
        KVSClient client = new KVSClient(channel);
//
//        ExecutorService executor = Executors.newFixedThreadPool(10);
//        CompletionService<Boolean> completionService = new ExecutorCompletionService<>(executor);
//        for (int i = 0 ; i < 100; i++) {
//            completionService.submit(() -> {
//                client.put(1, 5);
//                return true;
//            });
//        }
//
//        Thread.sleep(15000);

//        client.put(1, 3);
//        client.put(1, 6);
//        client.put(1, 8);

//        client.appendEntries(0, -1, -1, 1, 2);
//        client.appendEntries(0, 0, 0, 1, 3);
//        client.appendEntries(0, 1, 0, 3, 4);



//        LogStore logStore1 = new LogStoreImpl("log5050.txt");
//        LogStore logStore2 = new LogStoreImpl("log5051.txt");
        LogStore logStore3 = new LogStoreImpl("log5051.txt", "meta5051.txt");
        System.out.println(logStore3.ReadAtIndex(0).get().getIndex());
        System.out.println(logStore3.ReadAtIndex(1).get().getIndex());
        System.out.println(logStore3.ReadAtIndex(2).get().getIndex());
        System.out.println(logStore3.ReadAtIndex(3).get().getIndex());
//        logStore3.WriteToIndex(new Log(0,0,1,2), 0);
//        logStore3.WriteToIndex(new Log(1,0,1,2), 1);
//        logStore3.WriteToIndex(new Log(2,0,1,2), 2);
        List<Log> logs;
//
//        logs = logStore1.readAllLogs();
//        for(Log log:logs) {
//            System.out.println("idx: "+ log.getIndex() + ", term: "+ log.getTerm() + ", key: " + log.getKey() + ", value: " + log.getValue());
//        }
//        System.out.println(logs.size());
//
//        logs = logStore2.readAllLogs();
//        for(Log log:logs) {
//            System.out.println("idx: "+ log.getIndex() + ", term: "+ log.getTerm() + ", key: " + log.getKey() + ", value: " + log.getValue());
//        }
//        System.out.println(logs.size());

        logs = logStore3.readAllLogs();
        for(Log log:logs) {
            System.out.println("idx: "+ log.getIndex() + ", term: "+ log.getTerm() + ", key: " + log.getKey() + ", value: " + log.getValue());
        }
        System.out.println(logs.size());

    }
}