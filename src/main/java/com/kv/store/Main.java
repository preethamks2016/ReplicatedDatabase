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

        ManagedChannel channel = ManagedChannelBuilder.forTarget("127.0.0.1:5052")
                .usePlaintext()
                .build();
        KVSClient client = new KVSClient(channel);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CompletionService<Boolean> completionService = new ExecutorCompletionService<>(executor);
        for (int i = 0 ; i < 1; i++) {
            client.put(i+1, i*2);
        }



        Thread.sleep(10000);




        LogStore logStore1 = new LogStoreImpl("log5050.txt", "meta5050.txt");
        LogStore logStore2 = new LogStoreImpl("log5051.txt", "meta5051.txt");
        LogStore logStore3 = new LogStoreImpl("log5052.txt", "meta5052.txt");
        List<Log> logs;
//
        System.out.println(logStore3.ReadAtIndex(11).get().getKey());

        logs = logStore1.readAllLogs();
        for(Log log:logs) {
            System.out.println("idx: "+ log.getIndex() + ", term: "+ log.getTerm() + ", key: " + log.getKey() + ", value: " + log.getValue());
        }
        System.out.println(logs.size());

        logs = logStore2.readAllLogs();
        for(Log log:logs) {
            System.out.println("idx: "+ log.getIndex() + ", term: "+ log.getTerm() + ", key: " + log.getKey() + ", value: " + log.getValue());
        }
        System.out.println(logs.size());

        logs = logStore3.readAllLogs();
        for(Log log:logs) {
            System.out.println("idx: "+ log.getIndex() + ", term: "+ log.getTerm() + ", key: " + log.getKey() + ", value: " + log.getValue());
        }
        System.out.println(logs.size());

    }
}