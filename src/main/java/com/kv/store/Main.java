package com.kv.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kv.service.grpc.KVSClient;
import com.kv.service.grpc.exception.NoLongerLeaderException;
import com.kvs.Kvservice;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) throws Exception {

        ObjectMapper objectMapper = new ObjectMapper();
        File configFile = new File("servers.json");
        Map<String, Object> configMap = objectMapper.readValue(configFile, Map.class);
        List<Map<String, Object>> allServers = (List<Map<String, Object>>) configMap.get("servers");
        Map<String, KVSClient> portToClient = new HashMap<String, KVSClient>();

        for (Map<String, Object> server : allServers) {
            ManagedChannel channel = ManagedChannelBuilder.forTarget(server.get("ip").toString() + ":" + server.get("port").toString())
                    .usePlaintext()
                    .enableRetry()
                    .maxRetryAttempts(10000)
                    .build();

            portToClient.put(server.get("port").toString(), new KVSClient(channel));
        }

        KVSClient client = portToClient.get("5050");

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("> ");
            String input = scanner.nextLine();
            input = input.trim();
            try {
                if (input.startsWith("get")) {
                    String key = input.substring(4).trim();
                    int response = client.get(Integer.parseInt(key));
                    System.out.println("Value: " + response);
                } else if (input.startsWith("put")) {
                    String[] parts = input.split(" ");
                    if (parts.length < 3) {
                        System.out.println("Invalid command");
                    } else {
                        int key = Integer.parseInt(parts[1]);
                        int value = Integer.parseInt(parts[2]);
                        client.put(key, value);
                    }
                } else if (input.length() == 0) {
                    continue;
                } else {
                    System.out.println("Invalid command");
                }
            }
            catch (NoLongerLeaderException ex) {
                System.out.println("Changing to new leader with port: " + ex.leaderPort);
                client = portToClient.get(Integer.toString(ex.leaderPort));
            }
            catch (StatusRuntimeException ex) {
                String p = allServers.get((int)Math.random()%allServers.size()).get("port").toString();
                System.out.println("Server unavailable ! trying another port " + p);
                client = portToClient.get(p);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }



//        System.out.println("Hello world!");
//        KVStore kvStore = new KVStoreImpl();
//        kvStore.put(1, 5);
//        kvStore.put(2, 3);
//        kvStore.put(3, 7);
//
//        ManagedChannel channel = ManagedChannelBuilder.forTarget("127.0.0.1:5051")
//                .usePlaintext()
//                .build();
//        KVSClient client = new KVSClient(channel);
//
//        ExecutorService executor = Executors.newFixedThreadPool(5);
//        CompletionService<Kvservice.PutResponse> completionService = new ExecutorCompletionService<Kvservice.PutResponse>(executor);
//
////        for (int i = 0; i < 10; i++) {
////            int finalI = i;
////            completionService.submit(() -> {
////                Kvservice.PutResponse res =  null;
////                client.put(1, finalI *2);
////                System.out.println("Put request completed for i = " + finalI);
////                return res;
////            });
////        }
////
////        Thread.sleep(3000);
//
//        System.out.println("Got response: " + client.get(1));




//        LogStore logStore1 = new LogStoreImpl("log5050.txt", "meta5050.txt");
//        LogStore logStore2 = new LogStoreImpl("log5051.txt", "meta5051.txt");
//        LogStore logStore3 = new LogStoreImpl("log5052.txt", "meta5052.txt");
//        List<Log> logs;
////
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
//
//        logs = logStore3.readAllLogs();
//        for(Log log:logs) {
//            System.out.println("idx: "+ log.getIndex() + ", term: "+ log.getTerm() + ", key: " + log.getKey() + ", value: " + log.getValue());
//        }
//        System.out.println(logs.size());

    }
}