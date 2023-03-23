package org.example;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");
        KVStore kvStore = new KVStoreImpl();
        kvStore.put(1, 5);
        System.out.println(kvStore.get(1));

    }
}