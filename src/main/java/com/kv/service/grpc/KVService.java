package com.kv.service.grpc;

import com.kvs.Kvservice;
import io.grpc.stub.StreamObserver;

public abstract class KVService {


    public abstract void put(int key, int value);
    public abstract void start();

}
