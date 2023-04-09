package com.kv.service.grpc.exception;

public class NoLongerLeaderException extends Exception {
    public int leaderPort = -1;
    public NoLongerLeaderException(int newPort) {
        super();
        leaderPort = newPort;
    }


}
