package com.kv.service.grpc;

import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;

import java.util.List;

public class CustomNameResolver extends NameResolver {



    List<EquivalentAddressGroup> addresses;

    private volatile Listener2 listener;

    public CustomNameResolver(List<EquivalentAddressGroup> addressGroups) {
        addresses = addressGroups;
    }
    @Override
    public String getServiceAuthority() {
        return "fakeAuthority";
    }

    public void start(Listener2 listener) {
        listener.onResult(ResolutionResult.newBuilder().setAddresses(addresses).setAttributes(Attributes.EMPTY).build());
    }

    public void resolve(List<EquivalentAddressGroup> addresses) {
        this.addresses = addresses;
        refresh();
    }

    @Override
    public void refresh() {
        // Get the list of server addresses from the configuration source (e.g. ZooKeeper)
        // Notify the listener with the updated server list
        listener.onAddresses(addresses, Attributes.EMPTY);
    }

    @Override
    public void shutdown() {

    }

    public List<EquivalentAddressGroup> getAddresses() {
        return addresses;
    }

    public void setAddresses(List<EquivalentAddressGroup> addresses) {
        this.addresses = addresses;
    }
}
