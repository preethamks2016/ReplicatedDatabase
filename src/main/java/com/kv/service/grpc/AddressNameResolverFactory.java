package com.kv.service.grpc;

import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;

import java.net.SocketAddress;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

class AddressNameResolverFactory extends NameResolver.Factory {

    private List<EquivalentAddressGroup> addresses;

    private CustomNameResolver resolver;

    public CustomNameResolver getResolver() {
        return resolver;
    }

    public void setResolver(CustomNameResolver resolver) {
        this.resolver = resolver;
    }

    public List<EquivalentAddressGroup> getAddresses() {
        return addresses;
    }

    public void setAddresses(List<SocketAddress> addresses){
        this.addresses = addresses.stream()
                .map(EquivalentAddressGroup::new)
                .collect(Collectors.toList());

    }


    AddressNameResolverFactory(List<SocketAddress> addresses) {
        setAddresses(addresses);
    }

    public NameResolver newNameResolver(URI notUsedUri, NameResolver.Args args) {

        resolver = new CustomNameResolver(addresses);
        return resolver;
    }

    @Override
    public String getDefaultScheme() {
        return "multiaddress";
    }

    public void refresh(List<SocketAddress> serverDetails) {
        setAddresses(serverDetails);
        resolver.resolve(addresses);
    }
}
