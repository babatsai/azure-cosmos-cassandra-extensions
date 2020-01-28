/*
 * The MIT License (MIT)
 *
 * Copyright (c) Microsoft. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
 * Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.microsoft.azure.cosmos.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.google.common.collect.AbstractIterator;

import javafx.util.Pair;
import sun.java2d.pipe.SpanShapeRenderer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CosmosLoadBalancingPolicy allows you to specify readDC, writeDC, and globalEndpoint
 * If readDC is specified, we considers nodes in readDC at distance LOCAL for read requests.
 * If writeDC is specified, we considers nodes in writeDC at distance LOCAL for write requests.
 * The policy also makes use of the globalContactPoint. If writeDC is not specified, we prefer to send the write
 * requests to the default write region, retrieved from the globalContactPoint. When there is a failover in the default
 * write region, we refresh the host every dnsExpirationInSeconds to retrieve the new default write region.
 * By default, it is 60 seconds.
 */
public class CosmosLoadBalancingPolicy implements LoadBalancingPolicy {
    private final AtomicInteger index = new AtomicInteger();
    private long lastDnsLookupTime = Long.MIN_VALUE;
    private InetAddress[] localAddresses = null;

    private CopyOnWriteArrayList<Host> readLocalDCHosts;
    private CopyOnWriteArrayList<Host> writeLocalDCHosts;
    private CopyOnWriteArrayList<Host> localDCHosts;
    private CopyOnWriteArrayList<Host> remoteDCHosts;

    private String readDC;
    private String writeDC;
    private String globalContactPoint;
    private int dnsExpirationInSeconds;

    public static CosmosLoadBalancingPolicy buildFrom(Builder builder) {
        return new CosmosLoadBalancingPolicy(builder.readDC, builder.writeDC, builder.globalDns, builder.dnsExpirationInSeconds);
    }

    private CosmosLoadBalancingPolicy(String readDC, String writeDC, String globalContactPoint, int dnsExpirationInSeconds) {
        this.readDC = readDC;
        this.writeDC = writeDC;
        this.globalContactPoint = globalContactPoint;
        this.dnsExpirationInSeconds = dnsExpirationInSeconds;
    }

    @Override
    public void init(Cluster cluster, Collection<Host> hosts) {
        CopyOnWriteArrayList<Host> readLocalDCAddresses = new CopyOnWriteArrayList<Host>();
        CopyOnWriteArrayList<Host> writeLocalDCAddresses = new CopyOnWriteArrayList<Host>();
        CopyOnWriteArrayList<Host> localDCAddresses = new CopyOnWriteArrayList<Host>();
        CopyOnWriteArrayList<Host> remoteDCAddresses = new CopyOnWriteArrayList<Host>();

        List<InetAddress> dnsLookupAddresses = new ArrayList<InetAddress>();
        if (!globalContactPoint.isEmpty()){
            dnsLookupAddresses = Arrays.asList(getLocalAddresses());
        }

        for (Host host : hosts) {
            if (!this.readDC.isEmpty() && host.getDatacenter().equals(readDC)){
                readLocalDCAddresses.add(host);
            }

            if (!this.writeDC.isEmpty() && host.getDatacenter().equals(writeDC)){
                writeLocalDCAddresses.add(host);
            }

            if (dnsLookupAddresses.contains(host.getAddress())) {
                localDCAddresses.add(host);
            } else {
                remoteDCAddresses.add(host);
            }
        }

        this.readLocalDCHosts = readLocalDCAddresses;
        this.writeLocalDCHosts = writeLocalDCAddresses;
        this.localDCHosts = localDCAddresses;
        this.remoteDCHosts = remoteDCAddresses;
        this.index.set(new Random().nextInt(Math.max(hosts.size(), 1)));
    }

    /**
     * Return the HostDistance for the provided host.
     *
     * <p>This policy consider the nodes in the readLocalDC and writeLocalDC at distance {@code LOCAL}.
     *
     * @param host the host of which to return the distance of.
     * @return the HostDistance to {@code host}.
     */
    @Override
    public HostDistance distance(Host host) {
        /*
        for (Host readHost: this.readLocalDCHosts) {
            if (readHost.getAddress().equals(host.getAddress())) return HostDistance.LOCAL;
        }

        for (Host writeHost: this.writeLocalDCHosts) {
            if (writeHost.getAddress().equals(host.getAddress())) return HostDistance.LOCAL;
        }
         */

        if (Arrays.asList(getLocalAddresses()).contains(host.getAddress())) {
            return HostDistance.LOCAL;
        }

        return HostDistance.REMOTE;
    }

    /**
     * Returns the hosts to use for a new query.
     *
     * <p>The returned plan will always try each known host in the default write region first, and then,
     * if none of the host is reachable, it will try all other regions.
     * The order of the local node in the returned query plan will follow a
     * Round-robin algorithm.
     *
     * @param loggedKeyspace the keyspace currently logged in on for this query.
     * @param statement the query for which to build the plan.
     * @return a new query plan, i.e. an iterator indicating which host to try first for querying,
     *     which one to use as failover, etc...
     */
    @Override
    public Iterator<Host> newQueryPlan(String loggedKeyspace, final Statement statement) {
        refreshHostsIfDnsExpired();

        final List<Host> readHosts = cloneList(this.readLocalDCHosts);
        final List<Host> writeHosts = cloneList(this.writeLocalDCHosts);
        final List<Host> localHosts = cloneList(this.localDCHosts);
        final List<Host> remoteHosts = cloneList(this.remoteDCHosts);

        final int startIdx = index.getAndIncrement();

        // Overflow protection; not theoretically thread safe but should be good enough
        if (startIdx > Integer.MAX_VALUE - 10000) {
            index.set(0);
        }

        return new AbstractIterator<Host>() {
            private int idx = startIdx;
            public int remainingRead = readHosts.size();
            public int remainingWrite = writeHosts.size();
            private int remainingLocal = localHosts.size();
            private int remainingRemote = remoteHosts.size();

            protected Host computeNext() {
                while (true) {
                    if (remainingRead > 0 && isReadRequest(statement)){
                        remainingRead--;
                        return readHosts.get(idx ++ % readHosts.size());
                    }

                    if (remainingWrite > 0 && isWriteRequest(statement)){
                        remainingWrite--;
                        return writeHosts.get(idx ++ % writeHosts.size());
                    }

                    if (remainingLocal > 0) {
                        remainingLocal--;
                        return localHosts.get(idx ++ % localHosts.size());
                    }

                    if (remainingRemote > 0) {
                        remainingRemote--;
                        return remoteHosts.get(idx ++ % remoteHosts.size());
                    }

                    return endOfData();
                }
            }
        };
    }

    private boolean isReadRequest(Statement statement) {
        if (statement instanceof RegularStatement) {
            if (statement instanceof SimpleStatement) {
                SimpleStatement simpleStatement = (SimpleStatement) statement;
                return isReadRequest(simpleStatement.getQueryString());
            } else if (statement instanceof BuiltStatement) {
                BuiltStatement builtStatement = (BuiltStatement) statement;
                return isReadRequest(builtStatement.getQueryString());
            }
        } else if (statement instanceof BoundStatement) {
            BoundStatement boundStatement = (BoundStatement) statement;
            return isReadRequest(boundStatement.preparedStatement().getQueryString());
        } else if (statement instanceof BatchStatement) {
            return false;
        }

        return false;
    }

    private boolean isWriteRequest(Statement statement) {
        if (statement instanceof RegularStatement) {
            if (statement instanceof SimpleStatement) {
                SimpleStatement simpleStatement = (SimpleStatement) statement;
                return isWriteRequest(simpleStatement.getQueryString());
            } else if (statement instanceof BuiltStatement) {
                BuiltStatement builtStatement = (BuiltStatement) statement;
                return isWriteRequest(builtStatement.getQueryString());
            }
        } else if (statement instanceof BoundStatement) {
            BoundStatement boundStatement = (BoundStatement) statement;
            return isWriteRequest(boundStatement.preparedStatement().getQueryString());
        } else if (statement instanceof BatchStatement) {
            return true;
        }

        return false;
    }

    private static boolean isWriteRequest(String query){
        String lowerCaseQuery = query.toLowerCase();
        return lowerCaseQuery.startsWith("insert") || lowerCaseQuery.startsWith("update") || lowerCaseQuery.startsWith("delete");
    }

    private static boolean isReadRequest(String query){
        return query.toLowerCase().startsWith("select");
    }

    @Override
    public void onUp(Host host) {
        if (!this.readDC.isEmpty() && host.getDatacenter().equals(this.readDC)){
            this.readLocalDCHosts.addIfAbsent(host);
        }

        if (!this.writeDC.isEmpty() && host.getDatacenter().equals(this.writeDC)){
            this.writeLocalDCHosts.addIfAbsent(host);
        }

        if (Arrays.asList(getLocalAddresses()).contains(host.getAddress())) {
            this.localDCHosts.addIfAbsent(host);
        } else {
            this.remoteDCHosts.addIfAbsent(host);
        }
    }

    @Override
    public void onDown(Host host) {
        if (!this.readDC.isEmpty() && host.getDatacenter().equals(this.readDC)){
            this.readLocalDCHosts.remove(host);
        }

        if (!this.writeDC.isEmpty() && host.getDatacenter().equals(this.writeDC)){
            this.writeLocalDCHosts.remove(host);
        }

        if (Arrays.asList(getLocalAddresses()).contains(host.getAddress())) {
            this.localDCHosts.remove(host);
        } else {
            this.remoteDCHosts.remove(host);
        }
    }

    @Override
    public void onAdd(Host host) {
        onUp(host);
    }

    @Override
    public void onRemove(Host host) {
        onDown(host);
    }

    public void close() {
        // nothing to do
    }

    @SuppressWarnings("unchecked")
    private static CopyOnWriteArrayList<Host> cloneList(CopyOnWriteArrayList<Host> list) {
        return (CopyOnWriteArrayList<Host>) list.clone();
    }

    private InetAddress[] getLocalAddresses() {
        if (this.localAddresses == null || dnsExpired()) {
            try {
                this.localAddresses = InetAddress.getAllByName(globalContactPoint);
                this.lastDnsLookupTime = System.currentTimeMillis()/1000;
            }
            catch (UnknownHostException ex) {
                // dns entry may be temporarily unavailable
                if (this.localAddresses == null) {
                    throw new IllegalArgumentException("The dns could not resolve the globalContactPoint the first time.");
                }
            }
        }

        return this.localAddresses;
    }

    private void refreshHostsIfDnsExpired() {
        if (this.localDCHosts != null && !dnsExpired()) {
            return;
        }

        CopyOnWriteArrayList<Host> oldLocalDCHosts = this.localDCHosts;
        CopyOnWriteArrayList<Host> oldRemoteDCHosts = this.remoteDCHosts;

        List<InetAddress> localAddresses = Arrays.asList(getLocalAddresses());
        CopyOnWriteArrayList<Host> localDcHosts = new CopyOnWriteArrayList<Host>();
        CopyOnWriteArrayList<Host> remoteDcHosts = new CopyOnWriteArrayList<Host>();

        for (Host host: oldLocalDCHosts) {
            if (localAddresses.contains(host.getAddress())) {
                localDcHosts.addIfAbsent(host);
            } else {
                remoteDcHosts.addIfAbsent(host);
            }
        }

        for (Host host: oldRemoteDCHosts) {
            if (localAddresses.contains(host.getAddress())) {
                localDcHosts.addIfAbsent(host);
            } else {
                remoteDcHosts.addIfAbsent(host);
            }
        }

        this.localDCHosts = localDcHosts;
        this.remoteDCHosts = remoteDcHosts;
    }

    private boolean dnsExpired() {
        return System.currentTimeMillis()/1000 > lastDnsLookupTime + dnsExpirationInSeconds;
    }

    public static class Builder {

        private String readDC = "";
        private String writeDC = "";
        private String globalDns = "";
        private int dnsExpirationInSeconds = 60;

        public Builder withReadDC(String readDC){
            this.readDC = readDC;
            return this;
        }

        public Builder withWriteDC(String writeDC){
            this.writeDC = writeDC;
            return this;
        }

        public Builder withGlobalDns(String globalDns){
            this.globalDns = globalDns;
            return this;
        }

        public Builder withDnsExpirationInSeconds(int dnsExpirationInSeconds){
            this.dnsExpirationInSeconds = dnsExpirationInSeconds;
            return this;
        }

        public CosmosLoadBalancingPolicy build(){
            return CosmosLoadBalancingPolicy.buildFrom(this);
        }
    }
}