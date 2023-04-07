package com.exactpro.cradle.cassandra.integration;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import org.testcontainers.containers.CassandraContainer;

import java.net.InetSocketAddress;

public class Connector implements AutoCloseable {

    private final CqlSession session;

    /**
     * Creates a {@link Connector} to the cassandra database with the specified parameters.
     *
     * @param host            The cluster host address.
     * @param port            The cluster port.
     * @param username        The username to be used when executing queries.
     * @param password        The password for the specified user.
     * @param localDataCenter The name of the datacenter to be used.
     * @param keyspace        [Optional] The name of the keyspace to be used.
     * @return instance of [Connector]
     */
    public static Connector connect(
            String host, Integer port, String username, String password, String localDataCenter, String keyspace
    ) {
        CqlSessionBuilder builder = CqlSession.builder();
        builder.addContactPoint(new InetSocketAddress(host, port));
        builder.withAuthCredentials(username, password);
        builder.withLocalDatacenter(localDataCenter);

        if (keyspace != null) {
            builder.withKeyspace(keyspace);
        }

        return new Connector(builder.build());
    }

    public static Connector connect(CassandraContainer cassandra) {
        return connect(
                cassandra.getHost(),
                cassandra.getFirstMappedPort(),
                cassandra.getUsername(),
                cassandra.getPassword(),
                cassandra.getLocalDatacenter(),
                null);
    }


    public CqlSession getSession() {
        return session;
    }

    public void close() {
        session.close();
    }

    private Connector(CqlSession session) {
        this.session = session;
    }
}
