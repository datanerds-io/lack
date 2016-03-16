package io.datanerds.lack.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraClient {

    private static final Logger logger = LoggerFactory.getLogger(CassandraClient.class);
    private Cluster cluster;

    public Session init(LackConfig config) {
        Cluster.Builder clusterBuilder = new Cluster.Builder().addContactPoints(config.nodes).withPort(config.port)
                .withSocketOptions(new SocketOptions().setTcpNoDelay(true).setReuseAddress(true));
        if (config.username != null && config.password != null) {
            clusterBuilder.withCredentials(config.username, config.password);
        }

        this.cluster = clusterBuilder.build();
        logClusterInfo();
        Session session = cluster.connect(config.keyspace);
        return session;
    }

    public void close() {
        cluster.close();
    }

    private void logClusterInfo() {
        Metadata metadata = cluster.getMetadata();
        logger.info("Connected to cluster: {}; Protocol Version: {}", metadata.getClusterName(),
                cluster.getConfiguration().getProtocolOptions().getProtocolVersionEnum());

        metadata.getAllHosts()
                .forEach(host -> logger.info("Added '{}' from DC '{}' which is running '{}'", host.getAddress(),
                        host.getDatacenter(), host.getCassandraVersion().toString()));
    }
}