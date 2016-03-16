package io.datanerds.lack.cassandra;

public class LackConfig {

    public final String username;
    public final String password;
    public final String[] nodes;
    public final String keyspace;
    public final int ttlInSeconds;
    public int port = 9142;

    public LackConfig(String username, String password, String[] nodes, String keyspace, int ttlInSeconds) {
        this.username = username;
        this.password = password;
        this.nodes = nodes;
        this.keyspace = keyspace;
        this.ttlInSeconds = ttlInSeconds;
    }

    public LackConfig(String username, String password, String[] nodes, int port, String keyspace, int ttlInSeconds) {
        this.username = username;
        this.password = password;
        this.nodes = nodes;
        this.keyspace = keyspace;
        this.ttlInSeconds = ttlInSeconds;
        this.port = port;
    }
}
