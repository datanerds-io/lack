package io.datanerds.lack;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import io.datanerds.lack.cassandra.CassandraClient;
import io.datanerds.lack.cassandra.LackConfig;
import io.datanerds.lack.cassandra.Statements;

public class Lack {

    private final String owner;
    private final CassandraClient client;
    private final Session session;
    private final Statements statements;

    public Lack(LackConfig lackConfig, String owner) {
        this.owner = owner;
        client = new CassandraClient();
        session = client.init(lackConfig);
        statements = new Statements(session);
        statements.create();
        statements.setTtl(lackConfig.ttlInSeconds);
    }

    public void acquire(String resource) {
        ResultSet result = statements.acquire(resource, owner);
        if (!result.wasApplied()) {
            throw new LackException(String.format("Could not acquire lock for '%s'", resource));
        }
    }

    public void renew(String resource) {
        ResultSet result = statements.renew(resource, owner);
        if (!result.wasApplied()) {
            throw new LackException(String.format("Could not renew lock for '%s'", resource));
        }
    }

    public void release(String resource) {
        ResultSet result = statements.release(resource, owner);
        if (!result.wasApplied()) {
            throw new LackException(String.format("Could not release lock for '%s'", resource));
        }
    }

    public void stop() {
        session.close();
        client.close();
    }
}
