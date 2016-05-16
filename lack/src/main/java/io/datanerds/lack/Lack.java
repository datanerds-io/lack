package io.datanerds.lack;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import io.datanerds.lack.cassandra.CassandraClient;
import io.datanerds.lack.cassandra.LackConfig;
import io.datanerds.lack.cassandra.Statements;

import static io.datanerds.lack.Messages.*;
import static io.datanerds.lack.cassandra.Constants.Columns.OWNER;

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

    public void acquire(String resource) throws LackException {
        ResultSet result = statements.acquire(resource, owner);
        if (!result.wasApplied()) {
            throw new LackException(String.format(MESSAGE_ACQUIRE, resource));
        }
    }

    public void claim(String resource) throws LackException {
        ResultSet result = statements.acquire(resource, owner);
        if (!result.wasApplied()) {
            checkLockOwner(resource, result);
        }
    }

    public void renew(String resource) throws LackException {
        ResultSet result = statements.renew(resource, owner);
        if (!result.wasApplied()) {
            throw new LackException(String.format(MESSAGE_RENEW, resource));
        }
    }

    public void release(String resource) throws LackException {
        ResultSet result = statements.release(resource, owner);
        if (!result.wasApplied()) {
            throw new LackException(String.format(MESSAGE_RELEASE, resource));
        }
    }

    public void stop() {
        session.close();
        client.close();
    }

    private void checkLockOwner(String resource,  ResultSet result) throws LackException {
        if (result.isExhausted()) {
            throw new LackException(String.format(MESSAGE_ACQUIRE, resource));
        }
        String lockOwner = result.one().getString(OWNER);
        if (!owner.equals(lockOwner)) {
            throw new LackException(String.format(MESSAGE_LOCK_TAKEN, resource, lockOwner));
        }
        renew(resource);
    }

}
