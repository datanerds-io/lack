package io.datanerds.lack.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.schemabuilder.Create;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.datastax.driver.core.DataType.text;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.datastax.driver.core.schemabuilder.SchemaBuilder.alterTable;
import static com.datastax.driver.core.schemabuilder.SchemaBuilder.createTable;
import static io.datanerds.lack.cassandra.Constants.Columns.OWNER;
import static io.datanerds.lack.cassandra.Constants.Columns.RESOURCE;
import static io.datanerds.lack.cassandra.Constants.TABLE;

public class Statements {

    private static final Logger logger = LoggerFactory.getLogger(Statements.class);
    private final Map<BuiltStatement, PreparedStatement> statements = new ConcurrentHashMap<>();
    private final Session session;

    private final Create create = createTable(TABLE).addPartitionKey(RESOURCE, text())
            .addColumn(OWNER, text())
            .ifNotExists();

    private final Insert acquire = insertInto(TABLE).value(RESOURCE, bindMarker())
            .value(OWNER, bindMarker())
            .ifNotExists();

    private final Update.Conditions renew = update(TABLE).with(set(OWNER, bindMarker()))
            .where(eq(RESOURCE, bindMarker()))
            .onlyIf(eq(OWNER, bindMarker()));

    private final Delete.Conditions release = delete().from(TABLE)
            .where(eq(RESOURCE, bindMarker()))
            .onlyIf(eq(OWNER, bindMarker()));

    public Statements(Session session) {
        this.session = session;
    }

    public void create() {
        session.execute(create);
    }

    public void setTtl(int ttl) {
        logger.info("Setting a default TTL of '{}' seconds", ttl);
        session.execute(alterTable(TABLE).withOptions().defaultTimeToLive(ttl));
    }

    public ResultSet acquire(String resource, String owner) {
        BoundStatement boundStatement = statements.computeIfAbsent(acquire, session::prepare).bind(resource, owner);
        return session.execute(boundStatement);
    }

    public ResultSet renew(String resource, String owner) {
        BoundStatement boundStatement = statements.computeIfAbsent(renew, session::prepare)
                .bind(owner, resource, owner);
        return session.execute(boundStatement);
    }

    public ResultSet release(String resource, String owner) {
        BoundStatement boundStatement = statements.computeIfAbsent(release, session::prepare).bind(resource, owner);
        return session.execute(boundStatement);

    }
}