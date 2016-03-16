package io.datanerds.lack;

import io.datanerds.lack.cassandra.LackConfig;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.util.UUID;

public class LackTest {

    private static LackConfig config = new LackConfig(null, null, new String[]{"127.0.0.1"}, "lack", 1);
    private static Lack lack;
    private static Lack otherLack;
    private String resource;
    private static boolean isSetupDone = false;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @ClassRule
    public static CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(
            new ClassPathCQLDataSet("setup.cql", "lack"));


    @BeforeClass
    public static void setupCassandra() throws Exception {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
    }

    @Before
    public void setup() {
        this.resource = UUID.randomUUID().toString();
        if (isSetupDone) {
            return;
        }
        this.lack = new Lack(config, "lack");
        this.otherLack = new Lack(config, "otherLack");
        isSetupDone = true;
    }


    @Test
    public void simpleAcquireRenewAndRelease() throws LackException {
        lack.acquire(resource);
        lack.renew(resource);
        lack.release(resource);
    }

    @Test
    public void alreadyLocked() throws LackException {
        lack.acquire(resource);
        thrown.expect(LackException.class);
        lack.acquire(resource);
    }

    @Test
    public void alreadyLockedByOther() throws LackException {
        lack.acquire(resource);
        thrown.expect(LackException.class);
        otherLack.acquire(resource);
    }

    @Test
    public void alreadyReleased() throws LackException {
        lack.acquire(resource);
        lack.release(resource);
        thrown.expect(LackException.class);
        otherLack.release(resource);
    }

    @Test
    public void testReleaseAfterTtl() throws Exception {
        lack.acquire(resource);
        Thread.sleep(1200);
        lack.acquire(resource);
    }
}