package io.datanerds.lack;

import io.datanerds.lack.cassandra.LackConfig;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.util.UUID;

public class LackTest {

    private static LackConfig config = new LackConfig(null, null, new String[]{"127.0.0.1"}, 9142, "lack", 3);
    private static Lack lack;
    private static Lack otherLack;
    private String resource;
    private static boolean isSetupDone = false;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @ClassRule
    public static CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(
            new ClassPathCQLDataSet("setup.cql", "lack"));

    @Before
    public void setup() throws InterruptedException {
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
        Thread.sleep(3400);
        otherLack.acquire(resource);
    }

    @Test
    public void renewLocks() throws LackException, InterruptedException {
        lack.acquire(resource);
        Thread.sleep(2000);
        lack.renew(resource);
        Thread.sleep(1500);
        thrown.expect(LackException.class);
        otherLack.acquire(resource);
    }

    @Test
    public void claimLocks() throws LackException, InterruptedException {
        lack.acquire(resource);
        Thread.sleep(2000);
        lack.claim(resource);
        Thread.sleep(1500);
        thrown.expect(LackException.class);
        otherLack.acquire(resource);
    }

    @Test
    public void simpleClaimRenewAndRelease() throws LackException {
        lack.claim(resource);
        lack.renew(resource);
        lack.release(resource);
    }

    @Test
    public void claimTwice() throws LackException {
        lack.claim(resource);
        lack.claim(resource);
    }

}