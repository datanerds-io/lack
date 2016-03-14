package io.datanerds.lack;

import io.datanerds.lack.cassandra.LackConfig;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.util.UUID;

public class LackTest {

    private static LackConfig config = new LackConfig(null, null, new String[]{"127.0.0.1"}, "lack", 1);
    private static Lack lack;
    private static Lack otherLack;
    private String resource;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setup() {
        lack = new Lack(config, "lack");
        otherLack = new Lack(config, "otherLack");
    }

    @AfterClass
    public static void tearDown() {
        lack.stop();
    }

    @Before
    public void setupResource() {
        this.resource = UUID.randomUUID().toString();
    }

    @Test
    public void simpleAcquireRenewAndRelease() {
        lack.acquire(resource);
        lack.renew(resource);
        lack.release(resource);
    }

    @Test
    public void alreadyLocked() {
        lack.acquire(resource);
        thrown.expect(LackException.class);
        lack.acquire(resource);
    }

    @Test
    public void alreadyLockedByOther() {
        lack.acquire(resource);
        thrown.expect(LackException.class);
        otherLack.acquire(resource);
    }

    @Test
    public void alreadyReleased() {
        lack.acquire(resource);
        lack.release(resource);
        thrown.expect(LackException.class);
        otherLack.release(resource);
    }

    @Test
    public void testReleaseAfterTtl() throws InterruptedException {
        lack.acquire(resource);
        Thread.sleep(1200);
        lack.acquire(resource);
    }
}