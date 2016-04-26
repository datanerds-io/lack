package io.datanerds.lack;

import com.google.common.cache.Cache;
import io.datanerds.lack.cassandra.LackConfig;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.UUID;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class CachedLackTest {

    private static LackConfig config = new LackConfig(null, null, new String[]{"127.0.0.1"}, 9142, "lack", 1);

    @Mock
    private static Cache<String, Boolean> cache;
    @Mock
    private static Cache<String, Boolean> otherCache;

    private static CachedLack cachedLack;
    private static CachedLack otherCachedLack;
    private String resource;

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

        cachedLack = new CachedLack(config, "lack", cache);
        otherCachedLack = new CachedLack(config, "otherLack", otherCache);
    }

    @Test
    public void simpleAcquireRenewAndRelease() throws LackException {
        cachedLack.acquire(resource);
        cachedLack.renew(resource);
        cachedLack.release(resource);

        verify(cache, times(1)).getIfPresent(resource);
        verify(cache, times(2)).put(resource, true);
        verify(cache, times(1)).put(resource, false);
    }

    @Test
    public void alreadyLocked() throws LackException {
        cachedLack.acquire(resource);
        thrown.expect(LackException.class);
        cachedLack.acquire(resource);

        verify(cache, times(2)).getIfPresent(resource);
        verify(cache, times(1)).put(resource, true);
        verify(cache, never()).put(resource, false);
    }

    @Test
    public void alreadyLockedByOther() throws LackException {
        cachedLack.acquire(resource);
        thrown.expect(LackException.class);
        otherCachedLack.acquire(resource);

        verify(cache, times(1)).getIfPresent(resource);
        verify(cache, times(1)).put(resource, true);
        verify(cache, never()).put(resource, false);

        verify(otherCache, times(1)).getIfPresent(resource);
        verify(otherCache, never()).put(resource, true);
        verify(otherCache, never()).put(resource, false);
    }

    @Test
    public void alreadyReleased() throws LackException {
        cachedLack.acquire(resource);
        cachedLack.release(resource);
        thrown.expect(LackException.class);
        otherCachedLack.release(resource);

        verify(cache, times(1)).getIfPresent(resource);
        verify(cache, never()).put(resource, true);
        verify(cache, times(1)).put(resource, false);

        verify(otherCache, never()).getIfPresent(resource);
        verify(otherCache, never()).put(resource, true);
        verify(otherCache, never()).put(resource, false);
    }

    @Test
    public void testReleaseAfterTtl() throws Exception {
        cachedLack.acquire(resource);
        Thread.sleep(1200);
        cachedLack.acquire(resource);

        verify(cache, times(2)).getIfPresent(resource);
        verify(cache, times(2)).put(resource, true);
        verify(cache, never()).put(resource, false);
    }
}
