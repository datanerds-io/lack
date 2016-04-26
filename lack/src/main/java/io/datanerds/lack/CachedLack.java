package io.datanerds.lack;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.datanerds.lack.cassandra.LackConfig;

import java.util.concurrent.TimeUnit;

import static io.datanerds.lack.Messages.MESSAGE_ACQUIRE;

public class CachedLack extends Lack {

    private final Cache<String, Boolean> cache;

    public CachedLack(LackConfig config, String owner, Cache<String, Boolean> cache) {
        super(config, owner);
        this.cache = cache;
    }

    public CachedLack(LackConfig config, String owner, long size) {
        this(config, owner, CacheBuilder.newBuilder()
                .maximumSize(size)
                .expireAfterWrite(config.ttlInSeconds, TimeUnit.SECONDS)
                .build());
    }

    @Override
    public void acquire(String resource) throws LackException {
        Boolean locked = cache.getIfPresent(resource);
        if (locked != null && locked) {
            throw new LackException(String.format(MESSAGE_ACQUIRE, resource));
        }

        super.acquire(resource);
        cache.put(resource, true);
    }

    @Override
    public void renew(String resource) throws LackException {
        super.renew(resource);
        cache.put(resource, true);
    }

    @Override
    public void release(String resource) throws LackException {
        super.release(resource);
        cache.put(resource, false);
    }

}
