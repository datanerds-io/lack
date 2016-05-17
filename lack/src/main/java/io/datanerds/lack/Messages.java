package io.datanerds.lack;

public interface Messages {

    String MESSAGE_ACQUIRE = "Could not acquire lock for '%s'";
    String MESSAGE_LOCK_TAKEN = "Lock for '%s' already held by '%s'";
    String MESSAGE_RENEW = "Could not renew lock for '%s'";
    String MESSAGE_RELEASE = "Could not release lock for '%s'";

}
