package io.datanerds.lack.cassandra;

public interface Constants {
    String TABLE = "leases";

    interface Columns {
        String RESOURCE = "resource";
        String OWNER = "owner";
    }
}
