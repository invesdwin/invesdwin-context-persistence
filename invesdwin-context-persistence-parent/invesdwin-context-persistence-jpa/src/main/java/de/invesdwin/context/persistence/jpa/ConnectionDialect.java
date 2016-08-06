package de.invesdwin.context.persistence.jpa;

import javax.annotation.concurrent.Immutable;

@Immutable
public enum ConnectionDialect {
    MYSQL(true),
    MSSQLSERVER(true),
    POSTGRESQL(true),
    HSQLDB(true),
    H2(true),
    ORACLE(true),
    CASSANDRA(false),
    HBASE(false),
    MONGODB(false);

    private boolean rdbms;

    ConnectionDialect(final boolean rdbms) {
        this.rdbms = rdbms;
    }

    public boolean isRdbms() {
        return rdbms;
    }
}
