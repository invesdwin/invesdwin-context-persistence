package de.invesdwin.context.persistence.jpa;

import javax.annotation.concurrent.Immutable;

@Immutable
public enum ConnectionDialect {
    MARIADB(true),
    MYSQL(true),
    MSSQLSERVER(true),
    POSTGRESQL(true),
    HSQLDB(true),
    H2(true),
    ORACLE(true),
    DERBY(true),
    SYBASE(true),
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
