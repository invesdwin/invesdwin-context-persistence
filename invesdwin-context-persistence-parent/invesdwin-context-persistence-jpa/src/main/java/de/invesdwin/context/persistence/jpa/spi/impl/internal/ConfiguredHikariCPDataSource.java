package de.invesdwin.context.persistence.jpa.spi.impl.internal;

import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import de.invesdwin.context.persistence.jpa.PersistenceUnitContext;
import de.invesdwin.context.persistence.jpa.scanning.datasource.ADelegateDataSource;
import de.invesdwin.context.persistence.jpa.scanning.datasource.ICloseableDataSource;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Reflections;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FTimeUnit;

@ThreadSafe
public class ConfiguredHikariCPDataSource extends ADelegateDataSource implements ICloseableDataSource {

    private final PersistenceUnitContext context;
    private final boolean logging;
    private HikariDataSource closeableDs;

    public ConfiguredHikariCPDataSource(final PersistenceUnitContext context, final boolean logging) {
        this.context = context;
        this.logging = logging;
    }

    @Override
    public void close() {
        if (closeableDs != null) {
            closeableDs.close();
            closeableDs = null;
            setDelegateDirect(null);
        }
    }

    @Override
    protected DataSource createDelegate() {
        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl(context.getConnectionUrl());
        config.setUsername(context.getConnectionUser());
        config.setPassword(context.getConnectionPassword());
        config.setDataSourceClassName(context.getConnectionDriver());

        config.setIdleTimeout(new Duration(1, FTimeUnit.MINUTES).intValue(FTimeUnit.MILLISECONDS));
        config.setMaximumPoolSize(100);
        config.setMinimumIdle(1);

        enableStatementCache(config);

        final HikariDataSource ds = new HikariDataSource(config);

        Assertions.assertThat(this.closeableDs).isNull();
        this.closeableDs = ds;

        if (logging && Reflections.classExists("org.jdbcdslog.ConnectionPoolDataSourceProxy")) {
            try {
                final org.jdbcdslog.ConnectionPoolDataSourceProxy proxy = new org.jdbcdslog.ConnectionPoolDataSourceProxy();
                proxy.setTargetDSDirect(ds);
                return proxy;
            } catch (final org.jdbcdslog.JdbcDsLogRuntimeException e) {
                throw new RuntimeException(e);
            }
        } else {
            return ds;
        }
    }

    protected void enableStatementCache(final HikariConfig config) {
        //mysql statement cache config
        switch (context.getConnectionDialect()) {
        case MYSQL:
            config.addDataSourceProperty("cachePrepStmts", "true");
            config.addDataSourceProperty("prepStmtCacheSize", "250");
            config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
            break;
        default:
            //none
        }
    }

}
