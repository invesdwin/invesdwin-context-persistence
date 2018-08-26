package de.invesdwin.context.persistence.jpa.spi.impl.internal;

import java.beans.PropertyVetoException;

import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.persistence.jpa.PersistenceUnitContext;
import de.invesdwin.context.persistence.jpa.scanning.datasource.ADelegateDataSource;
import de.invesdwin.context.persistence.jpa.scanning.datasource.ICloseableDataSource;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Reflections;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FTimeUnit;

@ThreadSafe
public class ConfiguredC3p0DataSource extends ADelegateDataSource implements ICloseableDataSource {

    private final PersistenceUnitContext context;
    private final boolean logging;
    private ComboPooledDataSource closeableDs;

    public ConfiguredC3p0DataSource(final PersistenceUnitContext context, final boolean logging) {
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
        final ComboPooledDataSource ds = new ComboPooledDataSource();
        try {
            ds.setDriverClass(context.getConnectionDriver());
        } catch (final PropertyVetoException e1) {
            throw Err.process(e1);
        }
        ds.setJdbcUrl(context.getConnectionUrl());
        ds.setUser(context.getConnectionUser());
        ds.setPassword(context.getConnectionPassword());

        ds.setMaxStatements(5000);
        ds.setMaxStatementsPerConnection(50);
        ds.setStatementCacheNumDeferredCloseThreads(1); //fix apparent deadlocks

        ds.setMaxPoolSize(100);
        ds.setMinPoolSize(1);
        ds.setMaxIdleTime(new Duration(1, FTimeUnit.MINUTES).intValue(FTimeUnit.SECONDS));
        ds.setTestConnectionOnCheckout(true);

        ds.setAutoCommitOnClose(true);

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

}
