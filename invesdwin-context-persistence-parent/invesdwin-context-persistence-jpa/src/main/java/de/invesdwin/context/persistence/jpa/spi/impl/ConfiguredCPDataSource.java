package de.invesdwin.context.persistence.jpa.spi.impl;

import java.beans.PropertyVetoException;

import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;

import org.jdbcdslog.ConnectionPoolDataSourceProxy;
import org.jdbcdslog.JDBCDSLogException;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.persistence.jpa.PersistenceUnitContext;
import de.invesdwin.context.persistence.jpa.scanning.datasource.ADelegateDataSource;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FTimeUnit;

@ThreadSafe
public class ConfiguredCPDataSource extends ADelegateDataSource {

    private final PersistenceUnitContext context;
    private final boolean logging;
    private ComboPooledDataSource closeableDs;

    public ConfiguredCPDataSource(final PersistenceUnitContext context, final boolean logging) {
        this.context = context;
        this.logging = logging;
    }

    public void close() {
        closeableDs.close();
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

        if (logging) {
            try {
                final ConnectionPoolDataSourceProxy proxy = new ConnectionPoolDataSourceProxy();
                proxy.setTargetDSDirect(ds);
                return proxy;
            } catch (final JDBCDSLogException e) {
                throw new RuntimeException(e);
            }
        } else {
            return ds;
        }
    }

}
