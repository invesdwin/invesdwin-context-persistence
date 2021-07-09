package de.invesdwin.context.persistence.jpa.spi.impl.internal;

import java.beans.PropertyVetoException;

import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.persistence.jpa.PersistenceProperties;
import de.invesdwin.context.persistence.jpa.PersistenceUnitContext;
import de.invesdwin.context.persistence.jpa.scanning.datasource.ADelegateDataSource;
import de.invesdwin.context.persistence.jpa.scanning.datasource.ICloseableDataSource;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

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
        if (logging && PersistenceProperties.IS_P6SPY_AVAILABLE) {
            final com.p6spy.engine.spy.P6DataSource proxy = new com.p6spy.engine.spy.P6DataSource(ds);
            return proxy;
        } else {
            return ds;
        }
    }

}
