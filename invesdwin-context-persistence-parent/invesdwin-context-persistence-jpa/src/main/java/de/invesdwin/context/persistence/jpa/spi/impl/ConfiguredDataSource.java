package de.invesdwin.context.persistence.jpa.spi.impl;

import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;

import de.invesdwin.context.persistence.jpa.PersistenceUnitContext;
import de.invesdwin.context.persistence.jpa.scanning.datasource.ADelegateDataSource;
import de.invesdwin.context.persistence.jpa.scanning.datasource.ICloseableDataSource;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Reflections;

/**
 * HikariCP is normally the default for performance pursposes. Though if you want to debug database deadlocks, add a
 * dependency to c3p0 and it will automatically be used instead.
 */
@ThreadSafe
public class ConfiguredDataSource extends ADelegateDataSource implements ICloseableDataSource {

    private static final String C3P0_CLASSNAME = "com.mchange.v2.c3p0.ComboPooledDataSource";
    private static final String HIKARICP_CLASSNAME = "com.zaxxer.hikari.HikariDataSource";

    private final PersistenceUnitContext context;
    private final boolean logging;
    private ICloseableDataSource closeableDs;

    public ConfiguredDataSource(final PersistenceUnitContext context, final boolean logging) {
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
        Assertions.assertThat(this.closeableDs).isNull();
        final ICloseableDataSource ds = newDelegate();
        this.closeableDs = ds;
        return ds;
    }

    protected ICloseableDataSource newDelegate() {
        if (Reflections.classExists(C3P0_CLASSNAME)) {
            return new de.invesdwin.context.persistence.jpa.spi.impl.internal.ConfiguredC3p0DataSource(context,
                    logging);
        } else if (Reflections.classExists(HIKARICP_CLASSNAME)) {
            return new de.invesdwin.context.persistence.jpa.spi.impl.internal.ConfiguredHikariCPDataSource(context,
                    logging);
        } else {
            throw new IllegalStateException(
                    "Neither " + C3P0_CLASSNAME + ", nor " + HIKARICP_CLASSNAME + " found in classpath");
        }
    }

}
