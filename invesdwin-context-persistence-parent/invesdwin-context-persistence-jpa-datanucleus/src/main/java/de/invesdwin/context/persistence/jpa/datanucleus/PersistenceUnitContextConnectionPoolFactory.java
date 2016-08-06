package de.invesdwin.context.persistence.jpa.datanucleus;

import javax.annotation.concurrent.Immutable;
import javax.sql.DataSource;

import org.datanucleus.store.StoreManager;
import org.datanucleus.store.rdbms.connectionpool.ConnectionPool;
import org.datanucleus.store.rdbms.connectionpool.ConnectionPoolFactory;

import de.invesdwin.context.persistence.jpa.PersistenceProperties;
import de.invesdwin.context.persistence.jpa.PersistenceUnitContext;
import de.invesdwin.context.persistence.jpa.spi.impl.ConfiguredCPDataSource;

@Immutable
public class PersistenceUnitContextConnectionPoolFactory implements ConnectionPoolFactory {

    @Override
    public ConnectionPool createConnectionPool(final StoreManager storeMgr) {
        final String persistenceUnitName = storeMgr.getConnectionUserName();
        final PersistenceUnitContext context = PersistenceProperties.getPersistenceUnitContext(persistenceUnitName);
        final ConfiguredCPDataSource ds = new ConfiguredCPDataSource(context, false);
        return new PersistenceUnitContextConnectionPool(ds);
    }

    public static class PersistenceUnitContextConnectionPool implements ConnectionPool {
        private final ConfiguredCPDataSource dataSource;

        public PersistenceUnitContextConnectionPool(final ConfiguredCPDataSource ds) {
            dataSource = ds;
        }

        @Override
        public void close() {
            dataSource.close();
        }

        @Override
        public DataSource getDataSource() {
            return dataSource;
        }
    }

}
