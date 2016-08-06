package de.invesdwin.context.persistence.jpa.hibernate;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;

import org.hibernate.HibernateException;
import org.hibernate.cfg.Environment;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.service.UnknownUnwrapTypeException;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.Stoppable;

import de.invesdwin.context.beans.init.MergedContext;

@ThreadSafe
public class BeanLookupDatasourceConnectionProvider implements ConnectionProvider, Configurable, Stoppable {
    private DataSource dataSource;

    private boolean available;

    @Override
    public boolean isUnwrappableAs(final Class unwrapType) {
        return ConnectionProvider.class.equals(unwrapType)
                || BeanLookupDatasourceConnectionProvider.class.isAssignableFrom(unwrapType)
                || DataSource.class.isAssignableFrom(unwrapType);
    }

    @Override
    @SuppressWarnings({ "unchecked" })
    public <T> T unwrap(final Class<T> unwrapType) {
        if (ConnectionProvider.class.equals(unwrapType)
                || BeanLookupDatasourceConnectionProvider.class.isAssignableFrom(unwrapType)) {
            return (T) this;
        } else if (DataSource.class.isAssignableFrom(unwrapType)) {
            return (T) dataSource;
        } else {
            throw new UnknownUnwrapTypeException(unwrapType);
        }
    }

    @Override
    public void configure(final Map configValues) {
        if (this.dataSource == null) {
            final Object dataSource = configValues.get(Environment.DATASOURCE);
            if (DataSource.class.isInstance(dataSource)) {
                this.dataSource = (DataSource) dataSource;
            } else {
                final String dataSourceBeanName = (String) dataSource;
                this.dataSource = (DataSource) MergedContext.getInstance().getBean(dataSourceBeanName);
            }
        }
        if (this.dataSource == null) {
            throw new HibernateException("Unable to determine appropriate DataSource to use");
        }

        available = true;
    }

    @Override
    public void stop() {
        available = false;
        dataSource = null;
    }

    @Override
    public Connection getConnection() throws SQLException {
        if (!available) {
            throw new HibernateException("Provider is closed!");
        }
        return dataSource.getConnection();
    }

    @Override
    public void closeConnection(final Connection connection) throws SQLException {
        connection.close();
    }

    @Override
    public boolean supportsAggressiveRelease() {
        return true;
    }
}
