package de.invesdwin.context.persistence.jpa.scanning.datasource;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;

@ThreadSafe
public abstract class ADelegateDataSource implements DataSource {

    private DataSource delegate;

    protected abstract DataSource createDelegate();

    public synchronized DataSource getDelegate() {
        if (delegate == null) {
            delegate = createDelegate();
        }
        return delegate;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return getDelegate().getLogWriter();
    }

    @Override
    public void setLogWriter(final PrintWriter out) throws SQLException {
        getDelegate().setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(final int seconds) throws SQLException {
        getDelegate().setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return getDelegate().getLoginTimeout();
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return getDelegate().getParentLogger();
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        return getDelegate().unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return getDelegate().isWrapperFor(iface);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return getDelegate().getConnection();
    }

    @Override
    public Connection getConnection(final String username, final String password) throws SQLException {
        return getDelegate().getConnection(username, password);
    }

}
