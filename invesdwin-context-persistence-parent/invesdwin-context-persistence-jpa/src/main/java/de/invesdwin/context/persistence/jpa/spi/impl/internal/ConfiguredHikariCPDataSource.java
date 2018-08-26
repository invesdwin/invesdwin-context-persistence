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
        switch (context.getConnectionDialect()) {
        case MYSQL:
            //https://github.com/brettwooldridge/HikariCP/wiki/MySQL-Configuration
            config.addDataSourceProperty("cachePrepStmts", "true");
            config.addDataSourceProperty("prepStmtCacheSize", "250");
            config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
            config.addDataSourceProperty("useServerPrepStmts", "true");
            config.addDataSourceProperty("useLocalSessionState", "true");
            config.addDataSourceProperty("rewriteBatchedStatements", "true");
            config.addDataSourceProperty("cacheResultSetMetadata", "true");
            config.addDataSourceProperty("cacheServerConfiguration", "true");
            config.addDataSourceProperty("elideSetAutoCommits", "true");
            config.addDataSourceProperty("maintainTimeStats", "true");
            break;
        case ORACLE:
            //https://stackoverflow.com/questions/43334145/caching-properties-using-hikaricp-in-oracle
            config.addDataSourceProperty("implicitCachingEnabled", "true");
            config.addDataSourceProperty("maxStatements", "250");
            break;
        case H2:
            //https://www.h2database.com/javadoc/org/h2/engine/DbSettings.html#QUERY_CACHE_SIZE
            config.addDataSourceProperty("queryCacheSize", "250");
            break;
        case HSQLDB:
            //https://stackoverflow.com/questions/32702866/how-to-set-cache-size-in-hsql-database
            config.addDataSourceProperty("hsqldb.cache_size", "250");
            break;
        case MSSQLSERVER:
            //mssql-jdbc https://github.com/Microsoft/mssql-jdbc/issues/166
            config.addDataSourceProperty("disableStatementPooling", "false");
            config.addDataSourceProperty("statementPoolingCacheSize", "250");
            //jtds http://jtds.sourceforge.net/faq.html
            config.addDataSourceProperty("maxStatements", "250");
            break;
        case POSTGRESQL:
            //pgjdbc-ng https://github.com/impossibl/pgjdbc-ng
            config.addDataSourceProperty("preparedStatementCacheSize", "250");
            //https://jdbc.postgresql.org/documentation/head/connect.html#connection-parameters
            config.addDataSourceProperty("preparedStatementCacheQueries", "250");
            break;
        default:
            //none
        }
    }

}
