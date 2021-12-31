package de.invesdwin.context.persistence.jpa.eclipselink;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Named;
import javax.persistence.spi.PersistenceProvider;
import javax.sql.DataSource;

import org.eclipse.persistence.config.PersistenceUnitProperties;
import org.eclipse.persistence.config.QueryHints;
import org.eclipse.persistence.platform.database.DatabasePlatform;
import org.eclipse.persistence.platform.database.DerbyPlatform;
import org.eclipse.persistence.platform.database.H2Platform;
import org.eclipse.persistence.platform.database.HSQLPlatform;
import org.eclipse.persistence.platform.database.MySQLPlatform;
import org.eclipse.persistence.platform.database.PostgreSQLPlatform;
import org.eclipse.persistence.platform.database.SQLServerPlatform;
import org.eclipse.persistence.platform.database.SybasePlatform;
import org.eclipse.persistence.platform.database.oracle.Oracle19Platform;
import org.springframework.orm.jpa.JpaDialect;
import org.springframework.orm.jpa.JpaVendorAdapter;
import org.springframework.orm.jpa.vendor.EclipseLinkJpaDialect;
import org.springframework.orm.jpa.vendor.EclipseLinkJpaVendorAdapter;

import de.invesdwin.context.persistence.jpa.ConnectionAutoSchema;
import de.invesdwin.context.persistence.jpa.ConnectionDialect;
import de.invesdwin.context.persistence.jpa.PersistenceUnitContext;
import de.invesdwin.context.persistence.jpa.api.index.Indexes;
import de.invesdwin.context.persistence.jpa.api.query.IConfigurableQuery;
import de.invesdwin.context.persistence.jpa.spi.delegate.IDialectSpecificDelegate;
import de.invesdwin.context.persistence.jpa.spi.impl.ConfiguredDataSource;
import de.invesdwin.context.persistence.jpa.spi.impl.NativeJdbcIndexCreationHandler;
import de.invesdwin.util.error.UnknownArgumentException;

@Named
@ThreadSafe
public class EclipseLinkDialectSpecificDelegate implements IDialectSpecificDelegate {

    private final NativeJdbcIndexCreationHandler nativeJdbcIndexCreationHandler = new NativeJdbcIndexCreationHandler();

    @Override
    public void createIndexes(final PersistenceUnitContext context, final Class<?> entityClass, final Indexes indexes) {
        nativeJdbcIndexCreationHandler.createIndexes(context, entityClass, indexes);
    }

    @Override
    public void dropIndexes(final PersistenceUnitContext context, final Class<?> entityClass, final Indexes indexes) {
        nativeJdbcIndexCreationHandler.dropIndexes(context, entityClass, indexes);
    }

    @Override
    public DataSource createDataSource(final PersistenceUnitContext context) {
        return new ConfiguredDataSource(context, true);
    }

    @Override
    public Map<String, String> getPersistenceProperties(final PersistenceUnitContext context) {
        final Map<String, String> props = new HashMap<String, String>();

        props.put(PersistenceUnitProperties.WEAVING, String.valueOf(true));
        props.put(PersistenceUnitProperties.DDL_GENERATION, getEclipseLinkDdlGeneration(context));
        props.put(PersistenceUnitProperties.ORM_SCHEMA_VALIDATION, String.valueOf(true));
        props.put(PersistenceUnitProperties.TARGET_DATABASE, getDatabasePlatform(context).getName());
        props.put(PersistenceUnitProperties.DDL_GENERATION_INDEX_FOREIGN_KEYS, String.valueOf(true));

        return props;
    }

    private String getEclipseLinkDdlGeneration(final PersistenceUnitContext context) {
        final ConnectionAutoSchema autoSchema = context.getConnectionAutoSchema();
        switch (autoSchema) {
        case CREATE:
            return PersistenceUnitProperties.CREATE_ONLY;
        case CREATE_DROP:
            return PersistenceUnitProperties.DROP_AND_CREATE;
        case UPDATE:
            return PersistenceUnitProperties.CREATE_OR_EXTEND;
        case VALIDATE:
            return PersistenceUnitProperties.NONE;
        default:
            throw UnknownArgumentException.newInstance(ConnectionAutoSchema.class, autoSchema);
        }
    }

    private Class<? extends DatabasePlatform> getDatabasePlatform(final PersistenceUnitContext context) {
        final ConnectionDialect dialect = context.getConnectionDialect();
        switch (dialect) {
        case MSSQLSERVER:
            return SQLServerPlatform.class;
        case MYSQL:
            return MySQLPlatform.class;
        case POSTGRESQL:
            return PostgreSQLPlatform.class;
        case ORACLE:
            return Oracle19Platform.class;
        case H2:
            return H2Platform.class;
        case HSQLDB:
            return HSQLPlatform.class;
        case DERBY:
            return DerbyPlatform.class;
        case SYBASE:
            return SybasePlatform.class;
        default:
            throw UnknownArgumentException.newInstance(ConnectionDialect.class, dialect);
        }
    }

    @Override
    public PersistenceProvider getPersistenceProvider(final PersistenceUnitContext context) {
        return new org.eclipse.persistence.jpa.PersistenceProvider();
    }

    @Override
    public JpaVendorAdapter getJpaVendorAdapter(final PersistenceUnitContext context) {
        return new EclipseLinkJpaVendorAdapter();
    }

    @Override
    public void setCacheable(final PersistenceUnitContext context, final IConfigurableQuery query,
            final boolean cacheable) {
        query.setHint(QueryHints.CACHE_STATMENT, cacheable);
    }

    @Override
    public Set<ConnectionDialect> getSupportedDialects() {
        final Set<ConnectionDialect> supportedDialects = new HashSet<ConnectionDialect>();
        for (final ConnectionDialect dialect : ConnectionDialect.values()) {
            if (dialect.isRdbms()) {
                supportedDialects.add(dialect);
            }
        }
        return supportedDialects;
    }

    @Override
    public IDialectSpecificDelegate getOverriddenParent() {
        return null;
    }

    @Override
    public JpaDialect getJpaDialect(final PersistenceUnitContext persistenceUnitContext) {
        return new EclipseLinkJpaDialect();
    }

}
