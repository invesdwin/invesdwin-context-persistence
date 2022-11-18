package de.invesdwin.context.persistence.jpa.hibernate;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;
import jakarta.inject.Named;
import javax.sql.DataSource;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.dialect.DerbyTenSevenDialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.H2Dialect;
import org.hibernate.dialect.HSQLDialect;
import org.hibernate.dialect.MariaDB106Dialect;
import org.hibernate.dialect.MySQL8Dialect;
import org.hibernate.dialect.Oracle12cDialect;
import org.hibernate.dialect.PostgreSQL95Dialect;
import org.hibernate.dialect.SQLServer2016Dialect;
import org.hibernate.dialect.SybaseASE157Dialect;
import org.hibernate.jpa.HibernatePersistenceProvider;
import org.springframework.orm.jpa.JpaDialect;
import org.springframework.orm.jpa.JpaVendorAdapter;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;

import de.invesdwin.context.jcache.CacheBuilder;
import de.invesdwin.context.persistence.jpa.ConnectionAutoSchema;
import de.invesdwin.context.persistence.jpa.ConnectionDialect;
import de.invesdwin.context.persistence.jpa.PersistenceUnitContext;
import de.invesdwin.context.persistence.jpa.api.index.Indexes;
import de.invesdwin.context.persistence.jpa.api.query.IConfigurableQuery;
import de.invesdwin.context.persistence.jpa.hibernate.internal.HibernateExtendedJpaDialect;
import de.invesdwin.context.persistence.jpa.spi.delegate.IDialectSpecificDelegate;
import de.invesdwin.context.persistence.jpa.spi.impl.ConfiguredDataSource;
import de.invesdwin.context.persistence.jpa.spi.impl.NativeJdbcIndexCreationHandler;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;
import jakarta.persistence.spi.PersistenceProvider;

@Named
@ThreadSafe
public class HibernateDialectSpecificDelegate implements IDialectSpecificDelegate {

    private static final String CLASS_JCACHEREGIONFACTORY_OLD = "org.hibernate.cache.jcache.JCacheRegionFactory";
    private static final String CLASS_JCACHEREGIONFACTORY_NEW = "org.hibernate.cache.jcache.internal.JCacheRegionFactory";
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
        //        <entry key="hibernate.dialect" value="org.hibernate.dialect.H2Dialect" />
        final Map<String, String> props = new HashMap<String, String>();

        //        <prop key="hibernate.dialect">${hibernate.dialect}</prop>
        props.put(AvailableSettings.DIALECT, getHibernateDialect(context).getName());
        //        <prop key="hibernate.hbm2ddl.auto">${hibernate.hbm2ddl.auto}</prop>
        props.put(AvailableSettings.HBM2DDL_AUTO, getHibernateHbm2DdlAuto(context));
        //        <!-- Performance settings -->
        //        <prop key="hibernate.bytecode.use_reflection_optimizer">true</prop>
        props.put(AvailableSettings.USE_REFLECTION_OPTIMIZER, String.valueOf(true));
        //        <prop key="hibernate.max_fetch_depth">3</prop>
        props.put(AvailableSettings.MAX_FETCH_DEPTH, String.valueOf(3));
        //        <prop key="hibernate.jdbc.fetch_size">${de.invesdwin.context.persistence.jpa.PersistenceProperties.CONNECTION_BATCH_SIZE}</prop>
        props.put(AvailableSettings.STATEMENT_FETCH_SIZE, String.valueOf(context.getConnectionBatchSize()));
        //        <prop key="hibernate.jdbc.batch_size">${de.invesdwin.context.persistence.jpa.PersistenceProperties.CONNECTION_BATCH_SIZE}</prop>
        props.put(AvailableSettings.STATEMENT_BATCH_SIZE, String.valueOf(context.getConnectionBatchSize()));
        //        <prop key="hibernate.default_batch_fetch_size">${de.invesdwin.context.persistence.jpa.PersistenceProperties.CONNECTION_BATCH_SIZE}</prop>
        props.put(AvailableSettings.DEFAULT_BATCH_FETCH_SIZE, String.valueOf(context.getConnectionBatchSize()));
        //        <prop key="hibernate.jdbc.batch_versioned_data">true</prop>
        props.put(AvailableSettings.BATCH_VERSIONED_DATA, String.valueOf(true));
        //        <prop key="hibernate.order_inserts">true</prop>
        props.put(AvailableSettings.ORDER_INSERTS, String.valueOf(true));
        //        <prop key="hibernate.order_updates">true</prop>
        props.put(AvailableSettings.ORDER_UPDATES, String.valueOf(true));
        //        <prop key="hibernate.cache.use_second_level_cache">true</prop>
        props.put(AvailableSettings.USE_SECOND_LEVEL_CACHE, String.valueOf(true));
        //        <prop key="hibernate.cache.use_query_cache">true</prop>
        props.put(AvailableSettings.USE_QUERY_CACHE, String.valueOf(true));
        //        <prop key="hibernate.cache.region.factory_class">org.hibernate.cache.ehcache.SingletonEhCacheRegionFactory</prop>
        initCaches();
        if (Reflections.classExists(CLASS_JCACHEREGIONFACTORY_OLD)) {
            props.put(AvailableSettings.CACHE_REGION_FACTORY, CLASS_JCACHEREGIONFACTORY_OLD);
        } else {
            props.put(AvailableSettings.CACHE_REGION_FACTORY, CLASS_JCACHEREGIONFACTORY_NEW);
        }
        //https://vladmihalcea.com/hibernate-hidden-gem-the-pooled-lo-optimizer/
        props.put(AvailableSettings.PREFERRED_POOLED_OPTIMIZER, "pooled-lo");
        return props;
    }

    private void initCaches() {
        Assertions.checkNotNull(new CacheBuilder<Object, Object>().setName("default-query-results-region")
                .setExpireAfterAccess(new Duration(2, FTimeUnit.MINUTES))
                .setMaximumSize(10000)
                .getOrCreate());
        Assertions.checkNotNull(new CacheBuilder<Object, Object>().setName("default-update-timestamps-region")
                .setMaximumSize(1000000)
                .getOrCreate());
    }

    private String getHibernateHbm2DdlAuto(final PersistenceUnitContext context) {
        final ConnectionAutoSchema autoSchema = context.getConnectionAutoSchema();
        switch (autoSchema) {
        case CREATE:
            return "create";
        case CREATE_DROP:
            return "create-drop";
        case UPDATE:
            return "update";
        case VALIDATE:
            return "validate";
        default:
            throw UnknownArgumentException.newInstance(ConnectionAutoSchema.class, autoSchema);
        }
    }

    private Class<? extends Dialect> getHibernateDialect(final PersistenceUnitContext context) {
        final ConnectionDialect dialect = context.getConnectionDialect();
        switch (dialect) {
        case MSSQLSERVER:
            return SQLServer2016Dialect.class;
        case MARIADB:
            return MariaDB106Dialect.class;
        case MYSQL:
            return MySQL8Dialect.class;
        case POSTGRESQL:
            return PostgreSQL95Dialect.class;
        case ORACLE:
            return Oracle12cDialect.class;
        case H2:
            return H2Dialect.class;
        case HSQLDB:
            return HSQLDialect.class;
        case SYBASE:
            return SybaseASE157Dialect.class;
        case DERBY:
            return DerbyTenSevenDialect.class;
        default:
            throw UnknownArgumentException.newInstance(ConnectionDialect.class, dialect);
        }
    }

    @Override
    public PersistenceProvider getPersistenceProvider(final PersistenceUnitContext context) {
        return new HibernatePersistenceProvider();
    }

    @Override
    public JpaVendorAdapter getJpaVendorAdapter(final PersistenceUnitContext context) {
        return new HibernateJpaVendorAdapter();
    }

    @Override
    public void setCacheable(final PersistenceUnitContext context, final IConfigurableQuery query,
            final boolean cacheable) {
        query.setHint(org.hibernate.annotations.QueryHints.CACHEABLE, cacheable);
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
        return new HibernateExtendedJpaDialect();
    }

}
