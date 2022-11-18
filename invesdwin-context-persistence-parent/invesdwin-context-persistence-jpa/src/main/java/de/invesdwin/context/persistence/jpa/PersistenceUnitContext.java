package de.invesdwin.context.persistence.jpa;

import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;

import org.springframework.orm.jpa.JpaDialect;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.JpaVendorAdapter;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.SharedEntityManagerCreator;
import org.springframework.orm.jpa.persistenceunit.PersistenceUnitManager;
import org.springframework.transaction.PlatformTransactionManager;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.persistence.jpa.api.index.Indexes;
import de.invesdwin.context.persistence.jpa.api.query.IConfigurableQuery;
import de.invesdwin.context.persistence.jpa.scanning.internal.PersistenceUnitContextManager;
import de.invesdwin.context.persistence.jpa.spi.impl.PersistenceUnitAnnotationUtil;
import de.invesdwin.context.persistence.jpa.test.internal.PersistenceContext;
import de.invesdwin.context.persistence.jpa.test.internal.ProdPersistenceContextLocation;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.lang.reflection.Reflections;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.spi.PersistenceProvider;

@ThreadSafe
public final class PersistenceUnitContext {

    private final PersistenceUnitManager persistenceUnitManager;
    private final PersistenceUnitContextManager persistenceUnitContextManager;
    private final SystemProperties systemProperties;
    private final String persistenceUnitName;
    private EntityManagerFactory entityManagerFactory;
    private PlatformTransactionManager transactionManager;
    private DataSource dataSource;

    /**
     * Package private so that only PersistenceProperties class can initialize this one.
     */
    public PersistenceUnitContext(final PersistenceUnitManager persistenceUnitManager,
            final PersistenceUnitContextManager persistenceUnitContextManager, final String persistenceUnitName) {
        this.persistenceUnitManager = persistenceUnitManager;
        this.persistenceUnitContextManager = persistenceUnitContextManager;
        this.systemProperties = new SystemProperties(getClass());
        this.persistenceUnitName = persistenceUnitName;
    }

    private EntityManagerFactory createEntityManagerFactory() {
        Assertions.assertThat(persistenceUnitContextManager.isValidPersistenceUnit(persistenceUnitName))
                .as("Not a valid persistence unit: %s", persistenceUnitName)
                .isTrue();
        final LocalContainerEntityManagerFactoryBean factory = new LocalContainerEntityManagerFactoryBean();
        factory.setPersistenceUnitManager(persistenceUnitManager);
        factory.setPersistenceUnitName(persistenceUnitName);
        final JpaVendorAdapter jpaVendorAdapter = getJpaVendorAdapter();
        if (jpaVendorAdapter != null) {
            factory.setJpaVendorAdapter(jpaVendorAdapter);
        } else {
            factory.setPersistenceProvider(getPersistenceProvider());
        }
        final JpaDialect jpaDialect = getJpaDialect();
        if (jpaDialect != null) {
            factory.setJpaDialect(jpaDialect);
        }
        factory.afterPropertiesSet();
        final String entityManagerFactoryBeanName = getPersistenceUnitName()
                + PersistenceProperties.ENTITY_MANAGER_FACTORY_NAME_SUFFIX;
        MergedContext.getInstance().registerBean(entityManagerFactoryBeanName, factory);
        Assertions.assertThat(MergedContext.getInstance().getBean(entityManagerFactoryBeanName)).isNotNull();
        return factory.getObject();
    }

    private PlatformTransactionManager createTransactionManager() {
        final JpaTransactionManager jpaTransactionManager = new JpaTransactionManager();
        jpaTransactionManager.setEntityManagerFactory(getEntityManagerFactory());
        final JpaDialect jpaDialect = getJpaDialect();
        if (jpaDialect != null) {
            jpaTransactionManager.setJpaDialect(jpaDialect);
        }
        final PlatformTransactionManager loggingDelegateTransactionManager;
        if (PersistenceProperties.IS_P6SPY_AVAILABLE) {
            loggingDelegateTransactionManager = new de.invesdwin.context.persistence.jpa.scanning.transaction.P6SpyLoggingDelegateTransactionManager(
                    this, jpaTransactionManager);
        } else {
            loggingDelegateTransactionManager = jpaTransactionManager;
        }
        final String transactionManagerBeanName = getPersistenceUnitName()
                + PersistenceProperties.TRANSACTION_MANAGER_NAME_SUFFIX;
        MergedContext.getInstance().registerBean(transactionManagerBeanName, loggingDelegateTransactionManager);
        Assertions.assertThat(MergedContext.getInstance().getBean(transactionManagerBeanName)).isNotNull();
        return loggingDelegateTransactionManager;
    }

    private DataSource createDataSource() {
        final String dataSourceBeanName = getPersistenceUnitName() + PersistenceProperties.DATA_SOURCE_NAME_SUFFIX;
        final DataSource dataSourceBean = persistenceUnitContextManager
                .getDialectSpecificDelegate(getConnectionDialect())
                .createDataSource(this);
        MergedContext.getInstance().registerBean(dataSourceBeanName, dataSourceBean);
        Assertions.assertThat(MergedContext.getInstance().getBean(dataSourceBeanName)).isNotNull();
        return dataSourceBean;
    }

    public String getPersistenceUnitName() {
        return persistenceUnitName;
    }

    private String getPersistenceUnitKey(final String key) {
        final String maybeRedirectedPersistenceUnitName;
        if (shouldRedirectPersistenceUnitProperty(key)) {
            maybeRedirectedPersistenceUnitName = PersistenceProperties.DEFAULT_PERSISTENCE_UNIT_NAME;
        } else {
            maybeRedirectedPersistenceUnitName = getPersistenceUnitName();
        }
        return key + PersistenceUnitAnnotationUtil.PERSISTENCE_UNIT_CONFIG_PREFIX + maybeRedirectedPersistenceUnitName;
    }

    private boolean shouldRedirectPersistenceUnitProperty(final String key) {
        if (ProdPersistenceContextLocation.getActivePersistenceContext() == PersistenceContext.TEST_MEMORY) {
            return true;
        } else if (ProdPersistenceContextLocation.getActivePersistenceContext() == PersistenceContext.TEST_SERVER) {
            return !systemProperties.containsKey(
                    key + PersistenceUnitAnnotationUtil.PERSISTENCE_UNIT_CONFIG_PREFIX + getPersistenceUnitName());
        } else {
            return false;
        }
    }

    public String getConnectionDriver() {
        return systemProperties.getString(getPersistenceUnitKey("CONNECTION_DRIVER"));
    }

    public String getConnectionUrl() {
        return systemProperties.getString(getPersistenceUnitKey("CONNECTION_URL"));
    }

    public String getConnectionUser() {
        return systemProperties.getString(getPersistenceUnitKey("CONNECTION_USER"));
    }

    public String getConnectionPassword() {
        return systemProperties.getStringWithSecurityWarning(getPersistenceUnitKey("CONNECTION_PASSWORD"),
                IProperties.INVESDWIN_DEFAULT_PASSWORD);
    }

    public int getConnectionBatchSize() {
        return systemProperties.getInteger(getPersistenceUnitKey("CONNECTION_BATCH_SIZE"));
    }

    public ConnectionAutoSchema getConnectionAutoSchema() {
        return systemProperties.getEnum(ConnectionAutoSchema.class, getPersistenceUnitKey("CONNECTION_AUTO_SCHEMA"));
    }

    public ConnectionDialect getConnectionDialect() {
        return systemProperties.getEnum(ConnectionDialect.class, getPersistenceUnitKey("CONNECTION_DIALECT"));
    }

    public Set<Class<?>> getEntityClasses() {
        return Collections.unmodifiableSet(persistenceUnitContextManager.getEntityClasses(persistenceUnitName));
    }

    public synchronized EntityManagerFactory getEntityManagerFactory() {
        if (entityManagerFactory == null) {
            entityManagerFactory = createEntityManagerFactory();
            Assertions.assertThat(getTransactionManager()).isNotNull();
        }
        return entityManagerFactory;
    }

    public EntityManager getEntityManager() {
        return SharedEntityManagerCreator.createSharedEntityManager(getEntityManagerFactory());
    }

    public synchronized PlatformTransactionManager getTransactionManager() {
        if (transactionManager == null) {
            transactionManager = createTransactionManager();
        }
        return transactionManager;
    }

    private JpaVendorAdapter getJpaVendorAdapter() {
        return persistenceUnitContextManager.getDialectSpecificDelegate(getConnectionDialect())
                .getJpaVendorAdapter(this);
    }

    private JpaDialect getJpaDialect() {
        return persistenceUnitContextManager.getDialectSpecificDelegate(getConnectionDialect()).getJpaDialect(this);
    }

    public PersistenceProvider getPersistenceProvider() {
        return persistenceUnitContextManager.getDialectSpecificDelegate(getConnectionDialect())
                .getPersistenceProvider(this);
    }

    public Map<String, String> getPersistenceProperties() {
        return Collections
                .unmodifiableMap(persistenceUnitContextManager.getDialectSpecificDelegate(getConnectionDialect())
                        .getPersistenceProperties(this));
    }

    public synchronized DataSource getDataSource() {
        if (dataSource == null) {
            dataSource = createDataSource();
        }
        return dataSource;
    }

    public void createIndexes(final Class<?> entityClass) {
        final Indexes annotation = Reflections.getAnnotation(entityClass, Indexes.class);
        //TODO: discover additional indexes in @Embeddables?
        if (annotation != null) {
            persistenceUnitContextManager.getDialectSpecificDelegate(getConnectionDialect())
                    .createIndexes(this, entityClass, annotation);
        }
    }

    public void dropIndexes(final Class<?> entityClass) {
        final Indexes annotation = Reflections.getAnnotation(entityClass, Indexes.class);
        //TODO: discover additional indexes in @Embeddables?
        if (annotation != null) {
            persistenceUnitContextManager.getDialectSpecificDelegate(getConnectionDialect())
                    .dropIndexes(this, entityClass, annotation);
        }
    }

    public void setCacheable(final IConfigurableQuery query, final boolean cacheable) {
        persistenceUnitContextManager.getDialectSpecificDelegate(getConnectionDialect())
                .setCacheable(this, query, cacheable);
    }

}
