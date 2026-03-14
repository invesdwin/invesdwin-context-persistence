package de.invesdwin.context.persistence.jpa;

import java.util.Set;

import javax.annotation.concurrent.Immutable;

import org.springframework.transaction.annotation.Transactional;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.persistence.jpa.scanning.internal.ClasspathScanningPersistenceUnitManager;
import de.invesdwin.context.persistence.jpa.scanning.internal.PersistenceUnitContextManager;
import de.invesdwin.context.persistence.jpa.spi.impl.PersistenceUnitAnnotationUtil;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.lang.string.Strings;

@Immutable
public final class PersistenceProperties {

    public static final boolean IS_P6SPY_AVAILABLE = Reflections.classExists("com.p6spy.engine.spy.P6DataSource");

    public static final String PERSISTENCE_UNIT_NAME_SUFFIX = "_pu";
    public static final String DEFAULT_PERSISTENCE_UNIT_NAME = "default" + PERSISTENCE_UNIT_NAME_SUFFIX;
    public static final String TRANSACTION_MANAGER_NAME_SUFFIX = "_tm";
    public static final String DEFAULT_TRANSACTION_MANAGER_NAME = DEFAULT_PERSISTENCE_UNIT_NAME
            + TRANSACTION_MANAGER_NAME_SUFFIX;
    public static final String ENTITY_MANAGER_FACTORY_NAME_SUFFIX = "_emf";
    public static final String DEFAULT_ENTITY_MANAGER_FACTORY_NAME = DEFAULT_PERSISTENCE_UNIT_NAME
            + ENTITY_MANAGER_FACTORY_NAME_SUFFIX;
    public static final String DATA_SOURCE_NAME_SUFFIX = "_ds";
    public static final String DEFAULT_DATA_SOURCE_NAME = DEFAULT_PERSISTENCE_UNIT_NAME + DATA_SOURCE_NAME_SUFFIX;

    public static final int DEFAULT_CONNECTION_BATCH_SIZE;
    public static final ConnectionAutoSchema DEFAULT_CONNECTION_AUTO_SCHEMA;
    private static final SystemProperties SYSTEM_PROPERTIES;

    static {
        SYSTEM_PROPERTIES = new SystemProperties(PersistenceProperties.class);
        DEFAULT_CONNECTION_BATCH_SIZE = SYSTEM_PROPERTIES.getInteger("DEFAULT_CONNECTION_BATCH_SIZE");
        DEFAULT_CONNECTION_AUTO_SCHEMA = SYSTEM_PROPERTIES.getEnum(ConnectionAutoSchema.class,
                "DEFAULT_CONNECTION_AUTO_SCHEMA");
    }

    private PersistenceProperties() {}

    private static PersistenceUnitContextManager getPersistenceUnitContextManager() {
        return MergedContext.getInstance()
                .getBean(ClasspathScanningPersistenceUnitManager.class)
                .getPersistenceUnitContextManager();
    }

    public static String getPersistenceUnitName(final Class<?> clazz) {
        final Transactional transactionalAnnotation = Reflections.getAnnotation(clazz, Transactional.class);
        if (transactionalAnnotation != null && Strings.isNotBlank(transactionalAnnotation.value())) {
            final String transactionManagerName = transactionalAnnotation.value();
            final String persistenceUnitName = Strings.removeEnd(transactionManagerName,
                    TRANSACTION_MANAGER_NAME_SUFFIX);
            final PersistenceUnitContextManager persistenceUnitContextManager = getPersistenceUnitContextManager();
            Assertions.assertThat(persistenceUnitContextManager.isValidPersistenceUnit(persistenceUnitName)).isTrue();
            return persistenceUnitName;
        }

        String persistenceUnitName = getPersistenceUnitContextManager()
                .getPersistenceUnitNameFromPersistenceUnitAwareClass(clazz);
        if (persistenceUnitName != null) {
            return persistenceUnitName;
        }

        persistenceUnitName = getPersistenceUnitContextManager().getPersistenceUnitNameFromEntityClass(clazz);
        if (persistenceUnitName != null) {
            return persistenceUnitName;
        }

        persistenceUnitName = PersistenceUnitAnnotationUtil
                .getPersistenceUnitNameFromPersistenceUnitNameAnnotation(clazz);
        if (persistenceUnitName != null) {
            return persistenceUnitName;
        }

        return PersistenceProperties.DEFAULT_PERSISTENCE_UNIT_NAME;
    }

    public static Set<String> getPersistenceUnitNames() {
        final PersistenceUnitContextManager persistenceUnitContextManager = getPersistenceUnitContextManager();
        final Set<String> validPersistenceUnitNames = ILockCollectionFactory.getInstance(false).newSet();
        for (final String persistenceUnitName : persistenceUnitContextManager.getPersistenceUnitNames()) {
            if (persistenceUnitContextManager.isValidPersistenceUnit(persistenceUnitName)) {
                validPersistenceUnitNames.add(persistenceUnitName);
            }
        }
        return Collections.unmodifiableSet(validPersistenceUnitNames);
    }

    public static PersistenceUnitContext getPersistenceUnitContext(final Class<?> clazz) {
        final String persistenceUnitName = getPersistenceUnitName(clazz);
        return getPersistenceUnitContext(persistenceUnitName);
    }

    public static PersistenceUnitContext getPersistenceUnitContext(final String persistenceUnitName) {
        return getPersistenceUnitContextManager().getPersistenceUnitContext(persistenceUnitName);
    }

    public static PersistenceUnitContext getDefaultPersistenceUnitContext() {
        return getPersistenceUnitContext(DEFAULT_PERSISTENCE_UNIT_NAME);
    }

}