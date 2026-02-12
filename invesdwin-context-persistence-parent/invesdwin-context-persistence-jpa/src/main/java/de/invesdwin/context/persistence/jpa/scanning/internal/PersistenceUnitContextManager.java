package de.invesdwin.context.persistence.jpa.scanning.internal;

import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.orm.jpa.persistenceunit.PersistenceUnitManager;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.persistence.jpa.ConnectionDialect;
import de.invesdwin.context.persistence.jpa.PersistenceProperties;
import de.invesdwin.context.persistence.jpa.PersistenceUnitContext;
import de.invesdwin.context.persistence.jpa.api.IPersistenceUnitAware;
import de.invesdwin.context.persistence.jpa.scanning.EntityClasspathScanningHookSupport;
import de.invesdwin.context.persistence.jpa.spi.delegate.IDialectSpecificDelegate;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;

/**
 * This class is ThreadSafe for read operations.
 * 
 */
@NotThreadSafe
@Configurable
public final class PersistenceUnitContextManager extends EntityClasspathScanningHookSupport {

    private final ALoadingCache<String, PersistenceUnitContext> persistenceUnitName_context = new ALoadingCache<String, PersistenceUnitContext>() {

        @Override
        protected PersistenceUnitContext loadValue(final String key) {
            return new PersistenceUnitContext(persistenceUnitManager, PersistenceUnitContextManager.this, key);
        }
    };
    private final Map<Class<?>, String> persistenceUnitAwareClass_persistenceUnitName = ILockCollectionFactory
            .getInstance(false)
            .newMap();
    private final ALoadingCache<String, Set<Class<?>>> persistenceUnitName_entityClasses = new ALoadingCache<String, Set<Class<?>>>() {
        @Override
        protected Set<Class<?>> loadValue(final String key) {
            return ILockCollectionFactory.getInstance(false).newSet();
        }
    };
    private final Map<Class<?>, String> entityClass_persistenceUnitName = ILockCollectionFactory.getInstance(false)
            .newMap();
    private final Map<ConnectionDialect, IDialectSpecificDelegate> dialect_dialectSpecificDelegate = ILockCollectionFactory
            .getInstance(false)
            .newMap();
    private final PersistenceUnitManager persistenceUnitManager;

    public PersistenceUnitContextManager(final PersistenceUnitManager persistenceUnitManager,
            final IDialectSpecificDelegate[] dialectSpecificDelegates) {
        Assertions.assertThat(persistenceUnitManager).isNotNull();
        this.persistenceUnitManager = persistenceUnitManager;
        associateDelegates(dialectSpecificDelegates);
    }

    private void associateDelegates(final IDialectSpecificDelegate[] dialectSpecificDelegates) {
        for (final ConnectionDialect dialect : ConnectionDialect.values()) {
            //add delegates that match this dialect
            final Set<IDialectSpecificDelegate> delegatesFound = ILockCollectionFactory.getInstance(false).newSet();
            for (final IDialectSpecificDelegate delegate : dialectSpecificDelegates) {
                if (delegate.getSupportedDialects().contains(dialect)) {
                    delegatesFound.add(delegate);
                }
            }
            //filter overriden parent delegates for this dialect
            for (final IDialectSpecificDelegate delegate : dialectSpecificDelegates) {
                if (delegate.getSupportedDialects().contains(dialect)) {
                    final IDialectSpecificDelegate parent = delegate.getOverriddenParent();
                    if (parent != null) {
                        delegatesFound.remove(parent);
                    }
                }
            }
            //throw an error if multiple delegates remain or add the delegate if only one was left
            if (delegatesFound.size() > 1) {
                throw new IllegalArgumentException("Found multiple " + IDialectSpecificDelegate.class.getSimpleName()
                        + "s for " + ConnectionDialect.class.getSimpleName() + " [" + dialect
                        + "]. You may override one by specifying an appropriate parent relationship in the overriding one: "
                        + delegatesFound);
            } else if (delegatesFound.size() != 0) {
                dialect_dialectSpecificDelegate.put(dialect, delegatesFound.iterator().next());
            }
        }
    }

    @Override
    public void entityAssociated(final Class<?> entityClass, final String persistenceUnitName) {
        Assertions.assertThat(persistenceUnitName)
                .as("As a naming convention, all persistence units should have the suffix \"%s\": %s",
                        PersistenceProperties.PERSISTENCE_UNIT_NAME_SUFFIX)
                .endsWith(PersistenceProperties.PERSISTENCE_UNIT_NAME_SUFFIX);
        persistenceUnitName_entityClasses.get(persistenceUnitName).add(entityClass);
        entityClass_persistenceUnitName.put(entityClass, persistenceUnitName);
    }

    @Override
    public void finished() {
        final Map<String, IPersistenceUnitAware> persistenceUnitAwares = MergedContext.getInstance()
                .getBeansOfType(IPersistenceUnitAware.class);
        for (final IPersistenceUnitAware aware : persistenceUnitAwares.values()) {
            //temporarily disable transactional to be able to call getPersistenceName without a transaction that cannot be created anyway
            final String persistenceUnitName = aware.getPersistenceUnitName();
            persistenceUnitAwareClass_persistenceUnitName.put(aware.getClass(), persistenceUnitName);
        }
        //add default persistence unit to list so it aways gets an entry generated in persistence.xml
        Assertions
                .assertThat(persistenceUnitName_entityClasses.get(PersistenceProperties.DEFAULT_PERSISTENCE_UNIT_NAME))
                .isNotNull();
    }

    public Set<Class<?>> getEntityClasses(final String persistenceUnitName) {
        return persistenceUnitName_entityClasses.get(persistenceUnitName);
    }

    public Set<String> getPersistenceUnitNames() {
        return persistenceUnitName_entityClasses.keySet();
    }

    public String getPersistenceUnitNameFromEntityClass(final Class<?> entityClass) {
        return entityClass_persistenceUnitName.get(entityClass);
    }

    public String getPersistenceUnitNameFromPersistenceUnitAwareClass(final Class<?> persistenceUnitAwareClass) {
        return persistenceUnitAwareClass_persistenceUnitName.get(persistenceUnitAwareClass);
    }

    public boolean isValidPersistenceUnit(final String persistenceUnitName) {
        return PersistenceProperties.DEFAULT_PERSISTENCE_UNIT_NAME.equals(persistenceUnitName)
                || !persistenceUnitName_entityClasses.get(persistenceUnitName).isEmpty();
    }

    public IDialectSpecificDelegate getDialectSpecificDelegate(final ConnectionDialect dialect) {
        final IDialectSpecificDelegate dialectSpecificDelegate = dialect_dialectSpecificDelegate.get(dialect);
        Assertions.assertThat(dialectSpecificDelegate)
                .as("No %s found for %s [%s]. Please ensure that the appropriate module is loaded.",
                        IDialectSpecificDelegate.class.getSimpleName(), ConnectionDialect.class.getSimpleName(),
                        dialect)
                .isNotNull();
        return dialectSpecificDelegate;
    }

    public PersistenceUnitContext getPersistenceUnitContext(final String persistenceUnitName) {
        return persistenceUnitName_context.get(persistenceUnitName);
    }

}
