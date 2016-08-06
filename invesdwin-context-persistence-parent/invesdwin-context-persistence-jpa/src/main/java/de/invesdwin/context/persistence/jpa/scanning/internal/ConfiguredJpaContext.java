package de.invesdwin.context.persistence.jpa.scanning.internal;

import javax.annotation.concurrent.NotThreadSafe;
import javax.persistence.EntityManager;

import org.springframework.data.jpa.repository.JpaContext;

import de.invesdwin.context.persistence.jpa.PersistenceProperties;

@NotThreadSafe
public class ConfiguredJpaContext implements JpaContext {

    @Override
    public EntityManager getEntityManagerByManagedType(final Class<?> managedType) {
        return PersistenceProperties.getPersistenceUnitContext(managedType).getEntityManager();
    }

}
