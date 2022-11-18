package de.invesdwin.context.persistence.jpa.scanning.internal;

import javax.annotation.concurrent.NotThreadSafe;

import org.springframework.data.jpa.repository.JpaContext;

import de.invesdwin.context.persistence.jpa.PersistenceProperties;
import jakarta.persistence.EntityManager;

@NotThreadSafe
public class ConfiguredJpaContext implements JpaContext {

    @Override
    public EntityManager getEntityManagerByManagedType(final Class<?> managedType) {
        return PersistenceProperties.getPersistenceUnitContext(managedType).getEntityManager();
    }

}
