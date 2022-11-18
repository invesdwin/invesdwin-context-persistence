package de.invesdwin.context.persistence.jpa.scanning;

import javax.annotation.concurrent.NotThreadSafe;

import jakarta.inject.Named;

@NotThreadSafe
@Named
public class EntityClasspathScanningHookSupport implements IEntityClasspathScanningHook {

    @Override
    public void entityAssociated(final Class<?> entityClass, final String persistenceUnitName) {}

    @Override
    public void finished() {}

}
