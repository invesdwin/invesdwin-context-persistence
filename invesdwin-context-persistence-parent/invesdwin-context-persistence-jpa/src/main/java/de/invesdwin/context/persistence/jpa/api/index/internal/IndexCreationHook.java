package de.invesdwin.context.persistence.jpa.api.index.internal;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.jpa.ConnectionAutoSchema;
import de.invesdwin.context.persistence.jpa.PersistenceProperties;
import de.invesdwin.context.persistence.jpa.PersistenceUnitContext;
import de.invesdwin.context.persistence.jpa.scanning.EntityClasspathScanningHookSupport;
import de.invesdwin.util.error.UnknownArgumentException;
import jakarta.inject.Named;

@NotThreadSafe
@Named
public class IndexCreationHook extends EntityClasspathScanningHookSupport {

    @Override
    public void entityAssociated(final Class<?> entityClass, final String persistenceUnitName) {
        final PersistenceUnitContext persistenceUnitContext = PersistenceProperties
                .getPersistenceUnitContext(persistenceUnitName);
        final ConnectionAutoSchema autoSchema = persistenceUnitContext.getConnectionAutoSchema();
        switch (autoSchema) {
        case CREATE:
        case CREATE_DROP:
        case UPDATE:
            persistenceUnitContext.createIndexes(entityClass);
            break;
        case VALIDATE:
            break;
        default:
            throw UnknownArgumentException.newInstance(ConnectionAutoSchema.class, autoSchema);
        }
    }

}
