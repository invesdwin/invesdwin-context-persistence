package de.invesdwin.context.persistence.jpa.scanning;

public interface IEntityClasspathScanningHook {

    void entityAssociated(Class<?> entityClass, String persistenceUnitName);

    void finished();

}
