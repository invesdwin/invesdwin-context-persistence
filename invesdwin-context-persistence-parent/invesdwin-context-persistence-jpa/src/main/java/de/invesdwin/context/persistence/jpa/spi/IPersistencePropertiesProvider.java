package de.invesdwin.context.persistence.jpa.spi;

import java.util.Map;

import javax.persistence.spi.PersistenceProvider;

import org.springframework.orm.jpa.JpaDialect;
import org.springframework.orm.jpa.JpaVendorAdapter;

import de.invesdwin.context.persistence.jpa.PersistenceUnitContext;

public interface IPersistencePropertiesProvider {

    Map<String, String> getPersistenceProperties(PersistenceUnitContext context);

    /**
     * May return null if PersistenceProvider is specified.
     */
    JpaVendorAdapter getJpaVendorAdapter(PersistenceUnitContext context);

    PersistenceProvider getPersistenceProvider(PersistenceUnitContext context);

    /**
     * May return to null to stick with default.
     */
    JpaDialect getJpaDialect(PersistenceUnitContext persistenceUnitContext);

}
