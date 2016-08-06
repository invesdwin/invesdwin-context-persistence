package de.invesdwin.context.persistence.jpa.api;

public interface IPersistenceUnitAware {

    /**
     * If null or an empty string is returned, PersistenceProperties.DEFAULT_PERSISTENCE_UNIT_NAME is assumed.
     * 
     * This value gets cached and will thus not be able to change dynamically.
     */
    String getPersistenceUnitName();

}
