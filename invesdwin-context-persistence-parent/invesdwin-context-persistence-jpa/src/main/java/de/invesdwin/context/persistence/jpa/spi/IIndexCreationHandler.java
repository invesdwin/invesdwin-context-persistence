package de.invesdwin.context.persistence.jpa.spi;

import de.invesdwin.context.persistence.jpa.PersistenceUnitContext;
import de.invesdwin.context.persistence.jpa.api.index.Indexes;

public interface IIndexCreationHandler {

    void createIndexes(PersistenceUnitContext context, Class<?> entityClass, Indexes indexes);

    void dropIndexes(PersistenceUnitContext context, Class<?> entityClass, Indexes indexes);

}
