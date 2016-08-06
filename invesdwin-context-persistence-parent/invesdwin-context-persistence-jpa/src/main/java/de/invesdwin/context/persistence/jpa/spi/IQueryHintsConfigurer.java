package de.invesdwin.context.persistence.jpa.spi;

import de.invesdwin.context.persistence.jpa.PersistenceUnitContext;
import de.invesdwin.context.persistence.jpa.api.query.IConfigurableQuery;

public interface IQueryHintsConfigurer {

    void setCacheable(PersistenceUnitContext context, IConfigurableQuery query, final boolean cacheable);

}
