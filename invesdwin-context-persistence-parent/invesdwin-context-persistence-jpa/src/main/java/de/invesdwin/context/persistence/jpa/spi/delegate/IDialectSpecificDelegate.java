package de.invesdwin.context.persistence.jpa.spi.delegate;

import java.util.Set;

import de.invesdwin.context.persistence.jpa.ConnectionDialect;
import de.invesdwin.context.persistence.jpa.spi.IDataSourceFactory;
import de.invesdwin.context.persistence.jpa.spi.IIndexCreationHandler;
import de.invesdwin.context.persistence.jpa.spi.IPersistencePropertiesProvider;
import de.invesdwin.context.persistence.jpa.spi.IQueryHintsConfigurer;

public interface IDialectSpecificDelegate extends IIndexCreationHandler, IDataSourceFactory,
        IPersistencePropertiesProvider, IQueryHintsConfigurer {

    Set<ConnectionDialect> getSupportedDialects();

    IDialectSpecificDelegate getOverriddenParent();

}
