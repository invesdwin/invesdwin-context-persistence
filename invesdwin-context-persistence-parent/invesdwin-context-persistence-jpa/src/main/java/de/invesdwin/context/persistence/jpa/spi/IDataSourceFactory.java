package de.invesdwin.context.persistence.jpa.spi;

import javax.sql.DataSource;

import de.invesdwin.context.persistence.jpa.PersistenceUnitContext;

public interface IDataSourceFactory {

    DataSource createDataSource(PersistenceUnitContext context);

}
