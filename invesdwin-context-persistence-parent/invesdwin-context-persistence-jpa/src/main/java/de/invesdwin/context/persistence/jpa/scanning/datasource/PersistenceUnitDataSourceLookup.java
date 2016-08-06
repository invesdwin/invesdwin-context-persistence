package de.invesdwin.context.persistence.jpa.scanning.datasource;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Named;
import javax.sql.DataSource;

import org.springframework.jdbc.datasource.lookup.DataSourceLookup;

import de.invesdwin.context.persistence.jpa.PersistenceProperties;
import de.invesdwin.util.lang.Strings;

@ThreadSafe
@Named
public class PersistenceUnitDataSourceLookup implements DataSourceLookup {

    @Override
    public DataSource getDataSource(final String dataSourceName) {
        final String persistenceUnitName = Strings.removeEnd(dataSourceName,
                PersistenceProperties.DATA_SOURCE_NAME_SUFFIX);
        return PersistenceProperties.getPersistenceUnitContext(persistenceUnitName).getDataSource();
    }
}
