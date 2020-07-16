package de.invesdwin.context.persistence.jpa.kundera.rdbms;

import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.inject.Named;
import javax.persistence.spi.PersistenceProvider;
import javax.sql.DataSource;

import org.hibernate.cfg.AvailableSettings;
import org.springframework.orm.jpa.JpaDialect;
import org.springframework.orm.jpa.JpaVendorAdapter;

import com.impetus.client.rdbms.RDBMSClientFactory;
import com.impetus.kundera.KunderaPersistence;

import de.invesdwin.context.persistence.jpa.ConnectionDialect;
import de.invesdwin.context.persistence.jpa.PersistenceProperties;
import de.invesdwin.context.persistence.jpa.PersistenceUnitContext;
import de.invesdwin.context.persistence.jpa.api.index.Indexes;
import de.invesdwin.context.persistence.jpa.api.query.IConfigurableQuery;
import de.invesdwin.context.persistence.jpa.hibernate.BeanLookupDatasourceConnectionProvider;
import de.invesdwin.context.persistence.jpa.hibernate.HibernateDialectSpecificDelegate;
import de.invesdwin.context.persistence.jpa.kundera.KunderaIndexCreationHandler;
import de.invesdwin.context.persistence.jpa.spi.delegate.IDialectSpecificDelegate;

@ThreadSafe
@Named
public class KunderaRdbmsDialectSpecificDelegate implements IDialectSpecificDelegate {

    @Inject
    private HibernateDialectSpecificDelegate hibernateDialectSpecificDelegate;
    private final KunderaIndexCreationHandler kunderaIndexCreationHandler = new KunderaIndexCreationHandler();

    @Override
    public void createIndexes(final PersistenceUnitContext context, final Class<?> entityClass, final Indexes indexes) {
        kunderaIndexCreationHandler.createIndexes(context, entityClass, indexes);
    }

    @Override
    public void dropIndexes(final PersistenceUnitContext context, final Class<?> entityClass, final Indexes indexes) {
        kunderaIndexCreationHandler.dropIndexes(context, entityClass, indexes);
    }

    @Override
    public DataSource createDataSource(final PersistenceUnitContext context) {
        return hibernateDialectSpecificDelegate.createDataSource(context);
    }

    //    <property name="kundera.client.lookup.class" value="com.impetus.client.rdbms.RDBMSClientFactory" />
    //    <property name="hibernate.show_sql" value="true" />
    //    <property name="hibernate.format_sql" value="true" />
    //    <property name="hibernate.dialect" value="org.hibernate.dialect.MySQL5Dialect" />
    //    <property name="hibernate.connection.driver_class" value="com.mysql.cj.jdbc.Driver" />
    //    <property name="hibernate.connection.url" value="jdbc:mysql://localhost:3306/hibernatepoc" />
    //    <property name="hibernate.connection.username" value="root" />
    //    <property name="hibernate.connection.password" value="impetus" />
    //    <property name="hibernate.current_session_context_class" value="org.hibernate.context.ThreadLocalSessionContext" />
    @Override
    public Map<String, String> getPersistenceProperties(final PersistenceUnitContext context) {
        final Map<String, String> properties = hibernateDialectSpecificDelegate.getPersistenceProperties(context);
        properties.put(com.impetus.kundera.PersistenceProperties.KUNDERA_CLIENT_FACTORY,
                RDBMSClientFactory.class.getName());
        properties.put(AvailableSettings.CONNECTION_PROVIDER, BeanLookupDatasourceConnectionProvider.class.getName());
        properties.put(AvailableSettings.DATASOURCE,
                context.getPersistenceUnitName() + PersistenceProperties.DATA_SOURCE_NAME_SUFFIX);
        return properties;
    }

    @Override
    public JpaVendorAdapter getJpaVendorAdapter(final PersistenceUnitContext context) {
        return null;
    }

    @Override
    public PersistenceProvider getPersistenceProvider(final PersistenceUnitContext context) {
        return new KunderaPersistence();
    }

    @Override
    public void setCacheable(final PersistenceUnitContext context, final IConfigurableQuery query,
            final boolean cacheable) {
        hibernateDialectSpecificDelegate.setCacheable(context, query, cacheable);
    }

    @Override
    public Set<ConnectionDialect> getSupportedDialects() {
        return hibernateDialectSpecificDelegate.getSupportedDialects();
    }

    @Override
    public IDialectSpecificDelegate getOverriddenParent() {
        return hibernateDialectSpecificDelegate;
    }

    @Override
    public JpaDialect getJpaDialect(final PersistenceUnitContext persistenceUnitContext) {
        return null;
    }

}
