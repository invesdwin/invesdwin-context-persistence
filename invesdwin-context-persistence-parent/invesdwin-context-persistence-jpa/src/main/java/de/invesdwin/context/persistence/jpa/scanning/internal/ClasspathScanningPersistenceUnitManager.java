package de.invesdwin.context.persistence.jpa.scanning.internal;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;

import org.springframework.data.jpa.support.MergingPersistenceUnitManager;
import org.springframework.orm.jpa.persistenceunit.MutablePersistenceUnitInfo;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.persistence.jpa.PersistenceProperties;
import de.invesdwin.context.persistence.jpa.PersistenceUnitContext;
import de.invesdwin.context.persistence.jpa.scanning.IEntityClasspathScanningHook;
import de.invesdwin.context.persistence.jpa.scanning.transaction.ContextDelegatingTransactionManager;
import de.invesdwin.context.persistence.jpa.spi.delegate.IDialectSpecificDelegate;
import de.invesdwin.context.persistence.jpa.spi.impl.PersistenceUnitAnnotationUtil;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Files;

/**
 * Collects all Entities from CLasspath and creates a complete PersistenceUnit out of them.
 * 
 * @see <a href="http://ancientprogramming.blogspot.com/2007/05/multiple-persistencexml-files-and.html">Source</a>
 * @see <a href="http://opensource.atlassian.com/projects/hibernate/browse/HHH-4864">Hibernate-Issue</a>
 * 
 * @author subes
 * 
 */
@NotThreadSafe
public final class ClasspathScanningPersistenceUnitManager extends MergingPersistenceUnitManager {

    private final Log log = new Log(this);

    @Inject
    private List<IEntityClasspathScanningHook> entityClasspathScanningHooks;
    @Inject
    private IDialectSpecificDelegate[] dialectSpecificDelegates;

    private PersistenceUnitContextManager persistenceUnitContextManager;

    public ClasspathScanningPersistenceUnitManager() {}

    public PersistenceUnitContextManager getPersistenceUnitContextManager() {
        return persistenceUnitContextManager;
    }

    @Override
    protected void postProcessPersistenceUnitInfo(final MutablePersistenceUnitInfo pu) {
        super.postProcessPersistenceUnitInfo(pu);
        final PersistenceUnitContext persistenceUnitContext = PersistenceProperties
                .getPersistenceUnitContext(pu.getPersistenceUnitName());
        final Set<Class<?>> entityClasses = persistenceUnitContext.getEntityClasses();
        for (final Class<?> entityClass : entityClasses) {
            pu.addManagedClassName(entityClass.getName());
        }
        pu.setNonJtaDataSource(persistenceUnitContext.getDataSource());
    }

    @Override
    public void afterPropertiesSet() {
        ContextDelegatingTransactionManager.setEnabled(false);
        persistenceUnitContextManager = new PersistenceUnitContextManager(this, dialectSpecificDelegates);
        //put manager as first hook
        entityClasspathScanningHooks.add(0, persistenceUnitContextManager);
        scan();
        writePersistenceXml();
        //        setDataSourceLookup(dataSourceLookup);
        ContextDelegatingTransactionManager.setEnabled(true);
        super.afterPropertiesSet();
        initializeContexts();
    }

    private void initializeContexts() {
        //initialize entitymanagerfactory and transactionmanager for scanned repositories
        for (final String persistenceUnitName : PersistenceProperties.getPersistenceUnitNames()) {
            Assertions
                    .assertThat(PersistenceProperties.getPersistenceUnitContext(persistenceUnitName)
                            .getTransactionManager())
                    .isNotNull();
        }
    }

    private void writePersistenceXml() {
        try {
            final File metaInfDir = new File(ContextProperties.TEMP_CLASSPATH_DIRECTORY, "META-INF");
            Files.forceMkdir(metaInfDir);
            final File file = new File(metaInfDir, "persistence.xml");
            Files.deleteQuietly(file);
            final String content = generatePersistenceXml();
            Files.write(file, content, Charset.defaultCharset());
        } catch (final IOException e) {
            throw Err.process(e);
        }
    }

    private String generatePersistenceXml() throws IOException {
        final StringBuilder sb = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        sb.append("\n<persistence xmlns=\"http://java.sun.com/xml/ns/persistence\"");
        sb.append(" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"");
        sb.append(
                " xsi:schemaLocation=\"http://java.sun.com/xml/ns/persistence http://java.sun.com/xml/ns/persistence/persistence_2_0.xsd\"");
        sb.append(" version=\"2.0\">");
        for (final String persistenceUnitName : PersistenceProperties.getPersistenceUnitNames()) {
            final String persistenceUnitXmlString = generatePersistenceUnitXml(persistenceUnitName);
            sb.append(persistenceUnitXmlString);
        }
        sb.append("\n</persistence>");
        return sb.toString();
    }

    private String generatePersistenceUnitXml(final String persistenceUnitName) {
        final PersistenceUnitContext context = PersistenceProperties.getPersistenceUnitContext(persistenceUnitName);
        final StringBuilder sb = new StringBuilder("\n\t<persistence-unit name=\"");
        sb.append(persistenceUnitName);
        sb.append("\" transaction-type=\"RESOURCE_LOCAL\">");
        sb.append("\n\t\t<provider>");
        sb.append(context.getPersistenceProvider().getClass().getName());
        sb.append("</provider>");
        final Set<Class<?>> entityClasses = context.getEntityClasses();
        for (final Class<?> entityClass : entityClasses) {
            sb.append("\n\t\t<class>");
            sb.append(entityClass.getName());
            sb.append("</class>");
        }
        sb.append("\n\t\t<exclude-unlisted-classes>true</exclude-unlisted-classes>");
        sb.append("\n\t\t<shared-cache-mode>ENABLE_SELECTIVE</shared-cache-mode>");
        final Map<String, String> properties = context.getPersistenceProperties();
        sb.append("\n\t\t<properties>");
        for (final Entry<String, String> e : properties.entrySet()) {
            sb.append("\n\t\t\t<property name=\"");
            sb.append(e.getKey());
            sb.append("\" value=\"");
            sb.append(e.getValue());
            sb.append("\" />");
        }
        sb.append("\n\t\t</properties>");
        sb.append("\n\t</persistence-unit>");
        return sb.toString();
    }

    /**
     * Scanning cannot happen in afterPropertiesSet because that would already be too late.
     */
    private void scan() {
        final Map<String, Set<Class<?>>> entities = PersistenceUnitAnnotationUtil.scanForEntities();
        for (final String persistenceUnitName : entities.keySet()) {
            for (final Class<?> entityClass : entities.get(persistenceUnitName)) {
                for (final IEntityClasspathScanningHook hook : entityClasspathScanningHooks) {
                    hook.entityAssociated(entityClass, persistenceUnitName);
                }
            }
        }

        for (final IEntityClasspathScanningHook hook : entityClasspathScanningHooks) {
            hook.finished();
        }

        if (log.isInfoEnabled()) {
            for (final String persistenceUnitName : PersistenceProperties.getPersistenceUnitNames()) {
                final Set<Class<?>> entityClasses = PersistenceProperties.getPersistenceUnitContext(persistenceUnitName)
                        .getEntityClasses();
                if (entityClasses.size() > 0) {
                    String entitesSingularPlural = "entit";
                    if (entityClasses.size() != 1) {
                        entitesSingularPlural += "ies";
                    } else {
                        entitesSingularPlural += "y";
                    }

                    final Set<String> entityNames = new HashSet<String>();
                    for (final Class<?> entityClass : entityClasses) {
                        entityNames.add(entityClass.getName());
                    }
                    log.info("Loading %s %s for [%s] persistence unit %s", entityClasses.size(), entitesSingularPlural,
                            persistenceUnitName, entityNames);
                } else {
                    log.info("No entities found for [%s] persistence unit", persistenceUnitName);
                }
            }
        }
    }

}
