package de.invesdwin.context.persistence.jpa.scanning.internal;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Named;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.data.jpa.repository.JpaRepository;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.beans.init.locations.IContextLocation;
import de.invesdwin.context.beans.init.locations.PositionedResource;
import de.invesdwin.context.beans.init.locations.PositionedResource.ResourcePosition;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.persistence.jpa.PersistenceProperties;
import de.invesdwin.context.persistence.jpa.api.dao.IDao;
import de.invesdwin.context.persistence.jpa.spi.impl.PersistenceUnitAnnotationUtil;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.classpath.ClassPathScanner;
import de.invesdwin.util.lang.Reflections;
import de.invesdwin.util.lang.Strings;

@ThreadSafe
@Named
public class RepositoryScanContextLocation implements IContextLocation {

    @Override
    public List<PositionedResource> getContextResources() {
        try {
            final String content = generateContextXml();
            final File xmlFile = new File(ContextProperties.TEMP_DIRECTORY, "ctx.repository.scan.xml");
            FileUtils.writeStringToFile(xmlFile, content);
            final FileSystemResource fsResource = new FileSystemResource(xmlFile);
            xmlFile.deleteOnExit();
            return Arrays.asList(PositionedResource.of(fsResource, ResourcePosition.END));
        } catch (final IOException e) {
            throw Err.process(e);
        }
    }

    private String generateContextXml() throws IOException {
        final StringBuilder sb = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        sb.append("\n<beans default-lazy-init=\"false\"");
        sb.append(" xmlns=\"http://www.springframework.org/schema/beans\"");
        sb.append(" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"");
        sb.append(" xmlns:jpa=\"http://www.springframework.org/schema/data/jpa\"");
        sb.append(" xmlns:repository=\"http://www.springframework.org/schema/data/repository\"");
        sb.append(
                " xsi:schemaLocation=\"http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd");
        sb.append(
                " http://www.springframework.org/schema/data/repository http://www.springframework.org/schema/data/repository/spring-repository.xsd");
        sb.append(
                " http://www.springframework.org/schema/data/jpa http://www.springframework.org/schema/data/jpa/spring-jpa.xsd\">");

        for (final String basePackage : ContextProperties.getBasePackages()) {
            final Map<String, Set<Class<?>>> entitiesMap = PersistenceUnitAnnotationUtil.scanForEntities(basePackage);
            final Map<Class<?>, Set<Class<?>>> jpaRepositoriesMap = scanForJpaRepositories(basePackage);
            for (final Entry<String, Set<Class<?>>> entitiesElement : entitiesMap.entrySet()) {
                final String persistenceUnitName = entitiesElement.getKey();
                final Set<Class<?>> entities = entitiesElement.getValue();
                final String jpaRepositoriesContent = generateJpaRepositoriesXml(basePackage, jpaRepositoriesMap,
                        persistenceUnitName, entities);
                sb.append(jpaRepositoriesContent);
            }
        }

        sb.append("\n</beans>");

        return sb.toString();
    }

    private String generateJpaRepositoriesXml(final String basePackage,
            final Map<Class<?>, Set<Class<?>>> jpaRepositoriesMap, final String persistenceUnitName,
            final Set<Class<?>> entities) {
        final StringBuilder includeFilters = new StringBuilder();
        for (final Entry<Class<?>, Set<Class<?>>> jpaRepositoriesElement : jpaRepositoriesMap.entrySet()) {
            final Class<?> entity = jpaRepositoriesElement.getKey();
            if (entities.contains(entity)) {
                final Set<Class<?>> jpaRepositories = jpaRepositoriesElement.getValue();
                for (final Class<?> jpaRepository : jpaRepositories) {
                    includeFilters.append("\n\t\t<repository:include-filter type=\"regex\" expression=\"");
                    includeFilters.append(jpaRepository.getName().replace(".", "\\."));
                    includeFilters.append("\"/>");
                }
            }
        }
        if (includeFilters.length() > 0) {
            final StringBuilder sb = new StringBuilder();
            sb.append("\n\t<jpa:repositories base-package=\"");
            sb.append(basePackage);
            sb.append("\" entity-manager-factory-ref=\"");
            sb.append(persistenceUnitName);
            sb.append(PersistenceProperties.ENTITY_MANAGER_FACTORY_NAME_SUFFIX);
            sb.append("\" transaction-manager-ref=\"");
            sb.append(persistenceUnitName);
            sb.append(PersistenceProperties.TRANSACTION_MANAGER_NAME_SUFFIX);
            sb.append("\" >");
            sb.append(includeFilters);
            sb.append("\n\t</jpa:repositories>");
            return sb.toString();
        } else {
            return Strings.EMPTY;
        }
    }

    private Map<Class<?>, Set<Class<?>>> scanForJpaRepositories(final String basePackage) {
        final Map<Class<?>, Set<Class<?>>> jpaRepositories = new HashMap<Class<?>, Set<Class<?>>>();
        final ClassPathScanner scanner = new ClassPathScanner().withInterfacesOnly();
        scanner.addIncludeFilter(new AssignableTypeFilter(JpaRepository.class));

        final Set<BeanDefinition> candidateComponents = scanner.findCandidateComponents(basePackage);
        for (final BeanDefinition bd : candidateComponents) {
            final String beanClassName = bd.getBeanClassName();
            final Class<?> repositoryClass = Reflections.classForName(beanClassName);
            if (!IDao.class.isAssignableFrom(repositoryClass)) {
                final Class<?>[] typeArguments = Reflections.resolveTypeArguments(repositoryClass, JpaRepository.class);
                Assertions.assertThat(typeArguments.length).isEqualTo(2);
                final Class<?> entity = typeArguments[0];
                Set<Class<?>> set = jpaRepositories.get(entity);
                if (set == null) {
                    set = new HashSet<Class<?>>();
                    jpaRepositories.put(entity, set);
                }
                Assertions.assertThat(set.add(repositoryClass)).isTrue();
            }
        }
        return jpaRepositories;
    }

}