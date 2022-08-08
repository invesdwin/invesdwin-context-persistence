package de.invesdwin.context.persistence.jpa.scanning.internal;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Named;

import org.springframework.core.io.FileSystemResource;
import org.springframework.data.jpa.repository.JpaRepository;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.beans.init.locations.IContextLocation;
import de.invesdwin.context.beans.init.locations.PositionedResource;
import de.invesdwin.context.beans.init.locations.position.ResourcePosition;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.persistence.jpa.PersistenceProperties;
import de.invesdwin.context.persistence.jpa.api.dao.IDao;
import de.invesdwin.context.persistence.jpa.spi.impl.PersistenceUnitAnnotationUtil;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.classpath.FastClassPathScanner;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.Strings;
import de.invesdwin.util.lang.reflection.Reflections;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;

@ThreadSafe
@Named
public class JpaRepositoryScanContextLocation implements IContextLocation {

    @Override
    public List<PositionedResource> getContextResources() {
        try {
            final String content = generateContextXml();
            final File xmlFile = new File(ContextProperties.TEMP_DIRECTORY, "ctx.jpa.repository.scan.xml");
            Files.writeStringToFile(xmlFile, content, Charset.defaultCharset());
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

        final Map<String, Set<Class<?>>> entitiesMap = PersistenceUnitAnnotationUtil.scanForEntities();
        final Map<String, Map<Class<?>, Set<Class<?>>>> basePackageJpaRepositories = scanForBasePackageJpaRepositories();
        for (final Entry<String, Map<Class<?>, Set<Class<?>>>> basePackageElement : basePackageJpaRepositories
                .entrySet()) {
            final String basePackage = basePackageElement.getKey();
            final Map<Class<?>, Set<Class<?>>> jpaRepositores = basePackageElement.getValue();
            for (final Entry<String, Set<Class<?>>> entitiesElement : entitiesMap.entrySet()) {
                final String persistenceUnitName = entitiesElement.getKey();
                final Set<Class<?>> entities = entitiesElement.getValue();
                final String jpaRepositoriesContent = generateJpaRepositoriesXml(basePackage, jpaRepositores,
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

    private Map<String, Map<Class<?>, Set<Class<?>>>> scanForBasePackageJpaRepositories() {
        final Map<String, Map<Class<?>, Set<Class<?>>>> basePackage_jpaRepositories = new HashMap<String, Map<Class<?>, Set<Class<?>>>>();
        final ScanResult scanner = FastClassPathScanner.getScanResult();

        for (final ClassInfo ci : scanner.getClassesImplementing(JpaRepository.class.getName())) {
            if (ci.isInterface()) {
                final String beanClassName = ci.getName();
                final Class<?> repositoryClass = Reflections.classForName(beanClassName);
                if (!IDao.class.isAssignableFrom(repositoryClass)) {
                    final Class<?>[] typeArguments = Reflections.resolveTypeArguments(repositoryClass,
                            JpaRepository.class);
                    Assertions.assertThat(typeArguments.length).isEqualTo(2);
                    final Class<?> entity = typeArguments[0];
                    final String basePackage = getBasePackage(repositoryClass);
                    if (basePackage != null) {
                        Map<Class<?>, Set<Class<?>>> jpaRepositories = basePackage_jpaRepositories.get(basePackage);
                        if (jpaRepositories == null) {
                            jpaRepositories = new HashMap<Class<?>, Set<Class<?>>>();
                            basePackage_jpaRepositories.put(basePackage, jpaRepositories);
                        }
                        Set<Class<?>> set = jpaRepositories.get(entity);
                        if (set == null) {
                            set = new HashSet<Class<?>>();
                            jpaRepositories.put(entity, set);
                        }
                        Assertions.assertThat(set.add(repositoryClass)).isTrue();
                    }
                }
            }
        }
        return basePackage_jpaRepositories;
    }

    private String getBasePackage(final Class<?> repositoryClass) {
        for (final String basePackage : ContextProperties.getBasePackages()) {
            if (repositoryClass.getName().startsWith(basePackage)) {
                return basePackage;
            }
        }
        return null;
    }

}