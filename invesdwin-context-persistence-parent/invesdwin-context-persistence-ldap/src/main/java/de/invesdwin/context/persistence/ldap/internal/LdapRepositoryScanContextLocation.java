package de.invesdwin.context.persistence.ldap.internal;

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
import org.springframework.ldap.repository.LdapRepository;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.beans.init.locations.IContextLocation;
import de.invesdwin.context.beans.init.locations.PositionedResource;
import de.invesdwin.context.beans.init.locations.PositionedResource.ResourcePosition;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.persistence.ldap.dao.ALdapDao;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.classpath.ClassPathScanner;
import de.invesdwin.util.lang.Reflections;
import de.invesdwin.util.lang.Strings;

@ThreadSafe
@Named
public class LdapRepositoryScanContextLocation implements IContextLocation {

    @Override
    public List<PositionedResource> getContextResources() {
        try {
            final String content = generateContextXml();
            final File xmlFile = new File(ContextProperties.TEMP_DIRECTORY, "ctx.ldap.repository.scan.xml");
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
        sb.append(" xmlns:ldap=\"http://www.springframework.org/schema/ldap\"");
        sb.append(" xmlns:repository=\"http://www.springframework.org/schema/data/repository\"");
        sb.append(
                " xsi:schemaLocation=\"http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd");
        sb.append(
                " http://www.springframework.org/schema/ldap http://www.springframework.org/schema/ldap/spring-ldap.xsd\">");

        for (final String basePackage : ContextProperties.getBasePackages()) {
            final Map<Class<?>, Set<Class<?>>> ldapRepositoriesMap = scanForLdapRepositories(basePackage);
            final String ldapRepositoriesContent = generateLdapRepositoriesXml(basePackage, ldapRepositoriesMap);
            sb.append(ldapRepositoriesContent);
        }

        sb.append("\n</beans>");

        return sb.toString();
    }

    private String generateLdapRepositoriesXml(final String basePackage,
            final Map<Class<?>, Set<Class<?>>> ldapRepositoriesMap) {
        final StringBuilder includeFilters = new StringBuilder();
        for (final Entry<Class<?>, Set<Class<?>>> ldapRepositoriesElement : ldapRepositoriesMap.entrySet()) {
            final Set<Class<?>> ldapRepositories = ldapRepositoriesElement.getValue();
            for (final Class<?> ldapRepository : ldapRepositories) {
                includeFilters.append("\n\t\t<include-filter type=\"regex\" expression=\"");
                includeFilters.append(ldapRepository.getName().replace(".", "\\."));
                includeFilters.append("\"/>");
            }
        }
        if (includeFilters.length() > 0) {
            final StringBuilder sb = new StringBuilder();
            sb.append("\n\t<ldap:repositories base-package=\"");
            sb.append(basePackage);
            sb.append("\" >");
            sb.append(includeFilters);
            sb.append("\n\t</ldap:repositories>");
            return sb.toString();
        } else {
            return Strings.EMPTY;
        }
    }

    private Map<Class<?>, Set<Class<?>>> scanForLdapRepositories(final String basePackage) {
        final Map<Class<?>, Set<Class<?>>> ldapRepositories = new HashMap<Class<?>, Set<Class<?>>>();
        final ClassPathScanner scanner = new ClassPathScanner().withInterfacesOnly();
        scanner.addIncludeFilter(new AssignableTypeFilter(LdapRepository.class));

        final Set<BeanDefinition> candidateComponents = scanner.findCandidateComponents(basePackage);
        for (final BeanDefinition bd : candidateComponents) {
            final String beanClassName = bd.getBeanClassName();
            final Class<?> repositoryClass = Reflections.classForName(beanClassName);
            if (!ALdapDao.class.isAssignableFrom(repositoryClass)) {
                final Class<?>[] typeArguments = Reflections.resolveTypeArguments(repositoryClass,
                        LdapRepository.class);
                Assertions.assertThat(typeArguments.length).isEqualTo(2);
                final Class<?> entity = typeArguments[0];
                Set<Class<?>> set = ldapRepositories.get(entity);
                if (set == null) {
                    set = new HashSet<Class<?>>();
                    ldapRepositories.put(entity, set);
                }
                Assertions.assertThat(set.add(repositoryClass)).isTrue();
            }
        }
        return ldapRepositories;
    }

}