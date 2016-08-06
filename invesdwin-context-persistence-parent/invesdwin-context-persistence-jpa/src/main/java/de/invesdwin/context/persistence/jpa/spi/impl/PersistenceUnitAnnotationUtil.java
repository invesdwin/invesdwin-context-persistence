package de.invesdwin.context.persistence.jpa.spi.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.Immutable;
import javax.persistence.Entity;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.core.type.filter.AnnotationTypeFilter;

import de.invesdwin.context.persistence.jpa.PersistenceProperties;
import de.invesdwin.context.persistence.jpa.api.PersistenceUnitName;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.classpath.ClassPathScanner;
import de.invesdwin.util.lang.Reflections;
import de.invesdwin.util.lang.Strings;

@Immutable
public final class PersistenceUnitAnnotationUtil {

    public static final String PERSISTENCE_UNIT_CONFIG_PREFIX = "@";

    private PersistenceUnitAnnotationUtil() {}

    public static String getPersistenceUnitNameFromPersistenceUnitNameAnnotation(final Class<?> clazz) {
        final PersistenceUnitName annotation = Reflections.getAnnotation(clazz, PersistenceUnitName.class);
        String persistenceUnitName = null;
        if (annotation != null) {
            Assertions.assertThat(annotation.value())
            .as("%s should not be empty: %s", PersistenceUnitName.class.getSimpleName(), clazz.getName())
            .isNotEmpty();
            Assertions.assertThat(annotation.value()).doesNotContain(PERSISTENCE_UNIT_CONFIG_PREFIX);
            persistenceUnitName = annotation.value();
        }
        return persistenceUnitName;
    }

    public static Map<String, Set<Class<?>>> scanForEntities(final String basePackage) {
        final Map<String, Set<Class<?>>> entities = new HashMap<String, Set<Class<?>>>();
        final ClassPathScanner scanner = new ClassPathScanner();
        /*
         * http://java.sun.com/xml/ns/persistence/persistence_2_0.xsd
         * 
         * Managed class to be included in the persistence unit and to scan for annotations. It should be annotated with
         * either @Entity, @Embeddable or @MappedSuperclass.
         * 
         * --> @Embeddable and @MappedSuperclass actually not needed here
         */
        scanner.addIncludeFilter(new AnnotationTypeFilter(Entity.class));

        for (final BeanDefinition bd : scanner.findCandidateComponents(basePackage)) {
            final String beanClassName = bd.getBeanClassName();
            final Class<?> entityClass = Reflections.classForName(beanClassName);
            String persistenceUnitName = PersistenceUnitAnnotationUtil.getPersistenceUnitNameFromPersistenceUnitNameAnnotation(entityClass);
            if (Strings.isBlank(persistenceUnitName)) {
                persistenceUnitName = PersistenceProperties.DEFAULT_PERSISTENCE_UNIT_NAME;
            }
            Set<Class<?>> set = entities.get(persistenceUnitName);
            if (set == null) {
                set = new HashSet<Class<?>>();
                entities.put(persistenceUnitName, set);
            }
            Assertions.assertThat(set.add(entityClass)).isTrue();
        }
        return entities;
    }
}
