package de.invesdwin.context.persistence.jpa.spi.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.persistence.jpa.PersistenceProperties;
import de.invesdwin.context.persistence.jpa.api.PersistenceUnitName;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.classpath.FastClassPathScanner;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.lang.string.Strings;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;
import jakarta.persistence.Entity;

@Immutable
public final class PersistenceUnitAnnotationUtil {

    public static final String PERSISTENCE_UNIT_CONFIG_PREFIX = "@";
    private static Map<String, Set<Class<?>>> cachedEntities;

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

    public static synchronized Map<String, Set<Class<?>>> scanForEntities() {
        if (cachedEntities == null) {
            final Map<String, Set<Class<?>>> entities = new HashMap<String, Set<Class<?>>>();
            final ScanResult scanner = FastClassPathScanner.getScanResult();
            /*
             * http://java.sun.com/xml/ns/persistence/persistence_2_0.xsd
             * 
             * Managed class to be included in the persistence unit and to scan for annotations. It should be annotated
             * with either @Entity, @Embeddable or @MappedSuperclass.
             * 
             * --> @Embeddable and @MappedSuperclass actually not needed here
             */
            for (final ClassInfo ci : scanner.getClassesWithAnnotation(Entity.class.getName())) {
                final String beanClassName = ci.getName();
                final Class<?> entityClass = Reflections.classForName(beanClassName);
                String persistenceUnitName = PersistenceUnitAnnotationUtil
                        .getPersistenceUnitNameFromPersistenceUnitNameAnnotation(entityClass);
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
            cachedEntities = entities;
        }
        return cachedEntities;
    }
}
