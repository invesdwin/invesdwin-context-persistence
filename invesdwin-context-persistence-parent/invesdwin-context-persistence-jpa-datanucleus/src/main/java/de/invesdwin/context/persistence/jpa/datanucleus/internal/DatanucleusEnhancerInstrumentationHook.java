package de.invesdwin.context.persistence.jpa.datanucleus.internal;

import java.lang.instrument.IllegalClassFormatException;
import java.lang.instrument.Instrumentation;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.datanucleus.enhancer.DataNucleusClassFileTransformer;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.core.type.filter.AssignableTypeFilter;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.PlatformInitializerProperties;
import de.invesdwin.context.beans.hook.IInstrumentationHook;
import de.invesdwin.context.beans.init.platform.DelegatePlatformInitializer;
import de.invesdwin.context.beans.init.platform.util.RegisterTypesForSerializationConfigurer;
import de.invesdwin.util.classpath.ClassPathScanner;
import de.invesdwin.util.lang.reflection.Reflections;

@NotThreadSafe
public class DatanucleusEnhancerInstrumentationHook implements IInstrumentationHook {

    static {
        Reflections.disableJavaModuleSystemRestrictions();
        PlatformInitializerProperties
                .setInitializer(new DelegatePlatformInitializer(PlatformInitializerProperties.getInitializer()) {
                    @Override
                    public void registerTypesForSerialization() {
                        new RegisterTypesForSerializationConfigurer() {
                            @Override
                            protected List<java.lang.Class<?>> scanSerializableClassesToRegister() {
                                //datanucleus somehow does not work when ClassGraph is used for scanning the classes
                                final ClassPathScanner scanner = new ClassPathScanner();
                                scanner.addIncludeFilter(new AssignableTypeFilter(SERIALIZABLE_INTERFACE));
                                final List<Class<?>> classesToRegister = new ArrayList<Class<?>>();
                                for (final String basePackage : ContextProperties.getBasePackages()) {
                                    for (final BeanDefinition bd : scanner.findCandidateComponents(basePackage)) {
                                        final Class<?> clazz = Reflections.classForName(bd.getBeanClassName());
                                        classesToRegister.add(clazz);
                                    }
                                }
                                return classesToRegister;
                            }
                        }.registerTypesForSerialization();
                    }
                });
    }

    @Override
    public void instrument(final Instrumentation instrumentation) {
        instrumentation.addTransformer(new DataNucleusClassFileTransformer("-api=Jakarta", null) {
            @Override
            public byte[] transform(final ClassLoader loader, final String className, final Class classBeingRedefined,
                    final ProtectionDomain protectionDomain, final byte[] classfileBuffer)
                    throws IllegalClassFormatException {
                final String name = className.replace('/', '.');
                if (name.startsWith("jakarta.")) {
                    return null;
                }
                return super.transform(loader, className, classBeingRedefined, protectionDomain, classfileBuffer);
            }
        });
    }

}
