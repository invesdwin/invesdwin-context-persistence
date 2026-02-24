package de.invesdwin.context.persistence.timeseriesdb.array;

import java.io.File;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.util.collections.attributes.IAttributesMap;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.lang.string.UniqueNameGenerator;

@ThreadSafe
public class TemporaryFlyweightPrimitiveArrayAllocator extends FlyweightPrimitiveArrayAllocator {

    private static final UniqueNameGenerator UNIQUE_NAME_GENERATOR = new UniqueNameGenerator();
    private static final File DEFAULT_BASE_DIRECTORY = new File(ContextProperties.TEMP_DIRECTORY,
            TemporaryFlyweightPrimitiveArrayAllocator.class.getSimpleName());

    private final TemporaryFlyweightPrimitiveArrayAllocatorFinalizer finalizer;

    public TemporaryFlyweightPrimitiveArrayAllocator(final String name) {
        this(name, DEFAULT_BASE_DIRECTORY);
    }

    public TemporaryFlyweightPrimitiveArrayAllocator(final String name, final File baseDirectory) {
        super(UNIQUE_NAME_GENERATOR.get(name), new File(baseDirectory, UNIQUE_NAME_GENERATOR.get(name)));
        this.finalizer = new TemporaryFlyweightPrimitiveArrayAllocatorFinalizer(getMap(), getAttributes(),
                getProperties(), getDirectory());
        this.finalizer.register(this);
    }

    @Override
    public void close() {
        super.close();
        finalizer.close();
    }

    private static final class TemporaryFlyweightPrimitiveArrayAllocatorFinalizer extends AFinalizer {

        private FlyweightPrimitiveArrayPersistentMap<String> map;
        private IAttributesMap attributes;
        private IProperties properties;
        private File directory;

        private TemporaryFlyweightPrimitiveArrayAllocatorFinalizer(
                final FlyweightPrimitiveArrayPersistentMap<String> map, final IAttributesMap attributes,
                final IProperties properties, final File directory) {
            this.map = map;
            this.attributes = attributes;
            this.properties = properties;
            this.directory = directory;
        }

        @Override
        protected void clean() {
            final FlyweightPrimitiveArrayPersistentMap<String> mapCopy = map;
            if (mapCopy != null) {
                mapCopy.clear();
                map = null;
            }
            final IAttributesMap attributesCopy = attributes;
            if (attributesCopy != null) {
                attributesCopy.clear();
                attributes = null;
            }
            final IProperties propertiesCopy = properties;
            if (propertiesCopy != null) {
                propertiesCopy.clear();
                properties = null;
            }
            final File directoryCopy = directory;
            if (directoryCopy != null) {
                Files.deleteNative(directoryCopy);
                directory = null;
            }
        }

        @Override
        protected boolean isCleaned() {
            return map == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }
    }

}
