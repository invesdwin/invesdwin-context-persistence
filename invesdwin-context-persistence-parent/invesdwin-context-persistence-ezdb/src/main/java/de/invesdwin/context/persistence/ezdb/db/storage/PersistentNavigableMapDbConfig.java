package de.invesdwin.context.persistence.ezdb.db.storage;

import java.io.File;
import java.util.Comparator;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.persistentmap.navigable.APersistentNavigableMap;
import de.invesdwin.context.integration.persistentmap.navigable.APersistentNavigableMapConfig;
import de.invesdwin.context.integration.persistentmap.navigable.IPersistentNavigableMapFactory;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.NioByteBufferSerde;

@Immutable
public final class PersistentNavigableMapDbConfig
        extends APersistentNavigableMapConfig<java.nio.ByteBuffer, java.nio.ByteBuffer> {
    private final IPersistentNavigableMapFactory<java.nio.ByteBuffer, java.nio.ByteBuffer> factory;
    private final File baseDirectory;
    private final Comparator<java.nio.ByteBuffer> comparator;

    public PersistentNavigableMapDbConfig(final String name,
            final IPersistentNavigableMapFactory<java.nio.ByteBuffer, java.nio.ByteBuffer> factory,
            final File baseDirectory, final Comparator<java.nio.ByteBuffer> comparator) {
        super(name);
        this.factory = factory;
        this.baseDirectory = baseDirectory;
        this.comparator = comparator;
    }

    @Override
    public boolean isDiskPersistence() {
        return factory.isDiskPersistenceSupported();
    }

    @Override
    public ISerde<java.nio.ByteBuffer> newKeySerde() {
        return NioByteBufferSerde.GET;
    }

    @Override
    public ISerde<java.nio.ByteBuffer> newValueSerde() {
        return NioByteBufferSerde.GET;
    }

    @Override
    public Class<java.nio.ByteBuffer> getKeyType() {
        return java.nio.ByteBuffer.class;
    }

    @Override
    public Class<java.nio.ByteBuffer> getValueType() {
        return java.nio.ByteBuffer.class;
    }

    @Override
    public File getDirectory() {
        return new File(new File(getBaseDirectory(), APersistentNavigableMap.class.getSimpleName()),
                Reflections.getClassSimpleNameNonBlank(factory.getClass()));
    }

    @Override
    public File getBaseDirectory() {
        return baseDirectory;
    }

    @Override
    public Comparator<java.nio.ByteBuffer> newComparator() {
        return comparator;
    }
}