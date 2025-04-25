package de.invesdwin.context.persistence.timeseriesdb;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.persistentmap.APersistentMapConfig;
import de.invesdwin.context.integration.persistentmap.large.LargePersistentMapFactory;
import de.invesdwin.context.integration.persistentmap.large.storage.IChunkStorage;
import de.invesdwin.context.integration.persistentmap.large.storage.MappedFileChunkStorage;
import de.invesdwin.context.integration.persistentmap.large.storage.ParallelFileChunkStorage;
import de.invesdwin.context.integration.persistentmap.large.storage.SequentialFileChunkStorage;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.math.decimal.scaled.ByteSizeScale;
import de.invesdwin.util.math.random.IRandomGenerator;
import de.invesdwin.util.math.random.PseudoRandomGenerators;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;

@Disabled("manual test")
@NotThreadSafe
public class PersistentMapTypeTest extends ATest {

    @Test
    public void testRandom() throws IOException {
        final File dir = new File(ContextProperties.getCacheDirectory(), PersistentMapTypeTest.class.getSimpleName());
        Files.deleteNative(dir);
        Files.forceMkdir(dir);

        final LargePersistentMapFactory<Integer, byte[]> factoryMappedFile = new LargePersistentMapFactory<Integer, byte[]>(
                PersistentMapType.DISK_SAFE.newFactory()) {
            @Override
            protected IChunkStorage<byte[]> newChunkStorage(final File directory, final ISerde<byte[]> valueSerde,
                    final boolean readOnly, final boolean closeAllowed) {
                return new MappedFileChunkStorage<byte[]>(directory, valueSerde, readOnly, closeAllowed);
            }
        };
        final LargePersistentMapFactory<Integer, byte[]> factoryParallelFile = new LargePersistentMapFactory<Integer, byte[]>(
                PersistentMapType.DISK_SAFE.newFactory()) {
            @Override
            protected IChunkStorage<byte[]> newChunkStorage(final File directory, final ISerde<byte[]> valueSerde,
                    final boolean readOnly, final boolean closeAllowed) {
                return new ParallelFileChunkStorage<byte[]>(directory, valueSerde);
            }
        };
        final LargePersistentMapFactory<Integer, byte[]> factorySequentialFile = new LargePersistentMapFactory<Integer, byte[]>(
                PersistentMapType.DISK_SAFE.newFactory()) {
            @Override
            protected IChunkStorage<byte[]> newChunkStorage(final File directory, final ISerde<byte[]> valueSerde,
                    final boolean readOnly, final boolean closeAllowed) {
                return new SequentialFileChunkStorage<byte[]>(directory, valueSerde);
            }
        };
        final APersistentMapConfig<Integer, byte[]> configMappedFile = new APersistentMapConfig<Integer, byte[]>(
                "testRandomMappedFile") {

            @Override
            public boolean isDiskPersistence() {
                return true;
            }

            @Override
            public File getDirectory() {
                return dir;
            }
        };
        final APersistentMapConfig<Integer, byte[]> configParallelFile = new APersistentMapConfig<Integer, byte[]>(
                "testRandomParallelFile") {

            @Override
            public boolean isDiskPersistence() {
                return true;
            }

            @Override
            public File getDirectory() {
                return dir;
            }
        };
        final APersistentMapConfig<Integer, byte[]> configSequentialFile = new APersistentMapConfig<Integer, byte[]>(
                "testRandomSequentialFile") {

            @Override
            public boolean isDiskPersistence() {
                return true;
            }

            @Override
            public File getDirectory() {
                return dir;
            }
        };
        final long maxSize = (long) ByteSizeScale.BYTES.convert(10, ByteSizeScale.GIGABYTES);
        long size = 0;

        final ConcurrentMap<Integer, byte[]> mapMappedFile = factoryMappedFile.newPersistentMap(configMappedFile);
        final ConcurrentMap<Integer, byte[]> mapParallelFile = factoryParallelFile.newPersistentMap(configParallelFile);
        final ConcurrentMap<Integer, byte[]> mapSequentialFile = factorySequentialFile
                .newPersistentMap(configSequentialFile);
        final IRandomGenerator random = PseudoRandomGenerators.newPseudoRandom();
        for (int i = 0; i < 100; i++) {
            final byte[] bytes = new byte[random
                    .nextInt((int) ByteSizeScale.BYTES.convert(100, ByteSizeScale.MEGABYTES))];
            random.nextBytes(bytes);
            mapMappedFile.put(i, bytes);
            mapParallelFile.put(i, bytes);
            mapSequentialFile.put(i, bytes);

            size += bytes.length;

            assertGet("mappedFile", size, mapMappedFile, i, bytes);
            assertGet("parallelFile", size, mapParallelFile, i, bytes);
            assertGet("sequentialFile", size, mapSequentialFile, i, bytes);

            if (size >= maxSize) {
                break;
            }

        }

    }

    private void assertGet(final String name, final long size, final ConcurrentMap<Integer, byte[]> map, final int i,
            final byte[] bytes) {
        final byte[] bytesFile = map.get(i);
        if (!ByteBuffers.equals(bytes, bytesFile)) {
            throw new IllegalStateException(name + " " + i + " " + size);
        }
    }

}
