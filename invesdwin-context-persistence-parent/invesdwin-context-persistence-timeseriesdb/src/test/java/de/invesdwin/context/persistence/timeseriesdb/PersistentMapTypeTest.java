package de.invesdwin.context.persistence.timeseriesdb;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.persistentmap.APersistentMapConfig;
import de.invesdwin.context.integration.persistentmap.large.LargePersistentMapFactory;
import de.invesdwin.context.integration.persistentmap.large.storage.FileChunkStorage;
import de.invesdwin.context.integration.persistentmap.large.storage.IChunkStorage;
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

        final LargePersistentMapFactory<Integer, byte[]> factoryMapped = new LargePersistentMapFactory<>(
                PersistentMapType.DISK_SAFE.newFactory());
        final LargePersistentMapFactory<Integer, byte[]> factoryFile = new LargePersistentMapFactory<Integer, byte[]>(
                PersistentMapType.DISK_SAFE.newFactory()) {
            @Override
            protected IChunkStorage<byte[]> newChunkStorage(final File directory, final ISerde<byte[]> valueSerde,
                    final boolean readOnly, final boolean closeAllowed) {
                return new FileChunkStorage<byte[]>(directory, valueSerde);
            }
        };
        final APersistentMapConfig<Integer, byte[]> configMapped = new APersistentMapConfig<Integer, byte[]>(
                "testRandomMapped") {

            @Override
            public boolean isDiskPersistence() {
                return true;
            }

            @Override
            public File getDirectory() {
                return dir;
            }
        };
        final APersistentMapConfig<Integer, byte[]> configFile = new APersistentMapConfig<Integer, byte[]>(
                "testRandomFile") {

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

        final ConcurrentMap<Integer, byte[]> mapMapped = factoryMapped.newPersistentMap(configMapped);
        final ConcurrentMap<Integer, byte[]> mapFile = factoryFile.newPersistentMap(configFile);
        final IRandomGenerator random = PseudoRandomGenerators.newPseudoRandom();
        for (int i = 0; i < 100; i++) {
            final byte[] bytes = new byte[random
                    .nextInt((int) ByteSizeScale.BYTES.convert(100, ByteSizeScale.MEGABYTES))];
            random.nextBytes(bytes);
            mapMapped.put(i, bytes);
            mapFile.put(i, bytes);

            size += bytes.length;

            final byte[] bytesFile = mapFile.get(i);
            if (!ByteBuffers.equals(bytes, bytesFile)) {
                throw new IllegalStateException("file " + i + " " + size);
            }
            final byte[] bytesMapped = mapMapped.get(i);
            if (!ByteBuffers.equals(bytes, bytesMapped)) {
                throw new IllegalStateException("mapped " + i + " " + size);
            }

            if (size >= maxSize) {
                break;
            }

        }

    }

}
