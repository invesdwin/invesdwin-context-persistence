package de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.FileInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.string.Charsets;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;

/**
 * The generation NAME a publish claims must come from the directory, not from the counter the table latched at
 * construction. Two tables over one key directory exist whenever a heartbeat update claim goes stale on a share and a
 * second process takes the key over - the same condition the epoch reclaim's liveness guard is written for - and each
 * instance latched the newest number at its own construction time. With a latched counter both pick the same
 * '&lt;N&gt;_memory.index', and because the channel publishes by rename (which replaces an existing target), the
 * second publish silently deletes the chain the first one published: no exception, and a reader only sees a shorter
 * store.
 */
@NotThreadSafe
public class VersionedTimeSeriesMemoryFileLookupTableTest extends ATest {

    private static final String KEY = "generationClaimKey";

    private File baseDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirectory = new File(ContextProperties.TEMP_DIRECTORY,
                VersionedTimeSeriesMemoryFileLookupTableTest.class.getSimpleName());
        Files.deleteQuietly(baseDirectory);
        Files.forceMkdir(baseDirectory);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Files.deleteQuietly(baseDirectory);
    }

    /**
     * Both tables are constructed before anything is published, which is the state two nodes reach when one opened the
     * key earlier and its claim then went stale. Each publish must still land in its own generation, and the first
     * generation's bytes must survive the second publish untouched.
     */
    @Test
    public void testPublishClaimsADiskDerivedGenerationNotTheLatchedCounter() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("generationClaim");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final File directory = lookup.getDirectoryHashKeyVersionMemory().getDirectoryHashKeyVersionDataShared();
        Files.forceMkdir(directory);

        final VersionedTimeSeriesMemoryFileLookupTable<FDate> first = new VersionedTimeSeriesMemoryFileLookupTable<>(
                lookup, directory, 0);
        final VersionedTimeSeriesMemoryFileLookupTable<FDate> second = new VersionedTimeSeriesMemoryFileLookupTable<>(
                lookup, directory, 0);
        Assertions.assertThat(first.getCurrentIndexNumber())
                .as("a table on an empty directory latches no generation")
                .isNegative();
        Assertions.assertThat(second.getCurrentIndexNumber())
                .as("and so does the second table over the same directory")
                .isNegative();

        first.put(new SingleSummaryIterator(summary(1, 10L)));
        final List<File> afterFirst = findGenerations(directory);
        Assertions.assertThat(afterFirst).as("the first publish lands one generation").hasSize(1);
        final byte[] firstGenerationBytes = readBytes(afterFirst.get(0));
        Assertions.checkTrue(firstGenerationBytes.length > 0, "the published generation must not be empty");

        second.put(new SingleSummaryIterator(summary(2, 20L)));

        final List<File> afterSecond = findGenerations(directory);
        Assertions.assertThat(afterSecond).as("the second publish must not reuse the first generation name").hasSize(2);
        Assertions.assertThat(AMemoryFileSummarySerializingCollection.indexNumberOf(afterSecond.get(0).getName()))
                .as("generation numbers must be distinct")
                .isNotEqualTo(
                        AMemoryFileSummarySerializingCollection.indexNumberOf(afterSecond.get(1).getName()));

        final File surviving = findGenerationNamed(directory, afterFirst.get(0).getName());
        Assertions.assertThat(readBytes(surviving))
                .as("the chain the first writer published must survive the second publish byte for byte")
                .isEqualTo(firstGenerationBytes);

        Assertions.checkTrue(second.getCurrentIndexNumber() > first.getCurrentIndexNumber(),
                "the later publish must carry the higher generation number, got " + first.getCurrentIndexNumber()
                        + " and " + second.getCurrentIndexNumber());
    }

    /**
     * A table constructed while generations exist must continue above the newest one on disk, even if its own counter
     * was latched before an older sibling published.
     */
    @Test
    public void testNewTableContinuesAboveTheGenerationOnDisk() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("generationContinue");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final File directory = lookup.getDirectoryHashKeyVersionMemory().getDirectoryHashKeyVersionDataShared();
        Files.forceMkdir(directory);

        final VersionedTimeSeriesMemoryFileLookupTable<FDate> early = new VersionedTimeSeriesMemoryFileLookupTable<>(
                lookup, directory, 0);
        final VersionedTimeSeriesMemoryFileLookupTable<FDate> other = new VersionedTimeSeriesMemoryFileLookupTable<>(
                lookup, directory, 0);
        other.put(new SingleSummaryIterator(summary(1, 10L)));
        other.put(new SingleSummaryIterator(summary(2, 20L)));
        final int newestOnDisk = newestGenerationNumber(directory);
        Assertions.assertThat(newestOnDisk).as("two publishes by the other table").isEqualTo(1);

        early.put(new SingleSummaryIterator(summary(3, 30L)));

        Assertions.assertThat(newestGenerationNumber(directory))
                .as("a table whose counter was latched earlier must still publish above the newest generation on disk")
                .isEqualTo(newestOnDisk + 1);
        Assertions.assertThat(findGenerations(directory)).hasSize(3);
    }

    /**
     * The window between the directory listing and the publish, closed: a second writer that takes the name in that
     * window must be answered with an error, not with a rename over its chain. The fixture is an iterator that creates
     * the generation name the publish is about to claim while it is being drained, which is what another process does
     * to that name; nothing in production code is aware of the fixture.
     */
    @Test
    public void testPublishIntoANameClaimedInsideTheWindowFailsLoudly() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("generationRace");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final File directory = lookup.getDirectoryHashKeyVersionMemory().getDirectoryHashKeyVersionDataShared();
        Files.forceMkdir(directory);

        final VersionedTimeSeriesMemoryFileLookupTable<FDate> winner = new VersionedTimeSeriesMemoryFileLookupTable<>(
                lookup, directory, 0);
        winner.put(new SingleSummaryIterator(summary(1, 10L)));
        final int takenNumber = newestGenerationNumber(directory) + 1;
        final String takenName = AMemoryFileSummarySerializingCollection.newIndexGenerationFileName(takenNumber);

        final VersionedTimeSeriesMemoryFileLookupTable<FDate> loser = new VersionedTimeSeriesMemoryFileLookupTable<>(
                lookup, directory, 0);
        final byte[] winnerBytes = "the other writer published this name first".getBytes(Charsets.UTF_8);
        try {
            loser.put(new ClaimStealingIterator(summary(2, 20L), new File(directory, takenName), winnerBytes));
            Assertions.fail("publishing into a generation another writer claimed must fail, not replace it");
        } catch (final IllegalStateException e) {
            Assertions.assertThat(e.getMessage())
                    .as("the failure must name the directory it could not claim in")
                    .contains(takenName).contains(directory.getName());
        }

        Assertions.assertThat(readBytes(new File(directory, takenName)))
                .as("the winner's chain must survive the refused publish untouched")
                .isEqualTo(digest(winnerBytes));
        Assertions.assertThat(newestGenerationNumber(directory))
                .as("the refused publish must not have added a generation of its own")
                .isEqualTo(takenNumber);
    }

    private static byte[] digest(final byte[] bytes) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(bytes);
    }

    /**
     * Serves one summary and creates {@code steal} on the call that ends the iteration - after the lookup table has
     * already picked its generation name, before it publishes it.
     */
    private static final class ClaimStealingIterator extends SingleSummaryIterator {

        private final File steal;
        private final byte[] bytes;

        private ClaimStealingIterator(final MemoryFileSummary summary, final File steal, final byte[] bytes) {
            super(summary);
            this.steal = steal;
            this.bytes = bytes;
        }

        @Override
        public MemoryFileSummary next() {
            try {
                return super.next();
            } catch (final NoSuchElementException e) {
                //the iteration ends here, and the publish of the generation name follows it
                try {
                    Files.write(steal.toPath(), bytes);
                } catch (final IOException io) {
                    throw new UncheckedIOException(io);
                }
                throw e;
            }
        }
    }

    private static MemoryFileSummary summary(final int year, final long length) {
        final FDate value = FDateBuilder.newDate(year, 1, 1);
        return new MemoryFileSummary(value, FDateSerde.GET, value, value, 0L, 1, "memory_" + year + "-gene0001.data",
                0L, 0L, length);
    }

    private static List<File> findGenerations(final File directory) {
        final List<File> found = new ArrayList<>();
        final File[] files = directory.listFiles();
        if (files != null) {
            for (final File file : files) {
                if (AMemoryFileSummarySerializingCollection.isIndexGenerationFileName(file.getName())) {
                    found.add(file);
                }
            }
        }
        found.sort((a, b) -> Integer.compare(AMemoryFileSummarySerializingCollection.indexNumberOf(a.getName()),
                AMemoryFileSummarySerializingCollection.indexNumberOf(b.getName())));
        return found;
    }

    private static File findGenerationNamed(final File directory, final String name) {
        final File file = new File(directory, name);
        Assertions.checkTrue(file.exists(), "generation [" + name + "] must still be on disk");
        return file;
    }

    private static int newestGenerationNumber(final File directory) {
        final List<File> generations = findGenerations(directory);
        return generations.isEmpty() ? Integer.MIN_VALUE
                : AMemoryFileSummarySerializingCollection.indexNumberOf(generations.get(generations.size() - 1).getName());
    }

    /**
     * A digest rather than the bytes: the generation file is LZ4-framed binary, and the assertion only needs to know
     * whether anything in it changed.
     */
    private static byte[] readBytes(final File file) throws Exception {
        final MessageDigest digest = MessageDigest.getInstance("SHA-256");
        final byte[] buffer = new byte[8192];
        try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(file))) {
            int read;
            while ((read = in.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
        }
        return digest.digest();
    }

    private ATimeSeriesDB<String, FDate> newTable(final String name) {
        return new ATimeSeriesDB<String, FDate>(name) {

            @Override
            protected ISerde<FDate> newValueSerde() {
                return FDateSerde.GET;
            }

            @Override
            protected Integer newValueFixedLength() {
                return FDateSerde.FIXED_LENGTH;
            }

            @Override
            protected int newBatchFlushInterval() {
                return 4;
            }

            @Override
            public ITimeSeriesBaseDirectory getBaseDirectory() {
                return new TimeSeriesBaseDirectory(baseDirectory);
            }

            @Override
            public String innerHashKeyToString(final String key) {
                return key;
            }

            @Override
            public FDate extractStartTime(final FDate value) {
                return value;
            }

            @Override
            public FDate extractEndTime(final FDate value) {
                return value;
            }
        };
    }

    private static class SingleSummaryIterator implements ICloseableIterator<MemoryFileSummary> {

        private final MemoryFileSummary summary;
        private boolean served;

        SingleSummaryIterator(final MemoryFileSummary summary) {
            this.summary = summary;
        }

        @Override
        public boolean hasNext() {
            return !served;
        }

        @Override
        public MemoryFileSummary next() {
            if (served) {
                throw FastNoSuchElementException.getInstance("end reached");
            }
            served = true;
            return summary;
        }

        @Override
        public void remove() {
            throw FastNoSuchElementException.getInstance("unsupported");
        }

        @Override
        public void close() {
            //nothing to release
        }
    }
}
