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
import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.filechannel.nio.atomic.AtomicNioFileChannel;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummarySerde;
import de.invesdwin.context.system.properties.ICloseableProperties;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.lang.string.description.TextDescription;
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

    /**
     * The publish writes no {@code memory.log} record: the trail moved to the update attempt, which leaves one record
     * stating what the store holds when it closes - committed or aborted. ("Nothing was added, so nothing is written"
     * is pinned where it is real, on the attempt: TimeSeriesResumableUpdateAttemptTest. A guard for an empty batch here
     * would be dead code - publishStagedSummaries() is only reached when the attempt staged a file.)
     */
    @Test
    public void testAPublishWritesNoLogRecord() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("publishLog");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final File directory = lookup.getDirectoryHashKeyVersionMemory().getDirectoryHashKeyVersionDataShared();
        Files.forceMkdir(directory);

        final VersionedTimeSeriesMemoryFileLookupTable<FDate> publisher = new VersionedTimeSeriesMemoryFileLookupTable<>(
                lookup, directory, 0);
        publisher.put(new ListSummaryIterator(List.of(summary(1, 10L), summary(2, 20L), summary(3, 30L))));
        final int generations = findGenerations(directory).size();
        Assertions.assertThat(generations).as("a real publish lands one generation").isEqualTo(1);
        Assertions.assertThat(new File(directory, "memory.log").exists())
                .as("a publish writes no memory.log record - the record belongs to the update attempt")
                .isFalse();

        Assertions.assertThat(new File(directory, "memory.log").exists())
                .as("and no publish writes a log record")
                .isFalse();
    }

    /**
     * The cost this layout exists to remove: a generation above the base carries the records its own publish staged,
     * not the whole chain. Pinned on record counts rather than wall time, because the point is that a publish stops
     * re-emitting what is already stored - the live measurement behind it is 4,302,137 B of index I/O and 27,533,743 B
     * of inflate+deflate to persist 1,457 B of new content (ptstaging worst key, 2026-11-14, lap-mid over ssh).
     */
    @Test
    public void testADeltaGenerationCarriesOnlyTheRecordsItsOwnPublishStaged() throws Exception {
        final PublishedChain published = publishChain("deltaShape", 2, 1);
        Assertions.assertThat(published.generations).as("two publishes land two generations").hasSize(2);
        Assertions.assertThat(readGenerationSummaries(published.db, published.generations.get(0)))
                .as("the base holds the chain as of that publish")
                .hasSize(2);
        Assertions.assertThat(readGenerationSummaries(published.db, published.generations.get(1)))
                .as("a generation above the base holds only what its own publish staged")
                .hasSize(1);
        try (ICloseableProperties properties = published.publisher.getMetadata().getProperties()) {
            Assertions.assertThat(published.publisher.getMetadata().getChainBase(properties))
                    .as("the base stays at the full snapshot")
                    .isZero();
        }
        Assertions.assertThat(publishedChain(published.publisher)).as("reads assemble the whole chain").hasSize(3);
    }

    /**
     * The one operation the index has that is not an append: the redo of the standalone '_incomplete_' last file, whose
     * replacement names the byte region the current tail already names. It rewrites the chain rather than punching a
     * hole in it, and that file becomes the base - which is what keeps the live chain a contiguous generation range a
     * reader can assemble without being told which generations were superseded.
     */
    @Test
    public void testAChainTailReplacementRewritesTheChainAndBecomesTheBase() throws Exception {
        final PublishedChain published = publishChain("baseRewrite", 2, 1);
        //the chain's last record is the day-3 position at extent 30; the replacement re-serialises it shorter
        published.publisher.put(new SingleSummaryIterator(summaryAtDay(3, 25L)));

        try (ICloseableProperties properties = published.publisher.getMetadata().getProperties()) {
            Assertions.assertThat(published.publisher.getMetadata().getChainBase(properties))
                    .as("the rewrite becomes the chain base")
                    .isEqualTo(published.publisher.getCurrentIndexNumber());
        }
        final List<MemoryFileSummary> chain = publishedChain(published.publisher);
        Assertions.assertThat(chain).as("the replacement leaves the chain the same length").hasSize(3);
        Assertions.assertThat(chain.get(chain.size() - 1).getCombinedMemoryFileSize())
                .as("and the replaced position now carries the shorter extent")
                .isEqualTo(25L);
    }

    /**
     * How far a chain may drift above its base before a publish folds it back. The delta count bounds both the files a
     * read assembles and the bytes the next rewrite costs, and the fold has to be the writer's own: the reclaim sweep
     * runs only at attempt end, and a live service's attempt does not end until the process restarts.
     */
    @Test
    public void testTheDeltaCountFoldsTheChainIntoANewBase() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("deltaFold");
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache(KEY);
        final File directory = lookup.getDirectoryHashKeyVersionMemory().getDirectoryHashKeyVersionDataShared();
        Files.forceMkdir(directory);
        final VersionedTimeSeriesMemoryFileLookupTable<FDate> publisher = new VersionedTimeSeriesMemoryFileLookupTable<>(
                lookup, directory, 0);

        for (int day = 1; day <= 400; day++) {
            publisher.put(new SingleSummaryIterator(summaryAtDay(day, day * 10L)));
        }

        final MemoryFileMetadata metadata = publisher.getMetadata();
        final int chainBase;
        try (ICloseableProperties properties = metadata.getProperties()) {
            chainBase = metadata.getChainBase(properties);
        }
        Assertions.checkTrue(chainBase > 0, "400 publishes must have folded the chain, base is still " + chainBase);
        final int liveRange = publisher.getCurrentIndexNumber() - chainBase + 1;
        Assertions.checkTrue(liveRange <= 257,
                "no read assembles more than the delta bound plus its base, got " + liveRange);
        Assertions.assertThat(publishedChain(publisher)).as("the folded chain still holds every record")
                .hasSize(400);
    }

    /**
     * A chain that lost a member is damage, not a shorter store. Answering it with whatever happens to be present
     * gives a chain whose byte space does not tile, and a shorter answer is what the derived-store consumers take as
     * licence to rebuild the key.
     */
    @Test
    public void testAMissingChainMemberIsReportedRatherThanAnsweredWithAShorterChain() throws Exception {
        final PublishedChain published = publishChain("chainHole", 1, 1, 1);
        final File hole = published.generations.get(1);
        Assertions.checkTrue(hole.delete(), "the scenario removes a middle generation: " + hole);

        final VersionedTimeSeriesMemoryFileLookupTable<FDate> reader = new VersionedTimeSeriesMemoryFileLookupTable<>(
                published.lookup, published.directory, 0);
        try (ICloseableIterator<MemoryFileSummary> ignored = reader.range()) {
            Assertions.fail("a chain missing one of its generations must not be served as a shorter chain");
        } catch (final IllegalStateException e) {
            Assertions.assertThat(e.getMessage()).as("the failure must name the member that is gone")
                    .contains("missing generation [1]");
        }
    }

    /**
     * The base is the only full snapshot, so a base above the newest generation present means the base file itself is
     * gone and the chain cannot be assembled from deltas. The sweep never creates this state (it prunes below the
     * base), so it is reported rather than papered over.
     */
    @Test
    public void testALostChainBaseIsReportedAndNotSilentlyReplacedByADelta() throws Exception {
        final PublishedChain published = publishChain("lostBase", 2, 1);
        published.publisher.put(new SingleSummaryIterator(summaryAtDay(3, 25L)));
        final File base = new File(published.directory, AMemoryFileSummarySerializingCollection
                .newIndexGenerationFileName(published.publisher.getCurrentIndexNumber()));
        Assertions.checkTrue(base.exists(), "the rewrite must have landed a base: " + base);
        Assertions.checkTrue(base.delete(), "the scenario removes the base generation: " + base);

        final VersionedTimeSeriesMemoryFileLookupTable<FDate> reader = new VersionedTimeSeriesMemoryFileLookupTable<>(
                published.lookup, published.directory, 0);
        try (ICloseableIterator<MemoryFileSummary> ignored = reader.range()) {
            Assertions.fail("a delta is not a chain: losing the base must be an error, not a shorter answer");
        } catch (final IllegalStateException e) {
            Assertions.assertThat(e.getMessage()).as("the failure must name the missing base")
                    .contains("Chain base [2]");
        }
    }

    /**
     * The other side of the last test: a chain that loses its newest DELTA is not damaged. The generations below it are
     * still contiguous with the base, so the chain is one record shorter - the publish that was in flight is gone, which
     * is exactly what a reclaim of a resolved generation costs - and the table moves to the generation before it instead
     * of reading an empty store.
     */
    @Test
    public void testATableWhoseNewestGenerationWasReclaimedMovesToTheOneBeforeIt() throws Exception {
        final PublishedChain published = publishChain("deltaReclaim", 2, 1);
        final List<MemoryFileSummary> before = publishedChain(published.publisher);
        Assertions.assertThat(before).as("the chain before the reclaim").hasSize(3);

        final File lostDelta = published.generations.get(1);
        Assertions.checkTrue(lostDelta.delete(), "the scenario reclaims the newest generation: " + lostDelta);

        final List<MemoryFileSummary> healed = publishedChain(published.publisher);
        Assertions.assertThat(healed).as("the healed chain is the same chain without the lost delta")
                .hasSize(before.size() - 1)
                .isEqualTo(before.subList(0, before.size() - 1));
    }

    // --- fixtures ---

    @NotThreadSafe
    private static final class PublishedChain {

        private ATimeSeriesDB<String, FDate> db;
        private TimeSeriesLookupStorageCache<String, FDate> lookup;
        private VersionedTimeSeriesMemoryFileLookupTable<FDate> publisher;
        private File directory;
        private List<File> generations;
    }

    /**
     * Publishes a chain of the given record counts, one generation per call.
     */
    private PublishedChain publishChain(final String tableName, final int... recordsPerPublish) throws Exception {
        final PublishedChain result = new PublishedChain();
        result.db = newTable(tableName);
        result.lookup = result.db.getLookupTableCache(KEY);
        result.directory = result.lookup.getDirectoryHashKeyVersionMemory().getDirectoryHashKeyVersionDataShared();
        Files.deleteQuietly(result.directory);
        Files.forceMkdir(result.directory);
        result.publisher = new VersionedTimeSeriesMemoryFileLookupTable<>(result.lookup, result.directory, 0);
        int day = 1;
        for (final int records : recordsPerPublish) {
            final List<MemoryFileSummary> batch = new ArrayList<>();
            for (int i = 0; i < records; i++, day++) {
                batch.add(summaryAtDay(day, day * 10L));
            }
            result.publisher.put(new ListSummaryIterator(batch));
        }
        result.generations = findGenerations(result.directory);
        return result;
    }

    private static List<MemoryFileSummary> publishedChain(
            final VersionedTimeSeriesMemoryFileLookupTable<FDate> publisher) {
        final List<MemoryFileSummary> chain = new ArrayList<>();
        try (ICloseableIterator<MemoryFileSummary> it = publisher.range()) {
            Lists.toListWithoutHasNext(it, chain);
        }
        return chain;
    }

    /**
     * One generation file read on its own, which is how to tell a base from a delta: the base's record count is the
     * chain's, a delta's is its publish's.
     */
    private static List<MemoryFileSummary> readGenerationSummaries(final ATimeSeriesDB<String, FDate> table,
            final File generationFile) throws Exception {
        final List<MemoryFileSummary> summaries = new ArrayList<>();
        try (AMemoryFileSummarySerializingCollection collection = new AMemoryFileSummarySerializingCollection(
                new TextDescription("test: read %s", generationFile),
                AtomicNioFileChannel.newFile(generationFile.toURI()), true) {

            @Override
            protected MemoryFileSummarySerde newSerde() {
                return new MemoryFileSummarySerde(table.getValueFixedLength());
            }

            @Override
            protected ICompressionFactory getCompressionFactory() {
                return table.getCompressionFactory();
            }
        }; ICloseableIterator<MemoryFileSummary> it = collection.iterator()) {
            Lists.toListWithoutHasNext(it, summaries);
        }
        return summaries;
    }

    private static MemoryFileSummary summaryAtDay(final int day, final long length) {
        final FDate value = FDateBuilder.newDate(2020, 1, 1).addDays(day);
        return new MemoryFileSummary(value, FDateSerde.GET, value, value, 0L, 1, "memory_" + day + "-gene.data", 0L,
                0L, length);
    }

    private static int countOccurrences(final String text, final String needle) {
        int count = 0;
        for (int i = text.indexOf(needle); i >= 0; i = text.indexOf(needle, i + needle.length())) {
            count++;
        }
        return count;
    }

    private static final class ListSummaryIterator implements ICloseableIterator<MemoryFileSummary> {

        private final List<MemoryFileSummary> summaries;
        private int index;

        private ListSummaryIterator(final List<MemoryFileSummary> summaries) {
            this.summaries = summaries;
        }

        @Override
        public boolean hasNext() {
            return index < summaries.size();
        }

        @Override
        public MemoryFileSummary next() {
            if (index >= summaries.size()) {
                throw FastNoSuchElementException.getInstance("end reached");
            }
            return summaries.get(index++);
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
