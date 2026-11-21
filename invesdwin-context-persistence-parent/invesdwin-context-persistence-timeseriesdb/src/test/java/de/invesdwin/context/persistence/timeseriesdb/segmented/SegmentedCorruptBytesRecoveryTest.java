package de.invesdwin.context.persistence.timeseriesdb.segmented;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache.InconsistencyReason;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.AMemoryFileSummarySerializingCollection;
import de.invesdwin.context.persistence.timeseriesdb.segmented.finder.HistoricalCacheSegmentFinder;
import de.invesdwin.context.persistence.timeseriesdb.segmented.finder.ISegmentFinder;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.collections.iterable.skip.ATimeRangeSkippingIterable;
import de.invesdwin.util.collections.loadingcache.historical.AHistoricalCache;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.disabled.DisabledLock;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.range.TimeRange;

/**
 * Suspect bytes in ONE segment: the check must answer, and the answer must be actionable.
 * <p>
 * {@code ASegmentedTimeSeriesLookupStorageCache.isEmptyOrInconsistent()} is the segmented copy of the store's
 * "can this key be reset and re-fetched?" question. The non-segmented original was fixed to answer that question when
 * the damage is a stream that will not decode (an {@code IOException} out of the LZ4 block reader), because a consumer
 * that is handed an exception instead of an answer never heals the state. This copy still assumes the only failure a
 * read can produce is a {@code SerializationException} and propagates everything else - so a segment with corrupted
 * bytes is a key that can be neither read nor reset through the normal path.
 * <p>
 * The cell below plants exactly that: a byte flip inside a published index generation, where the file is present and
 * its length is what the account claims, so no arithmetic check can see it and only the decode notices. Then it asks
 * the question, and finally performs the act the answer exists for - reset the key and let the segment rebuild from the
 * feed - asserting the values come back whole.
 */
@NotThreadSafe
public class SegmentedCorruptBytesRecoveryTest extends ATest {

    private static final int VALUES = 6;

    @Test
    public void testSuspectSegmentBytesAreAnsweredAndTheSegmentRebuilds() throws Exception {
        final List<FDate> feed = newValues(VALUES);
        final HealableSegmentedDB db = new HealableSegmentedDB("corruptSegment", feed);
        try {
            final String key = "k";
            final TimeRange segment = db.getSegmentFinder(key).getCacheQuery().getValue(feed.get(0));
            final SegmentedKey<String> segmentedKey = new SegmentedKey<>(key, segment);
            final ASegmentedTimeSeriesLookupStorageCache<String, FDate> cache = db.getSegmentedLookupTableCache(key);

            Assertions.checkTrue(cache.maybeInitSegmentSync(segmentedKey, segmentedKey1 -> db.newSource(segmentedKey1)),
                    "precondition: the segment must initialise cleanly before it is damaged");
            final List<FDate> landed = readSegment(cache, segment);
            Assertions.checkTrue(landed.size() == VALUES,
                    "precondition: the whole feed must be readable, was " + landed);

            //the damage: flip a byte inside the published index generation of THIS segment
            final TimeSeriesLookupStorageCache<SegmentedKey<String>, FDate> segmentLookup = db.getSegmentedTable()
                    .getLookupTableCache(segmentedKey);
            corruptMiddleByteOf(newestGenerationFile(segmentLookup));
            //the damage is on disk: without this the check would be answered from the index still held in memory, and a
            //"consistent" answer would be an artifact of the fixture rather than a property of the store
            segmentLookup.clearCaches();

            //the question the recovery decision rests on. Recorded rather than asserted-away: today the segmented
            //check lets the decode failure escape instead of answering
            boolean inconsistent = false;
            Throwable escaped = null;
            try {
                inconsistent = db.isEmptyOrInconsistent(key);
            } catch (final Throwable t) {
                escaped = t;
            }
            Assertions.checkTrue(escaped == null,
                    "the consistency check must ANSWER about a store it cannot decode, not throw at the caller that is "
                            + "asking whether it may reset it. escaped=" + escaped);
            Assertions.checkTrue(inconsistent,
                    "and the answer must be that this key is inconsistent (escaped=" + escaped + ")");
            Assertions.checkEquals(InconsistencyReason.CORRUPT_STORAGE, cache.getLastInconsistencyReason(),
                    "the reason must name the layer that noticed - the stored stream - so a recovery can act per "
                            + "reason instead of wiping on any true");

            //the act the answer exists for: reset the key, and the segment rebuilds from the feed
            db.deleteRange(key);
            Assertions.checkTrue(cache.maybeInitSegmentSync(segmentedKey, segmentedKey1 -> db.newSource(segmentedKey1)),
                    "after the reset the segment must initialise again");
            final List<FDate> rebuilt = readSegment(cache, segment);
            Assertions.checkTrue(rebuilt.size() == VALUES,
                    "and the whole feed must be readable again, was " + rebuilt);
        } finally {
            db.close();
        }
    }

    /**
     * The per-segment reason, carried up. Three segments, damage in the MIDDLE one: the head read still has the global
     * first and last values (they live in the outer segments), so the only path that can indict this key is the
     * status-table walk - and that walk has to report the damaged SEGMENT's reason rather than leave the default.
     * Until this cell the carry-up line was unverified; deleting it now reddens exactly here, because the boolean still
     * says true while the reason stays NONE - which is the all-or-nothing recovery this lane exists to retire.
     */
    @Test
    public void testADamagedMiddleSegmentCarriesItsOwnReasonUp() throws Exception {
        final List<FDate> feed = new ArrayList<>();
        feed.add(FDateBuilder.newDate(2020, 6, 1));
        feed.add(FDateBuilder.newDate(2023, 6, 1));
        feed.add(FDateBuilder.newDate(2025, 6, 1));
        final HealableSegmentedDB db = new HealableSegmentedDB("middleSegment", feed);
        try {
            final String key = "k";
            final ASegmentedTimeSeriesLookupStorageCache<String, FDate> cache = db.getSegmentedLookupTableCache(key);
            final List<SegmentedKey<String>> segments = new ArrayList<>();
            for (final FDate value : feed) {
                final TimeRange segment = db.getSegmentFinder(key).getCacheQuery().getValue(value);
                final SegmentedKey<String> segmentedKey = new SegmentedKey<>(key, segment);
                Assertions.checkTrue(
                        cache.maybeInitSegmentSync(segmentedKey, sk -> db.newSource(segmentedKey)),
                        "precondition: every segment must initialise cleanly, failed at " + segment);
                if (!segments.contains(segmentedKey)) {
                    segments.add(segmentedKey);
                }
            }
            Assertions.checkTrue(segments.size() == 3,
                    "the fixture needs three distinct segments so the head read cannot see the middle one, got "
                            + Integer.valueOf(segments.size()));

            //damage: the middle segment keeps its summaries but loses the bytes they name
            final TimeSeriesLookupStorageCache<SegmentedKey<String>, FDate> middleLookup = db.getSegmentedTable()
                    .getLookupTableCache(segments.get(1));
            final File named = new File(firstPublishedResourceUri(middleLookup));
            Assertions.checkTrue(named.exists(), "the middle segment must have a published payload: " + named);
            try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(named, "rw")) {
                raf.setLength(0L);
            }
            middleLookup.clearCaches();

            Assertions.checkTrue(db.isEmptyOrInconsistent(key),
                    "a summary naming bytes that are no longer there must indict the key");
            Assertions.checkEquals(InconsistencyReason.NAMED_FILE_SHORTER_THAN_CLAIMED,
                    cache.getLastInconsistencyReason(),
                    "the reason must come from the damaged SEGMENT - the head read cannot see this damage, so a NONE "
                            + "here means the status walk indicted the key without saying who");
        } finally {
            db.close();
        }
    }

    private static String firstPublishedResourceUri(
            final TimeSeriesLookupStorageCache<SegmentedKey<String>, FDate> lookup) {
        try (ICloseableIterator<MemoryFileSummary> it = lookup.getMemoryFileLookupTable().range()) {
            while (it.hasNext()) {
                final String uri = it.next().getMemoryResourceUri();
                if (uri != null) {
                    return uri;
                }
            }
        }
        Assertions.fail("the segment must have a published payload file to damage");
        return null;
    }

    private static List<FDate> readSegment(final ASegmentedTimeSeriesLookupStorageCache<String, FDate> cache,
            final TimeRange segment) {
        final List<FDate> out = new ArrayList<>();
        final ILock noLock = DisabledLock.INSTANCE;
        final ICloseableIterator<FDate> values = cache.readRangeValues(segment.getFrom(), segment.getTo(), noLock,
                null).iterator();
        try {
            while (values.hasNext()) {
                out.add(values.next());
            }
        } finally {
            values.close();
        }
        return out;
    }

    private static File memoryDirectoryOf(final TimeSeriesLookupStorageCache<?, ?> lookup) {
        return lookup.getDirectoryHashKeyVersionMemory().getDirectoryHashKeyVersionDataShared();
    }

    private static File newestGenerationFile(final TimeSeriesLookupStorageCache<?, ?> lookup) {
        final int generation = lookup.getMemoryFileLookupTable().getCurrentIndexNumber();
        final File memoryDirectory = memoryDirectoryOf(lookup);
        final File candidate = new File(memoryDirectory,
                AMemoryFileSummarySerializingCollection.newIndexGenerationFileName(generation));
        Assertions.checkTrue(candidate.exists(), "the newest generation must be present: " + candidate);
        return candidate;
    }

    private static void corruptMiddleByteOf(final File file) throws IOException {
        //RandomAccessFile, not java.nio.file.Files: the generated ruleset bans importing the latter ("Use
        //de.invesdwin...Files instead"), and a single in-place flip needs neither a whole-file read nor a write
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            final long length = raf.length();
            Assertions.checkTrue(length > 16L,
                    "the file must be large enough to damage meaningfully: " + Long.valueOf(length));
            final long middle = length / 2L;
            raf.seek(middle);
            final int b = raf.read();
            raf.seek(middle);
            raf.write(b ^ 0xFF);
        }
    }

    private static List<FDate> newValues(final int count) {
        final List<FDate> values = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            values.add(FDateBuilder.newDate(2022, 5, 1).addDays(i));
        }
        return values;
    }

    /** A segmented store whose feed serves the same complete range on every pass. */
    @NotThreadSafe
    private static final class HealableSegmentedDB extends ASegmentedTimeSeriesDB<String, FDate> {

        private final List<FDate> entities;
        private final ISegmentFinder segmentFinder;

        private HealableSegmentedDB(final String name, final List<FDate> entities) {
            super(name);
            this.entities = entities;
            final AHistoricalCache<TimeRange> segmentFinderCache = PeriodicalSegmentFinder
                    .newCache(new Duration(2, FTimeUnit.YEARS), false);
            this.segmentFinder = new HistoricalCacheSegmentFinder(segmentFinderCache, null);
        }

        @Override
        protected int newBatchFlushInterval() {
            return 2;
        }

        private ICloseableIterable<FDate> newSource(final SegmentedKey<String> segmentedKey) {
            return new ATimeRangeSkippingIterable<FDate>(
                    WrapperCloseableIterable.maybeWrap(entities), segmentedKey.getSegment()) {

                @Override
                protected FDate extractEndTime(final FDate element) {
                    return element;
                }

                @Override
                protected String getName() {
                    return "HealableSegmentedDB.newSource";
                }

            };
        }

        @Override
        protected String getElementsName() {
            return "HealableSegmentedDB";
        }

        @Override
        protected ICloseableIterable<? extends FDate> downloadSegmentElements(final SegmentedKey<String> key) {
            return newSource(key);
        }

        @Override
        public ISegmentFinder getSegmentFinder(final String key) {
            return segmentFinder;
        }

        @Override
        protected ISerde<FDate> newValueSerde() {
            return FDateSerde.GET;
        }

        @Override
        protected Integer newValueFixedLength() {
            return FDateSerde.FIXED_LENGTH;
        }

        @Override
        public String innerHashKeyToString(final String key) {
            return key;
        }

        @Override
        public ITimeSeriesBaseDirectory getBaseDirectory() {
            return new TimeSeriesBaseDirectory(ContextProperties.TEMP_DIRECTORY);
        }

        @Override
        public FDate extractStartTime(final FDate value) {
            return value;
        }

        @Override
        public FDate extractEndTime(final FDate value) {
            return value;
        }

        @Override
        public FDate getFirstAvailableHistoricalSegmentFrom(final String key) {
            return segmentFinder.getCacheQuery().getValue(entities.get(0)).getFrom();
        }

        @Override
        public FDate getLastAvailableHistoricalSegmentTo(final String key, final FDate updateTo) {
            return segmentFinder.getCacheQuery().getValue(entities.get(entities.size() - 1)).getTo();
        }

    }

}
