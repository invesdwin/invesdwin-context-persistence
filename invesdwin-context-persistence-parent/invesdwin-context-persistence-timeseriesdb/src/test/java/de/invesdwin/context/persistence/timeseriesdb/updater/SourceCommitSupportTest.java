package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.system.properties.ICloseableProperties;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;

/**
 * The commit-stamp mint rule: a content-commit stamp is a date-typed revision, minted as
 * {@code max(previous + 1ps, candidate)} - strictly after the previously persisted stamp even when two
 * commits start in the same millisecond (the same monotonisation idiom
 * {@link de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFiles} pins for the
 * epoch-local data ids), never rewinding on a clock rollback, and a plain copy of the
 * candidate while it is ahead of everything persisted before it (so provenance keeps the exact source
 * value including its picosecond fraction). Absence is the sentinel-equivalent state: no candidate and
 * no previous stays absent - a read, and a commit with nothing to record, never advance the stamp.
 */
@NotThreadSafe
public class SourceCommitSupportTest extends ATest {

    private static final String KEY = "TimeBarsLastRecalculationDate.INSTRUMENT";

    private File baseDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirectory = new File(ContextProperties.TEMP_DIRECTORY, SourceCommitSupportTest.class.getSimpleName());
        Files.deleteQuietly(baseDirectory);
        Files.forceMkdir(baseDirectory);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Files.deleteQuietly(baseDirectory);
    }

    @Test
    public void testTwoCommitsInSameMillisecondAreStrictlyOrdered() {
        final ATimeSeriesDB<String, FDate> table = newTable("sameMillisecond");
        try (ICloseableProperties properties = newUpdaterProperties(table)) {
            //both commits land in the same millisecond (same wall-clock candidate): the mint must still
            //strictly order them, the +1ps floor doing what the MemoryFiles idiom does for data ids
            final FDate sameMillis = FDateBuilder.newDate(2026, 11, 5, 12, 0, 0);
            final FDate first = SourceCommitSupport.nextCommitStamp(properties, KEY, sameMillis);
            final FDate second = SourceCommitSupport.nextCommitStamp(properties, KEY, sameMillis);
            Assertions.assertThat(first).isEqualTo(sameMillis);
            Assertions.assertThat(second).isEqualTo(sameMillis.addPicoseconds(1));
            final FDate third = SourceCommitSupport.nextCommitStamp(properties, KEY, sameMillis);
            Assertions.assertThat(third).isEqualTo(sameMillis.addPicoseconds(2));
        }
    }

    @Test
    public void testMonotonisationWithoutCandidateStaysAbsent() {
        final ATimeSeriesDB<String, FDate> table = newTable("noCandidate");
        try (ICloseableProperties properties = newUpdaterProperties(table)) {
            //no previous and no candidate: nothing to record, the key stays absent (a read never writes).
            //A commit with nothing to propagate must not invent a value: the clock only enters where the
            //caller passes it as the candidate.
            Assertions.assertThat(SourceCommitSupport.nextCommitStamp(properties, KEY, null)).isNull();
            Assertions.assertThat(properties.containsKey(KEY)).isFalse();
            Assertions.assertThat(SourceCommitSupport.nextCommitStamp(properties, KEY, null)).isNull();
            Assertions.assertThat(properties.containsKey(KEY)).isFalse();
            Assertions.assertThat(SourceCommitSupport.getCommitStamp(properties, KEY)).isNull();
            //two clock-candidate commits in quick succession: strictly ordered whichever way the clock goes
            final FDate first = SourceCommitSupport.nextCommitStamp(properties, KEY, FDate.now());
            Assertions.assertThat(first).isNotNull();
            final FDate second = SourceCommitSupport.nextCommitStamp(properties, KEY, FDate.now());
            Assertions.assertThat(second).isAfter(first);
        }
    }

    @Test
    public void testCommitStampNeverRewindsOnClockRollback() {
        final ATimeSeriesDB<String, FDate> table = newTable("clockRollback");
        try (ICloseableProperties properties = newUpdaterProperties(table)) {
            //a source that rolled its clock back must not move the stamp back: the floor wins
            final FDate ahead = FDateBuilder.newDate(2026, 11, 5, 12, 0, 0);
            Assertions.assertThat(SourceCommitSupport.nextCommitStamp(properties, KEY, ahead)).isEqualTo(ahead);
            final FDate behind = FDateBuilder.newDate(2026, 11, 4, 6, 0, 0);
            Assertions.assertThat(SourceCommitSupport.nextCommitStamp(properties, KEY, behind))
                    .isEqualTo(ahead.addPicoseconds(1));
            //the floor composes: the next floor sits one picosecond beyond the last persisted stamp
            Assertions.assertThat(SourceCommitSupport.nextCommitStamp(properties, KEY, behind))
                    .isEqualTo(ahead.addPicoseconds(2));
        }
    }

    @Test
    public void testLaterCandidateIsCopiedExactlyIncludingItsPicoseconds() {
        final ATimeSeriesDB<String, FDate> table = newTable("exactCopy");
        try (ICloseableProperties properties = newUpdaterProperties(table)) {
            //provenance: while the source stamp is ahead of everything seen before, the commit stamp IS
            //that stamp - no clock noise, the picosecond fraction preserved
            final FDate sourceReceipt = FDateBuilder.newDate(2026, 11, 5, 8, 30, 0).addPicoseconds(123456);
            Assertions.assertThat(SourceCommitSupport.nextCommitStamp(properties, KEY, sourceReceipt))
                    .isEqualTo(sourceReceipt);
            final FDate laterReceipt = FDateBuilder.newDate(2026, 11, 5, 9, 30, 0).addPicoseconds(654321);
            Assertions.assertThat(SourceCommitSupport.nextCommitStamp(properties, KEY, laterReceipt))
                    .isEqualTo(laterReceipt);
            Assertions.assertThat(SourceCommitSupport.getCommitStamp(properties, KEY)).isEqualTo(laterReceipt);
        }
    }

    @Test
    public void testStampSurvivesReopenWithItsPicosecondFraction() {
        final ATimeSeriesDB<String, FDate> table = newTable("reopen");
        final FDate stamp = FDateBuilder.newDate(2026, 11, 5, 10, 15, 30).addPicoseconds(987654321L);
        try (ICloseableProperties properties = newUpdaterProperties(table)) {
            Assertions.assertThat(SourceCommitSupport.nextCommitStamp(properties, KEY, stamp)).isEqualTo(stamp);
        }
        //reopened properties (restart semantics): the exact picosecond value round-tripped, and a decision
        //over it sees no advance - absence of a commit means absence of any change to the stamp
        try (ICloseableProperties properties = newUpdaterProperties(table)) {
            Assertions.assertThat(SourceCommitSupport.getCommitStamp(properties, KEY)).isEqualTo(stamp);
            Assertions.assertThat(properties.getDate(KEY)).isEqualTo(stamp);
        }
    }

    private ICloseableProperties newUpdaterProperties(final ATimeSeriesDB<String, FDate> table) {
        return table.getDirectoryVersionHashKey(KEY).getDirectoryHashKeyVersion().getUpdaterPropertiesShared();
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

}
