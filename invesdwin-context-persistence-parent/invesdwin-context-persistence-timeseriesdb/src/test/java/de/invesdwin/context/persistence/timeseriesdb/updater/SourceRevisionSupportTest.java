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
 * The two additive keys a source table writes next to its recalculation stamp when its content is reset and
 * reloaded: a monotonic source revision (read-modify-write on the shared per-hashKey updater properties, bumped
 * exactly once per reset) and the source watermark (the max data timestamp of that source range, written only
 * when it can be derived from the data at hand). Nothing reads these keys yet, so this test pins the persisted
 * convention at the same persistence point the updaters use.
 */
@NotThreadSafe
public class SourceRevisionSupportTest extends ATest {

    private static final String KEY = "sourceRevisionKey";
    private static final String BARS_SCOPE = "TimeBars.INSTRUMENT";
    private static final String TICKS_SCOPE = "TimeTicks.INSTRUMENT";

    private File baseDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirectory = new File(ContextProperties.TEMP_DIRECTORY, SourceRevisionSupportTest.class.getSimpleName());
        Files.deleteQuietly(baseDirectory);
        Files.forceMkdir(baseDirectory);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Files.deleteQuietly(baseDirectory);
    }

    @Test
    public void testSourceRevisionMonotonicAcrossResets() {
        final ATimeSeriesDB<String, FDate> table = newTable("sourceRevisionMonotonic");
        //absent on a fresh store: sentinel-equivalent, and a mere read never writes a key
        try (ICloseableProperties properties = newUpdaterProperties(table)) {
            Assertions.assertThat(SourceRevisionSupport.getSourceRevision(properties, BARS_SCOPE)).isZero();
            Assertions.assertThat(SourceRevisionSupport.getSourceWatermark(properties, BARS_SCOPE)).isNull();
            Assertions.assertThat(properties.containsKey(SourceRevisionSupport.KEY_SOURCE_REVISION_PREFIX + BARS_SCOPE))
                    .isFalse();
        }
        //first reset bumps 0 -> 1 and records the watermark of the range that is being reloaded
        final FDate firstWatermark = FDateBuilder.newDate(2026, 1, 5);
        try (ICloseableProperties properties = newUpdaterProperties(table)) {
            Assertions.assertThat(SourceRevisionSupport.recordSourceReset(properties, BARS_SCOPE, firstWatermark))
                    .isEqualTo(1L);
            Assertions.assertThat(SourceRevisionSupport.getSourceRevision(properties, BARS_SCOPE)).isEqualTo(1L);
            Assertions.assertThat(SourceRevisionSupport.getSourceWatermark(properties, BARS_SCOPE))
                    .isEqualTo(firstWatermark);
        }
        //reopened properties: revision and watermark survived (same persistence point, restart semantics)
        try (ICloseableProperties properties = newUpdaterProperties(table)) {
            Assertions.assertThat(SourceRevisionSupport.getSourceRevision(properties, BARS_SCOPE)).isEqualTo(1L);
            Assertions.assertThat(SourceRevisionSupport.getSourceWatermark(properties, BARS_SCOPE))
                    .isEqualTo(firstWatermark);
            //three further resets: strictly monotonic, exactly one bump per reset
            long previous = 1L;
            for (int i = 0; i < 3; i++) {
                final long bumped = SourceRevisionSupport.recordSourceReset(properties, BARS_SCOPE, null);
                Assertions.assertThat(bumped).isEqualTo(previous + 1);
                Assertions.assertThat(SourceRevisionSupport.getSourceRevision(properties, BARS_SCOPE))
                        .isEqualTo(bumped);
                previous = bumped;
            }
            Assertions.assertThat(previous).isEqualTo(4L);
            //an unknown watermark never clobbers the last known one
            Assertions.assertThat(SourceRevisionSupport.getSourceWatermark(properties, BARS_SCOPE))
                    .isEqualTo(firstWatermark);
        }
        try (ICloseableProperties properties = newUpdaterProperties(table)) {
            Assertions.assertThat(SourceRevisionSupport.getSourceRevision(properties, BARS_SCOPE)).isEqualTo(4L);
            final FDate secondWatermark = FDateBuilder.newDate(2026, 11, 3);
            SourceRevisionSupport.setSourceWatermark(properties, BARS_SCOPE, secondWatermark);
            Assertions.assertThat(SourceRevisionSupport.getSourceWatermark(properties, BARS_SCOPE))
                    .isEqualTo(secondWatermark);
        }
        //scoped per source family: another family's reset does not touch this family's counter
        try (ICloseableProperties properties = newUpdaterProperties(table)) {
            Assertions.assertThat(SourceRevisionSupport.recordSourceReset(properties, TICKS_SCOPE, null))
                    .isEqualTo(1L);
            Assertions.assertThat(SourceRevisionSupport.getSourceRevision(properties, BARS_SCOPE)).isEqualTo(4L);
            Assertions.assertThat(SourceRevisionSupport.getSourceRevision(properties, TICKS_SCOPE)).isEqualTo(1L);
        }
    }

    /**
     * The decision-side counterpart of the source revision: a derived store persists the source revision it was
     * built from under its own 'DerivedSourceRevision.' key, absent on a store that never did, scoped per derived
     * store key and namespaced apart from the source-side key.
     */
    @Test
    public void testDerivedSourceRevisionPersistedUnderItsOwnKey() {
        final ATimeSeriesDB<String, FDate> table = newTable("derivedSourceRevision");
        final String derivedKey = "CalculatedBarsLastRecalculationDate.FROM_TIME_BARS.INSTRUMENT_BID_1d";
        try (ICloseableProperties properties = newUpdaterProperties(table)) {
            //absent on a fresh store: sentinel-equivalent, and a mere read never writes a key
            Assertions.assertThat(SourceRevisionSupport.getDerivedSourceRevisionOptional(properties, derivedKey))
                    .isNull();
            Assertions.assertThat(
                    properties.containsKey(SourceRevisionSupport.getDerivedSourceRevisionKey(derivedKey))).isFalse();
            SourceRevisionSupport.setDerivedSourceRevision(properties, derivedKey, 7L);
            Assertions.assertThat(SourceRevisionSupport.getDerivedSourceRevisionOptional(properties, derivedKey))
                    .isEqualTo(7L);
            //namespaced apart from the source-side key of the same string
            Assertions.assertThat(properties.containsKey(SourceRevisionSupport.KEY_SOURCE_REVISION_PREFIX + derivedKey))
                    .isFalse();
        }
        try (ICloseableProperties properties = newUpdaterProperties(table)) {
            //survives a reopen of the properties (restart semantics)
            Assertions.assertThat(SourceRevisionSupport.getDerivedSourceRevisionOptional(properties, derivedKey))
                    .isEqualTo(7L);
            //scoped per derived store key: another key of the same properties is untouched
            Assertions.assertThat(SourceRevisionSupport.getDerivedSourceRevisionOptional(properties,
                    "SkippingTicksLastRecalculationDate.FROM_TIME_BARS.INSTRUMENT_BID_1d")).isNull();
        }
    }

    /**
     * Truth table of the revision-aware rebuild decision: a null verdict (fall back to the stamp comparison)
     * whenever the source publishes no revision; equal revisions say no rebuild even where the wall-clock stamps
     * would differ (the cross-JVM non-determinism case); differing or never-stored revisions say rebuild.
     */
    @Test
    public void testRevisionBasedRecalculationDecisionTruthTable() {
        //source never published a revision: no verdict, the stamp comparison decides (unchanged old path)
        Assertions.assertThat(SourceRevisionSupport.decideRevisionBasedRecalculation(null, null)).isNull();
        Assertions.assertThat(SourceRevisionSupport.decideRevisionBasedRecalculation(null, 3L)).isNull();
        //revision equal: no rebuild, no matter what the stamps say
        Assertions.assertThat(SourceRevisionSupport.decideRevisionBasedRecalculation(7L, 7L)).isFalse();
        //revision differs: rebuild fires
        Assertions.assertThat(SourceRevisionSupport.decideRevisionBasedRecalculation(8L, 7L)).isTrue();
        //derived store never built under revision awareness: sentinel-equivalent, one rebuild, then steady state
        Assertions.assertThat(SourceRevisionSupport.decideRevisionBasedRecalculation(1L, null)).isTrue();
        //after that rebuild the verdict is stable: re-deciding with the persisted revision says no rebuild
        Assertions.assertThat(SourceRevisionSupport.decideRevisionBasedRecalculation(1L, 1L)).isFalse();
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
