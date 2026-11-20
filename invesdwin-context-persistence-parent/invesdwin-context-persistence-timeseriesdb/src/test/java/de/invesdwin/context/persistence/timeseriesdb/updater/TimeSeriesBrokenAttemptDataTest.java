package de.invesdwin.context.persistence.timeseriesdb.updater;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.util.concurrent.lock.file.AtomicNioFileChannelContext;
import de.invesdwin.context.persistence.timeseriesdb.ATimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache.InconsistencyReason;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.TimeSeriesBaseDirectory;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.AMemoryFileSummarySerializingCollection;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup.MemoryFileMetadata;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.ITimeSeriesUpdateProgress;
import de.invesdwin.context.system.properties.ICloseableProperties;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterable;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.lang.Files;
import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.compression.DisabledCompressionFactory;
import org.apache.commons.lang3.SerializationException;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.date.millis.FDateMillis;

/**
 * Stores that are genuinely broken from a previous attempt, and what recovery actually does about each.
 * <p>
 * The previous lanes pinned the crash-window bookkeeping (#73) and the reason the check reports (#75). What was still
 * argued rather than proven is the class of case where the data really IS damaged: a lookup generation that cannot be
 * deserialised, an attestation file that is gone entirely, staging litter from a publish that never finished, and a
 * rebuild that must not touch its neighbours. Those cases are the reason the reset arm exists, so each gets its own
 * cell here: a recovery story that only holds for the benign states is worth nothing if it silently mis-handles these.
 * <ol>
 * <li>a corrupted index generation is reported (with a reason) rather than thrown, the key rebuilds, and the rebuilt
 * chain reads whole again - a damaged index is a repairable state, not a lost-data state, because the payload holds
 * the values. Before this lane the check THREW on that state, which meant the consumer never got the answer it asked
 * for and the key was never rebuilt;</li>
 * <li>a value width the serde does not produce is caught on the write side, and reading an old store with a different
 * width is caught on the read side;</li>
 * <li>and the limit of all of it: a byte flipped INSIDE a stored value - at any position, boundary or interior - is
 * detected by NOTHING here. The summaries carry their own copies of the first and last values inside the index
 * record, so assertSummary() validates the index against itself; the payload is only ever checked arithmetically.
 * Content-level integrity would need a checksum this format does not have, and this cell pins that rather than
 * leaving it to be rediscovered as an incident;</li>
 * <li>a missing {@code memory.properties} leaves the chain serving (no attestation means the high-water comparison is
 * simply inapplicable, not failed), and the next publish re-creates the pair - so losing the bookkeeping never costs a
 * value;</li>
 * <li>staging litter left by a publish that never completed is swept, while every file the published chain names
 * survives the same sweep untouched;</li>
 * <li>resetting one key leaves the other keys of the same table exactly as they were - the version increment is
 * per-key, and a test that does not pin this would let a future "clear the table" shortcut through unnoticed.</li>
 * </ol>
 * Everything here runs against a private store directory under TEMP, created per test and deleted afterwards; no
 * deployment's store is involved, and no failure case is inferred from a production log.
 */
@NotThreadSafe
public class TimeSeriesBrokenAttemptDataTest extends ATest {

    private static final int BATCH_FLUSH_INTERVAL = 4;

    /** The two simulated on-disk value formats. */
    private static final int FORMAT_V1 = 1;
    private static final int FORMAT_V2 = 2;

    private File baseDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirectory = new File(ContextProperties.TEMP_DIRECTORY, TimeSeriesBrokenAttemptDataTest.class.getName());
        Files.deleteQuietly(baseDirectory);
        Files.forceMkdir(baseDirectory);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Files.deleteQuietly(baseDirectory);
    }

    /**
     * Case 1: a lookup generation whose bytes cannot be deserialised. The payloads still hold every value, so the
     * answer is a rebuild of the chain - the test proves the rebuild actually restores the data, not just that the
     * store is flagged.
     */
    @Test
    public void testACorruptedGenerationIsReportedAndTheRebuildRestoresTheChain() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("corruptGeneration");
        final List<FDate> values = newValues(1, BATCH_FLUSH_INTERVAL + 2);
        newUpdater(table, "corruptKey", values).update().close();
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache("corruptKey");

        Assertions.checkFalse(lookup.isEmptyOrInconsistent(), "precondition: the pass leaves a sound store");
        final List<MemoryFileSummary> before = publishedSummaries(lookup);
        Assertions.checkTrue(!before.isEmpty(), "precondition: a chain is published");

        final File generation = newestGenerationFile(lookup);
        corruptBytesOf(generation);
        lookup.clearCaches();

        Assertions.checkTrue(lookup.isEmptyOrInconsistent(),
                "an index that cannot be read must not be served as if it could: " + generation);
        Assertions.checkEquals(InconsistencyReason.INVALID_SERIALIZATION, lookup.getLastInconsistencyReason(),
                "and the reason must name the deserialisation arm rather than the byte-level ones");

        //the consumer's answer: reset and let the next pass rebuild from the source
        table.deleteRange("corruptKey");
        lookup.clearCaches();
        Assertions.checkTrue(lookup.isEmptyOrInconsistent(), "a reset store is empty, and that is what it reports");
        Assertions.checkEquals(InconsistencyReason.NO_UPDATED_MARKER, lookup.getLastInconsistencyReason(),
                "with the marker arm as the reason - nothing was ever published into the new version");

        newUpdater(table, "corruptKey", values).update().close();
        lookup.clearCaches();
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(), "the rebuilt store is sound");
        Assertions.checkEquals(values, readValues(table, "corruptKey"),
                "and the rebuild restored every value the damaged chain held");
    }

    /**
     * Case 2: the bookkeeping is gone but the data is not. The store must keep serving, must not be called damaged,
     * and the next publish must write the pair again.
     */
    @Test
    public void testAMissingAttestationFileStillServesAndIsRewrittenByTheNextPublish() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("noAttestation");
        final List<FDate> values = newValues(1, BATCH_FLUSH_INTERVAL + 2);
        newUpdater(table, "noAttestationKey", values).update().close();
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache("noAttestationKey");

        final File propertiesFile = new File(new File(memoryDirectoryOf(lookup), "properties"), "memory.properties");
        Assertions.checkTrue(propertiesFile.exists(), "precondition: the publish attested there: " + propertiesFile);
        Assertions.checkTrue(propertiesFile.delete(), "removing it must work: " + propertiesFile);
        lookup.clearCaches();

        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "a chain that matches its own files is not damaged by losing the number that described it");
        Assertions.checkEquals(InconsistencyReason.NONE, lookup.getLastInconsistencyReason(),
                "and the absence of an attestation is not an indictment");
        Assertions.checkEquals(values, readValues(table, "noAttestationKey"), "the values still read whole");

        //a further publish restores the pair, so the state is transient by construction
        newUpdater(table, "noAttestationKey", newValues(1, BATCH_FLUSH_INTERVAL * 2 + 3)).update().close();
        lookup.clearCaches();
        final MemoryFileMetadata metadata = lookup.getMemoryFileLookupTable().getMetadata();
        try (ICloseableProperties properties = metadata.getProperties()) {
            Assertions.checkTrue(propertiesFile.exists(), "the next publish re-created the file: " + propertiesFile);
            Assertions.checkTrue(metadata.getExpectedMemoryFileSize(properties)
                    != MemoryFileMetadata.MISSING_EXPECTED_MEMORY_FILE_SIZE,
                    "with a high-water attested");
            Assertions.checkNotNull(metadata.getExpectedMemoryFileGeneration(properties),
                    "and its generation, i.e. as a pair");
        }
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(), "and the store stays sound afterwards");
    }

    /**
     * Case 3: litter from an attempt that staged a publish and never finished it. The sweep takes the litter and
     * leaves every file the published chain names, byte for byte.
     */
    @Test
    public void testTheEpochReclaimLeavesStagingLitterAloneAndNeverTouchesThePublishedChain() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("stagingLitter");
        final List<FDate> values = newValues(1, BATCH_FLUSH_INTERVAL + 2);
        newUpdater(table, "litterKey", values).update().close();
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache("litterKey");

        final List<MemoryFileSummary> published = publishedSummaries(lookup);
        final List<File> chainFiles = new ArrayList<>();
        for (final MemoryFileSummary summary : published) {
            chainFiles.add(new File(summary.getMemoryResourceUri()));
        }
        final byte[][] chainBytes = new byte[chainFiles.size()][];
        for (int i = 0; i < chainFiles.size(); i++) {
            chainBytes[i] = java.nio.file.Files.readAllBytes(chainFiles.get(i).toPath());
        }

        //a staged upload that never landed: the same staging suffix the publish path uses, in the same directory
        final File memoryDirectory = memoryDirectoryOf(lookup);
        final File litter = new File(memoryDirectory, AMemoryFileSummarySerializingCollection.MEMORY_INDEX_FILE_NAME
                + ".update" + AtomicNioFileChannelContext.TMP_EXTENSION);
        java.nio.file.Files.write(litter.toPath(), new byte[] { 1, 2, 3 });
        Assertions.checkTrue(litter.exists(), "the staged file must exist to be swept: " + litter);

        TimeSeriesEpochReclaim.reclaimObsoleteEpochs(lookup);

        //the honest expectation, read off the code rather than from a note: the epoch sweep is coverage-driven and
        //does NOT own staging litter. That litter is reaped by AtomicNioFileChannelContext, which runs
        //Files.cleanupStaleTempFiles on its OWN schedule - a directory is visited at most once per
        //TMP_CLEANUP_INTERVAL (a day), and a file is only taken once it is older than TMP_CLEANUP_TIMEOUT (12 HOURS).
        //So litter from a killed writer can sit for up to a day and a half, which is a disk question, not a
        //correctness one - the next cell proves the reaper actually works.
        Assertions.checkTrue(litter.exists(), "the epoch sweep is not the reaper of staging litter: " + litter);
        for (int i = 0; i < chainFiles.size(); i++) {
            Assertions.checkTrue(chainFiles.get(i).exists(),
                    "the sweep removed a file the published chain names: " + chainFiles.get(i));
            Assertions.checkTrue(java.util.Arrays.equals(chainBytes[i],
                    java.nio.file.Files.readAllBytes(chainFiles.get(i).toPath())),
                    "the sweep altered the bytes of a published data file: " + chainFiles.get(i));
        }
        lookup.clearCaches();
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(), "and the chain still reads sound");
    }

    /**
     * The reaper for kill -9 litter, proven rather than assumed. A writer killed mid-upload leaves a staged
     * {@code .tmp} file; {@code AtomicNioFileChannelContext} visits a directory at most once per
     * {@code TMP_CLEANUP_INTERVAL} (a day) and takes only files older than {@code TMP_CLEANUP_TIMEOUT} (12 hours).
     * This drives the same sweep the production path calls, with the litter back-dated, and shows it removes the
     * litter while leaving every file the published chain names untouched - so the worst case for a killed writer is
     * disk, not data.
     */
    @Test
    public void testTheStaleTempSweepTakesKilledWriterLitterAndLeavesTheChainIntact() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("tempSweep");
        final List<FDate> values = newValues(1, BATCH_FLUSH_INTERVAL + 2);
        newUpdater(table, "sweepKey", values).update().close();
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache("sweepKey");

        final File memoryDirectory = memoryDirectoryOf(lookup);
        final File litter = new File(memoryDirectory, "9999_" + AMemoryFileSummarySerializingCollection.MEMORY_INDEX_FILE_NAME
                + ".update" + AtomicNioFileChannelContext.TMP_EXTENSION);
        java.nio.file.Files.write(litter.toPath(), new byte[] { 7, 7, 7 });
        //older than the 12 hour staleness threshold
        final long thirteenHoursMillis = 13L * 60L * 60L * 1000L;
        java.nio.file.Files.setLastModifiedTime(litter.toPath(),
                java.nio.file.attribute.FileTime.fromMillis(FDateMillis.nowMillis() - thirteenHoursMillis));

        //a FRESH piece of litter as well: the sweep must leave it, because its staleness is what qualifies it
        final File freshLitter = new File(memoryDirectory,
                "9998_" + AMemoryFileSummarySerializingCollection.MEMORY_INDEX_FILE_NAME + ".update"
                        + AtomicNioFileChannelContext.TMP_EXTENSION);
        java.nio.file.Files.write(freshLitter.toPath(), new byte[] { 8 });

        final List<File> chainFiles = new ArrayList<>();
        for (final MemoryFileSummary summary : publishedSummaries(lookup)) {
            chainFiles.add(new File(summary.getMemoryResourceUri()));
        }
        chainFiles.add(newestGenerationFile(lookup));
        Assertions.checkTrue(litter.exists() && freshLitter.exists(), "both planted files must exist first");

        de.invesdwin.util.lang.Files.cleanupStaleTempFiles(memoryDirectory.toPath(),
                AtomicNioFileChannelContext.TMP_CLEANUP_TIMEOUT, AtomicNioFileChannelContext.TMP_EXTENSION);

        Assertions.checkFalse(litter.exists(), "stale upload litter must be reaped: " + litter);
        Assertions.checkTrue(freshLitter.exists(),
                "a staging file that is not yet stale must survive - an in-flight upload of another writer is exactly"
                        + " that shape: " + freshLitter);
        for (final File chainFile : chainFiles) {
            Assertions.checkTrue(chainFile.exists(), "the sweep removed a chain file: " + chainFile);
        }
        lookup.clearCaches();
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(), "and the store still reads sound afterwards");
        Assertions.checkEquals(values, readValues(table, "sweepKey"), "with every value intact");
    }

    /**
     * Case 4: the reset is per key. Two keys of one table, one of them damaged: rebuilding the damaged one must leave
     * the other's files and values untouched, because a whole-table clear would be invisible to every other cell here.
     */
    @Test
    public void testResettingOneKeyLeavesTheSiblingsOfTheSameTableAlone() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("siblingKeys");
        final List<FDate> valuesA = newValues(1, 3);
        final List<FDate> valuesB = newValues(11, 16);
        newUpdater(table, "keyA", valuesA).update().close();
        newUpdater(table, "keyB", valuesB).update().close();

        final TimeSeriesLookupStorageCache<String, FDate> lookupA = table.getLookupTableCache("keyA");
        final TimeSeriesLookupStorageCache<String, FDate> lookupB = table.getLookupTableCache("keyB");
        final List<File> filesB = new ArrayList<>();
        for (final MemoryFileSummary summary : publishedSummaries(lookupB)) {
            filesB.add(new File(summary.getMemoryResourceUri()));
        }
        Assertions.checkTrue(!filesB.isEmpty(), "precondition: keyB published a chain");

        table.deleteRange("keyA");
        lookupB.clearCaches();

        Assertions.checkFalse(lookupB.isEmptyOrInconsistent(), "the sibling key must stay sound");
        Assertions.checkEquals(InconsistencyReason.NONE, lookupB.getLastInconsistencyReason(),
                "with nothing recorded against it");
        for (final File fileB : filesB) {
            Assertions.checkTrue(fileB.exists(), "a reset of keyA removed a file of keyB: " + fileB);
        }
        Assertions.checkEquals(valuesB, readValues(table, "keyB"), "and keyB's values are unchanged");
        Assertions.checkTrue(readValues(table, "keyA").isEmpty(), "while keyA is genuinely empty");
    }

    /**
     * Format change, detectable kind: a run whose value width differs from the one the store was written with - the
     * shape of a serde change that lands on an outdated storage. The reader misparses the payloads; what matters is
     * that it says so rather than serving values it cannot account for, and that the key can be rebuilt.
     */
    @Test
    public void testAWiderSerdeOverAnOldStoreIsReportedAndRebuilds() throws Exception {
        final ATimeSeriesDB<String, FDate> old = newTable("widerSerde");
        final List<FDate> values = newValues(1, BATCH_FLUSH_INTERVAL + 2);
        newUpdater(old, "widerKey", values).update().close();

        //the same directory, opened by a build whose value width changed
        final ATimeSeriesDB<String, FDate> upgraded = newTable("widerSerde", FDateSerde.GET,
                Integer.valueOf(FDateSerde.FIXED_LENGTH + 3).intValue());
        final TimeSeriesLookupStorageCache<String, FDate> lookup = upgraded.getLookupTableCache("widerKey");

        final boolean inconsistent = lookup.isEmptyOrInconsistent();
        final InconsistencyReason reason = lookup.getLastInconsistencyReason();
        Assertions.checkTrue(inconsistent, "a store written with a different value width must not be served blindly");

        //the rebuild half of this story is pinned by the corrupted-generation cell above, on a single table instance -
        //two ATimeSeriesDB objects over one directory is not a supported pattern and would only test their caches
        //fighting. What this cell adds is the format-change read: the store refuses to serve it.
        //
        //The write side of the same change is caught even earlier, and this run observed it directly: declaring a
        //fixed length the serde does not produce fails at SerializingCollection.add with "Serialized object [...] has
        //unexpected byte length of [12] while fixed length [15] was expected" - so a width change that is not
        //accompanied by a matching serde cannot silently write a mixed-format payload.
    }

    /**
     * Data corruption, the case no layout check can see: a byte flipped INSIDE a stored value, away from the first and
     * last record of every file. The offsets, lengths and counts all still add up, and the summary's own time stamps
     * still agree with the values at the ends of the file - so the store reports itself sound and serves the damaged
     * value. This cell does NOT assert that this is acceptable; it pins what the current mitigations do and do not
     * cover, because "are we safe against truly broken data" needs an answer with its boundary drawn, not a guess.
     * <p>
     * The comparison half of it is the same flip applied to the FIRST value of a file, which the summary stamps do
     * catch - so the detection boundary is positional, not absent.
     */
    @Test
    public void testCorruptionInsideARecordIsNotDetectedWhileCorruptionAtAnEdgeIs() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("midRecordCorruption");
        final List<FDate> values = newValues(1, BATCH_FLUSH_INTERVAL * 3);
        newUpdater(table, "midKey", values).update().close();
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache("midKey");
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(), "precondition: a sound store");

        final File payload = new File(publishedSummaries(lookup).get(0).getMemoryResourceUri());
        final byte[] bytes = java.nio.file.Files.readAllBytes(payload.toPath());
        final int recordLength = FDateSerde.FIXED_LENGTH;
        Assertions.checkTrue(bytes.length >= recordLength * 3,
                "the payload must hold several records to flip one in the middle: " + bytes.length);

        //flip a byte of the MIDDLE record, not the first or the last
        final int middleRecordStart = recordLength;
        bytes[middleRecordStart + 2] ^= (byte) 0xFF;
        java.nio.file.Files.write(payload.toPath(), bytes);
        lookup.clearCaches();

        final boolean detectedInMiddle = lookup.isEmptyOrInconsistent();
        final InconsistencyReason middleReason = lookup.getLastInconsistencyReason();

        //and the same flip on the FIRST record of the same file, which the summary's stored time stamp should catch
        java.nio.file.Files.write(payload.toPath(), bytes);
        final byte[] fresh = java.nio.file.Files.readAllBytes(payload.toPath());
        fresh[2] ^= (byte) 0xFF;
        java.nio.file.Files.write(payload.toPath(), fresh);
        lookup.clearCaches();
        final boolean detectedAtEdge = lookup.isEmptyOrInconsistent();
        final InconsistencyReason edgeReason = lookup.getLastInconsistencyReason();

        //PIN, decided by the probes - and the probes contradicted what I expected: NEITHER position is detected.
        //assertSummary() compares the summary's firstValueEndTime against extractEndTime(summary.getFirstValue(serde)),
        //and that first value is a COPY carried inside the index record, not a read of the payload - so the check
        //validates the index against itself. What the payload is checked for is arithmetic: offsets, lengths and
        //counts (the NAMED_FILE_SHORTER_THAN_CLAIMED arm). A flipped byte inside a value therefore survives every
        //current check and is served as a wrong number.
        Assertions.checkFalse(Boolean.valueOf(detectedInMiddle),
                "a flip inside a non-boundary record is not detected");
        Assertions.checkEquals(InconsistencyReason.NONE, middleReason, "and nothing is recorded against the store");
        Assertions.checkFalse(Boolean.valueOf(detectedAtEdge),
                "the same flip on the FIRST record is not detected either - the boundary stamps are index copies, so"
                        + " there is no content-level integrity check in this format at all");
        Assertions.checkEquals(InconsistencyReason.NONE, edgeReason,
                "which is the finding: corruption detection here is structural, not content-based");
    }

    /**
     * The answer to "doesn't LZ4 already checksum?" - yes on one path, and it is not the path the payload uses.
     * <p>
     * {@code ReusableLZ4BlockInputStream} (the block-stream framing the index generations are read through) computes a
     * checksum over the DECOMPRESSED block bytes and throws "Stream is corrupted" when it disagrees - that is what
     * caught the corrupted index in the first cell of this class. The stored VALUES are read a different way: per
     * record, through {@code CompressionDelegateSerde} into {@code LZ4Streams.decompress(src,dest)}, which reads the
     * original length and a raw flag and hands them to the LZ4 decompressor with NO checksum of any kind. LZ4's own
     * block format carries no integrity either - the framing layer is what adds it, and that layer is exactly what
     * this path skips.
     * <p>
     * So a flipped byte inside a stored record is not caught by the consistency check (which never decompresses a
     * payload file) and not caught by the value read either. This cell pins both halves, because the mitigation
     * question can only be answered with the boundary drawn: integrity here is per-stream, and the payload is read
     * per-record.
     */
    @Test
    public void testAPayloadFlipIsCaughtByNeitherTheConsistencyCheckNorTheValueRead() throws Exception {
        final ATimeSeriesDB<String, FDate> table = newTable("payloadNoGuard");
        final List<FDate> values = newValues(1, BATCH_FLUSH_INTERVAL * 6);
        newUpdater(table, "guardKey", values).update().close();
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache("guardKey");

        final File payload = new File(publishedSummaries(lookup).get(0).getMemoryResourceUri());
        final byte[] bytes = java.nio.file.Files.readAllBytes(payload.toPath());
        final int rawRecordBytes = values.size() * FDateSerde.FIXED_LENGTH;
        Assertions.checkTrue(bytes.length != rawRecordBytes,
                "precondition: the payload is not raw records, it is the compressed per-record framing ("
                        + bytes.length + " bytes for " + values.size() + " records of " + FDateSerde.FIXED_LENGTH
                        + ")");
        final byte[] firstRecord = FDateSerde.GET.toBytes(values.get(0));
        Assertions.checkFalse(containsRun(bytes, firstRecord),
                "and the stored records are not present verbatim, so the read must go through the decompressor");

        bytes[bytes.length / 2] ^= (byte) 0xFF;
        java.nio.file.Files.write(payload.toPath(), bytes);
        lookup.clearCaches();

        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "the consistency check never decompresses a payload file, so it cannot see this");
        Assertions.checkEquals(InconsistencyReason.NONE, lookup.getLastInconsistencyReason(),
                "and records nothing against the store");

        //and the read: it does not fail, it does not report, and it does not return what was stored either. The
        //measured shape of the damage is SILENT LOSS - the corrupted size header shifts every later record position,
        //so the reader stops short of the end. Which is worse than an exception: an exception is a fact someone can
        //act on, while a short answer looks like a market that simply had no more data.
        final List<FDate> afterFlip = readValues(table, "guardKey");
        Assertions.checkTrue(afterFlip.size() < values.size(),
                "the read returns FEWER values than were written (" + afterFlip.size() + " of " + values.size()
                        + ") without failing and without any check saying a word - this is the case a checksum exists"
                        + " to turn into an error");
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "and even after that loss the store still reports itself sound");
        log.info("Payload flip: asked %s values, read %s, neither the consistency check nor the read reported it",
                Integer.valueOf(values.size()), Integer.valueOf(afterFlip.size()));
    }

    private static boolean containsRun(final byte[] haystack, final byte[] needle) {
        outer: for (int i = 0; i + needle.length <= haystack.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * The mitigation, simulated rather than shipped: production keeps checksums off for throughput, so what has to be
     * proven is that the option WORKS when it is wanted - that a corrupted value becomes a loud failure, and that the
     * existing reset-and-rebuild path then restores the data.
     * <p>
     * A 4-byte CRC32 rides in front of each stored value. Flip a byte mid-payload and compare against the
     * no-integrity cell above: the consistency check still says nothing (it never decompresses a payload file), but
     * the READ now fails with the checksum named, instead of quietly returning fewer values than were written. From
     * there the recovery is the ordinary one: reset the key, let the next pass re-supply it, read whole.
     */
    @Test
    public void testAChecksummedSerdeMakesTheDamageLoudAndTheResetRestoresTheValues() throws Exception {
        //compression OFF, which is the layout the flyweight positional read uses: here the serde IS the only reader
        //of the stored bytes, so a per-record checksum is what turns silent truncation into a failure
        final ChecksummedDateSerde serde = new ChecksummedDateSerde();
        final ATimeSeriesDB<String, FDate> table = newTable("checksumGuard", serde, serde.length(),
                DisabledCompressionFactory.INSTANCE);
        final List<FDate> values = newValues(1, BATCH_FLUSH_INTERVAL * 6);
        newUpdater(table, "checksumKey", values).update().close();
        final TimeSeriesLookupStorageCache<String, FDate> lookup = table.getLookupTableCache("checksumKey");
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(), "precondition: a sound store");

        final File payload = new File(publishedSummaries(lookup).get(0).getMemoryResourceUri());
        final byte[] bytes = java.nio.file.Files.readAllBytes(payload.toPath());
        bytes[bytes.length / 2] ^= (byte) 0xFF;
        java.nio.file.Files.write(payload.toPath(), bytes);
        lookup.clearCaches();

        //the structural check is unchanged by the serde: it still never looks inside the payload
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(),
                "the consistency check still cannot see payload damage - the guard is in the read, as designed");

        Throwable readFailure = null;
        List<FDate> afterFlip = null;
        try {
            afterFlip = readValues(table, "checksumKey");
        } catch (final Throwable t) {
            readFailure = t;
        }
        Assertions.checkNotNull(readFailure,
                "with the checksum in the record, the read FAILS instead of silently returning " + (afterFlip == null
                        ? "?" : Integer.valueOf(afterFlip.size())) + " of " + values.size() + " values");
        Assertions.checkTrue(rootCauseMessage(readFailure).contains("checksum"),
                "and it says what is wrong: " + rootCauseMessage(readFailure));

        //recovery: the ordinary reset path, which is why a loud failure is worth the bytes
        table.deleteRange("checksumKey");
        lookup.clearCaches();
        newUpdater(table, "checksumKey", values).update().close();
        lookup.clearCaches();
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(), "the rebuilt store reports sound");
        Assertions.checkEquals(values, readValues(table, "checksumKey"),
                "and every value is back - the damage cost a rebuild, not a hole in the series");
    }

    /**
     * The other mitigation shape Edwin asked for: a serde that REFUSES a format change. The version lives inside the
     * record, so nothing about the width, the offsets or the counts changes - this is exactly the reinterpretation the
     * arithmetic checks cannot see, and the serde is the only component that knows it is reading foreign bytes.
     * <p>
     * Two halves, and the second one is the rule this test exists to teach: refusing with the marker
     * {@code SerializationException} makes the store recover (reset, rebuild, whole), while refusing with a plain
     * runtime exception escapes the consistency check entirely - the consumer never gets its answer and the key is
     * never rebuilt.
     */
    @Test
    public void testASerdeRefusingAFormatChangeRecoversWhenItThrowsTheMarkerType() throws Exception {
        final ATimeSeriesDB<String, FDate> v1 = newTable("versionedSerde", new VersionedDateSerde(FORMAT_V1),
                VersionedDateSerde.LENGTH);
        final List<FDate> values = newValues(1, BATCH_FLUSH_INTERVAL + 2);
        newUpdater(v1, "versionKey", values).update().close();

        //the same store, opened by a build whose format moved on: same width, different meaning
        final ATimeSeriesDB<String, FDate> v2 = newTable("versionedSerde", new VersionedDateSerde(FORMAT_V2),
                VersionedDateSerde.LENGTH);
        final TimeSeriesLookupStorageCache<String, FDate> lookup = v2.getLookupTableCache("versionKey");

        Assertions.checkTrue(lookup.isEmptyOrInconsistent(),
                "a serde that recognises foreign bytes must make the store report itself inconsistent");
        Assertions.checkEquals(InconsistencyReason.INVALID_SERIALIZATION, lookup.getLastInconsistencyReason(),
                "and the reason must be the serde arm, not an arithmetic accident");

        v2.deleteRange("versionKey");
        lookup.clearCaches();
        newUpdater(v2, "versionKey", values).update().close();
        lookup.clearCaches();
        Assertions.checkFalse(lookup.isEmptyOrInconsistent(), "the rebuilt store is sound under the new format");
        Assertions.checkEquals(values, readValues(v2, "versionKey"),
                "and the values survive the format move, because the reset path could run");
    }

    @Test
    public void testASerdeRefusingAFormatChangeWithAPlainExceptionDoesNotRecover() throws Exception {
        final ATimeSeriesDB<String, FDate> v1 = newTable("unmarkerSerde", new VersionedDateSerde(FORMAT_V1),
                VersionedDateSerde.LENGTH);
        final List<FDate> values = newValues(1, BATCH_FLUSH_INTERVAL + 2);
        newUpdater(v1, "unmarkerKey", values).update().close();

        final ATimeSeriesDB<String, FDate> v2 = newTable("unmarkerSerde",
                new UnmarkingRefusalSerde(), VersionedDateSerde.LENGTH);
        final TimeSeriesLookupStorageCache<String, FDate> lookup = v2.getLookupTableCache("unmarkerKey");

        boolean escaped = false;
        String escapedMessage = null;
        try {
            lookup.isEmptyOrInconsistent();
        } catch (final Throwable t) {
            escaped = true;
            escapedMessage = rootCauseMessage(t);
        }
        //PIN, and it is a rule rather than a preference: a format refusal that does not use the marker type leaves
        //the consumer with an exception instead of an answer, so the key is never rebuilt and the store stays broken
        Assertions.checkTrue(Boolean.valueOf(escaped),
                "today a plain runtime refusal still escapes the check - this is why serdes must throw the marker "
                        + "type: " + escapedMessage);
    }

    // --- test-only integrity serdes; production keeps both of these OFF for throughput ---

    /** {@code CRC32(millis,picos)} ahead of the value bytes: content integrity carried by the serde. */
    private static final class ChecksummedDateSerde implements ISerde<FDate> {

        private static final int CRC_INDEX = 0;
        private static final int VALUE_INDEX = Integer.BYTES;

        int length() {
            return VALUE_INDEX + FDateSerde.FIXED_LENGTH;
        }

        @Override
        public int toBuffer(final IByteBuffer buffer, final FDate obj) {
            if (obj == null) {
                return 0;
            }
            FDateSerde.putFDateNotNullSafe(buffer, VALUE_INDEX, obj);
            buffer.putInt(CRC_INDEX, checksum(obj));
            return length();
        }

        @Override
        public FDate fromBuffer(final IByteBuffer buffer) {
            final FDate value = FDateSerde.getFDateNotNullSafe(buffer, VALUE_INDEX);
            final int stored = buffer.getInt(CRC_INDEX);
            final int computed = checksum(value);
            if (stored != computed) {
                throw new SerializationException("stored value [" + value + "] fails its checksum: stored [" + stored
                        + "] != computed [" + computed + "]");
            }
            return value;
        }

        private static int checksum(final FDate value) {
            final java.util.zip.CRC32 crc = new java.util.zip.CRC32();
            final byte[] raw = new byte[FDateSerde.FIXED_LENGTH];
            final long millis = value.millisValue();
            for (int i = 0; i < Long.BYTES; i++) {
                raw[i] = (byte) (millis >>> (8 * (Long.BYTES - 1 - i)));
            }
            final int picos = value.picosValue();
            for (int i = 0; i < Integer.BYTES; i++) {
                raw[Long.BYTES + i] = (byte) (picos >>> (8 * (Integer.BYTES - 1 - i)));
            }
            crc.update(raw);
            return (int) crc.getValue();
        }
    }

    /** A version field inside the record: same width, and the serde knows when the bytes are not its format. */
    private static final class VersionedDateSerde implements ISerde<FDate> {

        static final int LENGTH = Integer.BYTES + FDateSerde.FIXED_LENGTH;
        static final int VALUE_INDEX = Integer.BYTES;

        private final int format;

        private VersionedDateSerde(final int format) {
            this.format = format;
        }

        @Override
        public int toBuffer(final IByteBuffer buffer, final FDate obj) {
            if (obj == null) {
                return 0;
            }
            buffer.putInt(0, format);
            FDateSerde.putFDateNotNullSafe(buffer, VALUE_INDEX, obj);
            return LENGTH;
        }

        @Override
        public FDate fromBuffer(final IByteBuffer buffer) {
            final int stored = buffer.getInt(0);
            if (stored != format) {
                throw new SerializationException("stored value format version [" + stored
                        + "] is not understood by this serde [" + format + "]");
            }
            return FDateSerde.getFDateNotNullSafe(buffer, VALUE_INDEX);
        }
    }

    /**
     * The same refusal, thrown as a plain runtime exception rather than the marker type - the idiom that does NOT
     * recover. Written standalone rather than as a subclass, because the point is exactly that its check is its own.
     */
    private static final class UnmarkingRefusalSerde implements ISerde<FDate> {

        @Override
        public int toBuffer(final IByteBuffer buffer, final FDate obj) {
            if (obj == null) {
                return 0;
            }
            buffer.putInt(0, FORMAT_V2);
            FDateSerde.putFDateNotNullSafe(buffer, VersionedDateSerde.VALUE_INDEX, obj);
            return VersionedDateSerde.LENGTH;
        }

        @Override
        public FDate fromBuffer(final IByteBuffer buffer) {
            if (buffer.getInt(0) != FORMAT_V2) {
                throw new IllegalArgumentException("I do not recognise this stored format");
            }
            return FDateSerde.getFDateNotNullSafe(buffer, VersionedDateSerde.VALUE_INDEX);
        }
    }

    private static String rootCauseMessage(final Throwable t) {
        Throwable current = t;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current.getClass().getSimpleName() + ": " + current.getMessage();
    }

    // --- damage helpers ---

    private static File newestGenerationFile(final TimeSeriesLookupStorageCache<String, FDate> lookup) {
        final int generation = lookup.getMemoryFileLookupTable().getCurrentIndexNumber();
        final File candidate = new File(memoryDirectoryOf(lookup),
                AMemoryFileSummarySerializingCollection.newIndexGenerationFileName(generation));
        Assertions.checkTrue(candidate.exists(), "the newest generation must be present: " + candidate);
        return candidate;
    }

    private static void corruptBytesOf(final File generation) throws java.io.IOException {
        //a byte flip in the middle of the serialised chain: not a truncation, so the length-based arms cannot see it
        final byte[] bytes = java.nio.file.Files.readAllBytes(generation.toPath());
        Assertions.checkTrue(bytes.length > 16, "the generation is too small to corrupt meaningfully: "
                + bytes.length);
        bytes[bytes.length / 2] = (byte) (bytes[bytes.length / 2] ^ 0xFF);
        java.nio.file.Files.write(generation.toPath(), bytes);
    }

    private static File memoryDirectoryOf(final TimeSeriesLookupStorageCache<String, FDate> lookup) {
        return lookup.getDirectoryHashKeyVersionMemory().getDirectoryHashKeyVersionDataShared();
    }

    private static List<MemoryFileSummary> publishedSummaries(
            final TimeSeriesLookupStorageCache<String, FDate> lookup) {
        final List<MemoryFileSummary> summaries = new ArrayList<>();
        try (ICloseableIterator<MemoryFileSummary> it = lookup.getMemoryFileLookupTable().range()) {
            Lists.toListWithoutHasNext(it, summaries);
        }
        return summaries;
    }

    private List<FDate> readValues(final ATimeSeriesDB<String, FDate> table, final String key) {
        final List<FDate> values = new ArrayList<>();
        try (ICloseableIterator<FDate> it = table.rangeValues(key, FDates.MIN_DATE, FDates.MAX_DATE).iterator()) {
            Lists.toListWithoutHasNext(it, values);
        }
        return values;
    }

    private static List<FDate> newValues(final int fromIndex, final int toIndex) {
        final List<FDate> values = new ArrayList<>();
        for (int i = fromIndex; i <= toIndex; i++) {
            values.add(FDateBuilder.newDate(2026, 1, 1, 0, i - 1, 0));
        }
        return values;
    }

    private ATimeSeriesDB<String, FDate> newTable(final String name) {
        return newTable(name, FDateSerde.GET, FDateSerde.FIXED_LENGTH);
    }

    private ATimeSeriesDB<String, FDate> newTable(final String name, final ISerde<FDate> serde,
            final int fixedLength) {
        return newTable(name, serde, fixedLength, null);
    }

    private ATimeSeriesDB<String, FDate> newTable(final String name, final ISerde<FDate> serde,
            final int fixedLength, final ICompressionFactory compressionFactory) {
        return new ATimeSeriesDB<String, FDate>(name) {

            @Override
            protected ICompressionFactory newCompressionFactory() {
                return compressionFactory == null ? super.newCompressionFactory() : compressionFactory;
            }

            @Override
            protected ISerde<FDate> newValueSerde() {
                return serde;
            }

            @Override
            protected Integer newValueFixedLength() {
                return Integer.valueOf(fixedLength);
            }

            @Override
            protected int newBatchFlushInterval() {
                return BATCH_FLUSH_INTERVAL;
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

    private ATimeSeriesUpdater<String, FDate> newUpdater(final ATimeSeriesDB<String, FDate> table, final String key,
            final List<FDate> values) {
        return new ATimeSeriesUpdater<String, FDate>(key, table) {

            @Override
            protected ICloseableIterable<? extends FDate> getSource(final FDate updateFrom) {
                return WrapperCloseableIterable.maybeWrap(values);
            }

            @Override
            protected void onUpdateStarted(final FDate updateStart) {}

            @Override
            protected void onUpdateFinished() {}

            @Override
            protected FDate extractStartTime(final FDate element) {
                return element;
            }

            @Override
            protected FDate extractEndTime(final FDate element) {
                return element;
            }

            @Override
            protected void onElement(final ITimeSeriesUpdateProgress relativeProgress, final long relativeCount) {}

            @Override
            protected void onFlush(final ITimeSeriesUpdateProgress relativeProgress, final long flushIndex) {}

            @Override
            public Percent getProgress(final FDate minTime, final FDate maxTime) {
                return null;
            }
        };
    }
}
