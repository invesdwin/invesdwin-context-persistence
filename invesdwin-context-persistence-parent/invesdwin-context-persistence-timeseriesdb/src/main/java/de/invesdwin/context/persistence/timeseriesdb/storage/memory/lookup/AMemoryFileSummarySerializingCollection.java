package de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup;

import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.filechannel.IFileChannel;
import de.invesdwin.context.persistence.timeseriesdb.SerializingCollection;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummarySerde;
import de.invesdwin.context.persistence.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.util.lang.string.description.TextDescription;

@NotThreadSafe
public abstract class AMemoryFileSummarySerializingCollection extends SerializingCollection<MemoryFileSummary> {

    public static final String MEMORY_INDEX_FILE_NAME = "memory.index";

    public AMemoryFileSummarySerializingCollection(final TextDescription name, final IFileChannel fileChannel,
            final boolean readOnly) {
        super(name, fileChannel, readOnly);
    }

    @Override
    protected abstract MemoryFileSummarySerde newSerde();

    @Override
    protected abstract ICompressionFactory getCompressionFactory();

    @Override
    protected OutputStream newCompressor(final OutputStream out) {
        return getCompressionFactory().newCompressor(out, ATimeSeriesUpdater.LARGE_COMPRESSOR);
    }

    /**
     * The name a lookup table generation carries: {@value #MEMORY_INDEX_FILE_NAME} behind the generation number and an
     * underscore. The bare name without a prefix is the derived store's single rewritten index
     * (de.invesdwin.trading.financialdata.live.bars.calculated.internal.segment.LiveCalculatedSegmentFiles), which is
     * not a generation and never matches this.
     */
    public static String newIndexGenerationFileName(final int generationNumber) {
        return generationNumber + "_" + MEMORY_INDEX_FILE_NAME;
    }

    public static boolean isIndexGenerationFileName(final String fileName) {
        return indexNumberOf(fileName) != Integer.MIN_VALUE;
    }

    /**
     * The generation number a file name carries, Integer.MIN_VALUE for anything that is not a generation (a malformed
     * prefix included), which is how the lookup table has always treated those names.
     */
    public static int indexNumberOf(final String fileName) {
        //same reading the lookup table has always used: the first underscore ends the numeric prefix, so a name with a
        //malformed prefix is not a generation rather than a generation with a surprising number.
        final int underscoreIdx = fileName.indexOf('_');
        if (underscoreIdx <= 0 || !fileName.endsWith("_" + MEMORY_INDEX_FILE_NAME)) {
            return Integer.MIN_VALUE;
        }
        try {
            return Integer.parseInt(fileName.substring(0, underscoreIdx));
        } catch (final NumberFormatException e) {
            return Integer.MIN_VALUE;
        }
    }

}