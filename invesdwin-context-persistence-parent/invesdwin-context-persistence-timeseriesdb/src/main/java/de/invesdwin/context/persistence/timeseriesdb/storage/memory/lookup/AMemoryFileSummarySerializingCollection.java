package de.invesdwin.context.persistence.timeseriesdb.storage.memory.lookup;

import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.filechannel.nio.atomic.AtomicNioFileChannel;
import de.invesdwin.context.persistence.timeseriesdb.SerializingCollection;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummarySerde;
import de.invesdwin.context.persistence.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.util.lang.string.description.TextDescription;

@NotThreadSafe
public abstract class AMemoryFileSummarySerializingCollection extends SerializingCollection<MemoryFileSummary> {

    public static final String MEMORY_INDEX_FILE_NAME = "memory.index";

    public AMemoryFileSummarySerializingCollection(final TextDescription name, final AtomicNioFileChannel fileChannel,
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

}