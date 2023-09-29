package de.invesdwin.context.persistence.timeseriesdb.storage;

import java.io.File;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.persistence.timeseriesdb.SerializingCollection;
import de.invesdwin.context.persistence.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.util.lang.string.description.TextDescription;

@NotThreadSafe
public abstract class AMemoryFileSummarySerializingCollection extends SerializingCollection<MemoryFileSummary> {

    public AMemoryFileSummarySerializingCollection(final TextDescription name, final File file,
            final boolean readOnly) {
        super(name, file, readOnly);
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