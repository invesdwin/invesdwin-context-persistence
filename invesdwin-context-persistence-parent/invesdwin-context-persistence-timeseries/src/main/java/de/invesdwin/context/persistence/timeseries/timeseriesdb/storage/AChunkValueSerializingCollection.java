package de.invesdwin.context.persistence.timeseries.timeseriesdb.storage;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.streams.compressor.ICompressorFactory;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.SerializingCollection;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.util.lang.description.TextDescription;

@NotThreadSafe
public abstract class AChunkValueSerializingCollection extends SerializingCollection<ChunkValue> {

    public AChunkValueSerializingCollection(final TextDescription name, final File file, final boolean readOnly) {
        super(name, file, readOnly);
    }

    @Override
    protected abstract ChunkValueSerde newSerde();

    @Override
    protected Integer getFixedLength() {
        return newSerde().getFixedLength();
    }

    protected abstract ICompressorFactory getCompressorFactory();

    @Override
    protected OutputStream newCompressor(final OutputStream out) {
        return getCompressorFactory().newCompressor(out, ATimeSeriesUpdater.LARGE_COMPRESSOR);
    }

    @Override
    protected InputStream newDecompressor(final InputStream inputStream) {
        return getCompressorFactory().newDecompressor(inputStream);
    }
}