package de.invesdwin.context.persistence.timeseriesdb.storage;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.streams.compressor.ICompressionFactory;
import de.invesdwin.context.persistence.timeseriesdb.SerializingCollection;
import de.invesdwin.context.persistence.timeseriesdb.updater.ATimeSeriesUpdater;
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

    protected abstract ICompressionFactory getCompressionFactory();

    @Override
    protected OutputStream newCompressor(final OutputStream out) {
        return getCompressionFactory().newCompressor(out, ATimeSeriesUpdater.LARGE_COMPRESSOR);
    }

    @Override
    protected InputStream newDecompressor(final InputStream inputStream) {
        return getCompressionFactory().newDecompressor(inputStream);
    }
}