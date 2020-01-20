package de.invesdwin.context.persistence.timeseries.timeseriesdb.storage;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.timeseriesdb.SerializingCollection;
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
}