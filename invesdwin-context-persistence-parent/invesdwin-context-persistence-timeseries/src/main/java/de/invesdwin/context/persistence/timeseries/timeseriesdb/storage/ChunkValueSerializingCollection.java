package de.invesdwin.context.persistence.timeseries.timeseriesdb.storage;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.timeseriesdb.SerializingCollection;
import de.invesdwin.util.lang.description.TextDescription;
import ezdb.serde.Serde;

@NotThreadSafe
public final class ChunkValueSerializingCollection extends SerializingCollection<ChunkValue> {
    public ChunkValueSerializingCollection(final TextDescription name, final File file, final boolean readOnly) {
        super(name, file, readOnly);
    }

    @Override
    protected Serde<ChunkValue> newSerde() {
        return ChunkValueSerde.GET;
    }

    @Override
    protected Integer getFixedLength() {
        return ChunkValueSerde.FIXED_LENGTH;
    }
}