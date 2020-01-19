package de.invesdwin.context.persistence.timeseries.timeseriesdb.storage;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.serde.RemoteFastSerializingSerde;

@NotThreadSafe
public final class ChunkValueSerde extends RemoteFastSerializingSerde<ChunkValue> {

    public static final ChunkValueSerde GET = new ChunkValueSerde();
    public static final Integer FIXED_LENGTH = null;

    private ChunkValueSerde() {
        super(false, ChunkValue.class);
    }

}
