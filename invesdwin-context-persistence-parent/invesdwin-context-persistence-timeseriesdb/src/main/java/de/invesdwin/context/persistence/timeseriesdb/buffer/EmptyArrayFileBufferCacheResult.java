package de.invesdwin.context.persistence.timeseriesdb.buffer;

import java.util.ArrayList;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class EmptyArrayFileBufferCacheResult<V> extends ArrayFileBufferCacheResult<V> {

    @SuppressWarnings("rawtypes")
    private static final EmptyArrayFileBufferCacheResult INSTANCE = new EmptyArrayFileBufferCacheResult<>();

    private EmptyArrayFileBufferCacheResult() {
        super(new ArrayList<>());
    }

    @SuppressWarnings("unchecked")
    public static <V> EmptyArrayFileBufferCacheResult<V> getInstance() {
        return INSTANCE;
    }

}
