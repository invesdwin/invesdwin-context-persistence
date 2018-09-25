package de.invesdwin.context.persistence.leveldb.timeseries.storage;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class CorruptedTimeSeriesStorageException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public CorruptedTimeSeriesStorageException() {
        super();
    }

    public CorruptedTimeSeriesStorageException(final String message) {
        super(message);
    }

    public CorruptedTimeSeriesStorageException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public CorruptedTimeSeriesStorageException(final Throwable cause) {
        super(cause);
    }
}
