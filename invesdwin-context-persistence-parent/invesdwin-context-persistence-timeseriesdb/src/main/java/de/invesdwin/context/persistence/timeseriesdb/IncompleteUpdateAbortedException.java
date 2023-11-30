package de.invesdwin.context.persistence.timeseriesdb;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class IncompleteUpdateAbortedException extends RuntimeException {

    public IncompleteUpdateAbortedException() {
        super();
    }

    public IncompleteUpdateAbortedException(final String message) {
        super(message);
    }

    public IncompleteUpdateAbortedException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public IncompleteUpdateAbortedException(final Throwable cause) {
        super(cause);
    }

}
