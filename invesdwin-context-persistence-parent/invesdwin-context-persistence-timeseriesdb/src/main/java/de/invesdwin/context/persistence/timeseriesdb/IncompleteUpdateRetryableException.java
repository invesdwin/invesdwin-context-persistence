package de.invesdwin.context.persistence.timeseriesdb;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class IncompleteUpdateRetryableException extends Exception {

    public IncompleteUpdateRetryableException() {
        super();
    }

    public IncompleteUpdateRetryableException(final String message) {
        super(message);
    }

    public IncompleteUpdateRetryableException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public IncompleteUpdateRetryableException(final Throwable cause) {
        super(cause);
    }

}
