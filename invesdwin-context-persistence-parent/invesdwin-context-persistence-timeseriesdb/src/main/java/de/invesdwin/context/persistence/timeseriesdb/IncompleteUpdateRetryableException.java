package de.invesdwin.context.persistence.timeseriesdb;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.error.Throwables;

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

    public static IncompleteUpdateRetryableException propagateIncompleteUpdateException(final Throwable t)
            throws IncompleteUpdateRetryableException {
        if (Throwables.isCausedByType(t, IncompleteUpdateAbortedException.class)) {
            throw Throwables.propagate(t);
        }
        final IncompleteUpdateRetryableException incompleteException = Throwables.getCauseByType(t,
                IncompleteUpdateRetryableException.class);
        if (incompleteException != null) {
            return incompleteException;
        } else {
            return new IncompleteUpdateRetryableException("Something unexpected went wrong that could be retried", t);
        }
    }

}
