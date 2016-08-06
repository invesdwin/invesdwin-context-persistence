package de.invesdwin.context.persistence.leveldb.timeseries;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class IncompleteUpdateFoundException extends Exception {

    public IncompleteUpdateFoundException() {
        super();
    }

    public IncompleteUpdateFoundException(final String message) {
        super(message);
    }

    public IncompleteUpdateFoundException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public IncompleteUpdateFoundException(final Throwable cause) {
        super(cause);
    }

}
