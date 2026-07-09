package de.invesdwin.context.persistence.jpa.api.util;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.log.Log;
import de.invesdwin.util.log.LogLevel;

@Immutable
public final class SqlErr {

    private static final Log LOG = new Log("de.invesdwin.SQL_ERROR");

    private SqlErr() {}

    public static void logSqlException(final Throwable t) {
        LOG.catching(LogLevel.WARN, t);
    }

}
