package de.invesdwin.context.persistence.jpa.api.util;

import javax.annotation.concurrent.Immutable;

import org.slf4j.ext.XLogger.Level;

import de.invesdwin.context.log.Log;

@Immutable
public final class SqlErr {

    private static final Log LOG = new Log("de.invesdwin.SQL_ERROR");

    private SqlErr() {}

    public static void logSqlException(final Throwable t) {
        LOG.catching(Level.WARN, t);
    }

}
