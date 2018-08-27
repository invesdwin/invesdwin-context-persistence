package de.invesdwin.context.persistence.jpa.spi.impl.internal;

import javax.annotation.concurrent.ThreadSafe;

import com.p6spy.engine.logging.Category;

import de.invesdwin.util.lang.Strings;

@ThreadSafe
public class ConfiguredP6Logger extends com.p6spy.engine.spy.appender.Slf4JLogger {

    @Override
    public void logSQL(final int connectionId, final String now, final long elapsed, final Category category,
            final String prepared, final String sql) {
        if (Strings.isNotEmpty(sql)) {
            super.logSQL(connectionId, now, elapsed, category, prepared, sql);
        }
    }

    @Override
    public void logText(final String text) {
        if (Strings.isNotEmpty(text)) {
            super.logText(text);
        }
    }

}
