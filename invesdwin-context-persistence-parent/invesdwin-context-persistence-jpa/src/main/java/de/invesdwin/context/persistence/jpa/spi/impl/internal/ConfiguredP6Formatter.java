package de.invesdwin.context.persistence.jpa.spi.impl.internal;

import javax.annotation.concurrent.Immutable;

import com.p6spy.engine.spy.appender.MessageFormattingStrategy;

@Immutable
public class ConfiguredP6Formatter implements MessageFormattingStrategy {

    @Override
    public String formatMessage(final int connectionId, final String now, final long elapsed, final String category,
            final String prepared, final String sql) {
        return sql;
    }

}
