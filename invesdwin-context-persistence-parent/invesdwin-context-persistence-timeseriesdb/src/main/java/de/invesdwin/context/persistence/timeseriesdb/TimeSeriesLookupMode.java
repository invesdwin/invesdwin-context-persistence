package de.invesdwin.context.persistence.timeseriesdb;

import javax.annotation.concurrent.Immutable;

@Immutable
public enum TimeSeriesLookupMode {
    Value,
    ValueUntilIndexAvailable,
    Index;

    public static final TimeSeriesLookupMode DEFAULT = ValueUntilIndexAvailable;
}
