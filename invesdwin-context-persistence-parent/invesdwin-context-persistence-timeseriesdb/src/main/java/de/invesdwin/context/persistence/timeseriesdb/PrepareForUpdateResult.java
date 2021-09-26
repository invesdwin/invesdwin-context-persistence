package de.invesdwin.context.persistence.timeseriesdb;

import java.util.List;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.date.FDate;

@Immutable
public class PrepareForUpdateResult<V> {

    private final FDate updateFrom;
    private final List<V> lastValues;
    private final long addressOffset;

    public PrepareForUpdateResult(final FDate updateFrom, final List<V> lastValues, final long addressOffset) {
        this.updateFrom = updateFrom;
        Assertions.checkNotNull(lastValues);
        this.lastValues = lastValues;
        this.addressOffset = addressOffset;
    }

    public FDate getUpdateFrom() {
        return updateFrom;
    }

    public List<V> getLastValues() {
        return lastValues;
    }

    public long getAddressOffset() {
        return addressOffset;
    }

}
