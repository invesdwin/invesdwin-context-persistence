package de.invesdwin.context.persistence.timeseries.timeseriesdb;

import java.io.Closeable;
import java.io.File;

import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.concurrent.lock.readwrite.IReadWriteLock;
import de.invesdwin.util.time.fdate.FDate;

public interface ITimeSeriesDB<K, V> extends Closeable {

    File getDirectory();

    IReadWriteLock getTableLock(K key);

    ICloseableIterable<V> rangeValues(K key, FDate from, FDate to);

    /**
     * from should be greater than or equal to to, so it is inverted from rangeValues(...)
     */
    ICloseableIterable<V> rangeReverseValues(K key, FDate from, FDate to);

    V getLatestValue(K key, FDate date);

    FDate getLatestValueKey(K key, FDate date);

    V getPreviousValue(K key, FDate date, int shiftBackUnits);

    FDate getPreviousValueKey(K key, FDate date, int shiftBackUnits);

    boolean isEmptyOrInconsistent(K key);

    V getNextValue(K key, FDate date, int shiftForwardUnits);

    FDate getNextValueKey(K key, FDate date, int shiftForwardUnits);

    void deleteRange(K key);

    String getName();

}
