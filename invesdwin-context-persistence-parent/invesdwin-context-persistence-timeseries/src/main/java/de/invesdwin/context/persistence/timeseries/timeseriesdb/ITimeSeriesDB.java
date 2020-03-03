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
     * FROM should be greater than or equal to TO, so it is inverted from rangeValues(...)
     */
    ICloseableIterable<V> rangeReverseValues(K key, FDate from, FDate to);

    V getLatestValue(K key, FDate date);

    FDate getLatestValueKey(K key, FDate date);

    /**
     * 0 is invalid, 1 means current value, 2 means previous value
     */
    V getPreviousValue(K key, FDate date, int shiftBackUnits);

    /**
     * 0 is invalid, 1 means current value, 2 means previous value
     */
    FDate getPreviousValueKey(K key, FDate date, int shiftBackUnits);

    boolean isEmptyOrInconsistent(K key);

    /**
     * 0 is invalid, 1 means current value, 2 means next value
     */
    V getNextValue(K key, FDate date, int shiftForwardUnits);

    /**
     * 0 is invalid, 1 means current value, 2 means next value
     */
    FDate getNextValueKey(K key, FDate date, int shiftForwardUnits);

    void deleteRange(K key);

    String getName();

}
