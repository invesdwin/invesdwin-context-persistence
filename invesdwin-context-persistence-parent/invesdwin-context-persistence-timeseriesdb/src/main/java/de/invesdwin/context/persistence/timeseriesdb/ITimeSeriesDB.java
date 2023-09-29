package de.invesdwin.context.persistence.timeseriesdb;

import java.io.Closeable;
import java.io.File;

import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.concurrent.lock.readwrite.IReadWriteLock;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.time.date.FDate;

public interface ITimeSeriesDB<K, V> extends Closeable {

    File getBaseDirectory();

    File getDirectory();

    IReadWriteLock getTableLock(K key);

    ICloseableIterable<V> rangeValues(K key, FDate from, FDate to);

    /**
     * FROM should be greater than or equal to TO, so it is inverted from rangeValues(...)
     */
    ICloseableIterable<V> rangeReverseValues(K key, FDate from, FDate to);

    V getLatestValue(K key, FDate date);

    FDate getLatestValueKey(K key, FDate date);

    V getLatestValue(K key, long index);

    FDate getLatestValueKey(K key, long index);

    long getLatestValueIndex(K key, FDate date);

    long size(K key);

    /**
     * Jumps the specified shiftBackUnits to the past instead of only one unit. 0 results in current value.
     * 
     * key is inclusive
     * 
     * index 0 is the current value (below or equal to key), index 1 the previous value and so on
     */
    V getPreviousValue(K key, FDate date, int shiftBackUnits);

    /**
     * Jumps the specified shiftBackUnits to the past instead of only one unit. 0 results in current value.
     * 
     * key is inclusive
     * 
     * index 0 is the current value (below or equal to key), index 1 the previous value and so on
     */
    FDate getPreviousValueKey(K key, FDate date, int shiftBackUnits);

    boolean isEmptyOrInconsistent(K key);

    /**
     * Jumps the specified shiftForwardUnits to the future instead of only one unit.
     * 
     * key is inclusive
     * 
     * index 0 is the current value (above or equal to key), index 1 the next value and so on
     */
    V getNextValue(K key, FDate date, int shiftForwardUnits);

    /**
     * Jumps the specified shiftForwardUnits to the future instead of only one unit.
     * 
     * key is inclusive
     * 
     * index 0 is the current value (above or equal to key), index 1 the next value and so on
     */
    FDate getNextValueKey(K key, FDate date, int shiftForwardUnits);

    void deleteRange(K key);

    String getName();

    ISerde<V> getValueSerde();

    Integer getValueFixedLength();

    ICompressionFactory getCompressionFactory();

    String hashKeyToString(K key);

    FDate extractEndTime(V value);

}
