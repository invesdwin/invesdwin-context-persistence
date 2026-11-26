package de.invesdwin.context.persistence.timeseriesdb;

import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupStorageCache.InconsistencyReason;

import java.io.Closeable;

import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.persistence.timeseriesdb.directory.ITimeSeriesDirectory;
import de.invesdwin.context.persistence.timeseriesdb.directory.base.ITimeSeriesBaseDirectory;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.concurrent.lock.readwrite.IReentrantReadWriteLock;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.time.date.FDate;

public interface ITimeSeriesDB<K, V> extends Closeable {

    ITimeSeriesBaseDirectory getBaseDirectory();

    ITimeSeriesDirectory getDirectory();

    IReentrantReadWriteLock getTableLock(K key);

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

    long size(K key, FDate from, FDate to);

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
     * Which check indicted this key on the most recent {@link #isEmptyOrInconsistent(K)} pass, or {@code NONE} when
     * nothing did. Call the boolean first: the walk is the expensive part, and this reads the answer that walk already
     * recorded rather than re-checking.
     * <p>
     * It exists because one boolean is not a policy. Wiping a key is the right act for a chain that contradicts itself
     * or a stream that will not decode, and is data loss for a key that was simply never built - yet every consumer of
     * the boolean has so far had to guess which case it is holding, or probe the store a second time to find out.
     * <p>
     * The default answers {@code NONE} for stores that do not classify their own damage; the real timeseriesdb
     * implementations override it, and a consumer must treat {@code NONE} together with a {@code true} boolean as "this
     * store cannot tell me why", not as "the store is fine".
     */
    default InconsistencyReason getInconsistencyReason(final K key) {
        return InconsistencyReason.NONE;
    }

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

    String hashKeyToString(K key);

    String innerHashKeyToString(K key);

    ISerde<V> getValueSerde();

    Integer getValueFixedLength();

    ICompressionFactory getCompressionFactory();

    TimeSeriesLookupMode getLookupMode();

    int getBatchFlushInterval();

    FDate extractStartTime(V value);

    FDate extractEndTime(V value);

}
