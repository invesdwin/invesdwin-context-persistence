package de.invesdwin.context.persistence.leveldb.timeseries;

import java.io.Closeable;
import java.io.File;
import java.util.concurrent.locks.ReadWriteLock;

import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.time.fdate.FDate;

public interface ITimeSeriesDB<K, V> extends Closeable {

    File getDirectory();

    ReadWriteLock getTableLock(K key);

    ICloseableIterator<V> rangeValues(K key, FDate from, FDate to);

    ICloseableIterator<V> rangeReverseValues(K key, FDate from, FDate to);

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
