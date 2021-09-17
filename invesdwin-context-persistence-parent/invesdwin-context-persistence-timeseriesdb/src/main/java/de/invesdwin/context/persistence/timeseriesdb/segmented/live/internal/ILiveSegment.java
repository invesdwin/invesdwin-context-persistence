package de.invesdwin.context.persistence.timeseriesdb.segmented.live.internal;

import java.io.Closeable;
import java.util.concurrent.locks.Lock;

import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentedKey;
import de.invesdwin.context.persistence.timeseriesdb.storage.ISkipFileFunction;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.time.date.FDate;

public interface ILiveSegment<K, V> extends Closeable {

    V getFirstValue();

    V getLastValue();

    SegmentedKey<K> getSegmentedKey();

    ICloseableIterable<V> rangeValues(FDate from, FDate to, Lock readLock, ISkipFileFunction skipFileFunction);

    ICloseableIterable<V> rangeReverseValues(FDate from, FDate to, Lock readLock, ISkipFileFunction skipFileFunction);

    void putNextLiveValue(FDate nextLiveKey, V nextLiveValue);

    V getNextValue(FDate date, int shiftForwardUnits);

    V getLatestValue(FDate date);

    boolean isEmpty();

    void convertLiveSegmentToHistorical();

    FDate getFirstValueKey();

    FDate getLastValueKey();

    @Override
    void close();

}
