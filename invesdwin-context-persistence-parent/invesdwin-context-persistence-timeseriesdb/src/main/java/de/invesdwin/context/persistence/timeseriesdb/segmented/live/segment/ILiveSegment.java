package de.invesdwin.context.persistence.timeseriesdb.segmented.live.segment;

import java.io.Closeable;

import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentedKey;
import de.invesdwin.context.persistence.timeseriesdb.storage.ISkipFileFunction;
import de.invesdwin.norva.beanpath.spi.IUnwrap;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.time.date.FDate;

public interface ILiveSegment<K, V> extends Closeable, IUnwrap {

    V getFirstValue();

    V getLastValue();

    SegmentedKey<K> getSegmentedKey();

    ICloseableIterable<V> rangeValues(FDate from, FDate to, ILock readLock, ISkipFileFunction skipFileFunction);

    ICloseableIterable<V> rangeReverseValues(FDate from, FDate to, ILock readLock, ISkipFileFunction skipFileFunction);

    boolean putNextLiveValue(FDate nextLiveStartTime, FDate nextLiveEndTimeKey, V nextLiveValue);

    V getNextValue(FDate date, int shiftForwardUnits);

    V getLatestValue(FDate date);

    V getLatestValue(long index);

    long getLatestValueIndex(FDate date);

    boolean isEmpty();

    void convertLiveSegmentToHistorical();

    FDate getFirstValueKey();

    FDate getLastValueKey();

    long size();

    @Override
    void close();

}
