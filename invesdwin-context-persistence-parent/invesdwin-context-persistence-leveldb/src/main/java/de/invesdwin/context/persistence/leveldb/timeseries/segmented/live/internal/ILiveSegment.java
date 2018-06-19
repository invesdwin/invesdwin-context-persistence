package de.invesdwin.context.persistence.leveldb.timeseries.segmented.live.internal;

import java.io.Closeable;

import de.invesdwin.context.persistence.leveldb.timeseries.segmented.SegmentedKey;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.time.fdate.FDate;

public interface ILiveSegment<K, V> extends Closeable {

    V getFirstValue();

    V getLastValue();

    SegmentedKey<K> getSegmentedKey();

    ICloseableIterable<V> rangeValues(FDate from, FDate to);

    ICloseableIterable<V> rangeReverseValues(FDate from, FDate to);

    void putNextLiveValue(FDate nextLiveKey, V nextLiveValue);

    V getNextValue(FDate date, int shiftForwardUnits);

    V getLatestValue(FDate date);

    boolean isEmpty();

    void convertLiveSegmentToHistorical();

}
