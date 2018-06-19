package de.invesdwin.context.persistence.leveldb.timeseries.segmented.live.internal;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.leveldb.timeseries.ATimeSeriesDB;
import de.invesdwin.context.persistence.leveldb.timeseries.segmented.SegmentedKey;
import de.invesdwin.context.persistence.leveldb.timeseries.segmented.live.ALiveSegmentedTimeSeriesDB;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.time.fdate.FDate;
import ezdb.serde.Serde;

@NotThreadSafe
public class PersistentLiveSegment<K, V> implements ILiveSegment<K, V> {

    private final SegmentedKey<K> segmentedKey;
    private final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable;
    private final ATimeSeriesDB<K, V> table;

    public PersistentLiveSegment(final SegmentedKey<K> segmentedKey,
            final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable) {
        this.segmentedKey = segmentedKey;
        this.historicalSegmentTable = historicalSegmentTable;
        this.table = new ATimeSeriesDB<K, V>(PersistentLiveSegment.class.getSimpleName()) {

            @Override
            protected Integer newFixedLength() {
                return historicalSegmentTable.newFixedLength();
            }

            @Override
            protected Serde<V> newValueSerde() {
                return historicalSegmentTable.newValueSerde();
            }

            @Override
            protected FDate extractTime(final V value) {
                return historicalSegmentTable.extractTime(value);
            }

            @Override
            protected String hashKeyToString(final K key) {
                return historicalSegmentTable.hashKeyToString(key);
            }

            @Override
            protected File getBaseDirectory() {
                return historicalSegmentTable.getStorage()
                        .newDataDirectory(historicalSegmentTable.hashKeyToString(segmentedKey.getKey()));
            }
        };
    }

    @Override
    public V getFirstValue() {
        return table.getLatestValue(segmentedKey.getKey(), FDate.MIN_DATE);
    }

    @Override
    public FDate getFirstValueKey() {
        return table.getLatestValueKey(segmentedKey.getKey(), FDate.MIN_DATE);
    }

    @Override
    public V getLastValue() {
        return table.getLatestValue(segmentedKey.getKey(), FDate.MAX_DATE);
    }

    @Override
    public FDate getLastValueKey() {
        return table.getLatestValueKey(segmentedKey.getKey(), FDate.MAX_DATE);
    }

    @Override
    public SegmentedKey<K> getSegmentedKey() {
        return segmentedKey;
    }

    @Override
    public ICloseableIterable<V> rangeValues(final FDate from, final FDate to) {
        return table.rangeValues(segmentedKey.getKey(), from, to);
    }

    @Override
    public ICloseableIterable<V> rangeReverseValues(final FDate from, final FDate to) {
        return table.rangeReverseValues(segmentedKey.getKey(), from, to);
    }

    @Override
    public V getNextValue(final FDate date, final int shiftForwardUnits) {
        return table.getNextValue(segmentedKey.getKey(), date, shiftForwardUnits);
    }

    @Override
    public V getLatestValue(final FDate date) {
        return table.getLatestValue(segmentedKey.getKey(), date);
    }

    @Override
    public boolean isEmpty() {
        return table.isEmptyOrInconsistent(segmentedKey.getKey());
    }

    @Override
    public void close() {
        table.close();
    }

    @Deprecated
    @Override
    public void convertLiveSegmentToHistorical() {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    @Override
    public void putNextLiveValue(final FDate nextLiveKey, final V nextLiveValue) {
        throw new UnsupportedOperationException();
    }

    public void putNextLiveValues(final ICloseableIterable<V> memoryValues) {

    }

}
