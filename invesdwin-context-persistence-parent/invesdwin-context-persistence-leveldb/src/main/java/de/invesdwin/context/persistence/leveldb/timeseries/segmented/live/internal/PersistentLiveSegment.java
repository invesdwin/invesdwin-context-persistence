package de.invesdwin.context.persistence.leveldb.timeseries.segmented.live.internal;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.leveldb.ezdb.ADelegateRangeTable;
import de.invesdwin.context.persistence.leveldb.timeseries.ATimeSeriesUpdater;
import de.invesdwin.context.persistence.leveldb.timeseries.IncompleteUpdateFoundException;
import de.invesdwin.context.persistence.leveldb.timeseries.segmented.ASegmentedTimeSeriesDB;
import de.invesdwin.context.persistence.leveldb.timeseries.segmented.SegmentStatus;
import de.invesdwin.context.persistence.leveldb.timeseries.segmented.SegmentedKey;
import de.invesdwin.context.persistence.leveldb.timeseries.segmented.live.ALiveSegmentedTimeSeriesDB;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.range.TimeRange;

@NotThreadSafe
public class PersistentLiveSegment<K, V> implements ILiveSegment<K, V> {

    private final SegmentedKey<K> segmentedKey;
    private final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable;
    private final ASegmentedTimeSeriesDB<K, V>.SegmentedTable table;
    private boolean empty = true;
    private final String hashKey;;

    public PersistentLiveSegment(final SegmentedKey<K> segmentedKey,
            final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable) {
        this.segmentedKey = segmentedKey;
        this.historicalSegmentTable = historicalSegmentTable;
        this.table = historicalSegmentTable.getSegmentedTable();
        this.hashKey = historicalSegmentTable.hashKeyToString(segmentedKey.getKey());
    }

    @Override
    public V getFirstValue() {
        return table.getLatestValue(segmentedKey, FDate.MIN_DATE);
    }

    @Override
    public FDate getFirstValueKey() {
        return table.getLatestValueKey(segmentedKey, FDate.MIN_DATE);
    }

    @Override
    public V getLastValue() {
        return table.getLatestValue(segmentedKey, FDate.MAX_DATE);
    }

    @Override
    public FDate getLastValueKey() {
        return table.getLatestValueKey(segmentedKey, FDate.MAX_DATE);
    }

    @Override
    public SegmentedKey<K> getSegmentedKey() {
        return segmentedKey;
    }

    @Override
    public ICloseableIterable<V> rangeValues(final FDate from, final FDate to) {
        return table.rangeValues(segmentedKey, from, to);
    }

    @Override
    public ICloseableIterable<V> rangeReverseValues(final FDate from, final FDate to) {
        return table.rangeReverseValues(segmentedKey, from, to);
    }

    @Override
    public V getNextValue(final FDate date, final int shiftForwardUnits) {
        return table.getNextValue(segmentedKey, date, shiftForwardUnits);
    }

    @Override
    public V getLatestValue(final FDate date) {
        return table.getLatestValue(segmentedKey, date);
    }

    @Override
    public boolean isEmpty() {
        return empty;
    }

    @Override
    public void close() {}

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
        final ADelegateRangeTable<String, TimeRange, SegmentStatus> segmentStatusTable = historicalSegmentTable
                .getStorage()
                .getSegmentStatusTable();
        final SegmentStatus existingStatus = segmentStatusTable.get(hashKey, segmentedKey.getSegment());
        if (existingStatus == null) {
            segmentStatusTable.put(hashKey, segmentedKey.getSegment(), SegmentStatus.INITIALIZING);
        } else if (existingStatus != SegmentStatus.INITIALIZING) {
            throw UnknownArgumentException.newInstance(SegmentStatus.class, existingStatus);
        }
        final ATimeSeriesUpdater<SegmentedKey<K>, V> updater = new ATimeSeriesUpdater<SegmentedKey<K>, V>(segmentedKey,
                table) {
            @Override
            protected ICloseableIterable<? extends V> getSource(final FDate updateFrom) {
                return memoryValues;
            }

            @Override
            protected void onUpdateFinished(final Instant updateStart) {}

            @Override
            protected void onUpdateStart() {}

            @Override
            protected FDate extractTime(final V element) {
                return historicalSegmentTable.extractTime(element);
            }

            @Override
            protected FDate extractEndTime(final V element) {
                return historicalSegmentTable.extractEndTime(element);
            }

            @Override
            protected void onFlush(final int flushIndex, final Instant flushStart,
                    final ATimeSeriesUpdater<SegmentedKey<K>, V>.UpdateProgress updateProgress) {}

            @Override
            protected boolean shouldRedoLastFile() {
                return false;
            }

            @Override
            protected boolean shouldWriteInParallel() {
                return false;
            }
        };
        try {
            Assertions.checkTrue(updater.update());
        } catch (final IncompleteUpdateFoundException e) {
            throw new RuntimeException(e);
        }
        empty = false;
    }

    public void finish() {
        if (!isEmpty()) {
            final ADelegateRangeTable<String, TimeRange, SegmentStatus> segmentStatusTable = historicalSegmentTable
                    .getStorage()
                    .getSegmentStatusTable();
            final SegmentStatus existingStatus = segmentStatusTable.get(hashKey, segmentedKey.getSegment());
            if (existingStatus == SegmentStatus.INITIALIZING) {
                segmentStatusTable.put(hashKey, segmentedKey.getSegment(), SegmentStatus.COMPLETE);
            }
        }
    }

}
