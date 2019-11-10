package de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.live.internal;

import java.io.OutputStream;
import java.util.concurrent.locks.Lock;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.ezdb.ADelegateRangeTable;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.IncompleteUpdateFoundException;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.ASegmentedTimeSeriesDB;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.SegmentStatus;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.SegmentedKey;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.live.ALiveSegmentedTimeSeriesDB;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.concurrent.lock.Locks;
import de.invesdwin.util.concurrent.lock.disabled.DisabledLock;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.range.TimeRange;
import net.jpountz.lz4.LZ4BlockOutputStream;

@NotThreadSafe
public class PersistentLiveSegment<K, V> implements ILiveSegment<K, V> {

    private final SegmentedKey<K> segmentedKey;
    private final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable;
    private final ASegmentedTimeSeriesDB<K, V>.SegmentedTable table;
    private boolean empty = true;
    private final String hashKey;

    public PersistentLiveSegment(final SegmentedKey<K> segmentedKey,
            final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable) {
        this.segmentedKey = segmentedKey;
        this.historicalSegmentTable = historicalSegmentTable;
        this.table = historicalSegmentTable.getSegmentedTable();
        this.hashKey = historicalSegmentTable.hashKeyToString(segmentedKey.getKey());

        final ADelegateRangeTable<String, TimeRange, SegmentStatus> segmentStatusTable = historicalSegmentTable
                .getStorage()
                .getSegmentStatusTable();
        final SegmentStatus existingStatus = segmentStatusTable.get(hashKey, segmentedKey.getSegment());
        if (existingStatus == SegmentStatus.INITIALIZING) {
            //cleanup initially
            this.table.deleteRange(segmentedKey);
        } else if (existingStatus != null) {
            throw UnknownArgumentException.newInstance(SegmentStatus.class, existingStatus);
        }
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
    public ICloseableIterable<V> rangeValues(final FDate from, final FDate to, final Lock readLock) {
        return new ICloseableIterable<V>() {
            @Override
            public ICloseableIterator<V> iterator() {
                final Lock compositeReadLock = Locks.newCompositeLock(readLock,
                        table.getTableLock(segmentedKey).readLock());
                return table.getLookupTableCache(segmentedKey).readRangeValues(from, to, compositeReadLock);
            }
        };
    }

    @Override
    public ICloseableIterable<V> rangeReverseValues(final FDate from, final FDate to, final Lock readLock) {
        return new ICloseableIterable<V>() {
            @Override
            public ICloseableIterator<V> iterator() {
                final Lock compositeReadLock = Locks.newCompositeLock(readLock,
                        table.getTableLock(segmentedKey).readLock());
                return table.getLookupTableCache(segmentedKey).readRangeValuesReverse(from, to, compositeReadLock);
            }
        };
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
                return ATimeSeriesUpdater.DEFAULT_SHOULD_WRITE_IN_PARALLEL;
            }

            @Override
            protected LZ4BlockOutputStream newCompressor(final OutputStream out) {
                return historicalSegmentTable.newCompressor(out);
            }

            @Override
            public Percent getProgress() {
                return null;
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
                final ICloseableIterable<V> rangeValues = rangeValues(segmentedKey.getSegment().getFrom(),
                        segmentedKey.getSegment().getTo(), DisabledLock.INSTANCE);
                historicalSegmentTable.getLookupTableCache(segmentedKey.getKey())
                        .onSegmentCompleted(segmentedKey, rangeValues);
            }
        }
    }

}
