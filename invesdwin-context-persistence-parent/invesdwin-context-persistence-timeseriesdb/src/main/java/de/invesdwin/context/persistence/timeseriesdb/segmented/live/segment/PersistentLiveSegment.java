package de.invesdwin.context.persistence.timeseriesdb.segmented.live.segment;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.ezdb.table.range.ADelegateRangeTable;
import de.invesdwin.context.persistence.timeseriesdb.ITimeSeriesDBInternals;
import de.invesdwin.context.persistence.timeseriesdb.IncompleteUpdateRetryableException;
import de.invesdwin.context.persistence.timeseriesdb.segmented.ISegmentedTimeSeriesDBInternals;
import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentStatus;
import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentedKey;
import de.invesdwin.context.persistence.timeseriesdb.storage.ISkipFileFunction;
import de.invesdwin.context.persistence.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.context.persistence.timeseriesdb.updater.progress.IUpdateProgress;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.lock.Locks;
import de.invesdwin.util.concurrent.lock.disabled.DisabledLock;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.range.TimeRange;

@NotThreadSafe
public class PersistentLiveSegment<K, V> implements ILiveSegment<K, V> {

    private final SegmentedKey<K> segmentedKey;
    private final ISegmentedTimeSeriesDBInternals<K, V> historicalSegmentTable;
    private final ITimeSeriesDBInternals<SegmentedKey<K>, V> table;
    private final String hashKey;
    private boolean empty = true;

    public PersistentLiveSegment(final SegmentedKey<K> segmentedKey,
            final ISegmentedTimeSeriesDBInternals<K, V> historicalSegmentTable) {
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
            throw new IllegalStateException("Existing " + SegmentStatus.class.getSimpleName() + " [" + existingStatus
                    + "] should be " + SegmentStatus.INITIALIZING + ": " + segmentedKey);
        }
    }

    @Override
    public V getFirstValue() {
        return table.getLatestValue(segmentedKey, FDates.MIN_DATE);
    }

    @Override
    public FDate getFirstValueKey() {
        return table.getLatestValueKey(segmentedKey, FDates.MIN_DATE);
    }

    @Override
    public V getLastValue() {
        return table.getLatestValue(segmentedKey, FDates.MAX_DATE);
    }

    @Override
    public FDate getLastValueKey() {
        return table.getLatestValueKey(segmentedKey, FDates.MAX_DATE);
    }

    @Override
    public SegmentedKey<K> getSegmentedKey() {
        return segmentedKey;
    }

    @Override
    public ICloseableIterable<V> rangeValues(final FDate from, final FDate to, final ILock readLock,
            final ISkipFileFunction skipFileFunction) {
        return new ICloseableIterable<V>() {
            @Override
            public ICloseableIterator<V> iterator() {
                final ILock compositeReadLock = Locks.newCompositeLock(readLock,
                        table.getTableLock(segmentedKey).readLock());
                return table.getLookupTableCache(segmentedKey)
                        .readRangeValues(from, to, compositeReadLock, skipFileFunction);
            }
        };
    }

    @Override
    public ICloseableIterable<V> rangeReverseValues(final FDate from, final FDate to, final ILock readLock,
            final ISkipFileFunction skipFileFunction) {
        return new ICloseableIterable<V>() {
            @Override
            public ICloseableIterator<V> iterator() {
                final ILock compositeReadLock = Locks.newCompositeLock(readLock,
                        table.getTableLock(segmentedKey).readLock());
                return table.getLookupTableCache(segmentedKey)
                        .readRangeValuesReverse(from, to, compositeReadLock, skipFileFunction);
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
    public V getLatestValue(final long index) {
        return table.getLatestValue(segmentedKey, index);
    }

    @Override
    public long getLatestValueIndex(final FDate date) {
        return table.getLatestValueIndex(segmentedKey, date);
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
    public boolean putNextLiveValue(final FDate nextLiveStartTime, final FDate nextLiveEndTimeKey,
            final V nextLiveValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long size() {
        return table.size(segmentedKey);
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
            protected FDate extractStartTime(final V element) {
                return historicalSegmentTable.extractStartTime(element);
            }

            @Override
            protected FDate extractEndTime(final V element) {
                return historicalSegmentTable.extractEndTime(element);
            }

            @Override
            protected void onElement(final IUpdateProgress<SegmentedKey<K>, V> updateProgress) {}

            @Override
            protected void onFlush(final int flushIndex, final IUpdateProgress<SegmentedKey<K>, V> updateProgress) {}

            @Override
            protected boolean shouldRedoLastFile() {
                return false;
            }

            @Override
            public Percent getProgress(final FDate minTime, final FDate maxTime) {
                return null;
            }
        };
        try {
            Assertions.checkTrue(updater.update());
        } catch (final IncompleteUpdateRetryableException e) {
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
                        segmentedKey.getSegment().getTo(), DisabledLock.INSTANCE, null);
                historicalSegmentTable.getSegmentedLookupTableCache(segmentedKey.getKey())
                        .onSegmentCompleted(segmentedKey, rangeValues);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(final Class<T> type) {
        if (type.isAssignableFrom(getClass())) {
            return (T) this;
        } else {
            return null;
        }
    }

}
