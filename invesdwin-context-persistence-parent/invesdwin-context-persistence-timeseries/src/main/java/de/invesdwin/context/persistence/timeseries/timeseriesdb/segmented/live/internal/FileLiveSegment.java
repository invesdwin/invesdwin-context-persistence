package de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.live.internal;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseries.timeseriesdb.HeapSerializingCollection;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.SerializingCollection;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.ASegmentedTimeSeriesStorageCache;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.SegmentedKey;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.live.ALiveSegmentedTimeSeriesDB;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.storage.ISkipFileFunction;
import de.invesdwin.util.collections.iterable.ATimeRangeSkippingIterable;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.IBufferingIterator;
import de.invesdwin.util.concurrent.lock.disabled.DisabledLock;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.description.TextDescription;
import de.invesdwin.util.time.date.FDate;
import ezdb.serde.Serde;

@NotThreadSafe
public class FileLiveSegment<K, V> implements ILiveSegment<K, V> {

    private final SegmentedKey<K> segmentedKey;
    private final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable;
    private SerializingCollection<V> values;
    @GuardedBy("this")
    private boolean needsFlush;
    private FDate firstValueKey;
    private final IBufferingIterator<V> firstValue = new BufferingIterator<>();
    private FDate lastValueKey;
    private final IBufferingIterator<V> lastValue = new BufferingIterator<>();
    private File file;

    public FileLiveSegment(final SegmentedKey<K> segmentedKey,
            final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable) {
        this.segmentedKey = segmentedKey;
        this.historicalSegmentTable = historicalSegmentTable;
    }

    private SerializingCollection<V> newSerializingCollection() {
        final File file = getFile();
        Files.deleteQuietly(file);
        try {
            Files.forceMkdirParent(file);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        final TextDescription name = new TextDescription("%s[%s]: newSerializingCollection()",
                FileLiveSegment.class.getSimpleName(), segmentedKey);
        return new SerializingCollection<V>(name, file, false) {
            @Override
            protected Serde<V> newSerde() {
                return historicalSegmentTable.newValueSerde();
            }

            @Override
            protected Integer getFixedLength() {
                return historicalSegmentTable.newFixedLength();
            }

            @Override
            protected InputStream newFileInputStream(final File file) throws IOException {
                throw new UnsupportedOperationException("use getFlushedValues() instead");
            }
        };
    }

    private File getFile() {
        if (file == null) {
            file = new File(historicalSegmentTable.getDirectory(),
                    historicalSegmentTable.hashKeyToString(segmentedKey).replace("/", "_").replace("\\", "_") + "_"
                            + "inProgress.data");
        }
        return file;
    }

    @Override
    public V getFirstValue() {
        return firstValue.getHead();
    }

    @Override
    public V getLastValue() {
        return lastValue.getTail();
    }

    @Override
    public SegmentedKey<K> getSegmentedKey() {
        return segmentedKey;
    }

    @Override
    public ICloseableIterable<V> rangeValues(final FDate from, final FDate to, final Lock readLock,
            final ISkipFileFunction skipFileFunction) {
        //we expect the read lock to be already locked from the outside
        if (values == null || from != null && to != null && from.isAfterNotNullSafe(to)) {
            return EmptyCloseableIterable.getInstance();
        }
        if (from != null && !lastValue.isEmpty() && from.isAfterOrEqualToNotNullSafe(lastValueKey)) {
            if (from.isAfterNotNullSafe(lastValueKey)) {
                return EmptyCloseableIterable.getInstance();
            } else {
                return lastValue.snapshot();
            }
        }
        if (to != null && !firstValue.isEmpty() && to.isBeforeOrEqualToNotNullSafe(firstValueKey)) {
            if (to.isBeforeNotNullSafe(firstValueKey)) {
                return EmptyCloseableIterable.getInstance();
            } else {
                return firstValue.snapshot();
            }
        }
        return new ATimeRangeSkippingIterable<V>(from, to, getFlushedValues()) {

            @Override
            protected FDate extractEndTime(final V element) {
                return historicalSegmentTable.extractEndTime(element);
            }

            @Override
            protected boolean isReverse() {
                return false;
            }

            @Override
            protected String getName() {
                return "FileLiveSegment rangeValues";
            }
        };
    }

    @Override
    public ICloseableIterable<V> rangeReverseValues(final FDate from, final FDate to, final Lock readLock,
            final ISkipFileFunction skipFileFunction) {
        //we expect the read lock to be already locked from the outside
        if (values == null || from != null && to != null && from.isBeforeNotNullSafe(to)) {
            return EmptyCloseableIterable.getInstance();
        }
        if (from != null && !firstValue.isEmpty() && from.isBeforeOrEqualToNotNullSafe(firstValueKey)) {
            if (from.isBeforeNotNullSafe(firstValueKey)) {
                return EmptyCloseableIterable.getInstance();
            } else {
                return firstValue.snapshot();
            }
        }
        if (to != null && !lastValue.isEmpty() && to.isAfterOrEqualToNotNullSafe(lastValueKey)) {
            if (to.isAfterNotNullSafe(lastValueKey)) {
                return EmptyCloseableIterable.getInstance();
            } else {
                return lastValue.snapshot();
            }
        }
        return new ATimeRangeSkippingIterable<V>(from, to, getFlushedValues().reverseIterable()) {

            @Override
            protected FDate extractEndTime(final V element) {
                return historicalSegmentTable.extractEndTime(element);
            }

            @Override
            protected boolean isReverse() {
                return true;
            }

            @Override
            protected String getName() {
                return "FileLiveSegment rangeReverseValues";
            }
        };
    }

    @Override
    public void putNextLiveValue(final FDate nextLiveKey, final V nextLiveValue) {
        if (!lastValue.isEmpty() && lastValueKey.isAfter(nextLiveKey)) {
            throw new IllegalStateException(segmentedKey + ": nextLiveKey [" + nextLiveKey
                    + "] should be after or equal to lastLiveKey [" + lastValueKey + "]");
        }
        synchronized (this) {
            if (values == null) {
                values = newSerializingCollection();
            }
            values.add(nextLiveValue);
            needsFlush = true;
        }
        if (firstValue.isEmpty() || firstValueKey.equalsNotNullSafe(nextLiveKey)) {
            firstValue.add(nextLiveValue);
            firstValueKey = nextLiveKey;
        }
        if (!lastValue.isEmpty() && !lastValueKey.equalsNotNullSafe(nextLiveKey)) {
            lastValue.clear();
        }
        lastValue.add(nextLiveValue);
        lastValueKey = nextLiveKey;
    }

    @Override
    public V getNextValue(final FDate date, final int shiftForwardUnits) {
        if (!lastValue.isEmpty() && (date == null || date.isAfterOrEqualToNotNullSafe(lastValueKey))) {
            //we always return the last last value
            return lastValue.getTail();
        }
        if (!firstValue.isEmpty() && (date != null && date.isBeforeNotNullSafe(firstValueKey))) {
            //we always return the first first value
            return firstValue.getHead();
        }
        V nextValue = null;
        try (ICloseableIterator<V> rangeValues = rangeValues(date, null, DisabledLock.INSTANCE, null).iterator()) {
            for (int i = 0; i < shiftForwardUnits; i++) {
                nextValue = rangeValues.next();
            }
        } catch (final NoSuchElementException e) {
            //ignore
        }
        if (nextValue != null) {
            return nextValue;
        } else {
            return lastValue.getTail();
        }
    }

    @Override
    public V getLatestValue(final FDate date) {
        if (!lastValue.isEmpty() && (date == null || date.isAfterOrEqualToNotNullSafe(lastValueKey))) {
            //we always return the last last value
            return lastValue.getTail();
        }
        if (!firstValue.isEmpty() && date != null && date.isBeforeOrEqualToNotNullSafe(firstValueKey)) {
            //we always return the first first value
            return firstValue.getHead();
        }
        V nextValue = null;
        try (ICloseableIterator<V> reverse = rangeReverseValues(date, null, DisabledLock.INSTANCE, null).iterator()) {
            nextValue = reverse.next();
        } catch (final NoSuchElementException e) {
            //ignore
        }
        if (nextValue != null) {
            return nextValue;
        } else {
            return firstValue.getHead();
        }
    }

    @Override
    public boolean isEmpty() {
        return values == null || values.isEmpty();
    }

    @Override
    public void close() {
        synchronized (this) {
            if (values != null) {
                values.close();
                values.clear();
                values = null;
            }
            needsFlush = false;
        }
        firstValue.clear();
        firstValueKey = null;
        lastValue.clear();
        lastValueKey = null;
    }

    @Override
    public void convertLiveSegmentToHistorical() {
        synchronized (this) {
            values.close();
            needsFlush = false;
        }
        final ASegmentedTimeSeriesStorageCache<K, V> lookupTableCache = historicalSegmentTable
                .getLookupTableCache(getSegmentedKey().getKey());
        final boolean initialized = lookupTableCache.maybeInitSegment(getSegmentedKey(),
                new Function<SegmentedKey<K>, ICloseableIterable<? extends V>>() {
                    @Override
                    public ICloseableIterable<? extends V> apply(final SegmentedKey<K> t) {
                        return rangeValues(t.getSegment().getFrom(), t.getSegment().getTo(), DisabledLock.INSTANCE,
                                null);
                    }
                });
        if (!initialized) {
            throw new IllegalStateException("true expected");
        }
    }

    private HeapSerializingCollection<V> getFlushedValues() {
        synchronized (this) {
            if (needsFlush) {
                values.flush();
                needsFlush = false;
            }
        }
        try {
            final TextDescription name = new TextDescription("%s[%s]: getFlushedValues()",
                    FileLiveSegment.class.getSimpleName(), segmentedKey);
            final byte[] bytes = Files.readFileToByteArray(values.getFile());
            return new HeapSerializingCollection<V>(name, bytes) {
                @Override
                protected Serde<V> newSerde() {
                    return historicalSegmentTable.newValueSerde();
                }

                @Override
                protected Integer getFixedLength() {
                    return historicalSegmentTable.newFixedLength();
                }
            };
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public FDate getFirstValueKey() {
        return firstValueKey;
    }

    @Override
    public FDate getLastValueKey() {
        return lastValueKey;
    }

}
