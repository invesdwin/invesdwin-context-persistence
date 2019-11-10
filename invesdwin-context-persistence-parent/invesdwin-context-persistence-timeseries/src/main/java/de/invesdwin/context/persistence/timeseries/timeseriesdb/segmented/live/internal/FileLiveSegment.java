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
import de.invesdwin.util.collections.iterable.ATimeRangeSkippingIterable;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.SingleValueIterable;
import de.invesdwin.util.concurrent.lock.disabled.DisabledLock;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.description.TextDescription;
import de.invesdwin.util.time.fdate.FDate;
import ezdb.serde.Serde;

@NotThreadSafe
public class FileLiveSegment<K, V> implements ILiveSegment<K, V> {

    private final SegmentedKey<K> segmentedKey;
    private final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable;
    private SerializingCollection<V> values;
    @GuardedBy("this")
    private boolean needsFlush;
    private FDate firstValueKey;
    private V firstValue;
    private FDate lastValueKey;
    private V lastValue;

    public FileLiveSegment(final SegmentedKey<K> segmentedKey,
            final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable) {
        this.segmentedKey = segmentedKey;
        this.historicalSegmentTable = historicalSegmentTable;
    }

    private SerializingCollection<V> newSerializingCollection() {
        final File file = new File(
                new File(historicalSegmentTable.getDirectory(), historicalSegmentTable.hashKeyToString(segmentedKey)),
                "inProgress.data");
        try {
            Files.forceMkdirParent(file);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        Files.deleteQuietly(file);
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

    @Override
    public V getFirstValue() {
        return firstValue;
    }

    @Override
    public V getLastValue() {
        return lastValue;
    }

    @Override
    public SegmentedKey<K> getSegmentedKey() {
        return segmentedKey;
    }

    //CHECKSTYLE:OFF
    @Override
    public ICloseableIterable<V> rangeValues(final FDate from, final FDate to, final Lock readLock) {
        //CHECKSTYLE:ON
        readLock.lock();
        try {
            if (values == null || from != null && to != null && from.isAfterNotNullSafe(to)) {
                return EmptyCloseableIterable.getInstance();
            }
            if (from != null && lastValue != null && from.isAfterOrEqualToNotNullSafe(lastValueKey)) {
                if (from.isAfterNotNullSafe(lastValueKey)) {
                    return EmptyCloseableIterable.getInstance();
                } else {
                    return new SingleValueIterable<V>(lastValue);
                }
            }
            if (to != null && firstValue != null && to.isBeforeOrEqualToNotNullSafe(firstValueKey)) {
                if (to.isBeforeNotNullSafe(firstValueKey)) {
                    return EmptyCloseableIterable.getInstance();
                } else {
                    return new SingleValueIterable<V>(firstValue);
                }
            }
            return new ATimeRangeSkippingIterable<V>(from, to, getFlushedValues()) {

                @Override
                protected FDate extractTime(final V element) {
                    return historicalSegmentTable.extractTime(element);
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
        } finally {
            readLock.unlock();
        }
    }

    //CHECKSTYLE:OFF
    @Override
    public ICloseableIterable<V> rangeReverseValues(final FDate from, final FDate to, final Lock readLock) {
        //CHECKSTYLE:ON
        readLock.lock();
        try {
            if (values == null || from != null && to != null && from.isBeforeNotNullSafe(to)) {
                return EmptyCloseableIterable.getInstance();
            }
            if (from != null && firstValue != null && from.isBeforeOrEqualToNotNullSafe(firstValueKey)) {
                if (from.isBeforeNotNullSafe(firstValueKey)) {
                    return EmptyCloseableIterable.getInstance();
                } else {
                    return new SingleValueIterable<V>(firstValue);
                }
            }
            if (to != null && lastValue != null && to.isAfterOrEqualToNotNullSafe(lastValueKey)) {
                if (to.isAfterNotNullSafe(lastValueKey)) {
                    return EmptyCloseableIterable.getInstance();
                } else {
                    return new SingleValueIterable<V>(lastValue);
                }
            }
            return new ATimeRangeSkippingIterable<V>(from, to, getFlushedValues().reverseIterable()) {

                @Override
                protected FDate extractTime(final V element) {
                    return historicalSegmentTable.extractTime(element);
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
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void putNextLiveValue(final FDate nextLiveKey, final V nextLiveValue) {
        synchronized (this) {
            if (values == null) {
                values = newSerializingCollection();
            }
            values.add(nextLiveValue);
            needsFlush = true;
        }
        if (firstValue == null) {
            firstValue = nextLiveValue;
            firstValueKey = nextLiveKey;
        }
        lastValue = nextLiveValue;
        lastValueKey = nextLiveKey;
    }

    @Override
    public V getNextValue(final FDate date, final int shiftForwardUnits) {
        if (lastValue != null && (date == null || date.isAfterOrEqualToNotNullSafe(lastValueKey))) {
            return lastValue;
        }
        if (firstValue != null && (date != null && date.isBeforeNotNullSafe(firstValueKey))) {
            return firstValue;
        }
        V nextValue = null;
        try (ICloseableIterator<V> rangeValues = rangeValues(date, null, DisabledLock.INSTANCE).iterator()) {
            for (int i = 0; i < shiftForwardUnits; i++) {
                nextValue = rangeValues.next();
            }
        } catch (final NoSuchElementException e) {
            //ignore
        }
        if (nextValue != null) {
            return nextValue;
        } else {
            return lastValue;
        }
    }

    @Override
    public V getLatestValue(final FDate date) {
        if (lastValue != null && (date == null || date.isAfterOrEqualToNotNullSafe(lastValueKey))) {
            return lastValue;
        }
        if (firstValue != null && (date != null && date.isBeforeOrEqualToNotNullSafe(firstValueKey))) {
            return firstValue;
        }
        V nextValue = null;
        try (ICloseableIterator<V> reverse = rangeReverseValues(date, null, DisabledLock.INSTANCE).iterator()) {
            nextValue = reverse.next();
        } catch (final NoSuchElementException e) {
            //ignore
        }
        if (nextValue != null) {
            return nextValue;
        } else {
            return firstValue;
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
        firstValue = null;
        firstValueKey = null;
        lastValue = null;
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
                        return rangeValues(t.getSegment().getFrom(), t.getSegment().getTo(), DisabledLock.INSTANCE);
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
