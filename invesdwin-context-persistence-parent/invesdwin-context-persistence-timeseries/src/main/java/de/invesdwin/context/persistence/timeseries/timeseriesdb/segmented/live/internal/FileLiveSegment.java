package de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.live.internal;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.FileUtils;

import de.invesdwin.context.persistence.timeseries.timeseriesdb.HeapSerializingCollection;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.SerializingCollection;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.ASegmentedTimeSeriesStorageCache;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.SegmentedKey;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.segmented.live.ALiveSegmentedTimeSeriesDB;
import de.invesdwin.util.collections.iterable.ASkippingIterable;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.time.fdate.FDate;
import ezdb.serde.Serde;

@NotThreadSafe
public class FileLiveSegment<K, V> implements ILiveSegment<K, V> {

    private final SegmentedKey<K> segmentedKey;
    private final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable;
    private SerializingCollection<V> values;
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
            FileUtils.forceMkdirParent(file);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        FileUtils.deleteQuietly(file);
        return new SerializingCollection<V>(file, false) {
            @Override
            protected Serde<V> newSerde() {
                return historicalSegmentTable.newValueSerde();
            }

            @Override
            protected Integer getFixedLength() {
                return historicalSegmentTable.newFixedLength();
            }

            @Override
            protected InputStream newFileInputStream(final File file) throws FileNotFoundException {
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

    @Override
    public ICloseableIterable<V> rangeValues(final FDate from, final FDate to) {
        if (values == null) {
            return EmptyCloseableIterable.getInstance();
        }
        if (from == null && to == null) {
            return getFlushedValues();
        } else if (from != null && to != null) {
            return new ASkippingIterable<V>(getFlushedValues()) {
                @Override
                protected boolean skip(final V element) {
                    final FDate time = historicalSegmentTable.extractTime(element);
                    if (time.isBeforeNotNullSafe(from)) {
                        return true;
                    }
                    if (time.isAfterNotNullSafe(to)) {
                        throw new FastNoSuchElementException("LiveSegment rangeValues end reached");
                    }
                    return false;
                }
            };
        } else if (from != null) {
            return new ASkippingIterable<V>(getFlushedValues()) {
                @Override
                protected boolean skip(final V element) {
                    final FDate time = historicalSegmentTable.extractTime(element);
                    if (time.isBeforeNotNullSafe(from)) {
                        return true;
                    }
                    return false;
                }
            };
        } else if (to != null) {
            return new ASkippingIterable<V>(getFlushedValues()) {
                @Override
                protected boolean skip(final V element) {
                    final FDate time = historicalSegmentTable.extractTime(element);
                    if (time.isAfterNotNullSafe(to)) {
                        throw new FastNoSuchElementException("LiveSegment rangeValues end reached");
                    }
                    return false;
                }
            };
        } else {
            throw new IllegalStateException("missing another condition?");
        }
    }

    @Override
    public ICloseableIterable<V> rangeReverseValues(final FDate from, final FDate to) {
        if (values == null) {
            return EmptyCloseableIterable.getInstance();
        }
        if (from == null && to == null) {
            return getFlushedValues().reverseIterable();
        } else if (from != null && to != null) {
            return new ASkippingIterable<V>(getFlushedValues().reverseIterable()) {
                @Override
                protected boolean skip(final V element) {
                    final FDate time = historicalSegmentTable.extractTime(element);
                    if (time.isAfterNotNullSafe(from)) {
                        return true;
                    }
                    if (time.isBeforeNotNullSafe(to)) {
                        throw new FastNoSuchElementException("LiveSegment rangeValues end reached");
                    }
                    return false;
                }
            };
        } else if (from != null) {
            return new ASkippingIterable<V>(getFlushedValues().reverseIterable()) {
                @Override
                protected boolean skip(final V element) {
                    final FDate time = historicalSegmentTable.extractTime(element);
                    if (time.isAfterNotNullSafe(from)) {
                        return true;
                    }
                    return false;
                }
            };
        } else if (to != null) {
            return new ASkippingIterable<V>(getFlushedValues().reverseIterable()) {
                @Override
                protected boolean skip(final V element) {
                    final FDate time = historicalSegmentTable.extractTime(element);
                    if (time.isBeforeNotNullSafe(to)) {
                        throw new FastNoSuchElementException("LiveSegment rangeValues end reached");
                    }
                    return false;
                }
            };
        } else {
            throw new IllegalStateException("missing another condition?");
        }
    }

    @Override
    public void putNextLiveValue(final FDate nextLiveKey, final V nextLiveValue) {
        if (values == null) {
            values = newSerializingCollection();
        }
        values.add(nextLiveValue);
        needsFlush = true;
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
        try (ICloseableIterator<V> rangeValues = rangeValues(date, null).iterator()) {
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
        try (ICloseableIterator<V> reverse = rangeReverseValues(date, null).iterator()) {
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
        if (values != null) {
            values.close();
            values.clear();
            values = null;
        }
        firstValue = null;
        firstValueKey = null;
        lastValue = null;
        lastValueKey = null;
        needsFlush = false;
    }

    @Override
    public void convertLiveSegmentToHistorical() {
        values.close();
        needsFlush = false;
        final ASegmentedTimeSeriesStorageCache<K, V> lookupTableCache = historicalSegmentTable
                .getLookupTableCache(getSegmentedKey().getKey());
        final boolean initialized = lookupTableCache.maybeInitSegment(getSegmentedKey(),
                new Function<SegmentedKey<K>, ICloseableIterable<? extends V>>() {
                    @Override
                    public ICloseableIterable<? extends V> apply(final SegmentedKey<K> t) {
                        return rangeValues(t.getSegment().getFrom(), t.getSegment().getTo());
                    }
                });
        if (!initialized) {
            throw new IllegalStateException("true expected");
        }
    }

    private HeapSerializingCollection<V> getFlushedValues() {
        if (needsFlush) {
            values.flush();
            needsFlush = false;
        }
        try {
            final byte[] bytes = FileUtils.readFileToByteArray(values.getFile());
            return new HeapSerializingCollection<V>(bytes) {
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
