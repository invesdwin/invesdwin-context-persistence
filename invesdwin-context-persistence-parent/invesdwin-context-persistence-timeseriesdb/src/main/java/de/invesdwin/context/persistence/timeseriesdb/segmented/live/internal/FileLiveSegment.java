package de.invesdwin.context.persistence.timeseriesdb.segmented.live.internal;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;

import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.persistence.timeseriesdb.SerializingCollection;
import de.invesdwin.context.persistence.timeseriesdb.buffer.ArrayFileBufferCacheResult;
import de.invesdwin.context.persistence.timeseriesdb.segmented.ASegmentedTimeSeriesStorageCache;
import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentedKey;
import de.invesdwin.context.persistence.timeseriesdb.segmented.live.ALiveSegmentedTimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.storage.ISkipFileFunction;
import de.invesdwin.util.collections.circular.CircularGenericArray;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterable;
import de.invesdwin.util.collections.iterable.FlatteningIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.IReverseCloseableIterable;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.IBufferingIterator;
import de.invesdwin.util.collections.iterable.skip.ATimeRangeSkippingIterable;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.concurrent.lock.disabled.DisabledLock;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.lang.string.description.TextDescription;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.pool.PooledFastByteArrayOutputStream;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class FileLiveSegment<K, V> implements ILiveSegment<K, V> {

    private static final boolean LARGE_COMPRESSOR = false;
    private static final int LAST_VALUE_HISTORY = 3;
    private static final Log LOG = new Log(FileLiveSegment.class);
    private final String hashKey;
    private final SegmentedKey<K> segmentedKey;
    private final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable;
    private final ICompressionFactory compressionFactory;
    private SerializingCollection<V> values;
    @GuardedBy("this")
    private boolean needsFlush;
    private FDate firstValueKey;
    private final IBufferingIterator<V> firstValue = new BufferingIterator<>();
    private final CircularGenericArray<LastValue<V>> lastValues = new CircularGenericArray<LastValue<V>>(
            LAST_VALUE_HISTORY);
    private File file;
    private WeakReference<ArrayFileBufferCacheResult<V>> inMemoryCacheHolder;

    public FileLiveSegment(final SegmentedKey<K> segmentedKey,
            final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable) {
        this.hashKey = historicalSegmentTable.hashKeyToString(segmentedKey);
        this.segmentedKey = segmentedKey;
        this.historicalSegmentTable = historicalSegmentTable;
        this.compressionFactory = historicalSegmentTable.getStorage().getCompressionFactory();
        for (int i = 0; i < LAST_VALUE_HISTORY; i++) {
            lastValues.add(new LastValue<>());
        }
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
            protected ISerde<V> newSerde() {
                return historicalSegmentTable.newValueSerde();
            }

            @Override
            protected Integer getFixedLength() {
                return historicalSegmentTable.newValueFixedLength();
            }

            @Override
            protected OutputStream newCompressor(final OutputStream out) {
                return compressionFactory.newCompressor(out, LARGE_COMPRESSOR);
            }

            @Override
            protected InputStream newDecompressor(final InputStream inputStream) {
                return compressionFactory.newDecompressor(inputStream);
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
                    Files.normalizePath(
                            historicalSegmentTable.hashKeyToString(segmentedKey).replace("/", "_").replace("\\", "_")
                                    + "_" + "inProgress.data"));
        }
        return file;
    }

    @Override
    public V getFirstValue() {
        return firstValue.getHead();
    }

    @Override
    public V getLastValue() {
        return lastValues.getReverse(0).values.getTail();
    }

    @Override
    public SegmentedKey<K> getSegmentedKey() {
        return segmentedKey;
    }

    //CHECKSTYLE:OFF
    @Override
    public ICloseableIterable<V> rangeValues(final FDate from, final FDate to, final Lock readLock,
            final ISkipFileFunction skipFileFunction) {
        //CHECKSTYLE:ON
        //we expect the read lock to be already locked from the outside
        if (values == null || from != null && to != null && from.isAfterNotNullSafe(to)) {
            return EmptyCloseableIterable.getInstance();
        }
        final LastValue<V> lastValue = lastValues.getReverse(0);
        if (from != null && !lastValue.values.isEmpty() && from.isAfterOrEqualToNotNullSafe(lastValue.key)) {
            return rangeValuesFromLastValue(from, lastValue);
        }
        for (int i = 1; i < LAST_VALUE_HISTORY; i++) {
            final LastValue<V> prevLastValue = lastValues.getReverse(i);
            if (prevLastValue.values.isEmpty()) {
                break;
            }
            if (from != null && from.isAfterOrEqualToNotNullSafe(prevLastValue.key)) {
                return rangeValuesFromPrevLastValue(from, to, i);
            }
        }
        if (to != null && !firstValue.isEmpty() && to.isBeforeOrEqualToNotNullSafe(firstValueKey)) {
            return rangeValuesToFirstValue(to);
        }
        return new SkippingRangeValues(from, to,
                getFlushedValues().iterable(historicalSegmentTable::extractEndTime, from, to));
    }

    private ICloseableIterable<V> rangeValuesFromLastValue(final FDate from, final LastValue<V> lastValue) {
        if (from.isAfterNotNullSafe(lastValue.key)) {
            return EmptyCloseableIterable.getInstance();
        } else {
            return lastValue.values.snapshot();
        }
    }

    @SuppressWarnings("resource")
    private ICloseableIterable<V> rangeValuesFromPrevLastValue(final FDate from, final FDate to,
            final int fromLastValue) {
        final BufferingIterator<ICloseableIterable<V>> iterablesAscending = new BufferingIterator<>();
        for (int i = fromLastValue; i >= 0; i--) {
            final LastValue<V> prevLastValue = lastValues.getReverse(i);
            if (from.isAfterNotNullSafe(prevLastValue.key)) {
                //we are below the oldest/min time allowed, we can try the next that is further into the future
                //try some newer values
                continue;
            } else if (to != null && to.isBeforeNotNullSafe(prevLastValue.key)) {
                //we are above the newest/max time allowed, we can stop as we would go further into the future
                //don't try newer values
                break;
            } else {
                //add these values
                iterablesAscending.add(prevLastValue.values.snapshot());
            }
        }
        if (iterablesAscending.size() == 1) {
            return new SkippingRangeValues(from, to, iterablesAscending.getHead());
        } else {
            return new SkippingRangeValues(from, to, new FlatteningIterable<>(iterablesAscending));
        }
    }

    private ICloseableIterable<V> rangeValuesToFirstValue(final FDate to) {
        if (to.isBeforeNotNullSafe(firstValueKey)) {
            return EmptyCloseableIterable.getInstance();
        } else {
            return firstValue.snapshot();
        }
    }

    //CHECKSTYLE:OFF
    @Override
    public ICloseableIterable<V> rangeReverseValues(final FDate from, final FDate to, final Lock readLock,
            final ISkipFileFunction skipFileFunction) {
        //CHECKSTYLE:ON
        //we expect the read lock to be already locked from the outside
        if (values == null || from != null && to != null && from.isBeforeNotNullSafe(to)) {
            return EmptyCloseableIterable.getInstance();
        }
        if (from != null && !firstValue.isEmpty() && from.isBeforeOrEqualToNotNullSafe(firstValueKey)) {
            return rangeReverseValuesToFirstValue(from);
        }
        final LastValue<V> lastValue = lastValues.getReverse(0);
        if (to != null && !lastValue.values.isEmpty() && to.isAfterOrEqualToNotNullSafe(lastValue.key)) {
            return rangeReverseValuesFromLastValue(to, lastValue);
        }
        for (int i = 1; i < LAST_VALUE_HISTORY; i++) {
            final LastValue<V> prevLastValue = lastValues.getReverse(i);
            if (prevLastValue.values.isEmpty()) {
                break;
            }
            if (to != null && to.isAfterOrEqualToNotNullSafe(prevLastValue.key)) {
                return rangeReverseValuesToPrevLastValue(from, to, i);
            }
        }
        return new SkippingRangeReverseValues(from, to,
                getFlushedValues().reverseIterable(historicalSegmentTable::extractEndTime, from, to));
    }

    private ICloseableIterable<V> rangeReverseValuesToFirstValue(final FDate from) {
        if (from.isBeforeNotNullSafe(firstValueKey)) {
            return EmptyCloseableIterable.getInstance();
        } else {
            return firstValue.snapshot();
        }
    }

    private ICloseableIterable<V> rangeReverseValuesFromLastValue(final FDate to, final LastValue<V> lastValue) {
        if (to.isAfterNotNullSafe(lastValue.key)) {
            return EmptyCloseableIterable.getInstance();
        } else {
            return lastValue.values.snapshot();
        }
    }

    @SuppressWarnings("resource")
    private ICloseableIterable<V> rangeReverseValuesToPrevLastValue(final FDate from, final FDate to,
            final int toLastValue) {
        final BufferingIterator<ICloseableIterable<V>> iterablesDescending = new BufferingIterator<>();
        for (int i = 0; i <= toLastValue; i++) {
            final LastValue<V> prevLastValue = lastValues.getReverse(i);
            if (to.isAfterNotNullSafe(prevLastValue.key)) {
                //we are below the oldest/min time allowed, we can stop as we would go further into the past
                //don't try older values
                break;
            } else if (from != null && from.isBeforeNotNullSafe(prevLastValue.key)) {
                //we are above the newest/max time allowed, we can try the next that is further into the past
                //maybe skip newer values
                continue;
            } else {
                //add these values
                iterablesDescending.add(prevLastValue.values.snapshot());
            }
        }
        if (iterablesDescending.size() == 1) {
            return new SkippingRangeReverseValues(from, to, iterablesDescending.getHead());
        } else {
            return new SkippingRangeReverseValues(from, to, new FlatteningIterable<>(iterablesDescending));
        }
    }

    @Override
    public void putNextLiveValue(final FDate nextLiveKey, final V nextLiveValue) {
        LastValue<V> lastValue = lastValues.getReverse(0);
        if (!lastValue.values.isEmpty() && lastValue.key.isAfter(nextLiveKey)) {
            LOG.warn("%s: nextLiveKey [%s] should be after or equal to lastLiveKey [%s]", segmentedKey, nextLiveKey,
                    lastValue.key);
            //            throw new IllegalStateException(segmentedKey + ": nextLiveKey [" + nextLiveKey
            //                    + "] should be after or equal to lastLiveKey [" + lastValue.key + "]");
            return;
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
        if (!lastValue.values.isEmpty() && !lastValue.key.equalsNotNullSafe(nextLiveKey)) {
            //roll over to next
            lastValues.pretendAdd();
            lastValue = lastValues.getReverse(0);
            lastValue.values.clear();
        }
        lastValue.values.add(nextLiveValue);
        lastValue.key = nextLiveKey;
        if (inMemoryCacheHolder != null) {
            final ArrayFileBufferCacheResult<V> inMemoryCache = inMemoryCacheHolder.get();
            if (inMemoryCache != null) {
                inMemoryCache.getList().add(nextLiveValue);
            }
        }
    }

    @Override
    public V getNextValue(final FDate date, final int shiftForwardUnits) {
        final LastValue<V> lastValue = lastValues.getReverse(0);
        if (!lastValue.values.isEmpty() && (date == null || date.isAfterOrEqualToNotNullSafe(lastValue.key))) {
            //we always return the last last value
            return lastValue.values.getTail();
        }
        if (date != null) {
            for (int i = 1; i < LAST_VALUE_HISTORY; i++) {
                final LastValue<V> prevLastValue = lastValues.getReverse(i);
                if (prevLastValue.values.isEmpty()) {
                    break;
                }
                if (date.isAfterOrEqualToNotNullSafe(prevLastValue.key)) {
                    if (shiftForwardUnits == 0 && date.equalsNotNullSafe(prevLastValue.key)) {
                        return prevLastValue.values.getTail();
                    } else if (date.isAfterNotNullSafe(prevLastValue.key)) {
                        return getNextValueFromPrevLastValue(date, shiftForwardUnits, i);
                    }
                }
            }
            if (!firstValue.isEmpty() && date.isBeforeNotNullSafe(firstValueKey)) {
                //we always return the first first value
                return firstValue.getHead();
            }
        }
        return getNextValueFromRangeValues(date, shiftForwardUnits);
    }

    private V getNextValueFromPrevLastValue(final FDate date, final int shiftForwardUnits, final int fromLastValue) {
        V nextValue = null;
        int shiftForwardRemaining = shiftForwardUnits;
        for (int i = fromLastValue; i >= 0 && shiftForwardRemaining >= 0; i--) {
            final LastValue<V> lastValue = lastValues.getReverse(i);
            if (date.isAfter(lastValue.key)) {
                //try a newer value
                continue;
            }
            try (ICloseableIterator<V> rangeValues = lastValue.values.iterator()) {
                while (shiftForwardRemaining >= 0) {
                    nextValue = rangeValues.next();
                    shiftForwardRemaining--;
                }
            } catch (final NoSuchElementException e) {
                //ignore
            }
        }
        if (nextValue != null) {
            return nextValue;
        } else {
            throw new IllegalStateException("should not get to here: date=" + date + " shiftForwardUnits="
                    + shiftForwardUnits + " fromLastValue=" + fromLastValue);
        }
    }

    private V getNextValueFromRangeValues(final FDate date, final int shiftForwardUnits) {
        V nextValue = null;
        int shiftForwardRemaining = shiftForwardUnits;
        try (ICloseableIterator<V> rangeValues = rangeValues(date, null, DisabledLock.INSTANCE, null).iterator()) {
            while (shiftForwardRemaining >= 0) {
                nextValue = rangeValues.next();
                shiftForwardRemaining--;
            }
        } catch (final NoSuchElementException e) {
            //ignore
        }
        if (nextValue != null) {
            return nextValue;
        } else {
            return lastValues.getReverse(0).values.getTail();
        }
    }

    @Override
    public V getLatestValue(final FDate date) {
        for (int i = 0; i < LAST_VALUE_HISTORY; i++) {
            final LastValue<V> lastValue = lastValues.getReverse(i);
            if (!lastValue.values.isEmpty() && (date == null || date.isAfterOrEqualToNotNullSafe(lastValue.key))) {
                //we always return the last last value
                return lastValue.values.getTail();
            }
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
        for (int i = 0; i < LAST_VALUE_HISTORY; i++) {
            final LastValue<V> lastValue = lastValues.get(i);
            lastValue.key = null;
            lastValue.values.clear();
        }
        inMemoryCacheHolder = null;
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

    private ArrayFileBufferCacheResult<V> getFlushedValues() {
        if (inMemoryCacheHolder != null) {
            final ArrayFileBufferCacheResult<V> inMemoryCache = inMemoryCacheHolder.get();
            if (inMemoryCache != null) {
                return inMemoryCache;
            } else {
                inMemoryCacheHolder = null;
            }
        }
        final ArrayList<V> fromFileList = (ArrayList<V>) Lists.toListWithoutHasNext(getFlushedValuesFromFile());
        final ArrayFileBufferCacheResult<V> inMemoryCache = new ArrayFileBufferCacheResult<V>(fromFileList);
        inMemoryCacheHolder = new WeakReference<ArrayFileBufferCacheResult<V>>(inMemoryCache);
        return inMemoryCache;
    }

    private IReverseCloseableIterable<V> getFlushedValuesFromFile() {
        synchronized (this) {
            if (needsFlush) {
                values.flush();
                needsFlush = false;
            }
        }
        final TextDescription name = new TextDescription("%s[%s]: getFlushedValues()",
                FileLiveSegment.class.getSimpleName(), segmentedKey);
        return new SerializingCollection<V>(name, values.getFile(), true) {
            @Override
            protected ISerde<V> newSerde() {
                return historicalSegmentTable.newValueSerde();
            }

            @Override
            protected Integer getFixedLength() {
                return historicalSegmentTable.newValueFixedLength();
            }

            @Override
            protected OutputStream newCompressor(final OutputStream out) {
                return compressionFactory.newCompressor(out, LARGE_COMPRESSOR);
            }

            @Override
            protected InputStream newDecompressor(final InputStream inputStream) {
                return compressionFactory.newDecompressor(inputStream);
            }

            @Override
            protected InputStream newFileInputStream(final File file) throws IOException {
                //keep file input stream open as shorty as possible to prevent too many open files error
                try (InputStream fis = super.newFileInputStream(file)) {
                    final PooledFastByteArrayOutputStream bos = PooledFastByteArrayOutputStream.newInstance();
                    IOUtils.copy(fis, bos.asNonClosing());
                    return bos.asInputStream();
                } catch (final FileNotFoundException e) {
                    //maybe retry because of this in the outer iterator?
                    throw new RetryLaterRuntimeException(
                            hashKey + ": File might have been deleted in the mean time between read locks: "
                                    + file.getAbsolutePath(),
                            e);
                }
            }
        };
    }

    @Override
    public FDate getFirstValueKey() {
        return firstValueKey;
    }

    @Override
    public FDate getLastValueKey() {
        return lastValues.getReverse(0).key;
    }

    private final class SkippingRangeValues extends ATimeRangeSkippingIterable<V> {
        private SkippingRangeValues(final FDate from, final FDate to, final ICloseableIterable<? extends V> delegate) {
            super(from, to, delegate);
        }

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
    }

    private final class SkippingRangeReverseValues extends ATimeRangeSkippingIterable<V> {
        private SkippingRangeReverseValues(final FDate from, final FDate to,
                final ICloseableIterable<? extends V> delegate) {
            super(from, to, delegate);
        }

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
    }

    private static class LastValue<V> {
        private FDate key = null;
        private final IBufferingIterator<V> values = new BufferingIterator<V>();

        @Override
        public String toString() {
            return Strings.asString(key);
        }
    }

}
