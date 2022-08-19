package de.invesdwin.context.persistence.timeseriesdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.mutable.MutableInt;

import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.persistence.timeseriesdb.buffer.FileBufferCache;
import de.invesdwin.context.persistence.timeseriesdb.buffer.IFileBufferCacheResult;
import de.invesdwin.context.persistence.timeseriesdb.storage.ISkipFileFunction;
import de.invesdwin.context.persistence.timeseriesdb.storage.MemoryFileSummary;
import de.invesdwin.context.persistence.timeseriesdb.storage.SingleValue;
import de.invesdwin.context.persistence.timeseriesdb.storage.TimeSeriesStorage;
import de.invesdwin.context.persistence.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.collections.eviction.EvictionMode;
import de.invesdwin.util.collections.iterable.ACloseableIterator;
import de.invesdwin.util.collections.iterable.ATransformingIterator;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterator;
import de.invesdwin.util.collections.iterable.FlatteningIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.PeekingCloseableIterator;
import de.invesdwin.util.collections.iterable.collection.ArrayListCloseableIterable;
import de.invesdwin.util.collections.iterable.skip.ASkippingIterator;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;
import de.invesdwin.util.collections.loadingcache.historical.AHistoricalCache;
import de.invesdwin.util.concurrent.lock.disabled.DisabledLock;
import de.invesdwin.util.concurrent.reference.MutableReference;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.description.TextDescription;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.PreLockedDelegateInputStream;
import de.invesdwin.util.streams.buffer.MemoryMappedFile;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.pool.PooledFastByteArrayOutputStream;
import de.invesdwin.util.streams.pool.buffered.BufferedFileDataInputStream;
import de.invesdwin.util.streams.pool.buffered.PreLockedBufferedFileDataInputStream;
import de.invesdwin.util.time.date.FDate;
import ezdb.table.RangeTableRow;

// CHECKSTYLE:OFF ClassDataAbstractionCoupling
@NotThreadSafe
public class TimeSeriesStorageCache<K, V> {
    //CHECKSTYLE:ON
    public static final Integer MAXIMUM_SIZE = 1_000;
    public static final EvictionMode EVICTION_MODE = AHistoricalCache.EVICTION_MODE;

    private static final String READ_RANGE_VALUES = "readRangeValues";
    private static final String READ_RANGE_VALUES_REVERSE = "readRangeValuesReverse";
    private final TimeSeriesStorage storage;
    private final ALoadingCache<FDate, RangeTableRow<String, FDate, MemoryFileSummary>> fileLookupTable_latestRangeKeyCache = new ALoadingCache<FDate, RangeTableRow<String, FDate, MemoryFileSummary>>() {

        @Override
        protected Integer getInitialMaximumSize() {
            return MAXIMUM_SIZE;
        }

        @Override
        protected EvictionMode getEvictionMode() {
            return EVICTION_MODE;
        }

        @Override
        protected RangeTableRow<String, FDate, MemoryFileSummary> loadValue(final FDate key) {
            return storage.getFileLookupTable().getLatest(hashKey, key);
        }
    };

    private final String hashKey;
    private final ISerde<V> valueSerde;
    private final Integer fixedLength;
    private final Function<V, FDate> extractEndTime;
    @GuardedBy("this")
    private File dataDirectory;

    private volatile Optional<V> cachedFirstValue;
    private volatile Optional<V> cachedLastValue;
    /**
     * keeping the range keys outside of the concurrent linked hashmap of the ADelegateRangeTable with memory write
     * through to disk is still better for increased parallelity and for not having to iterate through each element of
     * the other hashkeys.
     */
    private volatile ArrayListCloseableIterable<RangeTableRow<String, FDate, MemoryFileSummary>> cachedAllRangeKeys;
    private final Log log = new Log(this);

    public TimeSeriesStorageCache(final TimeSeriesStorage storage, final String hashKey, final ISerde<V> valueSerde,
            final Integer fixedLength, final Function<V, FDate> extractTime) {
        this.storage = storage;
        this.hashKey = hashKey;
        this.valueSerde = valueSerde;
        this.fixedLength = fixedLength;
        this.extractEndTime = extractTime;
    }

    public synchronized File getDataDirectory() {
        if (dataDirectory == null) {
            dataDirectory = newDataDirectory();
            try {
                Files.forceMkdir(dataDirectory);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
        return dataDirectory;
    }

    public File newDataDirectory() {
        return storage.newDataDirectory(hashKey);
    }

    public File getUpdateLockFile() {
        return new File(getDataDirectory(), "updateRunning.lock");
    }

    public File getMemoryFile() {
        return new File(getDataDirectory(), "memory.data");
    }

    public void finishFile(final FDate time, final V firstValue, final V lastValue, final int valueCount,
            final String memoryResourceUri, final long memoryOffset, final long memoryLength) {
        storage.getFileLookupTable()
                .put(hashKey, time, new MemoryFileSummary(valueSerde, firstValue, lastValue, valueCount,
                        memoryResourceUri, memoryOffset, memoryLength));
        clearCaches();
    }

    public void finishFile(final FDate time, final MemoryFileSummary summary) {
        storage.getFileLookupTable().put(hashKey, time, summary);
        clearCaches();
    }

    protected ICloseableIterable<MemoryFileSummary> readRangeFiles(final FDate from, final FDate to,
            final Lock readLock, final ISkipFileFunction skipFileFunction) {
        return new ICloseableIterable<MemoryFileSummary>() {

            @Override
            public ICloseableIterator<MemoryFileSummary> iterator() {
                final FDate usedFrom;
                if (from == null) {
                    final V firstValue = getFirstValue();
                    if (firstValue == null) {
                        return EmptyCloseableIterator.getInstance();
                    }
                    usedFrom = extractEndTime.apply(firstValue);
                } else {
                    usedFrom = from;
                }
                return new ACloseableIterator<MemoryFileSummary>(new TextDescription("%s[%s]: readRangeFiles(%s, %s)",
                        TimeSeriesStorageCache.class.getSimpleName(), hashKey, from, to)) {

                    //use latest time available even if delegate iterator has no values
                    private RangeTableRow<String, FDate, MemoryFileSummary> latestFirstTime = fileLookupTable_latestRangeKeyCache
                            .get(usedFrom);
                    private final ICloseableIterator<RangeTableRow<String, FDate, MemoryFileSummary>> delegate;

                    {
                        if (latestFirstTime == null) {
                            delegate = EmptyCloseableIterator.getInstance();
                        } else {
                            delegate = getRangeKeys(hashKey, latestFirstTime.getRangeKey().addMilliseconds(1), to);
                        }
                    }

                    @Override
                    protected boolean innerHasNext() {
                        return latestFirstTime != null || delegate.hasNext();
                    }

                    private ICloseableIterator<RangeTableRow<String, FDate, MemoryFileSummary>> getRangeKeys(
                            final String hashKey, final FDate from, final FDate to) {
                        readLock.lock();
                        try {
                            final ICloseableIterator<RangeTableRow<String, FDate, MemoryFileSummary>> range = getAllRangeKeys(
                                    readLock).iterator();
                            final GetRangeKeysIterator rangeFiltered = new GetRangeKeysIterator(range, from, to);
                            if (skipFileFunction != null) {
                                return new ASkippingIterator<RangeTableRow<String, FDate, MemoryFileSummary>>(
                                        rangeFiltered) {
                                    @Override
                                    protected boolean skip(
                                            final RangeTableRow<String, FDate, MemoryFileSummary> element) {
                                        if (!rangeFiltered.hasNext()) {
                                            /*
                                             * cannot optimize this further for multiple segments because we don't know
                                             * if a segment further back might be empty or not and thus the last segment
                                             * of interest might have been the previous one from which we skipped the
                                             * last file falsely
                                             */
                                            return false;
                                        }
                                        return skipFileFunction.skipFile(element.getValue());
                                    }
                                };
                            } else {
                                return rangeFiltered;
                            }
                        } finally {
                            readLock.unlock();
                        }
                    }

                    @Override
                    protected MemoryFileSummary innerNext() {
                        final MemoryFileSummary summary;
                        if (latestFirstTime != null) {
                            summary = latestFirstTime.getValue();
                            latestFirstTime = null;
                        } else {
                            summary = delegate.next().getValue();
                        }
                        return summary;
                    }

                    @Override
                    protected void innerClose() {
                        delegate.close();
                    }

                };
            }
        };
    }

    protected ICloseableIterable<MemoryFileSummary> readRangeFilesReverse(final FDate from, final FDate to,
            final Lock readLock, final ISkipFileFunction skipFileFunction) {
        return new ICloseableIterable<MemoryFileSummary>() {

            @Override
            public ICloseableIterator<MemoryFileSummary> iterator() {
                final FDate usedFrom;
                if (from == null) {
                    final V lastValue = getLastValue();
                    if (lastValue == null) {
                        return EmptyCloseableIterator.getInstance();
                    }
                    usedFrom = extractEndTime.apply(lastValue);
                } else {
                    usedFrom = from;
                }
                return new ACloseableIterator<MemoryFileSummary>(
                        new TextDescription("%s[%s]: readRangeFilesReverse(%s, %s)",
                                TimeSeriesStorageCache.class.getSimpleName(), hashKey, from, to)) {

                    //use latest time available even if delegate iterator has no values
                    private RangeTableRow<String, FDate, MemoryFileSummary> latestLastTime = fileLookupTable_latestRangeKeyCache
                            .get(usedFrom);
                    // add 1 ms to not collide with firstTime
                    private final ICloseableIterator<RangeTableRow<String, FDate, MemoryFileSummary>> delegate;

                    {
                        if (latestLastTime == null) {
                            delegate = EmptyCloseableIterator.getInstance();
                        } else {
                            delegate = getRangeKeysReverse(hashKey, latestLastTime.getRangeKey().addMilliseconds(-1),
                                    to);
                        }
                    }

                    @Override
                    protected boolean innerHasNext() {
                        return latestLastTime != null || delegate.hasNext();
                    }

                    private ICloseableIterator<RangeTableRow<String, FDate, MemoryFileSummary>> getRangeKeysReverse(
                            final String hashKey, final FDate from, final FDate to) {
                        readLock.lock();
                        try {
                            final ICloseableIterator<RangeTableRow<String, FDate, MemoryFileSummary>> range = getAllRangeKeys(
                                    readLock).reverseIterator();
                            final GetRangeKeysReverseIterator rangeFiltered = new GetRangeKeysReverseIterator(range,
                                    from, to);
                            if (skipFileFunction != null) {
                                return new ASkippingIterator<RangeTableRow<String, FDate, MemoryFileSummary>>(
                                        rangeFiltered) {

                                    @Override
                                    protected boolean skip(
                                            final RangeTableRow<String, FDate, MemoryFileSummary> element) {
                                        if (!rangeFiltered.hasNext()) {
                                            /*
                                             * cannot optimize this further for multiple segments because we don't know
                                             * if a segment further back might be empty or not and thus the last segment
                                             * of interest might have been the previous one from which we skipped the
                                             * last file falsely
                                             */
                                            return false;
                                        }
                                        return skipFileFunction.skipFile(element.getValue());
                                    }
                                };
                            } else {
                                return rangeFiltered;
                            }
                        } finally {
                            readLock.unlock();
                        }
                    }

                    @Override
                    protected MemoryFileSummary innerNext() {
                        final MemoryFileSummary summary;
                        if (latestLastTime != null) {
                            summary = latestLastTime.getValue();
                            latestLastTime = null;
                        } else {
                            summary = delegate.next().getValue();
                        }
                        return summary;
                    }

                    @Override
                    protected void innerClose() {
                        delegate.close();
                    }

                };
            }
        };
    }

    public ICloseableIterator<V> readRangeValues(final FDate from, final FDate to, final Lock readLock,
            final ISkipFileFunction skipFileFunction) {
        final PeekingCloseableIterator<MemoryFileSummary> fileIterator = new PeekingCloseableIterator<MemoryFileSummary>(
                readRangeFiles(from, to, readLock, skipFileFunction).iterator());
        final ICloseableIterator<ICloseableIterator<V>> chunkIterator = new ATransformingIterator<MemoryFileSummary, ICloseableIterator<V>>(
                fileIterator) {
            @Override
            protected ICloseableIterator<V> transform(final MemoryFileSummary value) {
                if (TimeseriesProperties.FILE_BUFFER_CACHE_PRELOAD_ENABLED) {
                    try {
                        preloadResultCached(READ_RANGE_VALUES, fileIterator.peek(), readLock);
                    } catch (final NoSuchElementException e) {
                        //end reached
                    }
                }
                try (IFileBufferCacheResult<V> serializingCollection = getResultCached(READ_RANGE_VALUES, value,
                        readLock)) {
                    return serializingCollection.iterator(extractEndTime, from, to);
                }
            }

        };

        final ICloseableIterator<V> rangeValues = new FlatteningIterator<V>(chunkIterator);
        return rangeValues;
    }

    public ICloseableIterator<V> readRangeValuesReverse(final FDate from, final FDate to, final Lock readLock,
            final ISkipFileFunction skipFileFunction) {
        final PeekingCloseableIterator<MemoryFileSummary> fileIterator = new PeekingCloseableIterator<MemoryFileSummary>(
                readRangeFilesReverse(from, to, readLock, skipFileFunction).iterator());
        final ICloseableIterator<ICloseableIterator<V>> chunkIterator = new ATransformingIterator<MemoryFileSummary, ICloseableIterator<V>>(
                fileIterator) {
            @Override
            protected ICloseableIterator<V> transform(final MemoryFileSummary value) {
                if (TimeseriesProperties.FILE_BUFFER_CACHE_PRELOAD_ENABLED) {
                    try {
                        preloadResultCached(READ_RANGE_VALUES_REVERSE, fileIterator.peek(), readLock);
                    } catch (final NoSuchElementException e) {
                        //end reached
                    }
                }
                try (IFileBufferCacheResult<V> serializingCollection = getResultCached(READ_RANGE_VALUES_REVERSE, value,
                        readLock)) {
                    return serializingCollection.reverseIterator(extractEndTime, from, to);
                }
            }

        };

        final ICloseableIterator<V> rangeValuesReverse = new FlatteningIterator<V>(chunkIterator);
        return rangeValuesReverse;
    }

    private IFileBufferCacheResult<V> getResultCached(final String method, final MemoryFileSummary summary,
            final Lock readLock) {
        return FileBufferCache.getResult(hashKey, summary, () -> newResult(method, summary, readLock));
    }

    private void preloadResultCached(final String method, final MemoryFileSummary summary, final Lock readLock) {
        FileBufferCache.preloadResult(hashKey, summary, () -> newResult(method, summary, readLock));
    }

    private SerializingCollection<V> newResult(final String method, final MemoryFileSummary summary,
            final Lock readLock) {
        final TextDescription name = new TextDescription("%s[%s]: %s(%s)", ATimeSeriesUpdater.class.getSimpleName(),
                hashKey, method, summary);
        final File memoryFile = new File(summary.getMemoryResourceUri());
        return new SerializingCollection<V>(name, memoryFile, true) {

            @Override
            protected ISerde<V> newSerde() {
                return new ISerde<V>() {
                    @Override
                    public V fromBytes(final byte[] bytes) {
                        return valueSerde.fromBytes(bytes);
                    }

                    @Override
                    public V fromBuffer(final IByteBuffer buffer, final int length) {
                        return valueSerde.fromBuffer(buffer, length);
                    }

                    @Override
                    public int toBuffer(final IByteBuffer buffer, final V obj) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public byte[] toBytes(final V obj) {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            protected InputStream newFileInputStream(final File file) throws IOException {
                if (TimeseriesProperties.FILE_BUFFER_CACHE_MMAP_ENABLED) {
                    readLock.lock();
                    final MemoryMappedFile mmapFile = FileBufferCache.getFile(hashKey, summary.getMemoryResourceUri());
                    if (mmapFile.incrementRefCount()) {
                        return new MmapInputStream(readLock, summary.newBuffer(mmapFile).asInputStream(), mmapFile);
                    } else {
                        readLock.unlock();
                    }
                }
                if (TimeseriesProperties.FILE_BUFFER_CACHE_SEGMENTS_ENABLED) {
                    readLock.lock();
                    //file buffer cache will close the file quickly
                    final PreLockedBufferedFileDataInputStream in = new PreLockedBufferedFileDataInputStream(readLock,
                            memoryFile);
                    in.position(summary.getMemoryOffset());
                    in.limit(summary.getMemoryOffset() + summary.getMemoryLength());
                    return in;
                } else {
                    //keep file input stream open as shorty as possible to prevent too many open files error
                    readLock.lock();
                    try (BufferedFileDataInputStream in = new BufferedFileDataInputStream(memoryFile)) {
                        in.position(summary.getMemoryOffset());
                        in.limit(summary.getMemoryOffset() + summary.getMemoryLength());
                        final PooledFastByteArrayOutputStream bos = PooledFastByteArrayOutputStream.newInstance();
                        IOUtils.copy(in, bos.asNonClosing());
                        return bos.asInputStream();
                    } catch (final FileNotFoundException e) {
                        //maybe retry because of this in the outer iterator?
                        throw new RetryLaterRuntimeException(
                                "File might have been deleted in the mean time between read locks: "
                                        + file.getAbsolutePath(),
                                e);
                    } finally {
                        readLock.unlock();
                    }
                }
            }

            @Override
            protected Integer getFixedLength() {
                return fixedLength;
            }

            @Override
            protected OutputStream newCompressor(final OutputStream out) {
                return storage.getCompressionFactory().newCompressor(out, ATimeSeriesUpdater.LARGE_COMPRESSOR);
            }

            @Override
            protected InputStream newDecompressor(final InputStream inputStream) {
                return storage.getCompressionFactory().newDecompressor(inputStream);
            }
        };
    }

    public V getFirstValue() {
        if (cachedFirstValue == null) {
            final ArrayList<? extends RangeTableRow<String, FDate, MemoryFileSummary>> list = getAllRangeKeys(
                    DisabledLock.INSTANCE).getArrayList();
            if (list.isEmpty()) {
                cachedFirstValue = Optional.empty();
            } else {
                final RangeTableRow<String, FDate, MemoryFileSummary> row = list.get(0);
                final MemoryFileSummary latestValue = row.getValue();
                final V firstValue;
                if (latestValue == null) {
                    firstValue = null;
                } else {
                    firstValue = latestValue.getFirstValue(valueSerde);
                }
                cachedFirstValue = Optional.ofNullable(firstValue);
            }
        }
        return cachedFirstValue.orElse(null);
    }

    public V getLastValue() {
        if (cachedLastValue == null) {
            final ArrayList<? extends RangeTableRow<String, FDate, MemoryFileSummary>> list = getAllRangeKeys(
                    DisabledLock.INSTANCE).getArrayList();
            if (list.isEmpty()) {
                cachedLastValue = Optional.empty();
            } else {
                final RangeTableRow<String, FDate, MemoryFileSummary> row = list.get(list.size() - 1);
                final MemoryFileSummary latestValue = row.getValue();
                final V lastValue;
                if (latestValue == null) {
                    lastValue = null;
                } else {
                    lastValue = latestValue.getLastValue(valueSerde);
                }
                cachedLastValue = Optional.ofNullable(lastValue);
            }
        }
        return cachedLastValue.orElse(null);
    }

    public synchronized void deleteAll() {
        storage.getFileLookupTable().deleteRange(hashKey);
        storage.deleteRange_latestValueLookupTable(hashKey);
        storage.deleteRange_nextValueLookupTable(hashKey);
        storage.deleteRange_previousValueLookupTable(hashKey);
        clearCaches();
        Files.deleteNative(newDataDirectory());
        dataDirectory = null;
    }

    private void clearCaches() {
        fileLookupTable_latestRangeKeyCache.clear();
        FileBufferCache.remove(hashKey);
        cachedAllRangeKeys = null;
        cachedFirstValue = null;
        cachedLastValue = null;
    }

    public V getLatestValue(final FDate date) {
        final SingleValue value = storage.getOrLoad_latestValueLookupTable(hashKey, date, () -> {
            final RangeTableRow<String, FDate, MemoryFileSummary> row = fileLookupTable_latestRangeKeyCache.get(date);
            if (row == null) {
                return null;
            }
            final MemoryFileSummary summary = row.getValue();
            if (summary == null) {
                return null;
            }
            try (IFileBufferCacheResult<V> result = getResultCached("latestValueLookupCache.loadValue", summary,
                    DisabledLock.INSTANCE)) {
                V latestValue = result.getLatestValue(extractEndTime, date);
                if (latestValue == null) {
                    latestValue = getFirstValue();
                }
                if (latestValue == null) {
                    return null;
                }
                return new SingleValue(valueSerde, latestValue);
            }
        });
        if (value == null) {
            return null;
        }
        return value.getValue(valueSerde);
    }

    public V getPreviousValue(final FDate date, final int shiftBackUnits) {
        assertShiftUnitsPositiveNonZero(shiftBackUnits);
        final V firstValue = getFirstValue();
        if (firstValue == null) {
            return null;
        }
        final FDate firstTime = extractEndTime.apply(firstValue);
        if (date.isBeforeOrEqualTo(firstTime)) {
            return firstValue;
        } else {
            final SingleValue value = storage.getOrLoad_previousValueLookupTable(hashKey, date, shiftBackUnits, () -> {
                final MutableReference<V> previousValue = new MutableReference<>();
                final MutableInt shiftBackRemaining = new MutableInt(shiftBackUnits);
                try (ICloseableIterator<V> rangeValuesReverse = readRangeValuesReverse(date, null,
                        DisabledLock.INSTANCE, file -> {
                            final boolean skip = previousValue.get() != null
                                    && file.getValueCount() < shiftBackRemaining.intValue();
                            if (skip) {
                                shiftBackRemaining.subtract(file.getValueCount());
                            }
                            return skip;
                        })) {
                    while (shiftBackRemaining.intValue() >= 0) {
                        previousValue.set(rangeValuesReverse.next());
                        shiftBackRemaining.decrement();
                    }
                } catch (final NoSuchElementException e) {
                    //ignore
                }
                return new SingleValue(valueSerde, previousValue.get());
            });
            return value.getValue(valueSerde);
        }
    }

    public V getNextValue(final FDate date, final int shiftForwardUnits) {
        assertShiftUnitsPositiveNonZero(shiftForwardUnits);
        final V lastValue = getLastValue();
        if (lastValue == null) {
            return null;
        }
        final FDate lastTime = extractEndTime.apply(lastValue);
        if (date.isAfterOrEqualTo(lastTime)) {
            return lastValue;
        } else {
            final SingleValue value = storage.getOrLoad_nextValueLookupTable(hashKey, date, shiftForwardUnits, () -> {
                final MutableReference<V> nextValue = new MutableReference<>();
                final MutableInt shiftForwardRemaining = new MutableInt(shiftForwardUnits);
                try (ICloseableIterator<V> rangeValues = readRangeValues(date, null, DisabledLock.INSTANCE,
                        new ISkipFileFunction() {
                            @Override
                            public boolean skipFile(final MemoryFileSummary file) {
                                final boolean skip = nextValue.get() != null
                                        && file.getValueCount() < shiftForwardRemaining.intValue();
                                if (skip) {
                                    shiftForwardRemaining.subtract(file.getValueCount());
                                }
                                return skip;
                            }
                        })) {
                    while (shiftForwardRemaining.intValue() >= 0) {
                        nextValue.set(rangeValues.next());
                        shiftForwardRemaining.decrement();
                    }
                } catch (final NoSuchElementException e) {
                    //ignore
                }
                return new SingleValue(valueSerde, nextValue.get());
            });
            return value.getValue(valueSerde);
        }
    }

    public boolean isEmptyOrInconsistent() {
        try {
            getFirstValue();
            getLastValue();
        } catch (final Throwable t) {
            if (Throwables.isCausedByType(t, SerializationException.class)) {
                //e.g. fst: unable to find class for code 88 after version upgrade
                log.warn("Table data for [%s] is inconsistent and needs to be reset. Exception during getLastValue: %s",
                        hashKey, t.toString());
                return true;
            } else {
                //unexpected exception, since RemoteFastSerializingSerde only throws SerializingException
                throw Throwables.propagate(t);
            }
        }
        try (ICloseableIterator<MemoryFileSummary> summaries = readRangeFiles(null, null, DisabledLock.INSTANCE, null)
                .iterator()) {
            boolean noFileFound = true;
            while (summaries.hasNext()) {
                final MemoryFileSummary summary = summaries.next();
                final File memoryFile = new File(summary.getMemoryResourceUri());
                final long memoryFileLength = memoryFile.length();
                final long segmentLength = summary.getMemoryOffset() + summary.getMemoryLength();
                if (segmentLength > memoryFileLength) {
                    log.warn("Table data for [%s] is inconsistent and needs to be reset. Empty file: [%s]", hashKey,
                            summary);
                    return true;
                }
                noFileFound = false;
            }
            return noFileFound;
        }
    }

    /**
     * When shouldRedoLastFile=true this deletes the last file in order to create a new updated one (so the files do not
     * get fragmented too much between updates
     */
    public synchronized PrepareForUpdateResult<V> prepareForUpdate(final boolean shouldRedoLastFile) {
        final ArrayList<? extends RangeTableRow<String, FDate, MemoryFileSummary>> list = getAllRangeKeys(
                DisabledLock.INSTANCE).getArrayList();
        final RangeTableRow<String, FDate, MemoryFileSummary> latestFile;
        if (list.isEmpty()) {
            latestFile = null;
        } else {
            latestFile = list.get(list.size() - 1);
        }
        final FDate updateFrom;
        final List<V> lastValues;
        final long addressOffset;
        if (latestFile != null) {
            final FDate latestRangeKey;
            final MemoryFileSummary latestSummary = latestFile.getValue();
            if (shouldRedoLastFile && latestSummary.getValueCount() < ATimeSeriesUpdater.BATCH_FLUSH_INTERVAL) {
                lastValues = new ArrayList<V>();
                try (ICloseableIterator<V> lastColl = newResult("prepareForUpdate", latestSummary,
                        DisabledLock.INSTANCE).iterator()) {
                    Lists.toListWithoutHasNext(lastColl, lastValues);
                }
                if (!lastValues.isEmpty()) {
                    //remove last value because it might be an incomplete bar
                    final V lastValue = lastValues.remove(lastValues.size() - 1);
                    addressOffset = latestSummary.getMemoryOffset();
                    updateFrom = extractEndTime.apply(lastValue);
                    latestRangeKey = latestFile.getRangeKey();
                } else {
                    addressOffset = latestSummary.getMemoryOffset() + latestSummary.getMemoryLength() + 1L;
                    updateFrom = latestFile.getRangeKey();
                    latestRangeKey = latestFile.getRangeKey().addMilliseconds(1);
                }
            } else {
                lastValues = Collections.emptyList();
                addressOffset = latestSummary.getMemoryOffset() + latestSummary.getMemoryLength() + 1L;
                updateFrom = latestFile.getRangeKey();
                latestRangeKey = latestFile.getRangeKey().addMilliseconds(1);
            }
            storage.getFileLookupTable().deleteRange(hashKey, latestRangeKey);
            storage.deleteRange_latestValueLookupTable(hashKey, latestRangeKey);
            storage.deleteRange_nextValueLookupTable(hashKey); //we cannot be sure here about the date since shift keys can be arbitrarily large
            storage.deleteRange_previousValueLookupTable(hashKey, latestRangeKey);
        } else {
            updateFrom = null;
            lastValues = Collections.emptyList();
            addressOffset = 0L;
        }
        clearCaches();
        return new PrepareForUpdateResult<>(updateFrom, lastValues, addressOffset);
    }

    private void assertShiftUnitsPositiveNonZero(final int shiftUnits) {
        if (shiftUnits < 0) {
            throw new IllegalArgumentException("shiftUnits needs to be a positive or zero value: " + shiftUnits);
        }
    }

    private ArrayListCloseableIterable<RangeTableRow<String, FDate, MemoryFileSummary>> getAllRangeKeys(
            final Lock readLock) {
        readLock.lock();
        try {
            if (cachedAllRangeKeys == null) {
                try (ICloseableIterator<RangeTableRow<String, FDate, MemoryFileSummary>> range = storage
                        .getFileLookupTable()
                        .range(hashKey, FDate.MIN_DATE, FDate.MAX_DATE)) {
                    final ArrayList<RangeTableRow<String, FDate, MemoryFileSummary>> allRangeKeys = new ArrayList<>();
                    Lists.toListWithoutHasNext(range, allRangeKeys);
                    cachedAllRangeKeys = new ArrayListCloseableIterable<RangeTableRow<String, FDate, MemoryFileSummary>>(
                            allRangeKeys);
                }
            }
            return cachedAllRangeKeys;
        } finally {
            readLock.unlock();
        }
    }

    private static final class MmapInputStream extends PreLockedDelegateInputStream {
        private MemoryMappedFile mmapFile;

        private MmapInputStream(final Lock lock, final InputStream delegate, final MemoryMappedFile mmapFile) {
            super(lock, delegate);
            this.mmapFile = mmapFile;
        }

        @Override
        public void close() throws IOException {
            if (mmapFile != null) {
                super.close();
                mmapFile.decrementRefCount();
                mmapFile = null;
            }
        }
    }

    private static final class GetRangeKeysReverseIterator
            extends ASkippingIterator<RangeTableRow<String, FDate, MemoryFileSummary>> {
        private final FDate from;
        private final FDate to;

        private GetRangeKeysReverseIterator(
                final ICloseableIterator<? extends RangeTableRow<String, FDate, MemoryFileSummary>> delegate,
                final FDate from, final FDate to) {
            super(delegate);
            this.from = from;
            this.to = to;
        }

        @Override
        protected boolean skip(final RangeTableRow<String, FDate, MemoryFileSummary> element) {
            if (element.getRangeKey().isAfter(from)) {
                return true;
            } else if (element.getRangeKey().isBefore(to)) {
                throw FastNoSuchElementException.getInstance("getRangeKeysReverse reached end");
            }
            return false;
        }
    }

    private static final class GetRangeKeysIterator
            extends ASkippingIterator<RangeTableRow<String, FDate, MemoryFileSummary>> {
        private final FDate from;
        private final FDate to;

        private GetRangeKeysIterator(
                final ICloseableIterator<? extends RangeTableRow<String, FDate, MemoryFileSummary>> delegate,
                final FDate from, final FDate to) {
            super(delegate);
            this.from = from;
            this.to = to;
        }

        @Override
        protected boolean skip(final RangeTableRow<String, FDate, MemoryFileSummary> element) {
            if (element.getRangeKey().isBefore(from)) {
                return true;
            } else if (element.getRangeKey().isAfter(to)) {
                throw FastNoSuchElementException.getInstance("getRangeKeys reached end");
            }
            return false;
        }
    }

}
