package de.invesdwin.context.persistence.timeseries.timeseriesdb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import de.invesdwin.context.persistence.timeseries.ezdb.ADelegateRangeTable.DelegateTableIterator;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.storage.ChunkValue;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.storage.ISkipFileFunction;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.storage.ShiftUnitsRangeKey;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.storage.SingleValue;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.storage.TimeSeriesStorage;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.updater.ATimeSeriesUpdater;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.bean.tuple.Pair;
import de.invesdwin.util.collections.eviction.EvictionMode;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.iterable.ACloseableIterator;
import de.invesdwin.util.collections.iterable.ASkippingIterator;
import de.invesdwin.util.collections.iterable.ATransformingIterator;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterator;
import de.invesdwin.util.collections.iterable.FlatteningIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.IReverseCloseableIterable;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;
import de.invesdwin.util.collections.loadingcache.historical.AHistoricalCache;
import de.invesdwin.util.concurrent.lock.disabled.DisabledLock;
import de.invesdwin.util.concurrent.reference.MutableReference;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.buffer.IByteBuffer;
import de.invesdwin.util.lang.description.TextDescription;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.time.date.FDate;
import ezdb.TableRow;

// CHECKSTYLE:OFF ClassDataAbstractionCoupling
@NotThreadSafe
public class TimeSeriesStorageCache<K, V> {
    //CHECKSTYLE:ON
    public static final Integer MAXIMUM_SIZE = 1_000;
    public static final EvictionMode EVICTION_MODE = AHistoricalCache.EVICTION_MODE;

    private final TimeSeriesStorage storage;
    private final ALoadingCache<FDate, V> latestValueLookupCache = new ALoadingCache<FDate, V>() {

        @Override
        protected Integer getInitialMaximumSize() {
            return MAXIMUM_SIZE;
        }

        @Override
        protected EvictionMode getEvictionMode() {
            return EVICTION_MODE;
        }

        @Override
        protected V loadValue(final FDate key) {
            final SingleValue value = storage.getLatestValueLookupTable()
                    .getOrLoad(hashKey, key, new Function<Pair<String, FDate>, SingleValue>() {

                        @Override
                        public SingleValue apply(final Pair<String, FDate> input) {
                            final FDate fileTime = storage.getFileLookupTable()
                                    .getLatestRangeKey(input.getFirst(), input.getSecond());
                            if (fileTime == null) {
                                return null;
                            }
                            final File file = newFile(fileTime);
                            final SerializingCollection<V> serializingCollection = newSerializingCollection(
                                    "latestValueLookupCache.loadValue", file, DisabledLock.INSTANCE);
                            V latestValue = null;
                            try (ICloseableIterator<V> it = serializingCollection.iterator()) {
                                while (true) {
                                    final V newValue = it.next();
                                    final FDate newValueTime = extractEndTime.apply(newValue);
                                    if (newValueTime.isAfter(key)) {
                                        break;
                                    } else {
                                        latestValue = newValue;
                                    }
                                }
                            } catch (final NoSuchElementException e) {
                                //end reached
                            }
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
    };
    private final ALoadingCache<Pair<FDate, Integer>, V> previousValueLookupCache = new ALoadingCache<Pair<FDate, Integer>, V>() {

        @Override
        protected Integer getInitialMaximumSize() {
            return MAXIMUM_SIZE;
        }

        @Override
        protected EvictionMode getEvictionMode() {
            return EVICTION_MODE;
        }

        @Override
        protected V loadValue(final Pair<FDate, Integer> key) {
            final FDate date = key.getFirst();
            final int shiftBackUnits = key.getSecond();
            final SingleValue value = storage.getPreviousValueLookupTable()
                    .getOrLoad(hashKey, new ShiftUnitsRangeKey(date, shiftBackUnits),
                            new Function<Pair<String, ShiftUnitsRangeKey>, SingleValue>() {

                                @Override
                                public SingleValue apply(final Pair<String, ShiftUnitsRangeKey> input) {
                                    final FDate date = key.getFirst();
                                    final int shiftBackUnits = key.getSecond();
                                    final MutableReference<V> previousValue = new MutableReference<>();
                                    final MutableInt shiftBackRemaining = new MutableInt(shiftBackUnits);
                                    try (ICloseableIterator<V> rangeValuesReverse = readRangeValuesReverse(date, null,
                                            DisabledLock.INSTANCE, new ISkipFileFunction() {
                                                @Override
                                                public boolean skipFile(final ChunkValue file) {
                                                    final boolean skip = previousValue.get() != null
                                                            && file.getCount() < shiftBackRemaining.intValue();
                                                    if (skip) {
                                                        shiftBackRemaining.subtract(file.getCount());
                                                    }
                                                    return skip;
                                                }
                                            })) {
                                        while (shiftBackRemaining.intValue() >= 0) {
                                            previousValue.set(rangeValuesReverse.next());
                                            shiftBackRemaining.decrement();
                                        }
                                    } catch (final NoSuchElementException e) {
                                        //ignore
                                    }
                                    return new SingleValue(valueSerde, previousValue.get());
                                }
                            });
            return value.getValue(valueSerde);
        }
    };
    private final ALoadingCache<Pair<FDate, Integer>, V> nextValueLookupCache = new ALoadingCache<Pair<FDate, Integer>, V>() {

        @Override
        protected Integer getInitialMaximumSize() {
            return MAXIMUM_SIZE;
        }

        @Override
        protected EvictionMode getEvictionMode() {
            return EVICTION_MODE;
        }

        @Override
        protected V loadValue(final Pair<FDate, Integer> key) {
            final FDate date = key.getFirst();
            final int shiftForwardUnits = key.getSecond();
            final SingleValue value = storage.getNextValueLookupTable()
                    .getOrLoad(hashKey, new ShiftUnitsRangeKey(date, shiftForwardUnits),
                            new Function<Pair<String, ShiftUnitsRangeKey>, SingleValue>() {

                                @Override
                                public SingleValue apply(final Pair<String, ShiftUnitsRangeKey> input) {
                                    final FDate date = key.getFirst();
                                    final int shiftForwardUnits = key.getSecond();
                                    final MutableReference<V> nextValue = new MutableReference<>();
                                    final MutableInt shiftForwardRemaining = new MutableInt(shiftForwardUnits);
                                    try (ICloseableIterator<V> rangeValues = readRangeValues(date, null,
                                            DisabledLock.INSTANCE, new ISkipFileFunction() {
                                                @Override
                                                public boolean skipFile(final ChunkValue file) {
                                                    final boolean skip = nextValue.get() != null
                                                            && file.getCount() < shiftForwardRemaining.intValue();
                                                    if (skip) {
                                                        shiftForwardRemaining.subtract(file.getCount());
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
                                }
                            });
            return value.getValue(valueSerde);
        }
    };
    private final ALoadingCache<FDate, TableRow<String, FDate, ChunkValue>> fileLookupTable_latestRangeKeyCache = new ALoadingCache<FDate, TableRow<String, FDate, ChunkValue>>() {

        @Override
        protected Integer getInitialMaximumSize() {
            return MAXIMUM_SIZE;
        }

        @Override
        protected EvictionMode getEvictionMode() {
            return EVICTION_MODE;
        }

        @Override
        protected TableRow<String, FDate, ChunkValue> loadValue(final FDate key) {
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
    private volatile ICloseableIterable<TableRow<String, FDate, ChunkValue>> cachedAllRangeKeys;
    private volatile ICloseableIterable<TableRow<String, FDate, ChunkValue>> cachedAllRangeKeysReverse;
    private final Log log = new Log(this);
    private Map<FDate, File> redirectedFiles;

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

    public File newFile(final FDate time) {
        if (time == null) {
            throw new NullPointerException("time should not be null");
        }
        if (redirectedFiles != null) {
            final File redirectedFile = redirectedFiles.get(time);
            if (redirectedFile != null) {
                return redirectedFile;
            }
        }
        return new File(getDataDirectory(), time.toString(FDate.FORMAT_UNDERSCORE_DATE_TIME_MS) + ".data");
    }

    public synchronized void redirectFileInMemory(final FDate time, final File redirect) {
        if (redirectedFiles == null) {
            redirectedFiles = ILockCollectionFactory.getInstance(true).newConcurrentMap();
        }
        Assertions.checkNull(redirectedFiles.put(time, redirect));
    }

    public void finishFile(final FDate time, final V firstValue, final V lastValue, final int count) {
        storage.getFileLookupTable().put(hashKey, time, new ChunkValue(valueSerde, firstValue, lastValue, count));
        clearCaches();
    }

    protected ICloseableIterable<File> readRangeFiles(final FDate from, final FDate to, final Lock readLock,
            final ISkipFileFunction skipFileFunction) {
        return new ICloseableIterable<File>() {

            @Override
            public ICloseableIterator<File> iterator() {
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
                return new ACloseableIterator<File>(new TextDescription("%s[%s]: readRangeFiles(%s, %s)",
                        TimeSeriesStorageCache.class.getSimpleName(), hashKey, from, to)) {

                    //use latest time available even if delegate iterator has no values
                    private TableRow<String, FDate, ChunkValue> latestFirstTime = fileLookupTable_latestRangeKeyCache
                            .get(usedFrom);
                    private final ICloseableIterator<TableRow<String, FDate, ChunkValue>> delegate;

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

                    private ICloseableIterator<TableRow<String, FDate, ChunkValue>> getRangeKeys(final String hashKey,
                            final FDate from, final FDate to) {
                        readLock.lock();
                        try {
                            final ICloseableIterator<TableRow<String, FDate, ChunkValue>> range = getAllRangeKeys(
                                    readLock);
                            final GetRangeKeysIterator rangeFiltered = new GetRangeKeysIterator(range, from, to);
                            final BufferingIterator<TableRow<String, FDate, ChunkValue>> buffer = new BufferingIterator<>(
                                    rangeFiltered);
                            if (skipFileFunction != null) {
                                return new ASkippingIterator<TableRow<String, FDate, ChunkValue>>(buffer) {
                                    @Override
                                    protected boolean skip(final TableRow<String, FDate, ChunkValue> element) {
                                        if (element == buffer.getTail()) {
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
                                return buffer;
                            }
                        } finally {
                            readLock.unlock();
                        }
                    }

                    @Override
                    protected File innerNext() {
                        final FDate time;
                        if (latestFirstTime != null) {
                            time = latestFirstTime.getRangeKey();
                            latestFirstTime = null;
                        } else {
                            time = delegate.next().getRangeKey();
                        }
                        return newFile(time);
                    }

                    @Override
                    protected void innerClose() {
                        delegate.close();
                    }

                };
            }
        };
    }

    protected ICloseableIterable<File> readRangeFilesReverse(final FDate from, final FDate to, final Lock readLock,
            final ISkipFileFunction skipFileFunction) {
        return new ICloseableIterable<File>() {

            @Override
            public ICloseableIterator<File> iterator() {
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
                return new ACloseableIterator<File>(new TextDescription("%s[%s]: readRangeFilesReverse(%s, %s)",
                        TimeSeriesStorageCache.class.getSimpleName(), hashKey, from, to)) {

                    //use latest time available even if delegate iterator has no values
                    private TableRow<String, FDate, ChunkValue> latestLastTime = fileLookupTable_latestRangeKeyCache
                            .get(usedFrom);
                    // add 1 ms to not collide with firstTime
                    private final ICloseableIterator<TableRow<String, FDate, ChunkValue>> delegate;

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

                    private ICloseableIterator<TableRow<String, FDate, ChunkValue>> getRangeKeysReverse(
                            final String hashKey, final FDate from, final FDate to) {
                        readLock.lock();
                        try {
                            final ICloseableIterator<TableRow<String, FDate, ChunkValue>> range = getAllRangeKeysReverse(
                                    readLock);
                            final GetRangeKeysReverseIterator rangeFiltered = new GetRangeKeysReverseIterator(range,
                                    from, to);
                            final BufferingIterator<TableRow<String, FDate, ChunkValue>> buffer = new BufferingIterator<>(
                                    rangeFiltered);
                            if (skipFileFunction != null) {
                                return new ASkippingIterator<TableRow<String, FDate, ChunkValue>>(buffer) {

                                    @Override
                                    protected boolean skip(final TableRow<String, FDate, ChunkValue> element) {
                                        if (element == buffer.getTail()) {
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
                                return buffer;
                            }
                        } finally {
                            readLock.unlock();
                        }
                    }

                    @Override
                    protected File innerNext() {
                        final FDate time;
                        if (latestLastTime != null) {
                            time = latestLastTime.getRangeKey();
                            latestLastTime = null;
                        } else {
                            time = delegate.next().getRangeKey();
                        }
                        return newFile(time);
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
        final ICloseableIterator<File> fileIterator = readRangeFiles(from, to, readLock, skipFileFunction).iterator();
        final ICloseableIterator<ICloseableIterator<V>> chunkIterator = new ATransformingIterator<File, ICloseableIterator<V>>(
                fileIterator) {
            @Override
            protected ICloseableIterator<V> transform(final File value) {
                final ICloseableIterable<V> serializingCollection = newSerializingCollection("readRangeValues", value,
                        readLock);
                if (from == null && to == null) {
                    return serializingCollection.iterator();
                } else {
                    return new ASkippingIterator<V>(serializingCollection.iterator()) {
                        @Override
                        protected boolean skip(final V element) {
                            final FDate time = extractEndTime.apply(element);
                            if (time.isBefore(from)) {
                                return true;
                            } else if (time.isAfter(to)) {
                                throw new FastNoSuchElementException("getRangeValues reached end");
                            }
                            return false;
                        }
                    };
                }
            }

        };

        final ICloseableIterator<V> rangeValues = new FlatteningIterator<V>(chunkIterator);
        return rangeValues;
    }

    public ICloseableIterator<V> readRangeValuesReverse(final FDate from, final FDate to, final Lock readLock,
            final ISkipFileFunction skipFileFunction) {
        final ICloseableIterator<File> fileIterator = readRangeFilesReverse(from, to, readLock, skipFileFunction)
                .iterator();
        final ICloseableIterator<ICloseableIterator<V>> chunkIterator = new ATransformingIterator<File, ICloseableIterator<V>>(
                fileIterator) {
            @Override
            protected ICloseableIterator<V> transform(final File value) {
                final IReverseCloseableIterable<V> serializingCollection = newSerializingCollection(
                        "readRangeValuesReverse", value, readLock);
                if (from == null && to == null) {
                    return serializingCollection.reverseIterator();
                } else {
                    return new ASkippingIterator<V>(serializingCollection.reverseIterator()) {
                        @Override
                        protected boolean skip(final V element) {
                            final FDate time = extractEndTime.apply(element);
                            if (time.isAfter(from)) {
                                return true;
                            } else if (time.isBefore(to)) {
                                throw new FastNoSuchElementException("getRangeValues reached end");
                            }
                            return false;
                        }
                    };
                }
            }

        };
        final ICloseableIterator<V> rangeValuesReverse = new FlatteningIterator<V>(chunkIterator);
        return rangeValuesReverse;
    }

    private SerializingCollection<V> newSerializingCollection(final String method, final File file,
            final Lock readLock) {
        final TextDescription name = new TextDescription("%s[%s]: %s(%s)", ATimeSeriesUpdater.class.getSimpleName(),
                hashKey, method, file);
        return new SerializingCollection<V>(name, file, true) {

            @Override
            protected ISerde<V> newSerde() {
                return new ISerde<V>() {
                    @Override
                    public V fromBytes(final byte[] bytes) {
                        return valueSerde.fromBytes(bytes);
                    }

                    @Override
                    public V fromBuffer(final IByteBuffer buffer) {
                        return valueSerde.fromBuffer(buffer);
                    }

                    @Override
                    public int toBuffer(final V obj, final IByteBuffer buffer) {
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
                //keep file input stream open as shorty as possible to prevent too many open files error
                readLock.lock();
                try (InputStream fis = super.newFileInputStream(file)) {
                    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    IOUtils.copy(fis, bos);
                    return new ByteArrayInputStream(bos.toByteArray());
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

            @Override
            protected Integer getFixedLength() {
                return fixedLength;
            }
        };
    }

    public V getFirstValue() {
        if (cachedFirstValue == null) {
            final ChunkValue latestValue = storage.getFileLookupTable().getLatestValue(hashKey, FDate.MIN_DATE);
            final V firstValue;
            if (latestValue == null) {
                firstValue = null;
            } else {
                firstValue = latestValue.getFirstValue(valueSerde);
            }
            cachedFirstValue = Optional.ofNullable(firstValue);
        }
        return cachedFirstValue.orElse(null);
    }

    public V getLastValue() {
        if (cachedLastValue == null) {
            final ChunkValue latestValue = storage.getFileLookupTable().getLatestValue(hashKey, FDate.MAX_DATE);
            final V lastValue;
            if (latestValue == null) {
                lastValue = null;
            } else {
                lastValue = latestValue.getLastValue(valueSerde);
            }
            cachedLastValue = Optional.ofNullable(lastValue);
        }
        return cachedLastValue.orElse(null);
    }

    public synchronized void deleteAll() {
        storage.getFileLookupTable().deleteRange(hashKey);
        storage.getLatestValueLookupTable().deleteRange(hashKey);
        storage.getNextValueLookupTable().deleteRange(hashKey);
        storage.getPreviousValueLookupTable().deleteRange(hashKey);
        clearCaches();
        Files.deleteNative(newDataDirectory());
        dataDirectory = null;
    }

    private void clearCaches() {
        latestValueLookupCache.clear();
        nextValueLookupCache.clear();
        previousValueLookupCache.clear();
        fileLookupTable_latestRangeKeyCache.clear();
        cachedAllRangeKeys = null;
        cachedAllRangeKeysReverse = null;
        cachedFirstValue = null;
        cachedLastValue = null;
    }

    public V getLatestValue(final FDate date) {
        return latestValueLookupCache.get(date);
    }

    public V getPreviousValue(final FDate date, final int shiftBackUnits) {
        assertShiftUnitsPositiveNonZero(shiftBackUnits);
        final V firstValue = getFirstValue();
        final FDate firstTime = extractEndTime.apply(firstValue);
        if (date.isBeforeOrEqualTo(firstTime)) {
            return firstValue;
        } else {
            return previousValueLookupCache.get(Pair.of(date, shiftBackUnits));
        }
    }

    public V getNextValue(final FDate date, final int shiftForwardUnits) {
        assertShiftUnitsPositiveNonZero(shiftForwardUnits);
        final V lastValue = getLastValue();
        final FDate lastTime = extractEndTime.apply(lastValue);
        if (date.isAfterOrEqualTo(lastTime)) {
            return lastValue;
        } else {
            return nextValueLookupCache.get(Pair.of(date, shiftForwardUnits));
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
        try (ICloseableIterator<File> files = readRangeFiles(null, null, DisabledLock.INSTANCE, null).iterator()) {
            boolean noFileFound = true;
            while (files.hasNext()) {
                final File file = files.next();
                if (!file.exists()) {
                    log.warn("Table data for [%s] is inconsistent and needs to be reset. Missing file: [%s]", hashKey,
                            file);
                    return true;
                }
                if (file.length() == 0) {
                    log.warn("Table data for [%s] is inconsistent and needs to be reset. Empty file: [%s]", hashKey,
                            file);
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
    public synchronized Pair<FDate, List<V>> prepareForUpdate(final boolean shouldRedoLastFile) {
        FDate latestRangeKey = storage.getFileLookupTable().getLatestRangeKey(hashKey, FDate.MAX_DATE);
        FDate updateFrom = latestRangeKey;
        final List<V> lastValues = new ArrayList<V>();
        if (latestRangeKey != null) {
            if (shouldRedoLastFile) {
                if (redirectedFiles != null) {
                    throw new IllegalStateException("redirectedFiles should be null when shouldRedoLastFile=true");
                }
                final File lastFile = newFile(latestRangeKey);
                try (SerializingCollection<V> lastColl = newSerializingCollection("prepareForUpdate", lastFile,
                        DisabledLock.INSTANCE)) {
                    lastValues.addAll(lastColl);
                }
                //remove last value because it might be an incomplete bar
                final V lastValue = lastValues.remove(lastValues.size() - 1);
                updateFrom = extractEndTime.apply(lastValue);
                lastFile.delete();
            } else {
                latestRangeKey = latestRangeKey.addMilliseconds(1);
            }
            storage.getFileLookupTable().deleteRange(hashKey, latestRangeKey);
            storage.getLatestValueLookupTable().deleteRange(hashKey, latestRangeKey);
            storage.getNextValueLookupTable().deleteRange(hashKey); //we cannot be sure here about the date since shift keys can be arbitrarily large
            storage.getPreviousValueLookupTable().deleteRange(hashKey, new ShiftUnitsRangeKey(latestRangeKey, 0));
        }
        clearCaches();
        return Pair.of(updateFrom, lastValues);
    }

    private void assertShiftUnitsPositiveNonZero(final int shiftUnits) {
        if (shiftUnits < 0) {
            throw new IllegalArgumentException("shiftUnits needs to be a positive or zero value: " + shiftUnits);
        }
    }

    private ICloseableIterator<TableRow<String, FDate, ChunkValue>> getAllRangeKeys(final Lock readLock) {
        readLock.lock();
        try {
            if (cachedAllRangeKeys == null) {
                final BufferingIterator<TableRow<String, FDate, ChunkValue>> allRangeKeys = new BufferingIterator<TableRow<String, FDate, ChunkValue>>();
                final DelegateTableIterator<String, FDate, ChunkValue> range = storage.getFileLookupTable()
                        .range(hashKey, FDate.MIN_DATE, FDate.MAX_DATE);
                while (range.hasNext()) {
                    allRangeKeys.add(range.next());
                }
                range.close();
                cachedAllRangeKeys = allRangeKeys;
            }
            return cachedAllRangeKeys.iterator();
        } finally {
            readLock.unlock();
        }
    }

    private ICloseableIterator<TableRow<String, FDate, ChunkValue>> getAllRangeKeysReverse(final Lock readLock) {
        readLock.lock();
        try {
            if (cachedAllRangeKeysReverse == null) {
                final BufferingIterator<TableRow<String, FDate, ChunkValue>> allRangeKeysReverse = new BufferingIterator<TableRow<String, FDate, ChunkValue>>();
                final DelegateTableIterator<String, FDate, ChunkValue> range = storage.getFileLookupTable()
                        .rangeReverse(hashKey, FDate.MAX_DATE, FDate.MIN_DATE);
                while (range.hasNext()) {
                    allRangeKeysReverse.add(range.next());
                }
                range.close();
                cachedAllRangeKeysReverse = allRangeKeysReverse;
            }
            return cachedAllRangeKeysReverse.iterator();
        } finally {
            readLock.unlock();
        }
    }

    private static final class GetRangeKeysReverseIterator
            extends ASkippingIterator<TableRow<String, FDate, ChunkValue>> {
        private final FDate from;
        private final FDate to;

        private GetRangeKeysReverseIterator(
                final ICloseableIterator<? extends TableRow<String, FDate, ChunkValue>> delegate, final FDate from,
                final FDate to) {
            super(delegate);
            this.from = from;
            this.to = to;
        }

        @Override
        protected boolean skip(final TableRow<String, FDate, ChunkValue> element) {
            if (element.getRangeKey().isAfter(from)) {
                return true;
            } else if (element.getRangeKey().isBefore(to)) {
                throw new FastNoSuchElementException("getRangeKeysReverse reached end");
            }
            return false;
        }
    }

    private static final class GetRangeKeysIterator extends ASkippingIterator<TableRow<String, FDate, ChunkValue>> {
        private final FDate from;
        private final FDate to;

        private GetRangeKeysIterator(final ICloseableIterator<? extends TableRow<String, FDate, ChunkValue>> delegate,
                final FDate from, final FDate to) {
            super(delegate);
            this.from = from;
            this.to = to;
        }

        @Override
        protected boolean skip(final TableRow<String, FDate, ChunkValue> element) {
            if (element.getRangeKey().isBefore(from)) {
                return true;
            } else if (element.getRangeKey().isAfter(to)) {
                throw new FastNoSuchElementException("getRangeKeys reached end");
            }
            return false;
        }
    }

}
