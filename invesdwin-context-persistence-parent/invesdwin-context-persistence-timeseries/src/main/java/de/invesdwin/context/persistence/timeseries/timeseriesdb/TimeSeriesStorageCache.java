package de.invesdwin.context.persistence.timeseries.timeseriesdb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SerializationException;

import com.google.common.base.Function;

import de.invesdwin.context.log.Log;
import de.invesdwin.context.persistence.timeseries.ezdb.ADelegateRangeTable.DelegateTableIterator;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.storage.ChunkValue;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.storage.ShiftUnitsRangeKey;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.storage.SingleValue;
import de.invesdwin.context.persistence.timeseries.timeseriesdb.storage.TimeSeriesStorage;
import de.invesdwin.util.bean.tuple.Pair;
import de.invesdwin.util.collections.eviction.EvictionMode;
import de.invesdwin.util.collections.iterable.ACloseableIterator;
import de.invesdwin.util.collections.iterable.ASkippingIterator;
import de.invesdwin.util.collections.iterable.ATransformingCloseableIterator;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterable;
import de.invesdwin.util.collections.iterable.FlatteningIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.IReverseCloseableIterable;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;
import de.invesdwin.util.collections.loadingcache.historical.AHistoricalCache;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.time.fdate.FDate;
import ezdb.serde.Serde;

@NotThreadSafe
public class TimeSeriesStorageCache<K, V> {
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
                            final SerializingCollection<V> serializingCollection = newSerializingCollection(file);
                            try (ICloseableIterator<V> it = serializingCollection.iterator()) {
                                V latestValue = null;
                                while (it.hasNext()) {
                                    final V newValue = it.next();
                                    final FDate newValueTime = extractTime.apply(newValue);
                                    if (newValueTime.isAfter(key)) {
                                        break;
                                    } else {
                                        latestValue = newValue;
                                    }
                                }
                                if (latestValue == null) {
                                    latestValue = getFirstValue();
                                }
                                if (latestValue == null) {
                                    return null;
                                }
                                return new SingleValue(valueSerde, latestValue);
                            }
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
                                    V previousValue = null;
                                    try (ICloseableIterator<V> rangeValuesReverse = readRangeValuesReverse(date,
                                            null)) {
                                        for (int i = 0; i < shiftBackUnits; i++) {
                                            previousValue = rangeValuesReverse.next();
                                        }
                                    } catch (final NoSuchElementException e) {
                                        //ignore
                                    }
                                    return new SingleValue(valueSerde, previousValue);
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
                                    V nextValue = null;
                                    try (ICloseableIterator<V> rangeValues = readRangeValues(date, null)) {
                                        for (int i = 0; i < shiftForwardUnits; i++) {
                                            nextValue = rangeValues.next();
                                        }
                                    } catch (final NoSuchElementException e) {
                                        //ignore
                                    }
                                    return new SingleValue(valueSerde, nextValue);
                                }
                            });
            return value.getValue(valueSerde);
        }
    };
    private final ALoadingCache<FDate, FDate> fileLookupTable_latestRangeKeyCache = new ALoadingCache<FDate, FDate>() {

        @Override
        protected Integer getInitialMaximumSize() {
            return MAXIMUM_SIZE;
        }

        @Override
        protected EvictionMode getEvictionMode() {
            return EVICTION_MODE;
        }

        @Override
        protected FDate loadValue(final FDate key) {
            return storage.getFileLookupTable().getLatestRangeKey(hashKey, key);
        }
    };

    private final String hashKey;
    private final Serde<V> valueSerde;
    private final Integer fixedLength;
    private final Function<V, FDate> extractTime;
    @GuardedBy("this")
    private File dataDirectory;

    private volatile Optional<V> cachedFirstValue;
    private volatile Optional<V> cachedLastValue;
    private volatile ICloseableIterable<FDate> cachedAllRangeKeys;
    private volatile ICloseableIterable<FDate> cachedAllRangeKeysReverse;
    private final Log log = new Log(this);

    public TimeSeriesStorageCache(final TimeSeriesStorage storage, final String hashKey, final Serde<V> valueSerde,
            final Integer fixedLength, final Function<V, FDate> extractTime) {
        this.storage = storage;
        this.hashKey = hashKey;
        this.valueSerde = valueSerde;
        this.fixedLength = fixedLength;
        this.extractTime = extractTime;
    }

    private synchronized File getDataDirectory() {
        if (dataDirectory == null) {
            dataDirectory = newDataDirectory();
            try {
                FileUtils.forceMkdir(dataDirectory);
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
        return new File(getDataDirectory(), time.toString(FDate.FORMAT_TIMESTAMP_UNDERSCORE) + ".data");
    }

    public void finishFile(final FDate time, final V firstValue, final V lastValue) {
        storage.getFileLookupTable().put(hashKey, time, new ChunkValue(valueSerde, firstValue, lastValue));
        clearCaches();
    }

    protected ICloseableIterable<File> readRangeFiles(final FDate from, final FDate to) {
        final FDate usedFrom;
        if (from == null) {
            final V firstValue = getFirstValue();
            if (firstValue == null) {
                return EmptyCloseableIterable.getInstance();
            }
            usedFrom = extractTime.apply(firstValue);
        } else {
            usedFrom = from;
        }
        return new ICloseableIterable<File>() {

            @Override
            public ACloseableIterator<File> iterator() {
                return new ACloseableIterator<File>() {

                    //use latest time available even if delegate iterator has no values
                    private FDate latestFirstTime = fileLookupTable_latestRangeKeyCache.get(usedFrom);
                    // add 1 ms to not collide with firstTime
                    private final ICloseableIterator<FDate> delegate = getRangeKeys(hashKey,
                            usedFrom.addMilliseconds(1), to);
                    private FDate delegateFirstTime = null;

                    {
                        AFinalizer.valueOfCloseable(delegate).register(this);
                    }

                    @Override
                    protected boolean innerHasNext() {
                        return latestFirstTime != null || delegateFirstTime != null || delegate.hasNext();
                    }

                    private ICloseableIterator<FDate> getRangeKeys(final String hashKey, final FDate from,
                            final FDate to) {
                        final ICloseableIterator<FDate> range = getAllRangeKeys();
                        return new ASkippingIterator<FDate>(range) {
                            @Override
                            protected boolean skip(final FDate element) {
                                if (element.isBefore(from)) {
                                    return true;
                                } else if (element.isAfter(to)) {
                                    throw new FastNoSuchElementException("getRangeKeys reached end");
                                }
                                return false;
                            }
                        };
                    }

                    @Override
                    protected File innerNext() {
                        final FDate time;
                        if (delegateFirstTime != null) {
                            time = delegateFirstTime;
                            delegateFirstTime = null;
                        } else if (latestFirstTime != null) {
                            time = latestFirstTime;
                            latestFirstTime = null;
                            if (delegate.hasNext()) {
                                //prevent duplicate first times
                                delegateFirstTime = delegate.next();
                                if (delegateFirstTime.isBeforeOrEqualTo(time)) {
                                    delegateFirstTime = null;
                                }
                            }
                        } else {
                            time = delegate.next();
                        }
                        return newFile(time);
                    }

                };
            }
        };
    }

    protected ICloseableIterable<File> readRangeFilesReverse(final FDate from, final FDate to) {
        final FDate usedFrom;
        if (from == null) {
            final V lastValue = getLastValue();
            if (lastValue == null) {
                return EmptyCloseableIterable.getInstance();
            }
            usedFrom = extractTime.apply(lastValue);
        } else {
            usedFrom = from;
        }
        return new ICloseableIterable<File>() {

            @Override
            public ACloseableIterator<File> iterator() {
                return new ACloseableIterator<File>() {

                    //use latest time available even if delegate iterator has no values
                    private FDate latestLastTime = fileLookupTable_latestRangeKeyCache.get(usedFrom);
                    // add 1 ms to not collide with firstTime
                    private final ICloseableIterator<FDate> delegate = getRangeKeysReverse(hashKey,
                            usedFrom.addMilliseconds(-1), to);
                    private FDate delegateLastTime = null;

                    {
                        AFinalizer.valueOfCloseable(delegate).register(this);
                    }

                    @Override
                    protected boolean innerHasNext() {
                        return latestLastTime != null || delegateLastTime != null || delegate.hasNext();
                    }

                    private ICloseableIterator<FDate> getRangeKeysReverse(final String hashKey, final FDate from,
                            final FDate to) {
                        final ICloseableIterator<FDate> range = getAllRangeKeysReverse();
                        return new ASkippingIterator<FDate>(range) {
                            @Override
                            protected boolean skip(final FDate element) {
                                if (element.isAfter(from)) {
                                    return true;
                                } else if (element.isBefore(to)) {
                                    throw new FastNoSuchElementException("getRangeKeysReverse reached end");
                                }
                                return false;
                            }
                        };
                    }

                    @Override
                    protected File innerNext() {
                        final FDate time;
                        if (delegateLastTime != null) {
                            time = delegateLastTime;
                            delegateLastTime = null;
                        } else if (latestLastTime != null) {
                            time = latestLastTime;
                            latestLastTime = null;
                            if (delegate.hasNext()) {
                                //prevent duplicate first times
                                delegateLastTime = delegate.next();
                                if (delegateLastTime.isAfterOrEqualTo(time)) {
                                    delegateLastTime = null;
                                }
                            }
                        } else {
                            time = delegate.next();
                        }
                        return newFile(time);
                    }

                };
            }
        };
    }

    private ICloseableIterator<FDate> getAllRangeKeys() {
        if (cachedAllRangeKeys == null) {
            final BufferingIterator<FDate> allRangeKeys = new BufferingIterator<FDate>();
            final DelegateTableIterator<String, FDate, ChunkValue> range = storage.getFileLookupTable()
                    .range(hashKey, FDate.MIN_DATE, FDate.MAX_DATE);
            while (range.hasNext()) {
                allRangeKeys.add(range.next().getRangeKey());
            }
            range.close();
            cachedAllRangeKeys = allRangeKeys;
        }
        return cachedAllRangeKeys.iterator();
    }

    private ICloseableIterator<FDate> getAllRangeKeysReverse() {
        if (cachedAllRangeKeysReverse == null) {
            final BufferingIterator<FDate> allRangeKeysReverse = new BufferingIterator<FDate>();
            final DelegateTableIterator<String, FDate, ChunkValue> range = storage.getFileLookupTable()
                    .rangeReverse(hashKey, FDate.MAX_DATE, FDate.MIN_DATE);
            while (range.hasNext()) {
                allRangeKeysReverse.add(range.next().getRangeKey());
            }
            range.close();
            cachedAllRangeKeysReverse = allRangeKeysReverse;
        }
        return cachedAllRangeKeysReverse.iterator();
    }

    protected ICloseableIterator<V> readRangeValues(final FDate from, final FDate to) {
        final ICloseableIterator<File> fileIterator = readRangeFiles(from, to).iterator();
        final ICloseableIterator<ICloseableIterator<V>> chunkIterator = new ATransformingCloseableIterator<File, ICloseableIterator<V>>(
                fileIterator) {
            private boolean first = true;

            @Override
            protected ICloseableIterator<V> transform(final File value) {
                final ICloseableIterable<V> serializingCollection = newSerializingCollection(value);
                if (first) {
                    first = false;
                    if (hasNext()) {
                        return new ASkippingIterator<V>(serializingCollection.iterator()) {
                            @Override
                            protected boolean skip(final V element) {
                                final FDate time = extractTime.apply(element);
                                return time.isBefore(from);
                            }
                        };
                        //first and last
                    } else {
                        return new ASkippingIterator<V>(serializingCollection.iterator()) {
                            @Override
                            protected boolean skip(final V element) {
                                final FDate time = extractTime.apply(element);
                                if (time.isBefore(from)) {
                                    return true;
                                } else if (time.isAfter(to)) {
                                    throw new FastNoSuchElementException("getRangeValues reached end");
                                }
                                return false;
                            }
                        };
                    }
                    //last
                } else if (!hasNext()) {
                    return new ASkippingIterator<V>(serializingCollection.iterator()) {

                        @Override
                        protected boolean skip(final V element) {
                            final FDate time = extractTime.apply(element);
                            if (time.isAfter(to)) {
                                throw new FastNoSuchElementException("getRangeValues reached end");
                            }
                            return false;
                        }
                    };
                } else {
                    return serializingCollection.iterator();
                }
            }

        };

        //        final ATransformingCloseableIterator<ICloseableIterator<V>, ICloseableIterator<V>> transformer = new ATransformingCloseableIterator<ICloseableIterator<V>, ICloseableIterator<V>>(
        //                chunkIterator) {
        //            @Override
        //            protected ICloseableIterator<V> transform(final ICloseableIterator<V> value) {
        //                //keep file open as shortly as possible to fix too many open files exception
        //                return new BufferingIterator<V>(value);
        //            }
        //        };
        //single threaded is 20% better than with producerqueue
        final FlatteningIterator<V> flatteningIterator = new FlatteningIterator<V>(chunkIterator);
        return flatteningIterator;
    }

    protected ICloseableIterator<V> readRangeValuesReverse(final FDate from, final FDate to) {
        final ICloseableIterator<File> fileIterator = readRangeFilesReverse(from, to).iterator();
        final ICloseableIterator<ICloseableIterator<V>> chunkIterator = new ATransformingCloseableIterator<File, ICloseableIterator<V>>(
                fileIterator) {
            private boolean first = true;

            @Override
            protected ICloseableIterator<V> transform(final File value) {
                final IReverseCloseableIterable<V> serializingCollection = newSerializingCollection(value);
                if (first) {
                    first = false;
                    if (hasNext()) {
                        return new ASkippingIterator<V>(serializingCollection.reverseIterator()) {
                            @Override
                            protected boolean skip(final V element) {
                                final FDate time = extractTime.apply(element);
                                return time.isAfter(from);
                            }
                        };
                        //first and last
                    } else {
                        return new ASkippingIterator<V>(serializingCollection.reverseIterator()) {
                            @Override
                            protected boolean skip(final V element) {
                                final FDate time = extractTime.apply(element);
                                if (time.isAfter(from)) {
                                    return true;
                                } else if (time.isBefore(to)) {
                                    throw new FastNoSuchElementException("getRangeValues reached end");
                                }
                                return false;
                            }
                        };
                    }
                    //last
                } else if (!hasNext()) {
                    return new ASkippingIterator<V>(serializingCollection.reverseIterator()) {

                        @Override
                        protected boolean skip(final V element) {
                            final FDate time = extractTime.apply(element);
                            if (time.isBefore(to)) {
                                throw new FastNoSuchElementException("getRangeValues reached end");
                            }
                            return false;
                        }
                    };
                } else {
                    return serializingCollection.reverseIterator();
                }
            }

        };

        //        final ATransformingCloseableIterator<ICloseableIterator<V>, ICloseableIterator<V>> transformer = new ATransformingCloseableIterator<ICloseableIterator<V>, ICloseableIterator<V>>(
        //                chunkIterator) {
        //            @Override
        //            protected ICloseableIterator<V> transform(final ICloseableIterator<V> value) {
        //                //keep file open as shortly as possible to fix too many open files exception
        //                return new BufferingIterator<V>(value);
        //            }
        //        };
        //single threaded is 20% better than with producerqueue
        final FlatteningIterator<V> flatteningIterator = new FlatteningIterator<V>(chunkIterator);
        return flatteningIterator;
    }

    private SerializingCollection<V> newSerializingCollection(final File file) {
        return new SerializingCollection<V>(file, true) {

            @Override
            protected Serde<V> newSerde() {
                return new Serde<V>() {
                    @Override
                    public V fromBytes(final byte[] bytes) {
                        return valueSerde.fromBytes(bytes);
                    }

                    @Override
                    public byte[] toBytes(final V obj) {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            protected InputStream newFileInputStream(final File file) throws FileNotFoundException {
                //keep file input stream open as shorty as possible to prevent too many open files error
                try (InputStream fis = super.newFileInputStream(file)) {
                    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    IOUtils.copy(fis, bos);
                    return new ByteArrayInputStream(bos.toByteArray());
                } catch (final IOException e) {
                    throw new RuntimeException(e);
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
        FileUtils.deleteQuietly(newDataDirectory());
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
        final FDate firstTime = extractTime.apply(firstValue);
        if (date.isBeforeOrEqualTo(firstTime)) {
            return firstValue;
        } else {
            return previousValueLookupCache.get(Pair.of(date, shiftBackUnits));
        }
    }

    public V getNextValue(final FDate date, final int shiftForwardUnits) {
        assertShiftUnitsPositiveNonZero(shiftForwardUnits);
        final V lastValue = getLastValue();
        final FDate lastTime = extractTime.apply(lastValue);
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
        try (ICloseableIterator<File> files = readRangeFiles(null, null).iterator()) {
            boolean noFileFound = true;
            while (files.hasNext()) {
                final File file = files.next();
                if (!file.exists()) {
                    log.warn("Table data for [%s] is inconsistent and needs to be reset. Missing file: [%s]", hashKey,
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
                final File lastFile = newFile(latestRangeKey);
                try (SerializingCollection<V> lastColl = newSerializingCollection(lastFile)) {
                    lastValues.addAll(lastColl);
                }
                //remove last value because it might be an incomplete bar
                final V lastValue = lastValues.remove(lastValues.size() - 1);
                updateFrom = extractTime.apply(lastValue);
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
        if (shiftUnits <= 0) {
            throw new IllegalArgumentException("shiftUnits needs to be a positive non zero value: " + shiftUnits);
        }
    }

}
