package de.invesdwin.context.persistence.leveldb.timeseries;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Optional;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SerializationException;

import com.google.common.base.Function;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.persistence.leveldb.ADelegateRangeTable;
import de.invesdwin.context.persistence.leveldb.ADelegateRangeTable.DelegateTableIterator;
import de.invesdwin.norva.marker.ISerializableValueObject;
import de.invesdwin.util.bean.tuple.Pair;
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
import de.invesdwin.util.time.fdate.FDate;
import ezdb.serde.Serde;

@ThreadSafe
public class TimeSeriesFileLookupTableCache<K, V> {

    private static final Void DUMMY_HASH_KEY = null;
    private final ADelegateRangeTable<Void, FDate, ChunkValue> fileLookupTable;
    private final ADelegateRangeTable<Void, FDate, V> latestValueLookupTable;
    private final ALoadingCache<FDate, V> latestValueLookupCache = new ALoadingCache<FDate, V>() {

        @Override
        protected Integer getInitialMaximumSize() {
            return AHistoricalCache.DEFAULT_MAXIMUM_SIZE;
        }

        @Override
        protected V loadValue(final FDate key) {
            final V value = latestValueLookupTable.getOrLoad(DUMMY_HASH_KEY, key, new Function<Pair<Void, FDate>, V>() {

                @Override
                public V apply(final Pair<Void, FDate> input) {
                    final FDate fileTime = fileLookupTable.getLatestRangeKey(input.getFirst(), input.getSecond());
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
                        return latestValue;
                    }
                }
            });
            return value;
        }
    };
    private final ADelegateRangeTable<Integer, FDate, V> previousValueLookupTable;
    private final ALoadingCache<Pair<FDate, Integer>, V> previousValueLookupCache = new ALoadingCache<Pair<FDate, Integer>, V>() {

        @Override
        protected Integer getInitialMaximumSize() {
            return AHistoricalCache.DEFAULT_MAXIMUM_SIZE;
        }

        @Override
        protected V loadValue(final Pair<FDate, Integer> key) {
            final FDate date = key.getFirst();
            final int shiftBackUnits = key.getSecond();
            final V value = previousValueLookupTable.getOrLoad(shiftBackUnits, date,
                    new Function<Pair<Integer, FDate>, V>() {

                        @Override
                        public V apply(final Pair<Integer, FDate> input) {
                            final FDate date = key.getFirst();
                            final int shiftBackUnits = key.getSecond();
                            V previousValue = null;
                            try (ICloseableIterator<V> rangeValuesReverse = readRangeValuesReverse(date, null)) {
                                for (int i = 0; i < shiftBackUnits; i++) {
                                    previousValue = rangeValuesReverse.next();
                                }
                            } catch (final NoSuchElementException e) {
                                //ignore
                            }
                            return previousValue;
                        }
                    });
            return value;
        }
    };
    private final ADelegateRangeTable<Integer, FDate, V> nextValueLookupTable;
    private final ALoadingCache<Pair<FDate, Integer>, V> nextValueLookupCache = new ALoadingCache<Pair<FDate, Integer>, V>() {

        @Override
        protected Integer getInitialMaximumSize() {
            return AHistoricalCache.DEFAULT_MAXIMUM_SIZE;
        }

        @Override
        protected V loadValue(final Pair<FDate, Integer> key) {
            final FDate date = key.getFirst();
            final int shiftBackUnits = key.getSecond();
            final V value = nextValueLookupTable.getOrLoad(shiftBackUnits, date,
                    new Function<Pair<Integer, FDate>, V>() {

                        @Override
                        public V apply(final Pair<Integer, FDate> input) {
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
                            return nextValue;
                        }
                    });
            return value;
        }
    };
    private final ALoadingCache<FDate, FDate> fileLookupTable_latestRangeKeyCache = new ALoadingCache<FDate, FDate>() {

        @Override
        protected Integer getInitialMaximumSize() {
            return AHistoricalCache.DEFAULT_MAXIMUM_SIZE;
        }

        @Override
        protected FDate loadValue(final FDate key) {
            return fileLookupTable.getLatestRangeKey(DUMMY_HASH_KEY, key);
        }
    };

    private final String key;
    private final Serde<V> valueSerde;
    private final Integer fixedLength;
    private final Function<V, FDate> extractTime;
    @GuardedBy("this")
    private File baseDir;

    private volatile Optional<V> cachedFirstValue;
    private volatile Optional<V> cachedLastValue;
    private volatile ICloseableIterable<FDate> cachedAllRangeKeys;
    private volatile ICloseableIterable<FDate> cachedAllRangeKeysReverse;
    private final Log log = new Log(this);

    public TimeSeriesFileLookupTableCache(final String key, final Serde<V> valueSerde, final Integer fixedLength,
            final Function<V, FDate> extractTime) {
        this.key = key;
        this.valueSerde = valueSerde;
        this.fixedLength = fixedLength;
        this.extractTime = extractTime;

        this.fileLookupTable = new ADelegateRangeTable<Void, FDate, ChunkValue>("fileLookupTable") {
            @Override
            protected boolean allowPutWithoutBatch() {
                return true;
            }

            @Override
            protected boolean allowHasNext() {
                return true;
            }

            @Override
            protected File getDirectory() {
                return getBaseDir();
            }

        };
        this.latestValueLookupTable = new ADelegateRangeTable<Void, FDate, V>("latestValueLookupTable") {
            @Override
            protected File getDirectory() {
                return getBaseDir();
            }

            @Override
            protected Serde<V> newValueSerde() {
                return valueSerde;
            }

        };
        this.nextValueLookupTable = new ADelegateRangeTable<Integer, FDate, V>("nextValueLookupTable") {

            @Override
            protected File getDirectory() {
                return getBaseDir();
            }

            @Override
            protected Serde<V> newValueSerde() {
                return valueSerde;
            }

        };
        this.previousValueLookupTable = new ADelegateRangeTable<Integer, FDate, V>("previousValueLookupTable") {

            @Override
            protected File getDirectory() {
                return getBaseDir();
            }

            @Override
            protected Serde<V> newValueSerde() {
                return valueSerde;
            }

        };

    }

    public synchronized File getBaseDir() {
        if (baseDir == null) {
            baseDir = new File(ContextProperties.getHomeDirectory(),
                    getClass().getSimpleName() + "/" + key.replace(":", "_"));
            try {
                FileUtils.forceMkdir(baseDir);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
        return baseDir;
    }

    public File getUpdateLockFile() {
        return new File(getBaseDir(), "updateRunning.lock");
    }

    public File newFile(final FDate time) {
        return new File(getBaseDir(), time.toString(FDate.FORMAT_TIMESTAMP_UNDERSCORE) + ".data");
    }

    public void finishFile(final FDate time, final V firstValue, final V lastValue) {
        fileLookupTable.put(DUMMY_HASH_KEY, time, new ChunkValue(valueSerde, firstValue, lastValue));
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
                    private final ICloseableIterator<FDate> delegate = getRangeKeys(key, usedFrom.addMilliseconds(1),
                            to);
                    private FDate delegateFirstTime = null;

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

                    @Override
                    protected void innerClose() {
                        delegate.close();
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
                    private final ICloseableIterator<FDate> delegate = getRangeKeysReverse(key,
                            usedFrom.addMilliseconds(-1), to);
                    private FDate delegateLastTime = null;

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

                    @Override
                    protected void innerClose() {
                        delegate.close();
                    }
                };
            }
        };
    }

    private ICloseableIterator<FDate> getAllRangeKeys() {
        if (cachedAllRangeKeys == null) {
            final BufferingIterator<FDate> allRangeKeys = new BufferingIterator<FDate>();
            final DelegateTableIterator<Void, FDate, ChunkValue> range = fileLookupTable.range(DUMMY_HASH_KEY,
                    FDate.MIN_DATE, FDate.MAX_DATE);
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
            final DelegateTableIterator<Void, FDate, ChunkValue> range = fileLookupTable.rangeReverse(DUMMY_HASH_KEY,
                    FDate.MAX_DATE, FDate.MIN_DATE);
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
            protected Integer getFixedLength() {
                return fixedLength;
            }
        };
    }

    public V getFirstValue() {
        if (cachedFirstValue == null) {
            final ChunkValue latestValue = fileLookupTable.getLatestValue(DUMMY_HASH_KEY, FDate.MIN_DATE);
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
            final ChunkValue latestValue = fileLookupTable.getLatestValue(DUMMY_HASH_KEY, FDate.MAX_DATE);
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
        fileLookupTable.deleteTable();
        latestValueLookupTable.deleteTable();
        nextValueLookupTable.deleteTable();
        previousValueLookupTable.deleteTable();
        clearCaches();
        FileUtils.deleteQuietly(getBaseDir());
        baseDir = null;
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

    private static class ChunkValue implements ISerializableValueObject {

        private final byte[] firstValue;
        private final byte[] lastValue;

        <_V> ChunkValue(final Serde<_V> serde, final _V firstValue, final _V lastValue) {
            this.firstValue = serde.toBytes(firstValue);
            this.lastValue = serde.toBytes(lastValue);
        }

        public <_V> _V getFirstValue(final Serde<_V> serde) {
            return serde.fromBytes(firstValue);
        }

        public <_V> _V getLastValue(final Serde<_V> serde) {
            return serde.fromBytes(lastValue);
        }
    }

    public V getLatestValue(final FDate date) {
        return latestValueLookupCache.get(date);
    }

    public V getPreviousValue(final FDate date, final int shiftBackUnits) {
        assertShiftUnitsPositiveNonZero(shiftBackUnits);
        return previousValueLookupCache.get(Pair.of(date, shiftBackUnits));
    }

    public V getNextValue(final FDate date, final int shiftForwardUnits) {
        assertShiftUnitsPositiveNonZero(shiftForwardUnits);
        return nextValueLookupCache.get(Pair.of(date, shiftForwardUnits));
    }

    public boolean isEmptyOrInconsistent() {
        try {
            getFirstValue();
            getLastValue();
        } catch (final Throwable t) {
            if (Throwables.isCausedByType(t, SerializationException.class)) {
                //e.g. fst: unable to find class for code 88 after version upgrade
                log.warn("Table data for [%s] is inconsistent and needs to be reset. Exception during getLastValue: %s",
                        key, t.toString());
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
                    log.warn("Table data for [%s] is inconsistent and needs to be reset. Missing file: [%s]", key,
                            file);
                    return true;
                }
                noFileFound = false;

            }
            return noFileFound;
        }
    }

    /**
     * Deletes the last file in order to create a new updated one (so the files do not get fragmented too much between
     * updates
     */
    public FDate prepareForUpdate() {
        final FDate latestRangeKey = fileLookupTable.getLatestRangeKey(null, FDate.MAX_DATE);
        if (latestRangeKey != null) {
            newFile(latestRangeKey).delete();
            latestValueLookupTable.deleteRange(null, latestRangeKey);
        }
        clearCaches();
        return latestRangeKey;
    }

    private void assertShiftUnitsPositiveNonZero(final int shiftUnits) {
        if (shiftUnits <= 0) {
            throw new IllegalArgumentException("shiftUnits needs to be a positive non zero value: " + shiftUnits);
        }
    }

}
