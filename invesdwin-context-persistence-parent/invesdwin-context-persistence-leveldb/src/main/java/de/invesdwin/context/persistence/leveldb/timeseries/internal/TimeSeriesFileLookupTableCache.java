package de.invesdwin.context.persistence.leveldb.timeseries.internal;

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
import de.invesdwin.context.persistence.leveldb.serde.lazy.ILazySerdeValue;
import de.invesdwin.context.persistence.leveldb.serde.lazy.ILazySerdeValueSerde;
import de.invesdwin.context.persistence.leveldb.serde.lazy.RemoteLazySerdeValueSerde;
import de.invesdwin.context.persistence.leveldb.timeseries.SerializingCollection;
import de.invesdwin.norva.marker.ISerializableValueObject;
import de.invesdwin.util.bean.tuple.Pair;
import de.invesdwin.util.collections.iterable.ACloseableIterator;
import de.invesdwin.util.collections.iterable.ASkippingIterator;
import de.invesdwin.util.collections.iterable.ATransformingCloseableIterator;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterable;
import de.invesdwin.util.collections.iterable.FlatteningIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;
import de.invesdwin.util.collections.loadingcache.historical.AHistoricalCache;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.time.fdate.FDate;
import ezdb.serde.Serde;

@ThreadSafe
public class TimeSeriesFileLookupTableCache<K, V> {

    private static final Void DUMMY_HASH_KEY = null;
    private final ADelegateRangeTable<Void, FDate, ChunkValue> fileLookupTable;
    private final ADelegateRangeTable<Void, FDate, byte[]> timeLookupTable;
    private final ILazySerdeValueSerde<V> lazyValueSerde;
    private final ALoadingCache<FDate, ILazySerdeValue<? extends V>> timeLookupTable_latestValueCache = new ALoadingCache<FDate, ILazySerdeValue<? extends V>>() {

        @Override
        protected Integer getMaximumSize() {
            return AHistoricalCache.DEFAULT_MAXIMUM_SIZE;
        }

        @Override
        protected ILazySerdeValue<? extends V> loadValue(final FDate key) {
            final byte[] value = timeLookupTable.getOrLoad(DUMMY_HASH_KEY, key,
                    new Function<Pair<Void, FDate>, byte[]>() {

                        @Override
                        public byte[] apply(final Pair<Void, FDate> input) {
                            final FDate fileTime = fileLookupTable.getLatestRangeKey(input.getFirst(),
                                    input.getSecond());
                            if (fileTime == null) {
                                return null;
                            }
                            final File file = newFile(fileTime);
                            final SerializingCollection<ILazySerdeValue<? extends V>> serializingCollection = newSerializingCollection(
                                    file, lazyValueSerde);
                            try (ICloseableIterator<ILazySerdeValue<? extends V>> it = serializingCollection
                                    .iterator()) {
                                ILazySerdeValue<? extends V> latestValue = null;
                                while (it.hasNext()) {
                                    final ILazySerdeValue<? extends V> newValue = it.next();
                                    final FDate newValueTime = extractTime.apply(newValue);
                                    if (newValueTime.isAfter(key)) {
                                        break;
                                    } else {
                                        latestValue = newValue;
                                    }
                                }
                                if (latestValue == null) {
                                    latestValue = getFirstValue(lazyValueSerde);
                                }
                                return lazyValueSerde.toBytes(latestValue);
                            }
                        }
                    });
            if (value == null) {
                return null;
            } else {
                return lazyValueSerde.fromBytes(value);
            }
        }
    };
    private final ALoadingCache<FDate, FDate> fileLookupTable_latestRangeKeyCache = new ALoadingCache<FDate, FDate>() {

        @Override
        protected Integer getMaximumSize() {
            return AHistoricalCache.DEFAULT_MAXIMUM_SIZE;
        }

        @Override
        protected FDate loadValue(final FDate key) {
            return fileLookupTable.getLatestRangeKey(DUMMY_HASH_KEY, key);
        }
    };

    private final String key;
    private final Integer fixedLength;
    private final Function<ILazySerdeValue<? extends V>, FDate> extractTime;
    @GuardedBy("this")
    private File baseDir;

    private volatile Optional<ILazySerdeValue<? extends V>> cachedFirstValue;
    private volatile Optional<ILazySerdeValue<? extends V>> cachedLastValue;
    private volatile ICloseableIterable<FDate> cachedAllRangeKeys;
    private final Log log = new Log(this);

    public TimeSeriesFileLookupTableCache(final String key, final Serde<? extends V> defaultValueSerde,
            final Integer fixedLength, final Function<ILazySerdeValue<? extends V>, FDate> extractTime) {
        this.key = key;
        this.lazyValueSerde = new RemoteLazySerdeValueSerde<V>(defaultValueSerde, false);
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
        this.timeLookupTable = new ADelegateRangeTable<Void, FDate, byte[]>("timeLookupTable") {

            @Override
            protected File getDirectory() {
                return getBaseDir();
            }

        };

    }

    public synchronized File getBaseDir() {
        if (baseDir == null) {
            baseDir = new File(ContextProperties.getHomeDirectory(), getClass().getSimpleName() + "/" + key);
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

    @SuppressWarnings("unchecked")
    public void finishFile(final FDate time, final V firstValue, final V lastValue) {
        fileLookupTable.put(DUMMY_HASH_KEY, time,
                new ChunkValue((Serde<V>) lazyValueSerde.getDelegate(), firstValue, lastValue));
        clearCaches();
    }

    protected ICloseableIterable<File> readRangeFiles(final FDate from, final FDate to,
            final ILazySerdeValueSerde<? extends V> valueSerde) {
        final FDate usedFrom;
        if (from == null) {
            final ILazySerdeValue<? extends V> firstValue = getFirstValue(valueSerde);
            if (firstValue == null) {
                return new EmptyCloseableIterable<File>();
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
                                    throw new NoSuchElementException();
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
                                if (delegateFirstTime.isBeforeOrEqual(time)) {
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

    public ICloseableIterator<ILazySerdeValue<? extends V>> readRangeValues(final FDate from, final FDate to,
            final ILazySerdeValueSerde<? extends V> valueSerde) {
        final ICloseableIterator<File> fileIterator = readRangeFiles(from, to, valueSerde).iterator();
        final ICloseableIterator<ICloseableIterator<ILazySerdeValue<? extends V>>> chunkIterator = new ATransformingCloseableIterator<File, ICloseableIterator<ILazySerdeValue<? extends V>>>(
                fileIterator) {
            private boolean first = true;

            @Override
            protected ICloseableIterator<ILazySerdeValue<? extends V>> transform(final File value) {
                final ICloseableIterable<ILazySerdeValue<? extends V>> serializingCollection = newSerializingCollection(
                        value, valueSerde);
                if (first) {
                    first = false;
                    if (hasNext()) {
                        return new ASkippingIterator<ILazySerdeValue<? extends V>>(serializingCollection.iterator()) {
                            @Override
                            protected boolean skip(final ILazySerdeValue<? extends V> element) {
                                final FDate time = extractTime.apply(element);
                                return time.isBefore(from);
                            }
                        };
                        //first and last
                    } else {
                        return new ASkippingIterator<ILazySerdeValue<? extends V>>(serializingCollection.iterator()) {
                            @Override
                            protected boolean skip(final ILazySerdeValue<? extends V> element) {
                                final FDate time = extractTime.apply(element);
                                if (time.isBefore(from)) {
                                    return true;
                                } else if (time.isAfter(to)) {
                                    throw new NoSuchElementException();
                                }
                                return false;
                            }
                        };
                    }
                    //last
                } else if (!hasNext()) {
                    return new ASkippingIterator<ILazySerdeValue<? extends V>>(serializingCollection.iterator()) {
                        @Override
                        protected boolean skip(final ILazySerdeValue<? extends V> element) {
                            final FDate time = extractTime.apply(element);
                            if (time.isAfter(to)) {
                                throw new NoSuchElementException();
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
        final FlatteningIterator<ILazySerdeValue<? extends V>> flatteningIterator = new FlatteningIterator<ILazySerdeValue<? extends V>>(
                chunkIterator);
        return flatteningIterator;
    }

    private <T> SerializingCollection<ILazySerdeValue<? extends T>> newSerializingCollection(final File file,
            final ILazySerdeValueSerde<? extends T> valueSerde) {
        return new SerializingCollection<ILazySerdeValue<? extends T>>(file, true) {

            @Override
            protected byte[] toBytes(final ILazySerdeValue<? extends T> element) {
                throw new UnsupportedOperationException();
            }

            @Override
            protected ILazySerdeValue<? extends T> fromBytes(final byte[] bytes) {
                return valueSerde.fromBytes(bytes);
            }

            @Override
            protected Integer getFixedLength() {
                return fixedLength;
            }
        };
    }

    public ILazySerdeValue<? extends V> getFirstValue(final ILazySerdeValueSerde<? extends V> valueSerde) {
        if (cachedFirstValue == null) {
            final ChunkValue latestValue = fileLookupTable.getLatestValue(DUMMY_HASH_KEY, FDate.MIN_DATE);
            final ILazySerdeValue<? extends V> firstValue;
            if (latestValue == null) {
                firstValue = null;
            } else {
                firstValue = latestValue.getFirstValue(valueSerde);
            }
            cachedFirstValue = Optional.ofNullable(firstValue);
        }
        return cachedFirstValue.orElse(null);
    }

    public ILazySerdeValue<? extends V> getLastValue(final ILazySerdeValueSerde<? extends V> valueSerde) {
        if (cachedLastValue == null) {
            final ChunkValue latestValue = fileLookupTable.getLatestValue(DUMMY_HASH_KEY, FDate.MAX_DATE);
            final ILazySerdeValue<? extends V> lastValue;
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
        timeLookupTable.deleteTable();
        clearCaches();
        FileUtils.deleteQuietly(getBaseDir());
        baseDir = null;
    }

    private void clearCaches() {
        timeLookupTable_latestValueCache.clear();
        fileLookupTable_latestRangeKeyCache.clear();
        cachedAllRangeKeys = null;
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

        public <_V> ILazySerdeValue<? extends _V> getFirstValue(final ILazySerdeValueSerde<_V> serde) {
            return serde.fromBytes(firstValue);
        }

        public <_V> ILazySerdeValue<? extends _V> getLastValue(final ILazySerdeValueSerde<_V> serde) {
            return serde.fromBytes(lastValue);
        }
    }

    public ILazySerdeValue<? extends V> getLatestValue(final FDate date,
            final ILazySerdeValueSerde<V> valueSerdeOverride) {
        return valueSerdeOverride.maybeRewrap(timeLookupTable_latestValueCache.get(date));
    }

    public boolean isEmptyOrInconsistent() {
        try {
            getFirstValue(lazyValueSerde);
            getLastValue(lazyValueSerde);
        } catch (final Throwable t) {
            if (Throwables.isCausedByType(t, SerializationException.class)) {
                //e.g. fst: unable to find class for code 88 after version upgrade
                log.warn("Table data for [%s] is inconsistent and needs to be reset. Exception during getLastValue: %s",
                        key, t.toString());
                return true;
            } else {
                //unexpected exception, since FastSerializingSerde only throws SerializingException
                throw Throwables.propagate(t);
            }
        }
        try (final ICloseableIterator<File> files = readRangeFiles(null, null, lazyValueSerde).iterator()) {
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

}
