package de.invesdwin.context.persistence.leveldb.chronicle;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.leveldb.serde.ExtendedTypeDelegateSerde;
import de.invesdwin.context.persistence.leveldb.timeseries.ATimeSeriesUpdater;
import de.invesdwin.util.lang.Reflections;
import ezdb.serde.Serde;
import net.openhft.chronicle.hash.serialization.impl.SerializationBuilder;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

@ThreadSafe
public abstract class ADelegateChronicleMap<K, V> implements ConcurrentMap<K, V>, Closeable {

    public static final int DEFAULT_EXPECTED_ENTRIES = ATimeSeriesUpdater.BATCH_FLUSH_INTERVAL;
    /**
     * don't force any maximum size, just degrade performance
     */
    public static final int DEFAULT_MAX_BLOAT_FACTOR = 1000;

    private final String name;
    private final ADelegateMarshallable<K> keyMarshallable;
    private final ADelegateMarshallable<V> valueMarshallable;
    @GuardedBy("this")
    private ChronicleMap<K, V> delegate;

    public ADelegateChronicleMap(final String name) {
        this.name = name;
        this.keyMarshallable = newKeyMarshallable();
        this.valueMarshallable = newValueMarshallable();
    }

    public String getName() {
        return name;
    }

    protected synchronized ChronicleMap<K, V> getDelegate() {
        if (delegate == null) {
            this.delegate = newDelegate();
        }
        return delegate;
    }

    protected ChronicleMap<K, V> newDelegate() {
        try {
            final ChronicleMapBuilder<K, V> builder = ChronicleMapBuilder.of(getKeyType(), getValueType());

            if (!isSizeStaticallyKnown(getKeyType())) {
                final Integer keyAverageLength = getKeyAverageLength();
                if (keyAverageLength != null) {
                    builder.averageKeySize(keyAverageLength);
                } else {
                    builder.averageKey(getKeyAverageSample());
                }
                builder.keyMarshallers(keyMarshallable, keyMarshallable);
            }
            if (!isSizeStaticallyKnown(getValueType())) {
                final Integer valueAverageLength = getValueAverageLength();
                if (valueAverageLength != null) {
                    builder.averageValueSize(valueAverageLength);
                } else {
                    builder.averageValue(getValueAverageSample());
                }
                builder.valueMarshallers(valueMarshallable, valueMarshallable);
            }

            builder.checksumEntries(isChecksumEnabled());
            builder.entries(getExpectedEntries());
            builder.maxBloatFactor(getMaximumBloatFactor());

            //create
            return create(builder);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isSizeStaticallyKnown(final Class<?> type) {
        return new SerializationBuilder<>(type).sizeIsStaticallyKnown;
    }

    protected ChronicleMap<K, V> create(final ChronicleMapBuilder<K, V> builder) throws IOException {
        final File file = getFile();
        if (file.exists() && file.length() == 0) {
            //startup failed, file is empty
            FileUtils.deleteQuietly(file);
        }
        FileUtils.forceMkdir(file.getParentFile());
        return builder.createOrRecoverPersistedTo(file);
    }

    protected int getExpectedEntries() {
        return DEFAULT_EXPECTED_ENTRIES;
    }

    protected int getMaximumBloatFactor() {
        return DEFAULT_MAX_BLOAT_FACTOR;
    }

    /**
     * Disabled for performance reasons.
     */
    protected boolean isChecksumEnabled() {
        return false;
    }

    protected Integer getKeyAverageLength() {
        return null;
    }

    protected K getKeyAverageSample() {
        throw new UnsupportedOperationException(
                "Not implemented. You must either provide an average length or an average sample.");
    }

    protected ADelegateMarshallable<K> newKeyMarshallable() {
        return newDefaultMarshallableStatic(getKeyType());
    }

    protected Integer getValueAverageLength() {
        return null;
    }

    protected V getValueAverageSample() {
        throw new UnsupportedOperationException(
                "Not implemented. You must either provide an average length or an average sample.");
    }

    protected ADelegateMarshallable<V> newValueMarshallable() {
        return newDefaultMarshallableStatic(getValueType());
    }

    private static <T> ADelegateMarshallable<T> newDefaultMarshallableStatic(final Class<T> type) {
        return new ADelegateMarshallable<T>() {
            @Override
            protected Serde<T> newSerde() {
                return new ExtendedTypeDelegateSerde<T>(type);
            }
        };
    }

    protected File getFile() {
        return new File(getDirectory(), name);
    }

    protected File getDirectory() {
        return new File(getBaseDirectory(), ADelegateChronicleMap.class.getSimpleName());
    }

    protected File getBaseDirectory() {
        return ContextProperties.getHomeDirectory();
    }

    @SuppressWarnings("unchecked")
    protected Class<K> getKeyType() {
        return (Class<K>) Reflections.resolveTypeArguments(getClass(), ADelegateChronicleMap.class)[0];
    }

    @SuppressWarnings("unchecked")
    protected Class<V> getValueType() {
        return (Class<V>) Reflections.resolveTypeArguments(getClass(), ADelegateChronicleMap.class)[1];
    }

    @Override
    public int size() {
        return getDelegate().size();
    }

    @Override
    public boolean isEmpty() {
        return getDelegate().isEmpty();
    }

    @Override
    public boolean containsKey(final Object key) {
        return getDelegate().containsKey(key);
    }

    @Override
    public boolean containsValue(final Object value) {
        return getDelegate().containsValue(value);
    }

    @Override
    public V get(final Object key) {
        return getDelegate().get(key);
    }

    @Override
    public V put(final K key, final V value) {
        return getDelegate().put(key, value);
    }

    @Override
    public V remove(final Object key) {
        return getDelegate().remove(key);
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> m) {
        getDelegate().putAll(m);
    }

    @Override
    public void clear() {
        getDelegate().clear();
    }

    @Override
    public Set<K> keySet() {
        return getDelegate().keySet();
    }

    @Override
    public Collection<V> values() {
        return getDelegate().values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return getDelegate().entrySet();
    }

    @Override
    public V putIfAbsent(final K key, final V value) {
        return getDelegate().putIfAbsent(key, value);
    }

    @Override
    public boolean remove(final Object key, final Object value) {
        return getDelegate().remove(key, value);
    }

    @Override
    public boolean replace(final K key, final V oldValue, final V newValue) {
        return getDelegate().replace(key, oldValue, newValue);
    }

    @Override
    public V replace(final K key, final V value) {
        return getDelegate().replace(key, value);
    }

    @Override
    public synchronized void close() {
        if (delegate != null) {
            delegate.close();
            delegate = null;
        }
    }

    public synchronized boolean isClosed() {
        return delegate == null;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        close();
    }
}
