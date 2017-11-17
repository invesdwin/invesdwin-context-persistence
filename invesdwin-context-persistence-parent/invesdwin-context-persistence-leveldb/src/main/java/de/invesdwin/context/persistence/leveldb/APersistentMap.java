package de.invesdwin.context.persistence.leveldb;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.leveldb.serde.ExtendedTypeDelegateSerde;
import de.invesdwin.context.persistence.leveldb.timeseries.ATimeSeriesUpdater;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Reflections;
import ezdb.serde.Serde;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

@ThreadSafe
public abstract class APersistentMap<K, V> implements ConcurrentMap<K, V>, Closeable {

    private final String name;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    @GuardedBy("this")
    private ChronicleMap<K, V> delegate;

    public APersistentMap(final String name) {
        this.name = name;
        this.keySerde = newKeySerde();
        this.valueSerde = newValueSerde();
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

            //length
            final Integer keyFixedLength = getKeyFixedLength();
            if (keyFixedLength != null) {
                builder.keySizeMarshaller(SizeMarshaller.constant(keyFixedLength));
                builder.averageKeySize(keyFixedLength);
            } else {
                builder.averageKeySize(getKeyAverageLength());
            }
            final Integer valueFixedLength = getValueFixedLength();
            if (valueFixedLength != null) {
                builder.keySizeMarshaller(SizeMarshaller.constant(valueFixedLength));
                builder.averageValueSize(valueFixedLength);
            } else {
                builder.averageValueSize(getValueAverageLength());
            }

            //serde
            final BytesReader<K> keyReader = newBytesReader(keySerde);
            final BytesWriter<? super K> keyWriter = newBytesWriter(keySerde);
            builder.keyMarshallers(keyReader, keyWriter);
            final BytesReader<V> valueReader = newBytesReader(valueSerde);
            final BytesWriter<? super V> valueWriter = newBytesWriter(valueSerde);
            builder.valueMarshallers(valueReader, valueWriter);

            builder.checksumEntries(isChecksumEnabled());
            builder.entries(getExpectedSize());
            builder.maxBloatFactor(Double.MAX_VALUE); //don't force any maximum size, just degrade performance

            //create
            return builder.createOrRecoverPersistedTo(new File(getDirectory(), name));
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected int getExpectedSize() {
        return ATimeSeriesUpdater.BATCH_FLUSH_INTERVAL;
    }

    /**
     * Disabled for performance reasons.
     */
    protected boolean isChecksumEnabled() {
        return false;
    }

    @SuppressWarnings("rawtypes")
    private static <T> BytesWriter<T> newBytesWriter(final Serde<T> serde) {
        return new BytesWriter<T>() {
            @Override
            public void write(final net.openhft.chronicle.bytes.Bytes out, final T toWrite) {
                final byte[] bytes = serde.toBytes(toWrite);
                out.write(bytes);
            }
        };
    }

    @SuppressWarnings("rawtypes")
    private static <T> BytesReader<T> newBytesReader(final Serde<T> serde) {
        return new BytesReader<T>() {
            @Override
            public T read(final net.openhft.chronicle.bytes.Bytes in, final T using) {
                Assertions.checkNull(using);
                final byte[] bytes = in.toByteArray();
                return serde.fromBytes(bytes);
            }
        };
    }

    protected Integer getKeyFixedLength() {
        return null;
    }

    protected Integer getKeyAverageLength() {
        throw new UnsupportedOperationException(
                "Not implemented. You must either provide a fixed length or an average length.");
    }

    protected Serde<K> newKeySerde() {
        return new ExtendedTypeDelegateSerde<K>(getKeyType());
    }

    protected Integer getValueFixedLength() {
        return null;
    }

    protected Integer getValueAverageLength() {
        throw new UnsupportedOperationException(
                "Not implemented. You must either provide a fixed length or an average length.");
    }

    protected Serde<V> newValueSerde() {
        return new ExtendedTypeDelegateSerde<V>(getValueType());
    }

    protected File getDirectory() {
        return new File(getBaseDirectory(), ADelegateRangeTable.class.getSimpleName());
    }

    protected File getBaseDirectory() {
        return ContextProperties.getHomeDirectory();
    }

    @SuppressWarnings("unchecked")
    protected Class<K> getKeyType() {
        return (Class<K>) Reflections.resolveTypeArguments(getClass(), APersistentMap.class)[0];
    }

    @SuppressWarnings("unchecked")
    protected Class<V> getValueType() {
        return (Class<V>) Reflections.resolveTypeArguments(getClass(), APersistentMap.class)[1];
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

}
