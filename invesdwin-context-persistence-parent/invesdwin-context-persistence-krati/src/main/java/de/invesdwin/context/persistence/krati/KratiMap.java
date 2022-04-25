package de.invesdwin.context.persistence.krati;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.marshallers.serde.ISerde;
import krati.store.DataStore;

@ThreadSafe
public class KratiMap<K, V> implements ConcurrentMap<K, V>, Closeable {

    private DataStore<byte[], byte[]> dataStore;
    private final ISerde<K> keySerde;
    private final ISerde<V> valueSerde;
    private KratiEntrySet<K, V> entrySet;
    private KratiValuesCollection<V> valuesCollection;
    private KratiKeySet<K> keySet;

    public KratiMap(final DataStore<byte[], byte[]> dataStore, final ISerde<K> keySerde, final ISerde<V> valueSerde) {
        this.dataStore = dataStore;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public DataStore<byte[], byte[]> getDataStore() {
        return dataStore;
    }

    public ISerde<K> getKeySerde() {
        return keySerde;
    }

    public ISerde<V> getValueSerde() {
        return valueSerde;
    }

    @Override
    public int size() {
        return dataStore.capacity();
    }

    @Override
    public boolean isEmpty() {
        return size() > 0;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean containsKey(final Object key) {
        return dataStore.get(keySerde.toBytes((K) key)) != null;
    }

    @Override
    public boolean containsValue(final Object value) {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public V get(final Object key) {
        final byte[] bytes = dataStore.get(keySerde.toBytes((K) key));
        return valueSerde.fromBytes(bytes);
    }

    @Override
    public V put(final K key, final V value) {
        try {
            dataStore.put(keySerde.toBytes(key), valueSerde.toBytes(value));
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public V remove(final Object key) {
        try {
            dataStore.delete(keySerde.toBytes((K) key));
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> m) {
        for (final Entry<? extends K, ? extends V> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public void clear() {
        try {
            dataStore.clear();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<K> keySet() {
        if (keySet == null) {
            keySet = new KratiKeySet<>(this);
        }
        return keySet;
    }

    @Override
    public Collection<V> values() {
        if (valuesCollection == null) {
            valuesCollection = new KratiValuesCollection<>(this);
        }
        return valuesCollection;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        if (entrySet == null) {
            entrySet = new KratiEntrySet<>(this);
        }
        return entrySet;
    }

    @Override
    public void close() throws IOException {
        dataStore.close();
        dataStore = null;
    }

    @Override
    public V putIfAbsent(final K key, final V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(final Object key, final Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(final K key, final V oldValue, final V newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V replace(final K key, final V value) {
        throw new UnsupportedOperationException();
    }

}
