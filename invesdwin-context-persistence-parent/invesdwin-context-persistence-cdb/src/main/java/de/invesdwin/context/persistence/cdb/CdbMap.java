package de.invesdwin.context.persistence.cdb;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.ThreadSafe;

import com.strangegizmo.cdb.Cdb;

import de.invesdwin.util.marshallers.serde.ISerde;

@ThreadSafe
public class CdbMap<K, V> implements ConcurrentMap<K, V>, Closeable {

    private final File file;
    private Cdb cdb;
    private final ISerde<K> keySerde;
    private final ISerde<V> valueSerde;
    private CdbEntrySet<K, V> entrySet;
    private CdbValuesCollection<V> valuesCollection;
    private CdbKeySet<K> keySet;

    public CdbMap(final File file, final Cdb cdb, final ISerde<K> keySerde, final ISerde<V> valueSerde) {
        this.file = file;
        this.cdb = cdb;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public File getFile() {
        return file;
    }

    public Cdb getCdb() {
        return cdb;
    }

    public ISerde<K> getKeySerde() {
        return keySerde;
    }

    public ISerde<V> getValueSerde() {
        return valueSerde;
    }

    @Override
    public int size() {
        return -1;
    }

    @Override
    public boolean isEmpty() {
        return size() > 0;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean containsKey(final Object key) {
        return cdb.find(keySerde.toBytes((K) key)) != null;
    }

    @Override
    public boolean containsValue(final Object value) {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public V get(final Object key) {
        final byte[] bytes = cdb.find(keySerde.toBytes((K) key));
        return valueSerde.fromBytes(bytes);
    }

    @Override
    public V put(final K key, final V value) {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    @Override
    public V remove(final Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> m) {
        for (final Entry<? extends K, ? extends V> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<K> keySet() {
        if (keySet == null) {
            keySet = new CdbKeySet<>(this);
        }
        return keySet;
    }

    @Override
    public Collection<V> values() {
        if (valuesCollection == null) {
            valuesCollection = new CdbValuesCollection<>(this);
        }
        return valuesCollection;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        if (entrySet == null) {
            entrySet = new CdbEntrySet<>(this);
        }
        return entrySet;
    }

    @Override
    public void close() throws IOException {
        cdb.close();
        cdb = null;
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
