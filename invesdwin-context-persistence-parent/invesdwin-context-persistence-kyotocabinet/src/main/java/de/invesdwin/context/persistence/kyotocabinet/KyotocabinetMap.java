package de.invesdwin.context.persistence.kyotocabinet;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.math.Integers;
import kyotocabinet.DB;

@ThreadSafe
public class KyotocabinetMap<K, V> implements ConcurrentMap<K, V>, Closeable {

    private DB db;
    private final ISerde<K> keySerde;
    private final ISerde<V> valueSerde;
    private KyotocabinetEntrySet<K, V> entrySet;
    private KyotocabinetValuesCollection<V> valuesCollection;
    private KyotocabinetKeySet<K> keySet;

    public KyotocabinetMap(final DB db, final ISerde<K> keySerde, final ISerde<V> valueSerde) {
        this.db = db;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public DB getDb() {
        return db;
    }

    public ISerde<K> getKeySerde() {
        return keySerde;
    }

    public ISerde<V> getValueSerde() {
        return valueSerde;
    }

    @Override
    public int size() {
        return Integers.checkedCast(db.count());
    }

    @Override
    public boolean isEmpty() {
        return db.count() > 0;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean containsKey(final Object key) {
        return db.get(keySerde.toBytes((K) key)) != null;
    }

    @Override
    public boolean containsValue(final Object value) {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public V get(final Object key) {
        final byte[] bytes = db.get(keySerde.toBytes((K) key));
        return valueSerde.fromBytes(bytes);
    }

    @Override
    public V put(final K key, final V value) {
        db.set(keySerde.toBytes(key), valueSerde.toBytes(value));
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public V remove(final Object key) {
        db.remove(keySerde.toBytes((K) key));
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
        db.clear();
    }

    @Override
    public Set<K> keySet() {
        if (keySet == null) {
            keySet = new KyotocabinetKeySet<>(this);
        }
        return keySet;
    }

    @Override
    public Collection<V> values() {
        if (valuesCollection == null) {
            valuesCollection = new KyotocabinetValuesCollection<>(this);
        }
        return valuesCollection;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        if (entrySet == null) {
            entrySet = new KyotocabinetEntrySet<>(this);
        }
        return entrySet;
    }

    @Override
    public void close() throws IOException {
        db.close();
        db = null;
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
