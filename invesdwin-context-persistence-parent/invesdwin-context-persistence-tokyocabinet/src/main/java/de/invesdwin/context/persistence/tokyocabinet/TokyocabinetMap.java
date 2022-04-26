package de.invesdwin.context.persistence.tokyocabinet;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.math.Integers;
import tokyocabinet.HDB;

@ThreadSafe
public class TokyocabinetMap<K, V> implements ConcurrentMap<K, V>, Closeable {

    private HDB dbm;
    private final ISerde<K> keySerde;
    private final ISerde<V> valueSerde;
    private TokyocabinetEntrySet<K, V> entrySet;
    private TokyocabinetValuesCollection<V> valuesCollection;
    private TokyocabinetKeySet<K> keySet;

    public TokyocabinetMap(final HDB dbm, final ISerde<K> keySerde, final ISerde<V> valueSerde) {
        this.dbm = dbm;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public HDB getDbm() {
        return dbm;
    }

    public ISerde<K> getKeySerde() {
        return keySerde;
    }

    public ISerde<V> getValueSerde() {
        return valueSerde;
    }

    @Override
    public int size() {
        return Integers.checkedCast(dbm.rnum());
    }

    @Override
    public boolean isEmpty() {
        return dbm.rnum() > 0;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean containsKey(final Object key) {
        return dbm.get(keySerde.toBytes((K) key)) != null;
    }

    @Override
    public boolean containsValue(final Object value) {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public V get(final Object key) {
        final byte[] bytes = dbm.get(keySerde.toBytes((K) key));
        return valueSerde.fromBytes(bytes);
    }

    @Override
    public V put(final K key, final V value) {
        dbm.put(keySerde.toBytes(key), valueSerde.toBytes(value));
        return null;
    }

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
            keySet = new TokyocabinetKeySet<>(this);
        }
        return keySet;
    }

    @Override
    public Collection<V> values() {
        if (valuesCollection == null) {
            valuesCollection = new TokyocabinetValuesCollection<>(this);
        }
        return valuesCollection;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        if (entrySet == null) {
            entrySet = new TokyocabinetEntrySet<>(this);
        }
        return entrySet;
    }

    @Override
    public void close() throws IOException {
        dbm.close();
        dbm = null;
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
