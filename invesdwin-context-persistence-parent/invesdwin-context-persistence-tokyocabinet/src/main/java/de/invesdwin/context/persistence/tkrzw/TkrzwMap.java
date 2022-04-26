package de.invesdwin.context.persistence.tkrzw;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.math.Integers;
import tkrzw.DBM;

@ThreadSafe
public class TkrzwMap<K, V> implements ConcurrentMap<K, V>, Closeable {

    private DBM dbm;
    private final ISerde<K> keySerde;
    private final ISerde<V> valueSerde;
    private TkrzwEntrySet<K, V> entrySet;
    private TkrzwValuesCollection<V> valuesCollection;
    private TkrzwKeySet<K> keySet;

    public TkrzwMap(final DBM dbm, final ISerde<K> keySerde, final ISerde<V> valueSerde) {
        this.dbm = dbm;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public DBM getDbm() {
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
        return Integers.checkedCast(dbm.count());
    }

    @Override
    public boolean isEmpty() {
        return dbm.count() > 0;
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
        dbm.set(keySerde.toBytes(key), valueSerde.toBytes(value));
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public V remove(final Object key) {
        dbm.remove(keySerde.toBytes((K) key));
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
        dbm.clear();
    }

    @Override
    public Set<K> keySet() {
        if (keySet == null) {
            keySet = new TkrzwKeySet<>(this);
        }
        return keySet;
    }

    @Override
    public Collection<V> values() {
        if (valuesCollection == null) {
            valuesCollection = new TkrzwValuesCollection<>(this);
        }
        return valuesCollection;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        if (entrySet == null) {
            entrySet = new TkrzwEntrySet<>(this);
        }
        return entrySet;
    }

    @Override
    public void close() throws IOException {
        dbm.close();
        dbm.destruct();
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
