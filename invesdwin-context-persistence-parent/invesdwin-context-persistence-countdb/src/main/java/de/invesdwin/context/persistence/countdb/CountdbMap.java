package de.invesdwin.context.persistence.countdb;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.ThreadSafe;

import be.bagofwords.db.DataInterface;
import de.invesdwin.util.math.Integers;

@ThreadSafe
public class CountdbMap<V> implements ConcurrentMap<Long, V>, Closeable {

    private DataInterface<V> dataInterface;
    private CountdbEntrySet<V> entrySet;
    private CountdbValuesCollection<V> valuesCollection;
    private CountdbKeySet keySet;

    public CountdbMap(final DataInterface<V> dataInterface) {
        this.dataInterface = dataInterface;
    }

    public DataInterface<V> getDataInterface() {
        return dataInterface;
    }

    @Override
    public int size() {
        return Integers.checkedCast(dataInterface.apprSize());
    }

    @Override
    public boolean isEmpty() {
        return size() > 0;
    }

    @Override
    public boolean containsKey(final Object key) {
        return dataInterface.read((Long) key) != null;
    }

    @Override
    public boolean containsValue(final Object value) {
        return false;
    }

    @Override
    public V get(final Object key) {
        return dataInterface.read((Long) key);
    }

    @Override
    public V put(final Long key, final V value) {
        try {
            dataInterface.write(key, value);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public V remove(final Object key) {
        try {
            dataInterface.remove((Long) key);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public void putAll(final Map<? extends Long, ? extends V> m) {
        for (final Entry<? extends Long, ? extends V> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public void clear() {
        dataInterface.dropAllData();
    }

    @Override
    public Set<Long> keySet() {
        if (keySet == null) {
            keySet = new CountdbKeySet(this);
        }
        return keySet;
    }

    @Override
    public Collection<V> values() {
        if (valuesCollection == null) {
            valuesCollection = new CountdbValuesCollection<>(this);
        }
        return valuesCollection;
    }

    @Override
    public Set<Entry<Long, V>> entrySet() {
        if (entrySet == null) {
            entrySet = new CountdbEntrySet<>(this);
        }
        return entrySet;
    }

    @Override
    public void close() throws IOException {
        dataInterface.close();
        dataInterface = null;
    }

    @Override
    public V putIfAbsent(final Long key, final V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(final Object key, final Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(final Long key, final V oldValue, final V newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V replace(final Long key, final V value) {
        throw new UnsupportedOperationException();
    }

}
