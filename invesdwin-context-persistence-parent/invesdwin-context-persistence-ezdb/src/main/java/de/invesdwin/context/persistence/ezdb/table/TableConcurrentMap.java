package de.invesdwin.context.persistence.ezdb.table;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.collections.iterable.ATransformingIterator;
import de.invesdwin.util.collections.iterable.WrapperCloseableIterator;
import de.invesdwin.util.lang.Objects;
import ezdb.table.Table;
import ezdb.table.TableRow;
import ezdb.util.TableIterator;

@ThreadSafe
public final class TableConcurrentMap<K, V> implements ConcurrentMap<K, V>, Closeable {
    private final IDelegateTable<K, V> table;
    private Set<K> keySet;
    private Collection<V> values;
    private Set<Entry<K, V>> entrySet;

    public TableConcurrentMap(final IDelegateTable<K, V> table) {
        this.table = table;
    }

    public Table<K, V> getTable() {
        return table;
    }

    @Override
    public int size() {
        if (isEmpty()) {
            return 0;
        } else {
            return 1;
        }
    }

    @Override
    public boolean isEmpty() {
        try (TableIterator<? extends TableRow<K, V>> range = table.range()) {
            return !range.hasNext();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean containsKey(final Object key) {
        return table.get((K) key) != null;
    }

    @Override
    public boolean containsValue(final Object value) {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    @Override
    public V get(final Object key) {
        return table.get((K) key);
    }

    @Override
    public V put(final K key, final V value) {
        table.put(key, value);
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public V remove(final Object key) {
        table.delete((K) key);
        return null;
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> m) {
        for (final Entry<? extends K, ? extends V> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {
        table.deleteTable();
    }

    @Override
    public Set<K> keySet() {
        if (keySet == null) {
            keySet = newKeySet();
        }
        return keySet;
    }

    @Override
    public V computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction) {
        return table.getOrLoad(key, mappingFunction);
    }

    private Set<K> newKeySet() {
        return new Set<K>() {

            @Override
            public int size() {
                return TableConcurrentMap.this.size();
            }

            @Override
            public boolean isEmpty() {
                return TableConcurrentMap.this.isEmpty();
            }

            @Override
            public boolean contains(final Object o) {
                return TableConcurrentMap.this.containsKey(o);
            }

            @SuppressWarnings("deprecation")
            @Override
            public Iterator<K> iterator() {
                return new ATransformingIterator<TableRow<K, V>, K>(WrapperCloseableIterator.maybeWrap(table.range())) {
                    @Override
                    protected K transform(final TableRow<K, V> value) {
                        return value.getKey();
                    }
                };
            }

            @Override
            public Object[] toArray() {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> T[] toArray(final T[] a) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean add(final K e) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean remove(final Object o) {
                return TableConcurrentMap.this.remove(o) != null;
            }

            @Override
            public boolean containsAll(final Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean addAll(final Collection<? extends K> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean retainAll(final Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean removeAll(final Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void clear() {
                TableConcurrentMap.this.clear();
            }
        };
    }

    @Override
    public Collection<V> values() {
        if (values == null) {
            values = newValues();
        }
        return values;
    }

    private Collection<V> newValues() {
        return new Collection<V>() {

            @Override
            public int size() {
                return TableConcurrentMap.this.size();
            }

            @Override
            public boolean isEmpty() {
                return TableConcurrentMap.this.isEmpty();
            }

            @Override
            public boolean contains(final Object o) {
                return TableConcurrentMap.this.containsValue(o);
            }

            @SuppressWarnings("deprecation")
            @Override
            public Iterator<V> iterator() {
                return new ATransformingIterator<TableRow<K, V>, V>(WrapperCloseableIterator.maybeWrap(table.range())) {
                    @Override
                    protected V transform(final TableRow<K, V> value) {
                        return value.getValue();
                    }
                };
            }

            @Override
            public Object[] toArray() {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> T[] toArray(final T[] a) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean add(final V e) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean remove(final Object o) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean containsAll(final Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean addAll(final Collection<? extends V> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean removeAll(final Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean retainAll(final Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void clear() {
                TableConcurrentMap.this.clear();
            }
        };
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        if (entrySet == null) {
            entrySet = newEntrySet();
        }
        return entrySet;
    }

    private Set<Entry<K, V>> newEntrySet() {
        return new Set<Entry<K, V>>() {

            @Override
            public int size() {
                return TableConcurrentMap.this.size();
            }

            @Override
            public boolean isEmpty() {
                return TableConcurrentMap.this.isEmpty();
            }

            @Override
            public boolean contains(final Object o) {
                throw new UnsupportedOperationException();
            }

            @SuppressWarnings("deprecation")
            @Override
            public Iterator<Entry<K, V>> iterator() {
                return WrapperCloseableIterator.maybeWrap(table.range());
            }

            @Override
            public Object[] toArray() {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> T[] toArray(final T[] a) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean add(final Entry<K, V> e) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean remove(final Object o) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean containsAll(final Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean addAll(final Collection<? extends Entry<K, V>> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean retainAll(final Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean removeAll(final Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void clear() {
                TableConcurrentMap.this.clear();
            }
        };
    }

    @Override
    public V putIfAbsent(final K key, final V value) {
        final V existing = get(key);
        if (existing == null) {
            put(key, value);
            return null;
        } else {
            return existing;
        }
    }

    @Override
    public boolean remove(final Object key, final Object value) {
        final V existing = get(key);
        if (Objects.equals(existing, value)) {
            remove(key);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean replace(final K key, final V oldValue, final V newValue) {
        final V existing = get(key);
        if (Objects.equals(existing, oldValue)) {
            put(key, newValue);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public V replace(final K key, final V value) {
        final V existing = get(key);
        if (existing != null) {
            put(key, value);
            return existing;
        } else {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        table.close();
    }
}