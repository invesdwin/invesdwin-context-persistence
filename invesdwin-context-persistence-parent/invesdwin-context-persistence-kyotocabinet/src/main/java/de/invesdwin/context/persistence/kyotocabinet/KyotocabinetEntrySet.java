package de.invesdwin.context.persistence.kyotocabinet;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.error.FastNoSuchElementException;
import kyotocabinet.Cursor;

@Immutable
public class KyotocabinetEntrySet<K, V> implements Set<Entry<K, V>> {

    private final KyotocabinetMap<K, V> parent;

    public KyotocabinetEntrySet(final KyotocabinetMap<K, V> parent) {
        this.parent = parent;
    }

    @Override
    public int size() {
        return parent.size();
    }

    @Override
    public boolean isEmpty() {
        return parent.isEmpty();
    }

    @Override
    public ICloseableIterator<Entry<K, V>> iterator() {
        final Cursor iterator = parent.getDb().cursor();
        return new ICloseableIterator<Entry<K, V>>() {

            private boolean status = iterator.jump();

            @Override
            public boolean hasNext() {
                return status;
            }

            @Override
            public Entry<K, V> next() {
                return new Entry<K, V>() {

                    @Override
                    public K getKey() {
                        return parent.getKeySerde().fromBytes(iterator.get_key(false));
                    }

                    @Override
                    public V getValue() {
                        if (!hasNext()) {
                            throw FastNoSuchElementException.getInstance("end reached");
                        }
                        final V next = parent.getValueSerde().fromBytes(iterator.get_value(false));
                        status = iterator.step();
                        return next;
                    }

                    @Override
                    public V setValue(final V value) {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public void close() {
                iterator.disable();
            }
        };
    }

    @Override
    public boolean contains(final Object o) {
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
    }

}
