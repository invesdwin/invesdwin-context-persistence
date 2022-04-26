package de.invesdwin.context.persistence.tokyocabinet;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.error.FastNoSuchElementException;

@Immutable
public class TokyocabinetEntrySet<K, V> implements Set<Entry<K, V>> {

    private final TokyocabinetMap<K, V> parent;

    public TokyocabinetEntrySet(final TokyocabinetMap<K, V> parent) {
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
        return new ICloseableIterator<Entry<K, V>>() {

            private boolean status = parent.getDbm().iterinit();

            @Override
            public boolean hasNext() {
                return status;
            }

            @Override
            public Entry<K, V> next() {
                final byte[] key = parent.getDbm().iternext();
                status = key != null;
                if (!hasNext()) {
                    throw new FastNoSuchElementException("end reached");
                }
                return new Entry<K, V>() {

                    @Override
                    public K getKey() {
                        return parent.getKeySerde().fromBytes(key);
                    }

                    @Override
                    public V getValue() {
                        final V next = parent.getValueSerde().fromBytes(parent.getDbm().get(key));
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
