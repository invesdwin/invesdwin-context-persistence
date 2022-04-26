package de.invesdwin.context.persistence.tokyocabinet;

import java.util.Collection;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.error.FastNoSuchElementException;

@Immutable
public class TokyocabinetKeySet<K> implements Set<K> {

    private final TokyocabinetMap<K, ?> parent;

    public TokyocabinetKeySet(final TokyocabinetMap<K, ?> parent) {
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
    public ICloseableIterator<K> iterator() {
        return new ICloseableIterator<K>() {

            private boolean status = parent.getDbm().iterinit();

            @Override
            public boolean hasNext() {
                return status;
            }

            @Override
            public K next() {
                final byte[] key = parent.getDbm().iternext();
                status = key != null;
                if (!hasNext()) {
                    throw new FastNoSuchElementException("end reached");
                }
                final K next = parent.getKeySerde().fromBytes(key);
                return next;
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
    public boolean remove(final Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(final Collection<?> c) {
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

    @Override
    public boolean add(final K e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(final Collection<? extends K> c) {
        throw new UnsupportedOperationException();
    }

}
