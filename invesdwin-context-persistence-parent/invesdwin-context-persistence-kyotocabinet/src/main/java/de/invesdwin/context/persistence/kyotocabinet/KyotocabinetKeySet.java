package de.invesdwin.context.persistence.kyotocabinet;

import java.util.Collection;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.error.FastNoSuchElementException;
import kyotocabinet.Cursor;

@Immutable
public class KyotocabinetKeySet<K> implements Set<K> {

    private final KyotocabinetMap<K, ?> parent;

    public KyotocabinetKeySet(final KyotocabinetMap<K, ?> parent) {
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
        final Cursor iterator = parent.getDb().cursor();
        return new ICloseableIterator<K>() {

            private boolean status = iterator.step();

            @Override
            public boolean hasNext() {
                return status;
            }

            @Override
            public K next() {
                final K next = parent.getKeySerde().fromBytes(iterator.get_key(false));
                status = iterator.step();
                if (!hasNext()) {
                    throw new FastNoSuchElementException("end reached");
                }
                return next;
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
