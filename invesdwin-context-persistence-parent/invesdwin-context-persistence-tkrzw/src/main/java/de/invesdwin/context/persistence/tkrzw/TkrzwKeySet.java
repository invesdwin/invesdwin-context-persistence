package de.invesdwin.context.persistence.tkrzw;

import java.util.Collection;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.error.FastNoSuchElementException;
import tkrzw.Status;

@Immutable
public class TkrzwKeySet<K> implements Set<K> {

    private final TkrzwMap<K, ?> parent;

    public TkrzwKeySet(final TkrzwMap<K, ?> parent) {
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
        final tkrzw.Iterator iterator = parent.getDbm().makeIterator();
        return new ICloseableIterator<K>() {

            private Status status = iterator.first();

            @Override
            public boolean hasNext() {
                return status.getCode() == Status.SUCCESS;
            }

            @Override
            public K next() {
                final K next = parent.getKeySerde().fromBytes(iterator.getKey());
                status = iterator.next();
                if (!hasNext()) {
                    throw FastNoSuchElementException.getInstance("end reached");
                }
                return next;
            }

            @Override
            public void close() {
                iterator.destruct();
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
