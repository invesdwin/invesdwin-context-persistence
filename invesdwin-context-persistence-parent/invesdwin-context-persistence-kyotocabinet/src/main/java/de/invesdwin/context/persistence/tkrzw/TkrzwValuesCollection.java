package de.invesdwin.context.persistence.tkrzw;

import java.util.Collection;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.error.FastNoSuchElementException;
import tkrzw.Status;

@Immutable
public class TkrzwValuesCollection<V> implements Collection<V> {

    private final TkrzwMap<?, V> parent;

    public TkrzwValuesCollection(final TkrzwMap<?, V> parent) {
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
    public ICloseableIterator<V> iterator() {
        final tkrzw.Iterator iterator = parent.getDbm().makeIterator();
        return new ICloseableIterator<V>() {

            private Status status = iterator.first();

            @Override
            public boolean hasNext() {
                return status.getCode() == Status.SUCCESS;
            }

            @Override
            public V next() {
                final V next = parent.getValueSerde().fromBytes(iterator.getValue());
                status = iterator.next();
                if (!hasNext()) {
                    throw new FastNoSuchElementException("end reached");
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
    public boolean add(final V e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(final Collection<? extends V> c) {
        throw new UnsupportedOperationException();
    }

}
