package de.invesdwin.context.persistence.countdb;

import java.util.Collection;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

import be.bagofwords.iterator.CloseableIterator;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.error.FastNoSuchElementException;

@Immutable
public class CountdbKeySet implements Set<Long> {

    private final CountdbMap<?> parent;

    public CountdbKeySet(final CountdbMap<?> parent) {
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
    public ICloseableIterator<Long> iterator() {
        final CloseableIterator<Long> iterator = parent.getDataInterface().keyIterator();
        return new ICloseableIterator<Long>() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Long next() {
                if (!hasNext()) {
                    throw FastNoSuchElementException.getInstance("end reached");
                }
                final Long next = iterator.next();
                return next;
            }

            @Override
            public void close() {
                iterator.close();
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
    public boolean add(final Long e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(final Collection<? extends Long> c) {
        throw new UnsupportedOperationException();
    }

}
