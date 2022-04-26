package de.invesdwin.context.persistence.cdb;

import java.io.IOException;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

import com.strangegizmo.cdb.Cdb;
import com.strangegizmo.cdb.CdbElement;

import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.error.FastNoSuchElementException;

@Immutable
public class CdbKeySet<K> implements Set<K> {

    private final CdbMap<K, ?> parent;

    public CdbKeySet(final CdbMap<K, ?> parent) {
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
        final Enumeration<CdbElement> iterator;
        try {
            iterator = Cdb.elements(parent.getFile().getAbsolutePath());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        return new ICloseableIterator<K>() {

            @Override
            public boolean hasNext() {
                return iterator.hasMoreElements();
            }

            @Override
            public K next() {
                if (!hasNext()) {
                    throw new FastNoSuchElementException("end reached");
                }
                final K next = parent.getKeySerde().fromBytes(iterator.nextElement().getKey());
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
