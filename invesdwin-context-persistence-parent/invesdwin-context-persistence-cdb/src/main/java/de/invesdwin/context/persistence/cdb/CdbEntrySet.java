package de.invesdwin.context.persistence.cdb;

import java.io.IOException;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

import com.strangegizmo.cdb.Cdb;
import com.strangegizmo.cdb.CdbElement;

import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.error.FastNoSuchElementException;

@Immutable
public class CdbEntrySet<K, V> implements Set<Entry<K, V>> {

    private final CdbMap<K, V> parent;

    public CdbEntrySet(final CdbMap<K, V> parent) {
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

    @SuppressWarnings("unchecked")
    @Override
    public ICloseableIterator<Entry<K, V>> iterator() {
        final Enumeration<CdbElement> iterator;
        try {
            iterator = Cdb.elements(parent.getFile().getAbsolutePath());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        return new ICloseableIterator<Entry<K, V>>() {

            @Override
            public boolean hasNext() {
                return iterator.hasMoreElements();
            }

            @Override
            public Entry<K, V> next() {
                final CdbElement nextEntry = iterator.nextElement();
                return new Entry<K, V>() {

                    @Override
                    public K getKey() {
                        return parent.getKeySerde().fromBytes(nextEntry.getKey());
                    }

                    @Override
                    public V getValue() {
                        if (!hasNext()) {
                            throw FastNoSuchElementException.getInstance("end reached");
                        }
                        final V next = parent.getValueSerde().fromBytes(nextEntry.getData());
                        return next;
                    }

                    @Override
                    public V setValue(final V value) {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public void close() {}
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
