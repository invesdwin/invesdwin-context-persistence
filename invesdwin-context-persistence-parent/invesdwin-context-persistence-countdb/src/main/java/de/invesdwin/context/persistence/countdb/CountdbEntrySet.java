package de.invesdwin.context.persistence.countdb;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.util.KeyValue;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.error.FastNoSuchElementException;

@Immutable
public class CountdbEntrySet<V> implements Set<Entry<Long, V>> {

    private final CountdbMap<V> parent;

    public CountdbEntrySet(final CountdbMap<V> parent) {
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
    public ICloseableIterator<Entry<Long, V>> iterator() {
        final CloseableIterator<KeyValue<V>> iterator = parent.getDataInterface().iterator();
        return new ICloseableIterator<Entry<Long, V>>() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Entry<Long, V> next() {
                final KeyValue<V> nextEntry = iterator.next();
                return new Entry<Long, V>() {

                    @Override
                    public Long getKey() {
                        return nextEntry.getKey();
                    }

                    @Override
                    public V getValue() {
                        if (!hasNext()) {
                            throw FastNoSuchElementException.getInstance("end reached");
                        }
                        final V next = nextEntry.getValue();
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
    public boolean add(final Entry<Long, V> e) {
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
    public boolean addAll(final Collection<? extends Entry<Long, V>> c) {
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
