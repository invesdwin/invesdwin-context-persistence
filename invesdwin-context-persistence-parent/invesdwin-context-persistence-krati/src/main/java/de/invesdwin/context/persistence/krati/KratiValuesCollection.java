package de.invesdwin.context.persistence.krati;

import java.util.Collection;
import java.util.Map.Entry;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.error.FastNoSuchElementException;
import krati.util.IndexedIterator;

@Immutable
public class KratiValuesCollection<V> implements Collection<V> {

    private final KratiMap<?, V> parent;

    public KratiValuesCollection(final KratiMap<?, V> parent) {
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
        final IndexedIterator<Entry<byte[], byte[]>> iterator = parent.getDataStore().iterator();
        return new ICloseableIterator<V>() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public V next() {
                if (!hasNext()) {
                    throw FastNoSuchElementException.getInstance("end reached");
                }
                final V next = parent.getValueSerde().fromBytes(iterator.next().getValue());
                return next;
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
