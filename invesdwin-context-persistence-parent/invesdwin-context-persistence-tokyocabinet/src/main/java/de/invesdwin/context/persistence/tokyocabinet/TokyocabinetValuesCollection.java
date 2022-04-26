package de.invesdwin.context.persistence.tokyocabinet;

import java.util.Collection;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.error.FastNoSuchElementException;

@Immutable
public class TokyocabinetValuesCollection<V> implements Collection<V> {

    private final TokyocabinetMap<?, V> parent;

    public TokyocabinetValuesCollection(final TokyocabinetMap<?, V> parent) {
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
        return new ICloseableIterator<V>() {

            private boolean status = parent.getDbm().iterinit();

            @Override
            public boolean hasNext() {
                return status;
            }

            @Override
            public V next() {
                final byte[] key = parent.getDbm().iternext();
                status = key != null;
                if (!hasNext()) {
                    throw new FastNoSuchElementException("end reached");
                }
                final V next = parent.getValueSerde().fromBytes(parent.getDbm().get(key));
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
    public boolean add(final V e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(final Collection<? extends V> c) {
        throw new UnsupportedOperationException();
    }

}
