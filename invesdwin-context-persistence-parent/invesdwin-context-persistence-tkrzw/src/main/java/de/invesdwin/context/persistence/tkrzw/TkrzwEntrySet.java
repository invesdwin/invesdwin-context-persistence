package de.invesdwin.context.persistence.tkrzw;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.error.FastNoSuchElementException;
import tkrzw.Status;

@Immutable
public class TkrzwEntrySet<K, V> implements Set<Entry<K, V>> {

    private final TkrzwMap<K, V> parent;

    public TkrzwEntrySet(final TkrzwMap<K, V> parent) {
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
    public ICloseableIterator<Entry<K, V>> iterator() {
        final tkrzw.Iterator iterator = parent.getDbm().makeIterator();
        return new ICloseableIterator<Entry<K, V>>() {

            private Status status = iterator.first();

            @Override
            public boolean hasNext() {
                return status.getCode() == Status.SUCCESS;
            }

            @Override
            public Entry<K, V> next() {
                return new Entry<K, V>() {

                    @Override
                    public K getKey() {
                        return parent.getKeySerde().fromBytes(iterator.getKey());
                    }

                    @Override
                    public V getValue() {
                        final V next = parent.getValueSerde().fromBytes(iterator.getValue());
                        status = iterator.next();
                        if (!hasNext()) {
                            throw FastNoSuchElementException.getInstance("end reached");
                        }
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
