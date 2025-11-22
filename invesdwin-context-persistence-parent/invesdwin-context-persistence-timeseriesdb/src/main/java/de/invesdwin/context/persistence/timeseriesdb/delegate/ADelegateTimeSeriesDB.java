package de.invesdwin.context.persistence.timeseriesdb.delegate;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.persistence.timeseriesdb.ITimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.TimeSeriesLookupMode;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.concurrent.lock.readwrite.IReadWriteLock;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.time.date.FDate;

@ThreadSafe
public abstract class ADelegateTimeSeriesDB<K, V> implements ITimeSeriesDB<K, V> {

    private final ITimeSeriesDB<K, V> delegate;

    public ADelegateTimeSeriesDB() {
        this.delegate = newDelegate();
    }

    ADelegateTimeSeriesDB(final ITimeSeriesDB<K, V> delegate) {
        this.delegate = delegate;
    }

    protected abstract ITimeSeriesDB<K, V> newDelegate();

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public File getBaseDirectory() {
        return delegate.getBaseDirectory();
    }

    @Override
    public File getDirectory() {
        return delegate.getDirectory();
    }

    @Override
    public IReadWriteLock getTableLock(final K key) {
        return delegate.getTableLock(key);
    }

    @Override
    public ICloseableIterable<V> rangeValues(final K key, final FDate from, final FDate to) {
        return delegate.rangeValues(key, from, to);
    }

    @Override
    public ICloseableIterable<V> rangeReverseValues(final K key, final FDate from, final FDate to) {
        return delegate.rangeReverseValues(key, from, to);
    }

    @Override
    public V getLatestValue(final K key, final FDate date) {
        return delegate.getLatestValue(key, date);
    }

    @Override
    public FDate getLatestValueKey(final K key, final FDate date) {
        return delegate.getLatestValueKey(key, date);
    }

    @Override
    public V getLatestValue(final K key, final long index) {
        return delegate.getLatestValue(key, index);
    }

    @Override
    public FDate getLatestValueKey(final K key, final long index) {
        return delegate.getLatestValueKey(key, index);
    }

    @Override
    public long getLatestValueIndex(final K key, final FDate date) {
        return delegate.getLatestValueIndex(key, date);
    }

    @Override
    public long size(final K key) {
        return delegate.size(key);
    }

    @Override
    public V getPreviousValue(final K key, final FDate date, final int shiftBackUnits) {
        return delegate.getPreviousValue(key, date, shiftBackUnits);
    }

    @Override
    public FDate getPreviousValueKey(final K key, final FDate date, final int shiftBackUnits) {
        return delegate.getPreviousValueKey(key, date, shiftBackUnits);
    }

    @Override
    public boolean isEmptyOrInconsistent(final K key) {
        return delegate.isEmptyOrInconsistent(key);
    }

    @Override
    public V getNextValue(final K key, final FDate date, final int shiftForwardUnits) {
        return delegate.getNextValue(key, date, shiftForwardUnits);
    }

    @Override
    public FDate getNextValueKey(final K key, final FDate date, final int shiftForwardUnits) {
        return delegate.getNextValueKey(key, date, shiftForwardUnits);
    }

    @Override
    public void deleteRange(final K key) {
        delegate.deleteRange(key);
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public String hashKeyToString(final K key) {
        return delegate.hashKeyToString(key);
    }

    @Override
    public ISerde<V> getValueSerde() {
        return delegate.getValueSerde();
    }

    @Override
    public Integer getValueFixedLength() {
        return delegate.getValueFixedLength();
    }

    @Override
    public ICompressionFactory getCompressionFactory() {
        return delegate.getCompressionFactory();
    }

    @Override
    public TimeSeriesLookupMode getLookupMode() {
        return delegate.getLookupMode();
    }

    @Override
    public FDate extractStartTime(final V value) {
        return delegate.extractStartTime(value);
    }

    @Override
    public FDate extractEndTime(final V value) {
        return delegate.extractEndTime(value);
    }

}
