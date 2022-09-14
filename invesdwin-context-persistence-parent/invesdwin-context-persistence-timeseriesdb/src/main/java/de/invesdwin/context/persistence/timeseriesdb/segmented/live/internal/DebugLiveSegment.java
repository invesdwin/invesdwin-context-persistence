package de.invesdwin.context.persistence.timeseriesdb.segmented.live.internal;

import java.util.concurrent.locks.Lock;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.timeseriesdb.segmented.SegmentedKey;
import de.invesdwin.context.persistence.timeseriesdb.segmented.live.ALiveSegmentedTimeSeriesDB;
import de.invesdwin.context.persistence.timeseriesdb.storage.ISkipFileFunction;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.time.date.FDate;

// CHECKSTYLE:OFF
@NotThreadSafe
public class DebugLiveSegment<K, V> implements ILiveSegment<K, V> {

    private final OneLatestFileLiveSegment<K, V> first;
    private final MultipleLatestFileLiveSegment<K, V> second;

    public DebugLiveSegment(final SegmentedKey<K> segmentedKey,
            final ALiveSegmentedTimeSeriesDB<K, V>.HistoricalSegmentTable historicalSegmentTable) {
        this.first = new OneLatestFileLiveSegment<>(segmentedKey, historicalSegmentTable);
        this.second = new MultipleLatestFileLiveSegment<>(segmentedKey, historicalSegmentTable);
    }

    @Override
    public V getFirstValue() {
        final V value = first.getFirstValue();
        final V value2 = second.getFirstValue();
        System.out.println("getFirstValue: " + value + " " + value2);
        Assertions.assertThat(value2).isEqualTo(value);
        return value2;
    }

    @Override
    public V getLastValue() {
        final V value = first.getLastValue();
        final V value2 = second.getLastValue();
        System.out.println("getLastValue: " + value + " " + value2);
        Assertions.assertThat(value2).isEqualTo(value);
        return value2;
    }

    @Override
    public SegmentedKey<K> getSegmentedKey() {
        return first.getSegmentedKey();
    }

    @Override
    public ICloseableIterable<V> rangeValues(final FDate from, final FDate to, final Lock readLock,
            final ISkipFileFunction skipFileFunction) {
        final ICloseableIterable<V> rangeValues = first.rangeValues(from, to, readLock, skipFileFunction);
        final ICloseableIterable<V> rangeValues2 = second.rangeValues(from, to, readLock, skipFileFunction);
        System.out.println("rangeValues(" + from + ", " + to + "): " + Lists.toListWithoutHasNext(rangeValues) + " "
                + Lists.toListWithoutHasNext(rangeValues2));
        Assertions.assertThat(Lists.toListWithoutHasNext(rangeValues2))
                .isEqualTo(Lists.toListWithoutHasNext(rangeValues));
        return rangeValues2;
    }

    @Override
    public ICloseableIterable<V> rangeReverseValues(final FDate from, final FDate to, final Lock readLock,
            final ISkipFileFunction skipFileFunction) {
        final ICloseableIterable<V> rangeReverseValues = first.rangeReverseValues(from, to, readLock, skipFileFunction);
        final ICloseableIterable<V> rangeReverseValues2 = second.rangeReverseValues(from, to, readLock,
                skipFileFunction);
        System.out.println(
                "rangeReverseValues(" + from + ", " + to + "): " + Lists.toListWithoutHasNext(rangeReverseValues) + " "
                        + Lists.toListWithoutHasNext(rangeReverseValues2));
        Assertions.assertThat(Lists.toListWithoutHasNext(rangeReverseValues2))
                .isEqualTo(Lists.toListWithoutHasNext(rangeReverseValues));
        return rangeReverseValues2;
    }

    @Override
    public void putNextLiveValue(final FDate nextLiveKey, final V nextLiveValue) {
        System.out.println("putNextLiveValue: " + nextLiveKey + " " + nextLiveValue);
        first.putNextLiveValue(nextLiveKey, nextLiveValue);
        second.putNextLiveValue(nextLiveKey, nextLiveValue);
    }

    @Override
    public V getNextValue(final FDate date, final int shiftForwardUnits) {
        final V nextValue = first.getNextValue(date, shiftForwardUnits);
        final V nextValue2 = second.getNextValue(date, shiftForwardUnits);
        System.out.println("getNextValue(" + date + "," + shiftForwardUnits + "): " + nextValue + " " + nextValue2);
        Assertions.assertThat(nextValue2).isEqualTo(nextValue);
        return nextValue2;
    }

    @Override
    public V getLatestValue(final FDate date) {
        final V latestValue = first.getLatestValue(date);
        final V latestValue2 = second.getLatestValue(date);
        System.out.println("getLatestValue(" + date + "): " + latestValue + " " + latestValue2);
        Assertions.assertThat(latestValue2).isEqualTo(latestValue);
        return latestValue2;
    }

    @Override
    public boolean isEmpty() {
        return first.isEmpty();
    }

    @Override
    public void convertLiveSegmentToHistorical() {
        first.close();
        second.convertLiveSegmentToHistorical();
    }

    @Override
    public FDate getFirstValueKey() {
        final FDate firstValueKey = first.getFirstValueKey();
        final FDate firstValueKey2 = second.getFirstValueKey();
        System.out.println("getFirstValueKey: " + firstValueKey + " " + firstValueKey2);
        Assertions.assertThat(firstValueKey2).isEqualTo(firstValueKey);
        return firstValueKey2;
    }

    @Override
    public FDate getLastValueKey() {
        final FDate lastValueKey = first.getLastValueKey();
        final FDate lastValueKey2 = second.getLastValueKey();
        System.out.println("getLastValueKey: " + lastValueKey + " " + lastValueKey2);
        Assertions.assertThat(lastValueKey2).isEqualTo(lastValueKey);
        return lastValueKey2;
    }

    @Override
    public void close() {
        first.close();
        second.close();
    }

}
