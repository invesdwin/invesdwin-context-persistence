package de.invesdwin.context.persistence.timeseriesdb.array;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.system.array.OnHeapPrimitiveArrayAllocator;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.array.IBooleanArray;
import de.invesdwin.util.collections.array.IDoubleArray;
import de.invesdwin.util.collections.bitset.IBitSet;

@NotThreadSafe
public class TemporaryDiskPrimitiveArrayAllocatorTest extends ATest {

    @Test
    public void testBitSet() {
        final OnHeapPrimitiveArrayAllocator onHeapAllocator = new OnHeapPrimitiveArrayAllocator();
        final TemporaryDiskPrimitiveArrayAllocator offHeapAllocator = new TemporaryDiskPrimitiveArrayAllocator(
                TemporaryDiskPrimitiveArrayAllocatorTest.class.getSimpleName());
        final int size = 1000;

        final IBitSet onHeapBitSet = onHeapAllocator.newBitSet("onHeapBitSet", size);
        final IBooleanArray onHeapBooleanArray = onHeapAllocator.newBooleanArray("onHeapBooleanArray", size);

        final IBitSet offHeapBitSet = offHeapAllocator.newBitSet("offHeapBitSet", size);
        final IBooleanArray offHeapBooleanArray = offHeapAllocator.newBooleanArray("offHeapBooleanArray", size);

        for (int i = 0; i < size; i++) {
            Assertions.checkFalse(onHeapBitSet.contains(i), "%s", i);
            Assertions.checkFalse(onHeapBooleanArray.get(i), "%s", i);
            Assertions.checkFalse(offHeapBitSet.contains(i), "%s", i);
            Assertions.checkFalse(offHeapBooleanArray.get(i), "%s", i);

            final boolean condition = i % 2 == 0;
            if (condition) {
                onHeapBitSet.add(i);
                onHeapBooleanArray.set(i, true);
                offHeapBitSet.add(i);
                offHeapBooleanArray.set(i, true);

                Assertions.checkTrue(onHeapBitSet.contains(i), "%s", i);
                Assertions.checkTrue(onHeapBooleanArray.get(i), "%s", i);
                Assertions.checkTrue(offHeapBitSet.contains(i), "%s", i);
                Assertions.checkTrue(offHeapBooleanArray.get(i), "%s", i);
            } else {
                onHeapBitSet.remove(i);
                onHeapBooleanArray.set(i, false);
                offHeapBitSet.remove(i);
                offHeapBooleanArray.set(i, false);

                Assertions.checkFalse(onHeapBitSet.contains(i), "%s", i);
                Assertions.checkFalse(onHeapBooleanArray.get(i), "%s", i);
                Assertions.checkFalse(offHeapBitSet.contains(i), "%s", i);
                Assertions.checkFalse(offHeapBooleanArray.get(i), "%s", i);
            }
        }

        for (int i = 0; i < size; i++) {
            final boolean condition = i % 2 == 0;
            Assertions.checkEquals(condition, onHeapBitSet.contains(i), "%s", i);
            Assertions.checkEquals(condition, onHeapBooleanArray.get(i), "%s", i);
            Assertions.checkEquals(condition, offHeapBitSet.contains(i), "%s", i);
            Assertions.checkEquals(condition, offHeapBooleanArray.get(i), "%s", i);
        }
    }

    @Test
    public void testDoubleNaN() {
        final OnHeapPrimitiveArrayAllocator onHeapAllocator = new OnHeapPrimitiveArrayAllocator();
        final TemporaryDiskPrimitiveArrayAllocator offHeapAllocator = new TemporaryDiskPrimitiveArrayAllocator(
                TemporaryDiskPrimitiveArrayAllocatorTest.class.getSimpleName());
        final int size = 1000;

        final IDoubleArray onHeapBooleanArray = onHeapAllocator.newDoubleArray("onHeapBooleanArray", size);
        final IDoubleArray offHeapBooleanArray = offHeapAllocator.newDoubleArray("offHeapBooleanArray", size);

        for (int i = 0; i < size; i++) {
            Assertions.checkEquals(0D, onHeapBooleanArray.get(i), "%s", i);
            Assertions.checkEquals(0D, offHeapBooleanArray.get(i), "%s", i);

            final boolean condition = i % 2 == 0;
            if (condition) {
                onHeapBooleanArray.set(i, Double.NaN);
                offHeapBooleanArray.set(i, Double.NaN);

                Assertions.checkEquals(Double.NaN, onHeapBooleanArray.get(i), "%s", i);
                Assertions.checkEquals(Double.NaN, offHeapBooleanArray.get(i), "%s", i);
            } else {
                onHeapBooleanArray.set(i, i);
                offHeapBooleanArray.set(i, i);

                Assertions.checkEquals((double) i, onHeapBooleanArray.get(i), "%s", i);
                Assertions.checkEquals((double) i, offHeapBooleanArray.get(i), "%s", i);
            }
        }

        for (int i = 0; i < size; i++) {
            final boolean condition = i % 2 == 0;
            if (condition) {
                Assertions.checkEquals(Double.NaN, onHeapBooleanArray.get(i), "%s", i);
                Assertions.checkEquals(Double.NaN, offHeapBooleanArray.get(i), "%s", i);
            } else {
                Assertions.checkEquals((double) i, onHeapBooleanArray.get(i), "%s", i);
                Assertions.checkEquals((double) i, offHeapBooleanArray.get(i), "%s", i);
            }
        }
    }

}
