package de.invesdwin.context.persistence.timeseriesdb.array.primitive;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.system.array.primitive.OnHeapPrimitiveArrayAllocator;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.array.primitive.IBooleanPrimitiveArray;
import de.invesdwin.util.collections.array.primitive.IDoublePrimitiveArray;
import de.invesdwin.util.collections.array.primitive.bitset.IPrimitiveBitSet;

@NotThreadSafe
public class TemporaryDiskPrimitiveArrayAllocatorTest extends ATest {

    @Test
    public void testBitSet() {
        final OnHeapPrimitiveArrayAllocator onHeapAllocator = new OnHeapPrimitiveArrayAllocator();
        final TemporaryDiskPrimitiveArrayAllocator offHeapAllocator = new TemporaryDiskPrimitiveArrayAllocator(
                TemporaryDiskPrimitiveArrayAllocatorTest.class.getSimpleName());
        final int size = 1000;

        final IPrimitiveBitSet onHeapBitSet = onHeapAllocator.newBitSet("onHeapBitSet", size);
        final IBooleanPrimitiveArray onHeapBooleanArray = onHeapAllocator.newBooleanArray("onHeapBooleanArray", size);

        final IPrimitiveBitSet offHeapBitSet = offHeapAllocator.newBitSet("offHeapBitSet", size);
        final IBooleanPrimitiveArray offHeapBooleanArray = offHeapAllocator.newBooleanArray("offHeapBooleanArray",
                size);

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

        final IDoublePrimitiveArray onHeapBooleanArray = onHeapAllocator.newDoubleArray("onHeapBooleanArray", size);
        final IDoublePrimitiveArray offHeapBooleanArray = offHeapAllocator.newDoubleArray("offHeapBooleanArray", size);

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
