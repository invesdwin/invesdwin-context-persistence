package de.invesdwin.context.persistence.timeseriesdb.array.primitive;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.system.array.primitive.IPrimitiveArrayAllocator;
import de.invesdwin.context.system.array.primitive.OnHeapPrimitiveArrayAllocator;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;

/**
 * Pins the equals/hashCode contract of the primitive-array allocator family (mirror of the large-side
 * AllocatorEqualsContractTest): Disk allocators are equal iff (name, directory) match — exactly the
 * fields their hashCode folds; the crossed name-vs-directory comparison that the previous equals
 * performed must never match; Threshold allocators hash over the exact equals field set (heap, disk).
 */
@NotThreadSafe
public class PrimitiveAllocatorEqualsContractTest extends ATest {

    private static final File DIR_A = new File(ContextProperties.TEMP_DIRECTORY, "ec1_prim_dirA");
    private static final File DIR_B = new File(ContextProperties.TEMP_DIRECTORY, "ec1_prim_dirB");

    private static final class FixedDiskThresholdAllocator extends ThresholdDiskPrimitiveArrayAllocator {

        private FixedDiskThresholdAllocator(final String name) {
            super(name);
        }

        @Override
        protected IPrimitiveArrayAllocator newHeapPrimitiveArrayAllocator(final String name) {
            return new OnHeapPrimitiveArrayAllocator();
        }

        @Override
        protected IPrimitiveArrayAllocator newDiskPrimitiveArrayAllocator(final String name) {
            return new DiskPrimitiveArrayAllocator(name, DIR_A);
        }

    }

    @Test
    public void testDiskAllocatorNameAndDirectoryPair() {
        final DiskPrimitiveArrayAllocator a = new DiskPrimitiveArrayAllocator("ec1name", DIR_A);
        final DiskPrimitiveArrayAllocator b = new DiskPrimitiveArrayAllocator("ec1name", DIR_A);
        Assertions.assertThat(a.equals(b)).isTrue();
        Assertions.assertThat(b.equals(a)).isTrue();
        Assertions.assertThat(a.hashCode()).isEqualTo(b.hashCode());
        final DiskPrimitiveArrayAllocator c = new DiskPrimitiveArrayAllocator("ec1name", DIR_B);
        Assertions.assertThat(a.equals(c)).isFalse();
        Assertions.assertThat(c.equals(a)).isFalse();
        final DiskPrimitiveArrayAllocator d = new DiskPrimitiveArrayAllocator("ec1other", DIR_A);
        Assertions.assertThat(a.equals(d)).isFalse();
        Assertions.assertThat(d.equals(a)).isFalse();
    }

    @Test
    public void testCrossedNameDirectoryNoFalseMatch() {
        final DiskPrimitiveArrayAllocator a = new DiskPrimitiveArrayAllocator("ec1name", DIR_A);
        final DiskPrimitiveArrayAllocator crossed = new DiskPrimitiveArrayAllocator(DIR_A.getAbsolutePath(), DIR_A);
        Assertions.assertThat(a.equals(crossed)).isFalse();
        Assertions.assertThat(crossed.equals(a)).isFalse();
        Assertions.assertThat(a.hashCode()).isNotEqualTo(crossed.hashCode());
    }

    @Test
    public void testThresholdPairConsistency() {
        final ThresholdDiskPrimitiveArrayAllocator t1 = new FixedDiskThresholdAllocator("ec1fixed");
        final ThresholdDiskPrimitiveArrayAllocator t2 = new FixedDiskThresholdAllocator("ec1fixed");
        Assertions.assertThat(t1.equals(t2)).isTrue();
        Assertions.assertThat(t2.equals(t1)).isTrue();
        Assertions.assertThat(t1.hashCode()).isEqualTo(t2.hashCode());
    }

    @Test
    public void testTemporaryStableHashAndDistinctInstances() {
        final TemporaryDiskPrimitiveArrayAllocator temp = new TemporaryDiskPrimitiveArrayAllocator("ec1temp");
        Assertions.assertThat(temp.hashCode()).isEqualTo(temp.hashCode());
        Assertions.assertThat(temp.equals(temp)).isTrue();
        final TemporaryDiskPrimitiveArrayAllocator other = new TemporaryDiskPrimitiveArrayAllocator("ec1temp");
        Assertions.assertThat(temp.equals(other)).isFalse();
        Assertions.assertThat(other.equals(temp)).isFalse();
    }

}
