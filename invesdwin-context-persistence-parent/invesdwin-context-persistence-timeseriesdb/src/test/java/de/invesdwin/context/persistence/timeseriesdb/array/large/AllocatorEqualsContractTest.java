package de.invesdwin.context.persistence.timeseriesdb.array.large;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.system.array.large.ILargeArrayAllocator;
import de.invesdwin.context.system.array.large.OnHeapLargeArrayAllocator;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;

/**
 * Pins the equals/hashCode contract of the large-array allocator family: Disk allocators are equal iff
 * (name, directory) match — exactly the fields their hashCode folds; the crossed name-vs-directory
 * comparison that the previous equals performed must never match; Threshold allocators hash over the
 * exact equals field set (heap, disk) so equal instances always agree on hash.
 */
@NotThreadSafe
public class AllocatorEqualsContractTest extends ATest {

    private static final File DIR_A = new File(ContextProperties.TEMP_DIRECTORY, "ec1_large_dirA");
    private static final File DIR_B = new File(ContextProperties.TEMP_DIRECTORY, "ec1_large_dirB");

    private static final class FixedDiskThresholdAllocator extends ThresholdDiskLargeArrayAllocator {

        private FixedDiskThresholdAllocator(final String name) {
            super(name);
        }

        @Override
        protected ILargeArrayAllocator newHeapLargeArrayAllocator(final String name) {
            return new OnHeapLargeArrayAllocator();
        }

        @Override
        protected ILargeArrayAllocator newDiskLargeArrayAllocator(final String name) {
            return new DiskLargeArrayAllocator(name, DIR_A);
        }

    }

    @Test
    public void testDiskAllocatorNameAndDirectoryPair() {
        final DiskLargeArrayAllocator a = new DiskLargeArrayAllocator("ec1name", DIR_A);
        final DiskLargeArrayAllocator b = new DiskLargeArrayAllocator("ec1name", DIR_A);
        Assertions.assertThat(a.equals(b)).isTrue();
        Assertions.assertThat(b.equals(a)).isTrue();
        Assertions.assertThat(a.hashCode()).isEqualTo(b.hashCode());
        final DiskLargeArrayAllocator c = new DiskLargeArrayAllocator("ec1name", DIR_B);
        Assertions.assertThat(a.equals(c)).isFalse();
        Assertions.assertThat(c.equals(a)).isFalse();
        final DiskLargeArrayAllocator d = new DiskLargeArrayAllocator("ec1other", DIR_A);
        Assertions.assertThat(a.equals(d)).isFalse();
        Assertions.assertThat(d.equals(a)).isFalse();
    }

    @Test
    public void testCrossedNameDirectoryNoFalseMatch() {
        // the old equals compared this.getName() against the other's getDirectory(); a name that
        // happens to equal the other allocator's directory path used to match in one direction
        final DiskLargeArrayAllocator a = new DiskLargeArrayAllocator("ec1name", DIR_A);
        final DiskLargeArrayAllocator crossed = new DiskLargeArrayAllocator(DIR_A.getAbsolutePath(), DIR_A);
        Assertions.assertThat(a.equals(crossed)).isFalse();
        Assertions.assertThat(crossed.equals(a)).isFalse();
        Assertions.assertThat(a.hashCode()).isNotEqualTo(crossed.hashCode());
    }

    @Test
    public void testThresholdPairConsistency() {
        final ThresholdDiskLargeArrayAllocator t1 = new FixedDiskThresholdAllocator("ec1fixed");
        final ThresholdDiskLargeArrayAllocator t2 = new FixedDiskThresholdAllocator("ec1fixed");
        // equals says (heap, disk) are equal — so the hash over exactly those fields must agree
        Assertions.assertThat(t1.equals(t2)).isTrue();
        Assertions.assertThat(t2.equals(t1)).isTrue();
        Assertions.assertThat(t1.hashCode()).isEqualTo(t2.hashCode());
    }

    @Test
    public void testTemporaryStableHashAndDistinctInstances() {
        final TemporaryDiskLargeArrayAllocator temp = new TemporaryDiskLargeArrayAllocator("ec1temp");
        Assertions.assertThat(temp.hashCode()).isEqualTo(temp.hashCode());
        Assertions.assertThat(temp.equals(temp)).isTrue();
        final TemporaryDiskLargeArrayAllocator other = new TemporaryDiskLargeArrayAllocator("ec1temp");
        // unique-name generator makes every instance's delegate distinct
        Assertions.assertThat(temp.equals(other)).isFalse();
        Assertions.assertThat(other.equals(temp)).isFalse();
    }

}
