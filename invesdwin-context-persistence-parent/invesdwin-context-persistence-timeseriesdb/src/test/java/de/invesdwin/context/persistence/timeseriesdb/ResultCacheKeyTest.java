package de.invesdwin.context.persistence.timeseriesdb;

import java.time.LocalDate;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.persistence.timeseriesdb.storage.memory.MemoryFileSummary;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.date.FDate;

/**
 * The resultCache of TimeSeriesLookupStorageCache locates entries by hashCode and then asks equals about identity,
 * so a ResultCacheKey must not report equality by hashCode alone: a 32-bit hash collision between two different
 * segment summaries would serve one segment's cached result for the other segment's read.
 *
 * The pair used here is a real collision in this codebase's own hash function: Long.hashCode(5L) equals
 * Long.hashCode(0x100000004L) and memoryOffset feeds MemoryFileSummary.hashCode via Objects.hashCode.
 */
// CHECKSTYLE:OFF
@NotThreadSafe
public class ResultCacheKeyTest {
    //CHECKSTYLE:ON

    private MemoryFileSummary newSummary(final long memoryOffset, final int valueCount) {
        //precedingValueCount and precedingMemoryOffset are non-zero, satisfying MemoryFiles.assertMaybeFirstSummary
        return new MemoryFileSummary(new FDate(LocalDate.of(2026, 11, 5)), new byte[] { 1 },
                new byte[] { 2 }, 1L, valueCount, "file:///segment.data", 1L, memoryOffset, 64L);
    }

    @Test
    public void testCollidingSummariesMustNotProduceEqualKeys() {
        final MemoryFileSummary s1 = newSummary(5L, 7);
        final MemoryFileSummary s2 = newSummary(0x100000004L, 9);
        //precondition: a genuine 32-bit hashCode collision between two summaries that are NOT equal
        Assertions.assertThat(s1.hashCode() == s2.hashCode()).isTrue();
        Assertions.assertThat(s1.equals(s2)).isFalse();
        final TimeSeriesLookupStorageCache.ResultCacheKey k1 =
                new TimeSeriesLookupStorageCache.ResultCacheKey(s1, null);
        final TimeSeriesLookupStorageCache.ResultCacheKey k2 =
                new TimeSeriesLookupStorageCache.ResultCacheKey(s2, null);
        Assertions.assertThat(k1.hashCode() == k2.hashCode()).isTrue();
        Assertions.assertThat(k1.equals(k2)).isFalse();
        Assertions.assertThat(k2.equals(k1)).isFalse();
    }

    @Test
    public void testValueEqualSummariesMustProduceEqualKeys() {
        //firstValue/lastValue byte payloads are not part of MemoryFileSummary identity, so these two are equal
        final MemoryFileSummary s1 = newSummary(5L, 7);
        final MemoryFileSummary s2 = newSummary(5L, 99);
        Assertions.assertThat(s1.equals(s2)).isTrue();
        final TimeSeriesLookupStorageCache.ResultCacheKey k1 =
                new TimeSeriesLookupStorageCache.ResultCacheKey(s1, null);
        final TimeSeriesLookupStorageCache.ResultCacheKey k2 =
                new TimeSeriesLookupStorageCache.ResultCacheKey(s2, null);
        Assertions.assertThat(k1.equals(k2)).isTrue();
        Assertions.assertThat(k1.hashCode() == k2.hashCode()).isTrue();
    }
}
