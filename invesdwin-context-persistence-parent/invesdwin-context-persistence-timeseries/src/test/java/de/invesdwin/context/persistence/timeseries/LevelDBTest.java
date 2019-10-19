package de.invesdwin.context.persistence.timeseries;

import java.util.NoSuchElementException;

import javax.annotation.concurrent.ThreadSafe;

import org.assertj.core.api.Fail;
import org.junit.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.timeseries.ezdb.ADelegateRangeTable;
import de.invesdwin.context.persistence.timeseries.ezdb.ADelegateRangeTable.DelegateTableIterator;
import de.invesdwin.context.persistence.timeseries.serde.FDateSerde;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.fdate.FDateBuilder;
import ezdb.Db;
import ezdb.Table;
import ezdb.leveldb.EzLevelDb;
import ezdb.leveldb.EzLevelDbJavaFactory;
import ezdb.serde.Serde;
import ezdb.serde.StringSerde;

@ThreadSafe
public class LevelDBTest extends ATest {

    private static final String HASHKEY = "one";

    @Test
    public void testLevelDB() {
        final Db ezdb = new EzLevelDb(ContextProperties.getCacheDirectory(), new EzLevelDbJavaFactory());
        final Table<String, String> table = ezdb.getTable(getClass().getSimpleName(), StringSerde.get, StringSerde.get);
        table.put(HASHKEY, "value");
        Assertions.assertThat(table.get(HASHKEY)).isEqualTo("value");
    }

    @Test
    public void testInverseOrder() {
        final ADelegateRangeTable<String, FDate, Integer> rangeTable = new ADelegateRangeTable<String, FDate, Integer>(
                "testInverseOrder") {
            @Override
            protected Serde<FDate> newRangeKeySerde() {
                return FDateSerde.GET;
            }

            @Override
            protected boolean allowHasNext() {
                return true;
            }

        };
        final FDate now = FDateBuilder.newDate(2000);
        final FDate oneDate = now.addDays(1);
        final FDate twoDate = now.addDays(2);
        final FDate threeDate = now.addDays(3);
        rangeTable.put(HASHKEY, oneDate, 1);
        rangeTable.put(HASHKEY, twoDate, 2);
        rangeTable.put(HASHKEY, threeDate, 3);
        final DelegateTableIterator<String, FDate, Integer> range3 = rangeTable.range(HASHKEY, now);
        Assertions.assertThat(range3.next().getValue()).isEqualTo(1);
        Assertions.assertThat(range3.next().getValue()).isEqualTo(2);
        Assertions.assertThat(range3.next().getValue()).isEqualTo(3);
        Assertions.assertThat(range3.hasNext()).isFalse();
        try {
            range3.next();
            Fail.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assertions.assertThat(e).isNotNull();
        }
        range3.close(); //should already be closed but should not cause an error when calling again

        final DelegateTableIterator<String, FDate, Integer> rangeNone = rangeTable.range(HASHKEY);
        Assertions.assertThat(rangeNone.next().getValue()).isEqualTo(1);
        Assertions.assertThat(rangeNone.next().getValue()).isEqualTo(2);
        Assertions.assertThat(rangeNone.next().getValue()).isEqualTo(3);
        Assertions.assertThat(rangeNone.hasNext()).isFalse();
        try {
            rangeNone.next();
            Fail.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assertions.assertThat(e).isNotNull();
        }

        final DelegateTableIterator<String, FDate, Integer> rangeMin = rangeTable.range(HASHKEY, FDate.MIN_DATE);
        Assertions.assertThat(rangeMin.next().getValue()).isEqualTo(1);
        Assertions.assertThat(rangeMin.next().getValue()).isEqualTo(2);
        Assertions.assertThat(rangeMin.next().getValue()).isEqualTo(3);
        Assertions.assertThat(rangeMin.hasNext()).isFalse();
        try {
            rangeMin.next();
            Fail.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assertions.assertThat(e).isNotNull();
        }

        final DelegateTableIterator<String, FDate, Integer> rangeMax = rangeTable.range(HASHKEY, FDate.MAX_DATE);
        Assertions.assertThat(rangeMax.hasNext()).isFalse();
        try {
            rangeMax.next();
            Fail.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assertions.assertThat(e).isNotNull();
        }

        final DelegateTableIterator<String, FDate, Integer> range2 = rangeTable.range(HASHKEY, twoDate);
        Assertions.assertThat(range2.next().getValue()).isEqualTo(2);
        Assertions.assertThat(range2.next().getValue()).isEqualTo(3);
        Assertions.assertThat(range2.hasNext()).isFalse();
        try {
            range2.next();
            Fail.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assertions.assertThat(e).isNotNull();
        }

        testReverse(rangeTable, oneDate, twoDate, threeDate);

        testGetLatestForRange(rangeTable, oneDate, twoDate, threeDate);

        rangeTable.deleteTable();
    }

    private void testGetLatestForRange(final ADelegateRangeTable<String, FDate, Integer> rangeTable,
            final FDate oneDate, final FDate twoDate, final FDate threeDate) {
        Assertions.assertThat(rangeTable.getLatest(HASHKEY, oneDate).getValue()).isEqualTo(1);
        Assertions.assertThat(rangeTable.getLatest(HASHKEY, twoDate).getValue()).isEqualTo(2);
        Assertions.assertThat(rangeTable.getLatest(HASHKEY, threeDate).getValue()).isEqualTo(3);

        Assertions.assertThat(rangeTable.getLatest(HASHKEY, oneDate.addMilliseconds(-1)).getValue()).isEqualTo(1);
        Assertions.assertThat(rangeTable.getLatest(HASHKEY, twoDate.addMilliseconds(-1)).getValue()).isEqualTo(1);
        Assertions.assertThat(rangeTable.getLatest(HASHKEY, threeDate.addMilliseconds(-1)).getValue()).isEqualTo(2);
        Assertions.assertThat(rangeTable.getLatest(HASHKEY, threeDate.addMilliseconds(1)).getValue()).isEqualTo(3);
        Assertions.assertThat(rangeTable.getLatest(HASHKEY, threeDate.addDays(1)).getValue()).isEqualTo(3);

        Assertions.assertThat(rangeTable.getLatest(HASHKEY, FDate.MIN_DATE).getValue()).isEqualTo(1);
        Assertions.assertThat(rangeTable.getLatest(HASHKEY, FDate.MAX_DATE).getValue()).isEqualTo(3);
    }

    private void testReverse(final ADelegateRangeTable<String, FDate, Integer> rangeTable, final FDate oneFDate,
            final FDate twoFDate, final FDate threeFDate) {
        final DelegateTableIterator<String, FDate, Integer> range3Reverse = rangeTable.rangeReverse(HASHKEY,
                threeFDate);
        Assertions.assertThat(range3Reverse.next().getValue()).isEqualTo(3);
        Assertions.assertThat(range3Reverse.next().getValue()).isEqualTo(2);
        Assertions.assertThat(range3Reverse.next().getValue()).isEqualTo(1);
        Assertions.assertThat(range3Reverse.hasNext()).isFalse();
        try {
            range3Reverse.next();
            Fail.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assertions.assertThat(e).isNotNull();
        }

        final DelegateTableIterator<String, FDate, Integer> rangeNoneReverse = rangeTable.rangeReverse(HASHKEY);
        Assertions.assertThat(rangeNoneReverse.next().getValue()).isEqualTo(3);
        Assertions.assertThat(rangeNoneReverse.next().getValue()).isEqualTo(2);
        Assertions.assertThat(rangeNoneReverse.next().getValue()).isEqualTo(1);
        Assertions.assertThat(rangeNoneReverse.hasNext()).isFalse();
        try {
            rangeNoneReverse.next();
            Fail.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assertions.assertThat(e).isNotNull();
        }

        final DelegateTableIterator<String, FDate, Integer> range2Reverse = rangeTable.rangeReverse(HASHKEY, twoFDate);
        Assertions.assertThat(range2Reverse.next().getValue()).isEqualTo(2);
        Assertions.assertThat(range2Reverse.next().getValue()).isEqualTo(1);
        Assertions.assertThat(range2Reverse.hasNext()).isFalse();
        try {
            range2Reverse.next();
            Fail.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assertions.assertThat(e).isNotNull();
        }

        final DelegateTableIterator<String, FDate, Integer> range32Reverse = rangeTable.rangeReverse(HASHKEY,
                threeFDate, twoFDate);
        Assertions.assertThat(range32Reverse.next().getValue()).isEqualTo(3);
        Assertions.assertThat(range32Reverse.next().getValue()).isEqualTo(2);
        Assertions.assertThat(range32Reverse.hasNext()).isFalse();
        try {
            range32Reverse.next();
            Fail.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assertions.assertThat(e).isNotNull();
        }

        final DelegateTableIterator<String, FDate, Integer> range21Reverse = rangeTable.rangeReverse(HASHKEY, twoFDate,
                oneFDate);
        Assertions.assertThat(range21Reverse.next().getValue()).isEqualTo(2);
        Assertions.assertThat(range21Reverse.next().getValue()).isEqualTo(1);
        Assertions.assertThat(range21Reverse.hasNext()).isFalse();
        try {
            range21Reverse.next();
            Fail.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assertions.assertThat(e).isNotNull();
        }
    }

}
